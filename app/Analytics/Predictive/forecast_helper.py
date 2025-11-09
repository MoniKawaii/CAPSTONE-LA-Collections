# app/Analytics/Predictive/forecast_helper.py
import pandas as pd
import numpy as np
from .time_gen import generate_future_dim_time
from .feature_engineer import build_time_series_features
import logging # Ensure logging is imported

def forecast_next_n_days(model, df_hist, target_col, features, horizon=90, lag_periods=[1,2,3], rolling_window=7, dynamic_features=None):
    """
    Performs recursive, step-by-step forecasting using a trained ML model.
    Dynamically inserts future external features (like price/discount) based on event flags.
    
    Includes CRITICAL FIX for recursive explosion (clipping) and FIX for exogenous feature flatness.
    """
    
    last_date = df_hist['date'].max()
    df_future_time = generate_future_dim_time(str(last_date), horizon)
    
    # 🌟 CRITICAL STABILIZATION STEP 1: Determine Max safe log-revenue
    MAX_HIST_LOG_REVENUE = np.nan_to_num(df_hist[target_col].max(), nan=70.0) 
    
    # 1. Combine historical + future for initial feature building
    df_full = pd.concat([df_hist, df_future_time], ignore_index=True)
    df_full['date'] = pd.to_datetime(df_full['date']).dt.normalize()

    # Apply initial dynamic feature fill to future rows before building time-series features
    # NOTE: This ensures the lag features are calculated correctly from the start.
    if dynamic_features and 'is_mega_sale_day' in df_full.columns:
        future_mask = df_full[target_col].isnull()
        if future_mask.any():
            is_event = (df_full.loc[future_mask, 'is_mega_sale_day'] == 1) | (df_full.loc[future_mask, 'is_payday'] == 1)
            
            df_full.loc[future_mask, 'avg_paid_price'] = np.where(is_event, dynamic_features['event_price'], dynamic_features['non_event_price'])
            df_full.loc[future_mask, 'avg_discount_rate'] = np.where(is_event, dynamic_features['event_discount'], dynamic_features['non_event_discount'])
            df_full.loc[future_mask, 'avg_original_price'] = df_full.loc[future_mask, 'avg_paid_price'] / \
                (1 - df_full.loc[future_mask, 'avg_discount_rate'].replace(1.0, 0.9999))

    # 2. Build time-series features (Lags, Rolling Means) on the full dataset
    df_full_features = build_time_series_features(df_full, target_col, lag_periods=lag_periods, rolling_window=rolling_window)
    
    # Filter for the rows we need to forecast
    df_forecast_input = df_full_features[df_full_features['date'] > last_date].copy()
    forecast_log = []
    
    # Prepare the initial features (last historical observation)
    last_row_features = df_full_features[df_full_features['date'] == last_date][features].copy()

    # 3. Setup for Recursion
    max_target_lag = max(lag_periods + [rolling_window]) if lag_periods else 1
    
    # a. Target History Queue: The last N historical values of the log-transformed target
    # This queue manages the values that feed the next day's 'lag' and 'rolling mean' features.
    target_history_queue = df_full_features[target_col].dropna().tail(max_target_lag).tolist()

    # b. 🌟 FIX: Exogenous History Queue for correct lag propagation 🌟
    # Since feature_engineer.py uses [1, 7] for lags, we need to store 7 days of history.
    exog_lags = [1, 7] 
    max_exog_lag = max(exog_lags) if exog_lags else 1
    exog_cols = ["avg_paid_price", "avg_discount_rate"]
    exog_histories = {}
    
    for col in exog_cols:
        # Check if the lag features are actually in the model's feature set
        if f"{col}_lag_1" in features: 
             exog_histories[col] = df_full_features[col].dropna().tail(max_exog_lag).tolist()
    
    # c. Map future event flags for easy access inside the loop
    future_time_map = df_forecast_input[['date', 'is_mega_sale_day', 'is_payday']].set_index('date')

    # 4. Recursive Forecasting Loop
    for future_date in df_forecast_input['date']:
        
        # --- STEP 1: Determine Dynamic Price/Discount for the CURRENT day (t+1) ---
        current_price = dynamic_features['non_event_price']
        current_discount = dynamic_features['non_event_discount']

        if dynamic_features and 'is_mega_sale_day' in last_row_features.columns:
            
            current_date_flags = future_time_map.loc[future_date]
            is_event = (current_date_flags['is_mega_sale_day'] == 1) | (current_date_flags['is_payday'] == 1)
            
            current_price = dynamic_features['event_price'] if is_event else dynamic_features['non_event_price']
            current_discount = dynamic_features['event_discount'] if is_event else dynamic_features['non_event_discount']
            
            # Update the CURRENT exogenous features in the feature set F(t+1) 
            # (These are the non-lagged exogenous features for the current day's prediction)
            last_row_features.loc[:, 'avg_paid_price'] = current_price
            last_row_features.loc[:, 'avg_discount_rate'] = current_discount
            
            # Update Interaction features
            if 'discount_on_event' in features:
                last_row_features.loc[:, 'discount_on_event'] = current_date_flags['is_mega_sale_day'] * current_discount
            if 'discount_on_payday' in features:
                last_row_features.loc[:, 'discount_on_payday'] = current_date_flags['is_payday'] * current_discount
        # -------------------------------------------------------------------------
        
        # --- STEP 2: Predict (F(t+1)) ---
        next_pred = model.predict(last_row_features)[0]
        
        # 🌟 CRITICAL STABILIZATION STEP 2: Clip the prediction for stable feedback 🌟
        next_pred_safe = np.clip(next_pred, a_min=None, a_max=MAX_HIST_LOG_REVENUE)
        
        # --- STEP 3: Log the result ---
        forecast_log.append({'date': future_date, 'forecasted_gross_revenue': next_pred_safe})
        
        # --- STEP 4: Update Features for the NEXT step (F(t+2)) ---
        new_row = last_row_features.copy()
        
        # 1. Update Target History Queue and Lag Features
        if target_history_queue:
            target_history_queue.pop(0) 
            target_history_queue.append(next_pred_safe)
        
        for lag in lag_periods:
            lag_col = f"{target_col}_lag_{lag}"
            if lag_col in features:
                new_row.loc[:, lag_col] = target_history_queue[-lag]

        # 2. Update Target Rolling Mean Feature
        roll_col = f"{target_col}_rolling_mean_{rolling_window}"
        if roll_col in features and target_history_queue:
            recent_history = target_history_queue[-rolling_window:] 
            new_row.loc[:, roll_col] = sum(recent_history) / len(recent_history)
        
        # 3. 🌟 FIX: Update Exogenous Lag Features 🌟
        for col in exog_cols:
            if col in exog_histories:
                # Update the Exogenous History Queue with the CURRENT day's actual price/discount (t+1)
                exog_histories[col].pop(0)
                value_to_queue = current_price if col == 'avg_paid_price' else current_discount
                exog_histories[col].append(value_to_queue)

                # Update the lagged feature columns in new_row (F(t+2))
                for lag in exog_lags:
                    lag_col = f"{col}_lag_{lag}"
                    if lag_col in features:
                        # The lag 'n' value for t+2 is the value 'n' steps back in the history queue.
                        new_row.loc[:, lag_col] = exog_histories[col][-lag]


        # Set up the features for the NEXT prediction (F(t+2))
        last_row_features = new_row[features].copy()
        
    return pd.DataFrame(forecast_log)