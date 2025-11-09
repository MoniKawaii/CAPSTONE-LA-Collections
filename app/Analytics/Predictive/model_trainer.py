import os
import sys
import logging
import warnings
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from sklearn.metrics import mean_absolute_error, mean_squared_error

# Regressors
from xgboost import XGBRegressor
from lightgbm import LGBMRegressor

# Time series / forecasting
from statsmodels.tsa.statespace.sarimax import SARIMAX
from prophet import Prophet

# Optional helper to auto-select SARIMA orders (install pmdarima)
try:
    from pmdarima import auto_arima
    _HAS_PMDARIMA = True
except Exception:
    _HAS_PMDARIMA = False

# Dynamic path (if you rely on project-root imports)
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..', '..', '..'))
if project_root not in sys.path:
    sys.path.append(project_root)

from app.Analytics.Predictive.feature_engineer import build_time_series_features
from app.Analytics.Predictive.plot_helper import save_plot, save_test_data_to_csv
from app.Analytics.Predictive.forecast_helper import forecast_next_n_days

# Logging config
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# Optionally silence specific LightGBM user warnings (harmless)
warnings.filterwarnings("ignore", category=UserWarning, module="lightgbm")


# ---- metrics ----
def calculate_mae(y_true, y_pred):
    return mean_absolute_error(y_true, y_pred)


def calculate_mse(y_true, y_pred):
    return mean_squared_error(y_true, y_pred)


def calculate_rmse(y_true, y_pred):
    return np.sqrt(mean_squared_error(y_true, y_pred))


# ---- Seasonal Naive ----
def seasonal_naive_forecast(train_series, test_series, season_length=7, horizon=90):
    """
    Seasonal naive: repeat the last observed season for the test set and horizon.
    """
    if len(train_series) < season_length:
        # fallback: repeat last value
        last_val = train_series.iloc[-1] if len(train_series) > 0 else 0.0
        test_pred = pd.Series(np.repeat(last_val, len(test_series)), index=test_series.index)
        forecast_90_days = np.repeat(last_val, horizon)
        return test_pred, forecast_90_days

    last_season = train_series.iloc[-season_length:].values
    n_test = len(test_series)
    test_pred = np.tile(last_season, (n_test // season_length) + 1)[:n_test]
    test_pred = pd.Series(test_pred, index=test_series.index)

    forecast_90_days = np.tile(last_season, (horizon // season_length) + 1)[:horizon]
    return test_pred, forecast_90_days

# ---- main train & forecast function ----
def train_and_forecast_model(
    df: pd.DataFrame,
    platform: str,
    model_name: str,
    target_col: str = 'gross_revenue',
    exog_cols: list = ['is_mega_sale_day', 'is_payday', 'avg_paid_price', 'avg_original_price', 'avg_discount_rate', 'is_event_day', 'discount_on_event'],    test_size_days: int = 365,          # changed per user request
    forecast_horizon: int = 90
) -> dict:
    """
    Train, evaluate, and forecast using multiple model types.
    Returns a dict with metrics for summary.
    """

    logging.info(f"--- Training {model_name} for {platform} ---")

    mae = mse = rmse = np.inf
    y_pred_test = pd.Series(dtype=float)
    forecast_90_days = np.repeat(0.0, forecast_horizon)
    y_test_linear_actual = pd.Series(dtype=float)

    # ---------- PREP ----------
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)

    # Safe numeric conversion
    for col in ['avg_paid_price', 'avg_original_price', 'avg_discount_rate']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # --- Determine first/last sale and include missing dates ---
    first_sale_date = df.loc[df[target_col] > 0, 'date'].min()
    last_sale_date = df.loc[df[target_col] > 0, 'date'].max()

    if pd.isna(first_sale_date) or pd.isna(last_sale_date):
        logging.warning(f"[{platform}] No valid sales records found. Skipping model.")
        return {'platform': platform, 'model': model_name, 'mae': np.inf, 'mse': np.inf, 'rmse': np.inf}

    # Aggregate daily
    agg_funcs = {
        'gross_revenue': 'sum',
        'total_items_sold': 'sum',
        'is_mega_sale_day': 'max',
        'is_payday': 'max',
        'avg_paid_price': 'mean',
        'avg_original_price': 'mean',
        'avg_discount_rate': 'mean',
        'platform_key': 'first',
        'platform_name': 'first',
    }

    cols_for_agg = [c for c in agg_funcs.keys() if c in df.columns]
    df_agg = df[['date'] + cols_for_agg].groupby('date').agg(agg_funcs).reset_index()
    df_agg = df_agg.sort_values('date').reset_index(drop=True)

    # Create full date range for this platform
    full_date_range = pd.date_range(start=first_sale_date, end=last_sale_date, freq='D')
    df_agg = df_agg.set_index('date').reindex(full_date_range).rename_axis('date').reset_index()
    
    if 'gross_revenue' in df_agg.columns:
        df_agg['gross_revenue'] = df_agg['gross_revenue'].fillna(0.0)

    # Event flags vs continuous features
    event_cols = ['is_mega_sale_day', 'is_payday', 'is_event_day', 'discount_on_event']
    for c in event_cols:
        if c in df_agg.columns:
            df_agg[c] = df_agg[c].fillna(0).astype(int)

    cont_cols = [c for c in ['avg_paid_price', 'avg_original_price', 'avg_discount_rate', 'total_items_sold', 'gross_revenue'] if c in df_agg.columns]
    for c in cont_cols:
        df_agg[c] = df_agg[c].ffill().bfill()
        df_agg[c] = df_agg[c].fillna(df_agg[c].median())

    df_agg['log_revenue'] = np.log1p(df_agg['gross_revenue'])

    df_agg['is_event_day_temp'] = ((df_agg['is_mega_sale_day'] == 1) | (df_agg['is_payday'] == 1)).astype(bool)

    # 2. Use the training data's average to avoid leakage from the test set
    split_date = df_agg['date'].max() - pd.Timedelta(days=test_size_days - 1)
    df_train_temp = df_agg[df_agg['date'] < split_date].copy()
    
    # Calculate the average values for key features based on event status
    event_metrics = df_train_temp.groupby('is_event_day_temp').agg(
        avg_paid_price_mean=('avg_paid_price', 'mean'),
        avg_discount_rate_mean=('avg_discount_rate', 'mean')
    )

    # 3. Create a dictionary to pass to the forecast helper
    # Use .get() with a safe fallback (general mean) in case a group is missing
    safe_mean_price = df_train_temp['avg_paid_price'].mean()
    safe_mean_discount = df_train_temp['avg_discount_rate'].mean()
    
    dynamic_feature_values = {
        'non_event_price': event_metrics.loc[False, 'avg_paid_price_mean'] if False in event_metrics.index else safe_mean_price,
        'non_event_discount': event_metrics.loc[False, 'avg_discount_rate_mean'] if False in event_metrics.index else safe_mean_discount,
        'event_price': event_metrics.loc[True, 'avg_paid_price_mean'] if True in event_metrics.index else safe_mean_price,
        'event_discount': event_metrics.loc[True, 'avg_discount_rate_mean'] if True in event_metrics.index else safe_mean_discount,
    }
    # ---------------------------------------------------------------------

    # --- Track global latest date across all platforms ---
    latest_date_file = "app/Analytics/Predictive/_latest_date.txt"
    global_latest = None
    if os.path.exists(latest_date_file):
        with open(latest_date_file, "r") as f:
            try:
                global_latest = pd.to_datetime(f.read().strip())
            except Exception:
                global_latest = None

    this_latest = df_agg['date'].max()
    if global_latest is None or this_latest > global_latest:
        with open(latest_date_file, "w") as f:
            f.write(str(this_latest.date()))
        global_latest = this_latest

    # Align all platforms to the same end date
    df_agg = df_agg[df_agg['date'] <= global_latest]

    # Fill exog variables
    for ex in [c for c in exog_cols if c in df_agg.columns]:
        if ex in ['is_mega_sale_day', 'is_payday']:
            df_agg[ex] = df_agg[ex].fillna(0).astype(int)
        else:
            mean_val = df_agg[ex][df_agg[ex] > 0].mean() if df_agg[ex][df_agg[ex] > 0].any() else 0.0
            df_agg[ex] = df_agg[ex].fillna(mean_val).astype(float)

    # Re-calculate interaction features (as they might have been lost in reindex/fill)
    if all(col in df_agg.columns for col in ['is_mega_sale_day', 'is_payday']):
        df_agg['is_event_day'] = ((df_agg['is_mega_sale_day'] == 1) | (df_agg['is_payday'] == 1)).astype(int)
    if all(col in df_agg.columns for col in ['avg_discount_rate', 'is_event_day']):
        df_agg['discount_on_event'] = df_agg['avg_discount_rate'] * df_agg['is_event_day']

    if 'is_event_day_temp' in df_agg.columns:
        df_agg = df_agg.drop(columns=['is_event_day_temp'])

    target_col_used = 'log_revenue'

    # Train/test chronological split
    split_date = df_agg['date'].max() - pd.Timedelta(days=test_size_days - 1)
    df_train = df_agg[df_agg['date'] < split_date].copy()
    df_test = df_agg[df_agg['date'] >= split_date].copy()
    
    if df_train.empty or df_test.empty:
        logging.warning(f"[{platform}] Train or Test set is empty. Skipping model.")
        return {'platform': platform, 'model': model_name, 'mae': np.inf, 'mse': np.inf, 'rmse': np.inf}

    y_test_actual = df_test[target_col_used]
    
    logging.info(f"[{platform}] Data points | train: {len(df_train)} | test: {len(df_test)} (target test_days={test_size_days})")

    # ---------- XGBoost & LightGBM ----------
    if model_name in ['XGBoost', 'LightGBM']:
        try:
            # ⚙️ Platform-specific lag/rolling config
            lag_periods = [1, 7, 14, 30, 60] if platform == 'Shopee' else [1, 7, 14]
            rolling_window = 14 if platform == 'Shopee' else 7

            df_full = build_time_series_features(df_agg, target_col_used, lag_periods=lag_periods, rolling_window=rolling_window)
            df_train_ml = df_full[df_full['date'] < split_date]
            df_test_ml = df_full[df_full['date'] >= split_date]

            features = df_train_ml.columns.drop(
                [target_col_used, 'date', 'platform_key', 'platform_name', 'total_items_sold'],
                errors='ignore'
            ).tolist()

            X_all, y_all = df_train_ml[features], df_train_ml[target_col_used]
            val_size = max(1, int(len(X_all) * 0.1))
            X_train, y_train_ml = X_all.iloc[:-val_size], y_all.iloc[:-val_size]
            X_valid, y_valid_ml = X_all.iloc[-val_size:], y_all.iloc[-val_size:]
            X_test, y_test_actual_ml = df_test_ml[features], df_test_ml[target_col_used]

            # Platform-specific LightGBM/XGBoost parameters
            if platform == 'Shopee':
                # LIghtGBM/General parameters
                tree_estimators = 300
                learning_rate = 0.03
                num_leaves = 40
                lgbm_min_data_in_leaf = 50
                lgbm_bagging_fraction = 0.8
                lgbm_feature_fraction = 0.8
                lgbm_reg_lambda = 0.0
                objective_lgbm = 'tweedie'
                
                # XGBoost/General parameters
                xgb_objective = 'reg:tweedie'
                xgb_tweedie_power = 1.2
                xgb_max_depth = 8
                xgb_reg_alpha = 0.5
                xgb_reg_lambda = 0.0

            else:  # Lazada
                # LightGBM/General parameters
                tree_estimators = 300
                learning_rate = 0.01
                num_leaves = 40
                lgbm_min_data_in_leaf = 15
                lgbm_bagging_fraction = 1.0
                lgbm_feature_fraction = 1.0
                lgbm_reg_lambda = 0.5
                objective_lgbm = 'tweedie'

                # XGBoost/General parameters
                xgb_objective = 'reg:tweedie'
                xgb_tweedie_power = 1.2
                xgb_max_depth = 5
                xgb_reg_alpha = 0.1
                xgb_reg_lambda = 0.5

            # --- XGBoost ---
            if model_name == 'XGBoost':
                import xgboost as xgb
                model_kwargs = dict(
                    n_estimators=tree_estimators,
                    learning_rate=learning_rate,
                    random_state=42,
                    tree_method='hist',
                    objective=xgb_objective,
                    tweedie_variance_power=xgb_tweedie_power,
                    max_depth=xgb_max_depth,
                    reg_alpha=xgb_reg_alpha,
                    reg_lambda=xgb_reg_lambda,
                    n_jobs=-1
                )

                version_str = xgb.__version__
                version_nums = tuple(int(v) for v in version_str.split(".")[:2])

                fit_successful = False

                try:
                    if version_nums >= (2, 0):
                        model = xgb.XGBRegressor(**model_kwargs, early_stopping_rounds=25)
                        model.fit(
                            X_train, y_train_ml,
                            eval_set=[(X_train, y_train_ml), (X_valid, y_valid_ml)],
                            verbose=False
                        )
                        fit_successful = True

                    else:
                        model = xgb.XGBRegressor(**model_kwargs)
                        model.fit(
                            X_train, y_train_ml,
                            eval_set=[(X_valid, y_valid_ml)],
                            early_stopping_rounds=25,
                            verbose=False
                        )
                        fit_successful = True

                except Exception as e:
                    logging.warning(f"[{platform} - XGBoost] fallback fit: {e}")
                    model = xgb.XGBRegressor(**model_kwargs)
                    model.fit(X_train, y_train_ml)

            # --- LightGBM ---
            else:
                import lightgbm as lgb
                
                lgbm_params = {
                    'n_estimators': tree_estimators,
                    'learning_rate': learning_rate,
                    'random_state': 42,
                    'objective': objective_lgbm,
                    # 'silent': True,
                    'num_leaves': num_leaves,
                    'min_child_samples': lgbm_min_data_in_leaf,
                    'subsample': lgbm_bagging_fraction,
                    'colsample_bytree': lgbm_feature_fraction,
                    'reg_lambda': lgbm_reg_lambda
                }

                if objective_lgbm == 'tweedie':
                    lgbm_params['tweedie_variance_power'] = xgb_tweedie_power      

                model = LGBMRegressor(**lgbm_params)

                try:
                    callbacks = [lgb.early_stopping(stopping_rounds=25, verbose=False)]
                    
                    model.fit(X_train, y_train_ml, 
                            eval_set=[(X_valid, y_valid_ml)],
                            eval_metric='mae',
                            callbacks=callbacks)
                    
                except Exception as e:
                    logging.warning(f"[{platform} - LightGBM] fallback fit: {e}. Fitting without early stopping.")
                    model.fit(X_train, y_train_ml, eval_metric='mae')

            # Test predictions
            y_pred_test_ml = pd.Series(model.predict(X_test), index=y_test_actual_ml.index)

            # 1. Combine training and test data for the history input to the forecasting helper
            df_history_for_forecast = df_agg[df_agg['date'] < df_test['date'].max()].copy()
            df_history_for_forecast['log_revenue'] = df_history_for_forecast['log_revenue'].fillna(0)
            
            # The last known sale date's log_revenue must be the actual value from the last day of the training set (or full set)
            # Find the last date *before* the forecast starts
            last_hist_date = df_test['date'].max() - pd.Timedelta(days=1)
            
            # Combine all history including the test set's *actual* values for feature building continuity
            df_history_all = df_full[df_full['date'] <= last_hist_date].copy().reset_index(drop=True)
            
            # Need to ensure the log_revenue value for the last date of history is correct
            df_history_all.loc[df_history_all['date'] == last_hist_date, 'log_revenue'] = \
                df_agg[df_agg['date'] == last_hist_date]['log_revenue'].values[0]

            forecast_df_log = forecast_next_n_days(
                model=model, 
                df_hist=df_history_all, 
                target_col=target_col_used, 
                features=features, 
                horizon=forecast_horizon,
                lag_periods=lag_periods,
                rolling_window=rolling_window,
                dynamic_features=dynamic_feature_values
                )

            forecast_90_days = forecast_df_log['forecasted_gross_revenue'].values
            
            y_pred_test = y_pred_test_ml
            y_test_actual = y_test_actual_ml
                
        except Exception as e:
            logging.error(f"[{platform} - {model_name}] failed: {e}")
            # Ensure fallback saving so files update
            y_pred_test = pd.Series(np.repeat(0.0, len(df_test)), index=df_test.index)
            forecast_90_days = np.repeat(0.0, forecast_horizon)
            return {'platform': platform, 'model': model_name, 'mae': np.inf, 'mse': np.inf, 'rmse': np.inf}

    # ---------- SARIMAX ----------
    elif model_name == 'SARIMAX':
        try:
            if len(df_train) < 10:
                raise ValueError("Not enough training observations for SARIMAX.")

            exog_train = df_train[[c for c in exog_cols if c in df_train.columns]].copy()
            exog_test = df_test[[c for c in exog_cols if c in df_test.columns]].copy()

            # auto_arima to pick orders if available, otherwise use fallback
            if _HAS_PMDARIMA:
                auto_mod = auto_arima(df_train[target_col_used], exogenous=exog_train, seasonal=True, m=7,
                                      suppress_warnings=True, error_action='ignore', max_order=10)
                order = auto_mod.order
                seasonal_order = auto_mod.seasonal_order
            else:
                order = (1, 1, 1)
                seasonal_order = (1, 0, 1, 7)
                logging.info("pmdarima not available — using fallback SARIMAX order=(1,1,1) seasonal=(1,0,1,7)")

            logging.info(f"SARIMAX order={order}, seasonal_order={seasonal_order}")

            model = SARIMAX(df_train[target_col_used], order=order, seasonal_order=seasonal_order, exog=exog_train,
                            enforce_stationarity=False, enforce_invertibility=False)
            model_fit = model.fit(disp=False)

            # Build future exogenous data (test + forecast)
            exog_future_dates = pd.date_range(start=df_test['date'].max() + timedelta(days=1), periods=forecast_horizon, freq='D')
            exog_future = pd.DataFrame({'date': exog_future_dates})

            for col in exog_cols:
                if col in df_agg.columns:
                    if col in ['is_mega_sale_day', 'is_payday', 'is_event_day', 'discount_on_event']:
                        # Assume no scheduled events in the forecast
                        exog_future[col] = 0.0
                    else:
                        # Carry forward the mean/last value for continuous variables
                        last_val = df_agg[col].iloc[-1]
                        exog_future[col] = last_val
                else:
                    exog_future[col] = 0.0

            exog_pred_all = pd.concat([exog_test.reset_index(drop=True), exog_future.drop(columns='date').reset_index(drop=True)], ignore_index=True)

            pred_start = len(df_train[target_col_used])
            pred_end = pred_start + len(df_test[target_col_used]) + forecast_horizon - 1

            all_preds = model_fit.predict(start=pred_start, end=pred_end, exog=exog_pred_all)
            y_pred_test = all_preds.iloc[:len(df_test[target_col_used])]
            y_pred_test.index = df_test.index
            forecast_90_days = all_preds.iloc[len(df_test[target_col_used]):].values

        except Exception as e:
            logging.error(f"SARIMAX failed: {e}")
            # fallback save placeholders
            y_pred_test = pd.Series(np.repeat(0.0, len(df_test)), index=df_test.index)
            forecast_90_days = np.repeat(0.0, forecast_horizon)

            return {'platform': platform, 'model': model_name, 'mae': np.inf, 'mse': np.inf, 'rmse': np.inf}

    # ---------- Prophet ----------
    elif model_name == 'Prophet':
        try:
            df_prophet_train = df_train.rename(columns={'date': 'ds', target_col_used: 'y'})[['ds', 'y'] + [c for c in exog_cols if c in df_train.columns]]
            df_prophet_test = df_test.rename(columns={'date': 'ds', target_col_used: 'y'})[['ds', 'y'] + [c for c in exog_cols if c in df_test.columns]]

            if df_prophet_train.empty:
                raise ValueError("Empty prophet training frame.")

            m = Prophet(daily_seasonality=True, weekly_seasonality=True, yearly_seasonality=False, changepoint_prior_scale=0.05)
            for col in [c for c in exog_cols if c in df_prophet_train.columns]:
                m.add_regressor(col)

            m.fit(df_prophet_train)

            future_dates_all = pd.date_range(start=df_prophet_train['ds'].max() + timedelta(days=1), periods=len(df_prophet_test) + forecast_horizon, freq='D')
            future_df = pd.DataFrame({'ds': future_dates_all})

            test_end_date = df_prophet_test['ds'].max()

            for col in [c for c in exog_cols if c in df_prophet_train.columns]:
                # Backfill test period exogenous (Prophet prediction will only use 'ds')
                test_exog = df_prophet_test[['ds', col]]
                future_df = future_df.merge(test_exog, on='ds', how='left')

                # Fill forecast period exog (i.e., dates > test_end_date)
                if col in ['is_mega_sale_day', 'is_payday', 'is_event_day', 'discount_on_event']:
                    # Assume no future events
                    future_df.loc[future_df['ds'] > test_end_date, col] = 0.0
                else:
                    # Carry forward the last known value for continuous variables
                    last_train_val = df_agg[col].iloc[-1]
                    future_df.loc[future_df['ds'] > test_end_date, col] = last_train_val
                
                # Final cleanup for merging/missing values in the test set part
                future_df[col] = future_df[col].fillna(0.0)

            forecast_df = m.predict(future_df)
            
            # Extract test predictions
            y_pred_test = forecast_df.loc[forecast_df['ds'].isin(df_prophet_test['ds']), 'yhat'].reset_index(drop=True)
            y_pred_test.index = df_test.index

            # Extract 90-day forecast
            forecast_dates = future_dates_all[future_dates_all > test_end_date]
            forecast_90_days = forecast_df.loc[forecast_df['ds'].isin(forecast_dates), 'yhat'].values

        except Exception as e:
            logging.error(f"Prophet failed: {e}")
            y_pred_test = pd.Series(np.repeat(0.0, len(df_test)), index=df_test.index)
            forecast_90_days = np.repeat(0.0, forecast_horizon)

            return {'platform': platform, 'model': model_name, 'mae': np.inf, 'mse': np.inf, 'rmse': np.inf}

    # ---------- SNaive ----------
    elif model_name == 'SNaive':
        try:
            season_len = min(7, max(1, len(df_train) // 2))
            y_pred_test, forecast_90_days = seasonal_naive_forecast(df_train[target_col_used], df_test[target_col_used], season_length=season_len, horizon=forecast_horizon)
            # ensure series index alignment
            y_pred_test.index = df_test.index
        except Exception as e:
            logging.error(f"SNaive failed: {e}")
            y_pred_test = pd.Series(np.repeat(0.0, len(df_test)), index=df_test.index)
            forecast_90_days = np.repeat(0.0, forecast_horizon)
            return {'platform': platform, 'model': model_name, 'mae': np.inf, 'mse': np.inf, 'rmse': np.inf}

    # ---------- Postprocess predictions ----------
    try:
        # Ensure y_test_actual is set (for models that changed it)
        y_pred_test[y_pred_test < 0] = 0
        forecast_90_days[forecast_90_days < 0] = 0

        y_pred_test_linear = np.expm1(y_pred_test)
        forecast_90_days_linear = np.expm1(forecast_90_days)
        y_test_linear_actual = np.expm1(y_test_actual)

        # Clip negative predictions to zero
        mae = calculate_mae(y_test_linear_actual, y_pred_test_linear)
        mse = calculate_mse(y_test_linear_actual, y_pred_test_linear)
        rmse = calculate_rmse(y_test_linear_actual, y_pred_test_linear)
    except Exception as e:
        logging.warning(f"Postprocessing predictions failed: {e}")
        y_pred_test_linear = pd.Series(np.repeat(0.0, len(y_test_linear_actual)), index=y_test_linear_actual.index)
        forecast_90_days = np.repeat(0.0, forecast_horizon)

    # Save test CSV via helper (redundant but safe)
    try:
        test_results_df = pd.DataFrame({
            'date': df_test['date'].iloc[:len(y_test_linear_actual)],
            'actual': np.array(y_test_linear_actual).astype(float),
            'prediction': np.array(y_pred_test_linear).astype(float)
        }).fillna(0.0)
        
        save_test_data_to_csv(test_results_df, f"{platform}_{model_name}")
        save_plot(test_results_df, f"{platform} - {model_name}", 'TEST_VS_ACTUAL')
    except Exception as e:
        logging.error(f"[{platform} - {model_name}] Test save/plot failed: {e}")

    # Build forecast_plot_df with aligned lengths: history_dates + future_dates
    try:
        future_dates = pd.date_range(start=df_test['date'].max() + pd.Timedelta(days=1), periods=forecast_horizon, freq='D')
        forecast_df_to_save = pd.DataFrame({
            'date': future_dates,
            'forecasted_gross_revenue': np.nan_to_num(forecast_90_days_linear, nan=0.0)
        })

        csv_file_name = f"{platform.lower()}_{model_name.lower()}_forecast_{forecast_horizon}_days.csv"
        csv_path = os.path.join('app/Analytics/csv_files', csv_file_name)
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        forecast_df_to_save.to_csv(csv_path, index=False)
        logging.info(f"✅ Saved CSV: {csv_file_name}")

        # Forecast Plot (History + Future)
        history_dates = df_agg['date']
        history_values_linear = df_agg['gross_revenue']
        
        full_dates = pd.concat([history_dates, pd.Series(future_dates)], ignore_index=True)
        full_history = np.concatenate([history_values_linear.values, np.repeat(np.nan, forecast_horizon)])
        full_forecast = np.concatenate([np.repeat(np.nan, len(history_values_linear)), np.nan_to_num(forecast_90_days_linear, nan=0.0)])

        forecast_plot_df = pd.DataFrame({
            'date': full_dates,
            'history': full_history,
            'forecast': full_forecast
        })
        save_plot(forecast_plot_df, f"{platform} - {model_name}", '90_DAY_FORECAST')

    except Exception as e:
        logging.error(f"[{platform} - {model_name}] Forecast save/plot failed: {e}")
        
    logging.info(f"✅ {platform} {model_name} — MAE: {mae:.2f}, RMSE: {rmse:.2f}")
    return {'platform': platform, 'model': model_name, 'mae': mae, 'mse': mse, 'rmse': rmse}
