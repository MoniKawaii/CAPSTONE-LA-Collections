# Analytics/Predictive_Modeling/sales_forecaster.py
import pandas as pd
import xgboost as xgb
from data_loader import load_base_sales_data
from feature_builder import prepare_data_for_xgb, create_time_features
from model_utils import evaluate_model, train_prophet_model, generate_prophet_forecast

def forecast_sales_iterative(model, last_date, platform_map, historical_data, horizon=90):
    """
    XGBoost Forecast: Iteratively predicts future values, generating new lag features in each step.
    """
    forecast_df = []
    future_dates = pd.date_range(start=last_date + pd.Timedelta(days=1), periods=horizon)

    for platform_name, platform_id in platform_map.items():
        
        # Use a copy of the historical data for lag calculations
        platform_rev_history = historical_data[historical_data['platform_encoded'] == platform_id].copy()
        
        for current_date in future_dates:
            
            # --- A. Generate features for the CURRENT date ---
            current_day_data = pd.DataFrame(index=[current_date])
            current_day_data['platform_encoded'] = platform_id
            current_day_data = create_time_features(current_day_data)
            
            # CRITICAL: Manually set future special event flags (MUST be sourced from marketing calendar)
            current_day_data['is_mega_sale_day'] = 0 
            current_day_data['is_payday'] = 1 if current_date.day in [15, 30] else 0
            
            # Fill Lag Features from the last N days of the RECENT history (which includes predictions)
            current_day_data['gross_revenue_lag_1'] = platform_rev_history['gross_revenue'].iloc[-1]
            current_day_data['gross_revenue_lag_7'] = platform_rev_history['gross_revenue'].iloc[-7]
            current_day_data['gross_revenue_lag_28'] = platform_rev_history['gross_revenue'].iloc[-28]
            current_day_data['gross_revenue_lag_90'] = platform_rev_history['gross_revenue'].iloc[-90]
            
            current_day_data['gross_revenue_rolling_mean_7'] = platform_rev_history['gross_revenue'].iloc[-7:].mean()

            # --- B. Predict the revenue ---
            current_day_features = current_day_data[model.feature_names_in_]
            predicted_revenue = model.predict(current_day_features)[0]
            
            # --- C. Update history with the prediction ---
            new_history_row = pd.DataFrame({
                'gross_revenue': [predicted_revenue],
                'platform_encoded': [platform_id]
            }, index=[current_date])
            platform_rev_history = pd.concat([platform_rev_history, new_history_row])
            
            # --- D. Store the forecast ---
            forecast_df.append({
                'date': current_date,
                'platform_name': platform_name,
                'predicted_gross_revenue_xgb': predicted_revenue
            })

    return pd.DataFrame(forecast_df)


def run_sales_forecast():
    
    # 1. Load and Prepare Data
    print("1. Loading and preparing data...")
    base_df = load_base_sales_data()
    X, y, platform_map = prepare_data_for_xgb(base_df.copy())
    last_historical_date = y.index.max() 
    
    # Combine X and y for the historical data used in iterative forecasting
    historical_data = X.copy()
    historical_data['gross_revenue'] = y
    
    # --- 2. XGBoost Pipeline (High-Accuracy Model) ---
    print("\n2. Training XGBoost Model...")
    
    # Train the final model on all available data
    final_xgb_model = xgb.XGBRegressor(
        n_estimators=1000, 
        learning_rate=0.01, 
        objective='reg:squarederror', 
        random_state=42,
        n_jobs=-1
    )
    final_xgb_model.fit(X, y)

    print("Generating 90-day XGBoost forecast...")
    xgb_forecast = forecast_sales_iterative(
        model=final_xgb_model,
        last_date=last_historical_date,
        platform_map=platform_map,
        historical_data=historical_data
    )
    
    # --- 3. Prophet Pipeline (Baseline Model) ---
    print("\n3. Running Prophet Baseline Forecast...")
    prophet_forecasts_list = []
    
    # Train and forecast for each platform
    for platform_name in platform_map.keys():
        prophet_model, _ = train_prophet_model(base_df.reset_index(), platform_name)
        prophet_forecast = generate_prophet_forecast(
            prophet_model, 
            base_df.reset_index(), 
            platform_name, 
            horizon=90
        )
        prophet_forecasts_list.append(prophet_forecast)

    final_prophet_forecast = pd.concat(prophet_forecasts_list)
    
    # --- 4. Final Reporting and Output ---
    
    # Merge both forecasts for easy comparison and saving
    final_forecast_comparison = pd.merge(
        xgb_forecast, 
        final_prophet_forecast, 
        on=['date', 'platform_name'], 
        how='outer'
    )

    print("\n--- Sales Forecast Complete ---")
    print(f"Forecast Horizon: {final_forecast_comparison['date'].min().date()} to {final_forecast_comparison['date'].max().date()}")
    print("\nSample Forecast Comparison:")
    print(final_forecast_comparison.head(10).to_markdown(index=False))

    # Save the final output
    final_forecast_comparison.to_csv('Analytics/sales_forecast_comparison.csv', index=False)
    print("\nOutput saved to Analytics/sales_forecast_comparison.csv")


if __name__ == '__main__':
    run_sales_forecast()