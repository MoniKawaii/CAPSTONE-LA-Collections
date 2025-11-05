# Analytics/Predictive_Modeling/sales_forecaster.py
import os
import sys
import pandas as pd
import xgboost as xgb
import matplotlib.pyplot as plt # NEW IMPORT
import seaborn as sns          # NEW IMPORT
from feature_builder import prepare_data_for_xgb, create_time_features
# Assuming model_utils contains the Prophet and evaluation functions
from model_utils import evaluate_model, train_prophet_model, generate_prophet_forecast

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..')) 

# Add the project root to the system path
if project_root not in sys.path:
    sys.path.append(project_root)

from data_loader import load_base_sales_data

OUTPUT_DIR = 'app/Analytics/'

def forecast_sales_iterative(model, last_date, platform_map, historical_data, horizon=90):
    """
    XGBoost Forecast: Iteratively predicts future values, generating new lag features in each step.
    """
    forecast_df = []
    # Generate future dates starting the day after the last historical date
    future_dates = pd.date_range(start=last_date + pd.Timedelta(days=1), periods=horizon)

    for platform_name, platform_id in platform_map.items():
        
        # Use a copy of the historical data for lag calculations
        platform_rev_history = historical_data[historical_data['platform_encoded'] == platform_id].copy()
        
        # Ensure platform_rev_history is sorted by date index
        platform_rev_history = platform_rev_history.sort_index()
        
        for current_date in future_dates:
            
            # --- A. Generate features for the CURRENT date ---
            current_day_data = pd.DataFrame(index=[current_date])
            current_day_data['platform_encoded'] = platform_id
            current_day_data = create_time_features(current_day_data)
            
            # CRITICAL: Manually set future special event flags 
            # (Assuming 0/1 integer flags are expected)
            current_day_data['is_mega_sale_day'] = 0 
            current_day_data['is_payday'] = 1 if current_date.day in [15, 30] else 0
            
            # Fill Lag Features from the last N days of the RECENT history 
            # NOTE: .iloc[-N] relies on the index being sequential (sorted by date)
            try:
                current_day_data['gross_revenue_lag_1'] = platform_rev_history['gross_revenue'].iloc[-1]
                current_day_data['gross_revenue_lag_7'] = platform_rev_history['gross_revenue'].iloc[-7]
                current_day_data['gross_revenue_lag_28'] = platform_rev_history['gross_revenue'].iloc[-28]
                current_day_data['gross_revenue_lag_90'] = platform_rev_history['gross_revenue'].iloc[-90]
                current_day_data['gross_revenue_rolling_mean_7'] = platform_rev_history['gross_revenue'].iloc[-7:].mean()
            except IndexError:
                # Handle cases where history is too short for a lag (e.g., first few predictions)
                # This should only happen if the training data was too small to establish the initial lag features.
                print(f"Warning: History too short for full lag features on {current_date}")
                # For robustness, we'll skip this iteration or use a filler, but for a trained model, 
                # this indicates the input training data was lacking the required lag window.
                continue

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

def plot_forecast_comparison(df_forecast, output_dir):
    """Generates and saves a time series plot comparing XGBoost and Prophet forecasts for each platform."""
    
    print("\n4b. Generating Forecast Comparison Plots...")
    
    # Ensure the date index is a proper datetime for plotting
    df_forecast = df_forecast.reset_index().rename(columns={'index': 'date'}).set_index('date')

    unique_platforms = df_forecast['platform_name'].unique()
    
    # Define consistent colors
    XGB_COLOR = '#DC143C' # Crimson Red for the high-impact model
    PROPHET_COLOR = '#4682B4' # Steel Blue for the baseline
    
    for platform in unique_platforms:
        # Filter data for the current platform
        df_platform = df_forecast[df_forecast['platform_name'] == platform]
        
        plt.figure(figsize=(12, 6))
        
        # Plot XGBoost Forecast
        plt.plot(
            df_platform.index, 
            df_platform['predicted_gross_revenue_xgb'], 
            label='XGBoost Forecast', 
            color=XGB_COLOR, 
            linewidth=2
        )
        
        # Plot Prophet Forecast
        plt.plot(
            df_platform.index, 
            df_platform['predicted_gross_revenue_prophet'], 
            label='Prophet Baseline', 
            color=PROPHET_COLOR, 
            linewidth=2,
            linestyle='--'
        )
        
        plt.title(f'{platform} 90-Day Revenue Forecast Comparison: XGBoost vs. Prophet', fontsize=14)
        plt.xlabel('Date')
        plt.ylabel('Predicted Gross Revenue (PHP)')
        plt.legend(loc='best')
        plt.grid(True, which='both', linestyle='--', linewidth=0.5)
        
        # Save the plot
        file_name = f'forecast_comparison_{platform.lower()}.png'
        plt.savefig(os.path.join(output_dir, file_name))
        plt.close()
        print(f"-> Saved {platform} forecast plot to {os.path.join(output_dir, file_name)}")


def run_sales_forecast():
    
    # 1. Load and Prepare Data
    print("1. Loading and preparing data...")
    base_df = load_base_sales_data()

    print(f"DEBUG: Last date in base_df: {base_df.index.max()}")

    X, y, platform_map = prepare_data_for_xgb(base_df.copy())

    historical_data_for_xgb_loop = X.join(y).copy()
    
    # Define last historical date and forecast start date
    last_historical_date = y.index.max() 
    forecast_start_date = last_historical_date + pd.Timedelta(days=1)
    
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
        # FIX: Correctly pass the historical data for lag feature calculation
        historical_data=historical_data_for_xgb_loop
    )
    
    # --- 3. Prophet Pipeline (Baseline Model) ---
    print("\n3. Running Prophet Baseline Forecast...")
    prophet_forecasts_list = []
    
    # Train and forecast for each platform
    for platform_name in platform_map.keys():
        # Prophet uses the raw data (date must be a column, not index)
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
    
    # Filter the final comparison table to include ONLY the future dates 
    final_forecast_comparison = final_forecast_comparison[
        final_forecast_comparison['date'] >= forecast_start_date
    ]

    # Set index to date for cleaner table view and ensure sorting
    final_forecast_comparison.set_index('date', inplace=True) 
    final_forecast_comparison = final_forecast_comparison.sort_values(by=['date', 'platform_name'])
    
    # --- 4b. Generate Plots ---
    plot_forecast_comparison(final_forecast_comparison.copy(), OUTPUT_DIR)
    
    print("\n--- Sales Forecast Complete ---")
    # FIX: Access the date from the index
    print(f"Forecast Horizon: {final_forecast_comparison.index.min().date()} to {final_forecast_comparison.index.max().date()}")
    
    print("\nSample Forecast Comparison:")
    # Remove index=False to ensure the date column is printed in the table
    print(final_forecast_comparison.head(10).to_markdown()) 

    # Save the final output
    output_path = 'app/Analytics/sales_forecast_comparison.csv'
    final_forecast_comparison.to_csv(output_path, index=True) # Keep index=True to save 'date' as the first column
    print(f"\nOutput saved to {output_path}")


if __name__ == '__main__':
    # Add try-except block here for clean startup logging if desired
    run_sales_forecast()