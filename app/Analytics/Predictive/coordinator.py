# app/Analytics/Predictive/coordinator.py

import pandas as pd
import os
import sys
import logging

# 🌟 DYNAMIC PATH ADJUSTMENT 🌟
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..', '..', '..'))

if project_root not in sys.path:
    sys.path.append(project_root)

from app.Analytics.data_loader import load_base_sales_data
from app.Analytics.Predictive.model_trainer import train_and_forecast_model

# Configuration
TARGET_COL = 'gross_revenue'
MODELS = ['XGBoost', 'LightGBM', 'SARIMAX', 'Prophet', 'SNaive']

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

def run_predictive_analysis():
    """
    Coordinates the full predictive modeling pipeline.
    """
    logging.info("--- Starting Predictive Analysis ---")

    # --- Reset global last-date tracking ---
    global_last_date_file = "app/Analytics/Predictive/_latest_date.txt"
    if os.path.exists(global_last_date_file):
        os.remove(global_last_date_file)
        logging.info("[Init] Cleared old _latest_date.txt (will be recalculated during this run)")

    # --- Load base data ---
    try:
        df_all = load_base_sales_data(start_date='2020-09-19')
    except Exception as e:
        logging.error(f"Data load failed: {e}")
        return

    if df_all.empty:
        logging.warning("Empty dataset received — stopping run.")
        return

    df_all = df_all.dropna(subset=[TARGET_COL, 'platform_name']).copy()
    platforms = df_all['platform_name'].unique()
    results = []

    for platform in platforms:
        logging.info(f"###################### PLATFORM: {platform} ######################")
        df_platform = df_all[df_all['platform_name'] == platform].copy()

        for model_name in MODELS:
            result = train_and_forecast_model(
                df=df_platform,
                platform=platform,
                model_name=model_name,
                target_col=TARGET_COL
            )

            # Append model + platform info for summary table
            result.update({
                "platform": platform,
                "model": model_name
            })
            results.append(result)

    # --- Results Summary ---
    results_df = pd.DataFrame(results)
    logging.info("\n###################### RESULTS SUMMARY ######################")

    for platform in platforms:
        platform_results = results_df[results_df['platform'] == platform].sort_values(by='mae')
        logging.info(f"\n--- {platform} MAE Comparison (Lower is Better) ---")
        logging.info(f"\n{platform_results.to_string(index=False)}")

        if not platform_results.empty:
            best_model = platform_results.iloc[0]
            logging.info(f"\n🥇 Best Model for {platform}: **{best_model['model']}** with MAE: {best_model['mae']:.2f}")

    logging.info("\n--- Predictive Analysis Complete ---")


if __name__ == '__main__':
    run_predictive_analysis()
