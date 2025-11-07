# app/Analytics/Predictive/plot_helper.py

import pandas as pd
import matplotlib.pyplot as plt
import os
from datetime import datetime
import logging

def save_plot(df: pd.DataFrame, platform_name: str, plot_type: str, folder_path: str = 'app/Analytics/png_files') -> None:
    """
    Generates and saves a plot with dynamic forecast length and timestamped filenames.
    """
    os.makedirs(folder_path, exist_ok=True)
    plt.figure(figsize=(14, 7))
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")

    if plot_type == 'TEST_VS_ACTUAL':
        plt.plot(df['date'], df['actual'], label='Actual', color='blue')
        plt.plot(df['date'], df['prediction'], label='Predicted', color='red', linestyle='--')
        title = f'{platform_name} - Test Period'
        file_name = f'{platform_name.lower()}_test_vs_actual.png'

    elif plot_type == '90_DAY_FORECAST':
        forecast_len = len(df[df["forecast"].notna()])
        plt.plot(df['date'], df['history'], label='History', color='blue')
        plt.plot(df['date'].iloc[-forecast_len:], df['forecast'].iloc[-forecast_len:], label=f'{forecast_len}-Day Forecast', color='green')
        title = f'{platform_name} - Forecast ({forecast_len} Days)'
        file_name = f'{platform_name.lower()}_forecast.png'

    else:
        return

    plt.title(title)
    plt.xlabel('Date')
    plt.ylabel('Gross Revenue')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(os.path.join(folder_path, file_name))
    plt.close()
    logging.info(f"✅ Saved plot: {file_name}")

def save_test_data_to_csv(df: pd.DataFrame, platform_name: str, folder_path: str = 'app/Analytics/csv_files') -> None:
    """
    Saves the test set comparison data to a CSV file.
    """
    os.makedirs(folder_path, exist_ok=True)
    file_name = f'{platform_name.lower()}_test_comparison.csv'
    df[['date', 'actual', 'prediction']].to_csv(os.path.join(folder_path, file_name), index=False)
    logging.info(f"✅ Saved CSV: {file_name}")
