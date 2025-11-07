# app/Analytics/Predictive/feature_engineer.py

import pandas as pd
import numpy as np

def build_time_series_features(df, target_col, lag_periods, rolling_window):
    """
    Builds lag, rolling, and calendar features for time series ML models.
    """
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date').sort_index()

    # --- NEW: Classification Target for Two-Part Model ---
    df['has_revenue'] = (df[target_col] > 0).astype(int)
    # ---------------------------------------------------

    # Calendar features
    df = df.assign(
        year=df.index.year,
        month=df.index.month,
        dayofweek=df.index.dayofweek,
        dayofyear=df.index.dayofyear,
        weekofyear=df.index.isocalendar().week.astype(int),
        quarter=df.index.quarter
    )

    # Lag features
    for lag in lag_periods:
        df[f'{target_col}_lag_{lag}'] = df[target_col].shift(lag)

    # Rolling window features
    df[f'{target_col}_rolling_mean_{rolling_window}'] = df[target_col].shift(1).rolling(window=rolling_window).mean()
    df[f'{target_col}_rolling_std_{rolling_window}'] = df[target_col].shift(1).rolling(window=rolling_window).std()

    # Drop initial NaNs caused by lagging
    df = df.dropna(subset=[f'{target_col}_lag_{lag_periods[0]}']) # Drop based on the smallest lag

    return df.reset_index()