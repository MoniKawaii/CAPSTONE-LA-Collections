# Analytics/Predictive_Modeling/feature_builder.py
import pandas as pd

def create_time_features(df):
    """Creates cyclical time features directly from the index."""
    df['year'] = df.index.year
    df['month'] = df.index.month
    df['day'] = df.index.day
    df['dayofweek'] = df.index.dayofweek
    df['dayofyear'] = df.index.dayofyear
    df['quarter'] = df.index.quarter
    return df

def create_lag_features(df, target_col='gross_revenue', lags=[1, 7, 28, 90]):
    """Creates platform-specific lagged features and rolling means."""
    
    df = df.sort_values(by=['platform_key', 'date'])
    
    for lag in lags:
        # Lagged Target Feature
        df[f'{target_col}_lag_{lag}'] = df.groupby('platform_key')[target_col].shift(lag)
        
    # Rolling Mean Feature (shifted by 1 day to prevent data leakage)
    df[f'{target_col}_rolling_mean_7'] = df.groupby('platform_key')[target_col].rolling(
        window=7, min_periods=1
    ).mean().reset_index(level=0, drop=True).shift(1)
    
    # Drop initial rows where lags cannot be computed (i.e., first 90 days)
    return df.dropna(subset=[f'{target_col}_lag_{lags[-1]}'])

def prepare_data_for_xgb(df):
    """Applies all feature engineering and prepares the final X and y."""
    
    df = create_time_features(df)
    df = create_lag_features(df)
    
    # Create encoding map for platform (XGBoost requires numerical input)
    platform_map = {name: idx for idx, name in enumerate(df['platform_name'].unique())}
    df['platform_encoded'] = df['platform_name'].map(platform_map)
    
    # Final Feature List
    FEATURES = [
        'platform_encoded', 'year', 'month', 'day', 'dayofweek', 'dayofyear', 'quarter', 
        'is_mega_sale_day', 'is_payday', 
        'gross_revenue_lag_1', 'gross_revenue_lag_7', 'gross_revenue_lag_28', 'gross_revenue_lag_90', 
        'gross_revenue_rolling_mean_7'
    ]
    TARGET = 'gross_revenue'
    
    return df[FEATURES], df[TARGET], platform_map