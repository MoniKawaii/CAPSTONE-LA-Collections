import pandas as pd
import numpy as np
import logging

def build_time_series_features(df, target_col, lag_periods=[1, 7, 14, 30, 60], rolling_window=14):
    """
    Builds time-series features with event flags, lagged exogenous variables, 
    and interaction terms for ML forecasting.
    
    Changes:
    - Added is_day_before_event and is_day_after_event flags.
    - Added 'non-zero' lagged features to handle data sparsity.
    """
    df = df.copy()
    df = df.sort_values("date").reset_index(drop=True)

    # ======================
    # 🧭 1. DATE FEATURES
    # ======================
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["dayofweek"] = df["date"].dt.dayofweek
    df["dayofyear"] = df["date"].dt.dayofyear
    df["weekofyear"] = df["date"].dt.isocalendar().week.astype(int)
    df["quarter"] = ((df["month"] - 1) // 3) + 1
    df["is_weekend"] = (df["dayofweek"] >= 5).astype(int)

    # ======================
    # 🎉 2. EVENT FEATURES (ENHANCED)
    # ======================
    
    # a. Unified Double-Day Sale Flags (e.g., 1.1, 2.2, 11.11)
    is_double_day = (df["date"].dt.month == df["date"].dt.day)
    df["is_double_day"] = is_double_day.astype(int)
    
    # Use 'is_double_day' as the generic mega-sale flag required for section 6.
    df["is_mega_sale_day"] = df["is_double_day"] 
    
    # b. Payday flags (1st, 15th, and end of month)
    df["is_payday"] = ((df["date"].dt.day == 1) | (df["date"].dt.day == 15) | (df["date"].dt.is_month_end)).astype(int)

    # c. Payday weekend (payday ±2 days that include a weekend)
    df["is_payday_weekend"] = 0
    for i in [-1, 0, 1]:
        # Using .copy() to avoid SettingWithCopyWarning
        shifted = df["date"].copy() + pd.Timedelta(days=i)
        payday_mask = ((shifted.dt.day == 1) | (shifted.dt.day == 15) | (shifted.dt.is_month_end))
        weekend_mask = (df["dayofweek"] >= 5)
        df.loc[payday_mask & weekend_mask, "is_payday_weekend"] = 1
        
    # d. Proximity to mega-sale events (days until/after next)
    def days_to_event(date, month, day):
        event = pd.Timestamp(year=date.year, month=month, day=day)
        if event < date:
            event = pd.Timestamp(year=date.year + 1, month=month, day=day)
        return (event - date).days

    df["days_until_11_11"] = df["date"].apply(lambda d: days_to_event(d, 11, 11))
    df["days_until_12_12"] = df["date"].apply(lambda d: days_to_event(d, 12, 12))
    
    # **NEW FEATURE: Pre/Post-Event Flags**
    event_days = set(df[df["is_double_day"] == 1]["date"].dt.date)
    
    def is_day_before_event(date, events):
        return int((date.date() + pd.Timedelta(days=1)) in events)
        
    def is_day_after_event(date, events):
        return int((date.date() - pd.Timedelta(days=1)) in events)

    df["is_day_before_event"] = df["date"].apply(lambda d: is_day_before_event(d, event_days))
    df["is_day_after_event"] = df["date"].apply(lambda d: is_day_after_event(d, event_days))


    # ======================
    # 💰 3. LAGGED FEATURES (ENHANCED)
    # ======================
    
    last_sale_series = df[target_col].where(df[target_col] > 0)
    last_sale_value = last_sale_series.ffill()
    df['last_non_zero_sale_value'] = last_sale_value.shift(1).fillna(0)

    # New: Non-Zero Lags (to focus on sales signal and ignore long zero periods)
    # Note: df[target_col] must be > 0 for this to work as intended.
    df[f"{target_col}_nonzero"] = df[target_col].apply(lambda x: x if x > 0 else 0)
    for lag in lag_periods:
        df[f"{target_col}_nonzero_lag_{lag}"] = df[f"{target_col}_nonzero"].shift(lag)

    # Original Lags (kept for comparison/completeness)
    for lag in lag_periods:
        df[f"{target_col}_lag_{lag}"] = df[target_col].shift(lag)

    # Year-over-year lags (same day last year)
    df[f"{target_col}_lag_364"] = df[target_col].shift(364)
    df[f"{target_col}_lag_365"] = df[target_col].shift(365)

    # Lagged exogenous features (price/discount)
    for lag in [1, 7]:
        for exog_col in ["avg_discount_rate", "avg_paid_price"]:
            if exog_col in df.columns:
                df[f"{exog_col}_lag_{lag}"] = df[exog_col].shift(lag)

    # ======================
    # 📊 4. ROLLING FEATURES 
    # ======================
    df[f"{target_col}_rolling_mean_{rolling_window}"] = df[target_col].shift(1).rolling(window=rolling_window).mean()
    df[f"{target_col}_rolling_std_{rolling_window}"] = df[target_col].shift(1).rolling(window=rolling_window).std()
    df[f"{target_col}_rolling_median_{rolling_window}"] = df[target_col].shift(1).rolling(window=rolling_window).median()

    # ======================
    # 🔄 5. FOURIER FEATURES 
    # ======================
    t = 2 * np.pi * df["dayofyear"] / 366
    for k in range(1, 6):
        df[f"fourier_sin_{k}"] = np.sin(k * t)
        df[f"fourier_cos_{k}"] = np.cos(k * t)

    # ======================
    # ⚡ 6. INTERACTION FEATURES 
    # ======================
    if "is_mega_sale_day" in df.columns and "avg_discount_rate" in df.columns:
        df["discount_on_event"] = df["is_mega_sale_day"] * df["avg_discount_rate"]
    if "avg_discount_rate" in df.columns and "is_payday" in df.columns:
        df["discount_on_payday"] = df["avg_discount_rate"] * df["is_payday"]
    if "last_non_zero_sale_value" in df.columns and "is_day_before_event" in df.columns:
        df["last_sale_event_interaction"] = df["last_non_zero_sale_value"] * df["is_day_before_event"]

    # ======================
    # 🧹 Final cleanup
    # ======================
    # Remove the intermediate non-zero column
    if f"{target_col}_nonzero" in df.columns:
        df = df.drop(columns=[f"{target_col}_nonzero"], errors='ignore')
        
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.fillna(0)

    logging.info(f"[FeatureBuilder] Generated {df.shape[1]} columns.")
    return df