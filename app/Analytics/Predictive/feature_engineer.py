import pandas as pd
import numpy as np
import logging

def build_time_series_features(df, target_col, lag_periods=[1, 7, 14, 30, 60], rolling_window=14):
    """
    Builds time-series features with lagged, event, Fourier, trend, and volatility signals.
    Recursion-safe version: preserves continuity, avoids flattening during forecasting.
    """

    df = df.copy().sort_values("date").reset_index(drop=True)

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
    df["days_since_start"] = (df["date"] - df["date"].min()).dt.days.astype(int)

    # ======================
    # 🎉 2. EVENT FLAGS
    # ======================
    is_double_day = (df["date"].dt.month == df["date"].dt.day)
    df["is_double_day"] = is_double_day.astype(int)
    df["is_mega_sale_day"] = df["is_double_day"]

    df["is_payday"] = ((df["date"].dt.day == 1) |
                       (df["date"].dt.day == 15) |
                       (df["date"].dt.is_month_end)).astype(int)

    df["is_payday_weekend"] = 0
    for i in [-1, 0, 1]:
        shifted = df["date"].copy() + pd.Timedelta(days=i)
        payday_mask = ((shifted.dt.day == 1) | (shifted.dt.day == 15) | (shifted.dt.is_month_end))
        weekend_mask = (df["dayofweek"] >= 5)
        df.loc[payday_mask & weekend_mask, "is_payday_weekend"] = 1

    # Pre/Post-event flags
    event_days = set(df[df["is_double_day"] == 1]["date"].dt.date)
    df["is_day_before_event"] = df["date"].apply(lambda d: int((d.date() + pd.Timedelta(days=1)) in event_days))
    df["is_day_after_event"] = df["date"].apply(lambda d: int((d.date() - pd.Timedelta(days=1)) in event_days))

    # ======================
    # 💰 3. LAG FEATURES (log-safe)
    # ======================
    if target_col == "gross_revenue" and "log_revenue" in df.columns:
        lag_source = "log_revenue"
    else:
        lag_source = target_col

    last_sale_series = df[lag_source].where(df[lag_source] > 0)
    df["last_non_zero_sale_value"] = last_sale_series.ffill().shift(1).fillna(0)

    for lag in lag_periods:
        df[f"{lag_source}_lag_{lag}"] = df[lag_source].shift(lag)
        df[f"{lag_source}_nonzero_lag_{lag}"] = df[lag_source].clip(lower=0).shift(lag)

    df[f"{lag_source}_lag_364"] = df[lag_source].shift(364)
    df[f"{lag_source}_lag_365"] = df[lag_source].shift(365)

    # Lagged exogenous features
    for lag in [1, 7]:
        for exog_col in ["avg_discount_rate", "avg_paid_price"]:
            if exog_col in df.columns:
                df[f"{exog_col}_lag_{lag}"] = df[exog_col].shift(lag)

    # ======================
    # 📊 4. ROLLING FEATURES 
    # ======================
    df[f"{lag_source}_rolling_mean_{rolling_window}"] = df[lag_source].shift(1).rolling(window=rolling_window).mean()
    df[f"{lag_source}_rolling_std_{rolling_window}"] = df[lag_source].shift(1).rolling(window=rolling_window).std()
    df[f"{lag_source}_rolling_median_{rolling_window}"] = df[lag_source].shift(1).rolling(window=rolling_window).median()

    # ======================
    # 📈 5. TREND / MOMENTUM / VOLATILITY
    # ======================
    sql_trend_cols = [
        'prev_day_revenue', 'daily_revenue_growth', 'rolling_revenue_7d',
        'rolling_revenue_growth_7d', 'rolling_discount_rate_7d',
        'discount_change_rate_1d', 'price_change_rate_1d'
    ]
    for col in sql_trend_cols:
        if col not in df.columns:
            df[col] = 0.0

    df['daily_revenue_growth_smoothed'] = df['daily_revenue_growth'].rolling(3, min_periods=1).mean()
    df['discount_change_rate_3d'] = df['discount_change_rate_1d'].rolling(3, min_periods=1).mean()

    df['growth_x_discount'] = df['daily_revenue_growth_smoothed'] * df['avg_discount_rate']
    df['growth_x_price_change'] = df['daily_revenue_growth_smoothed'] * df['price_change_rate_1d']

    df['revenue_volatility_7d'] = df[lag_source].rolling(7, min_periods=1).std().fillna(0)
    rolling_mean_7 = df[lag_source].rolling(7, min_periods=1).mean().replace(0, np.nan)
    df['revenue_vs_rolling_7d'] = (df[lag_source] - rolling_mean_7) / rolling_mean_7.fillna(1)

    if 'avg_paid_price' in df.columns:
        df['price_volatility_7d'] = df['avg_paid_price'].rolling(7, min_periods=1).std().fillna(0)

    # ======================
    # 🔄 6. FOURIER FEATURES (year-continuous)
    # ======================
    t = 2 * np.pi * (df["year"] * 366 + df["dayofyear"]) / (366 * df["year"].nunique())
    for k in range(1, 6):
        df[f"fourier_sin_{k}"] = np.sin(k * t)
        df[f"fourier_cos_{k}"] = np.cos(k * t)

    # ======================
    # ⚡ 7. INTERACTIONS
    # ======================
    if "is_mega_sale_day" in df.columns and "avg_discount_rate" in df.columns:
        df["discount_on_event"] = df["is_mega_sale_day"] * df["avg_discount_rate"]
    if "avg_discount_rate" in df.columns and "is_payday" in df.columns:
        df["discount_on_payday"] = df["avg_discount_rate"] * df["is_payday"]
    if "last_non_zero_sale_value" in df.columns and "is_day_before_event" in df.columns:
        df["last_sale_event_interaction"] = df["last_non_zero_sale_value"] * df["is_day_before_event"]

    if 'daily_revenue_growth_smoothed' in df.columns:
        df['growth_on_event'] = df['daily_revenue_growth_smoothed'] * df['is_mega_sale_day']
        df['growth_on_payday'] = df['daily_revenue_growth_smoothed'] * df['is_payday']
    if 'rolling_revenue_growth_7d' in df.columns:
        df['rolling_growth_on_event'] = df['rolling_revenue_growth_7d'] * df['is_mega_sale_day']

    # ======================
    # 🧹 8. SAFE FILL (recursive-aware)
    # ======================
    flag_cols = [c for c in df.columns if c.startswith("is_") or "event" in c]
    cont_cols = [c for c in df.columns if c not in flag_cols + ["date"]]

    df[flag_cols] = df[flag_cols].fillna(0)
    df[cont_cols] = df[cont_cols].ffill().bfill()
    df = df.replace([np.inf, -np.inf], 0)

    logging.info(f"[FeatureBuilder] Safe fill strategy applied ({len(flag_cols)} flag cols, {len(cont_cols)} continuous cols).")
    logging.info(f"[FeatureBuilder] Generated {df.shape[1]} columns.")

    return df
