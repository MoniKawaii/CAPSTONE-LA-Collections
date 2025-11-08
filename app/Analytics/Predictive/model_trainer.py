import os
import sys
import logging
import warnings
from datetime import datetime

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
    df_agg = df_agg.set_index('date').reindex(full_date_range).fillna(0.0).rename_axis('date').reset_index()

    df_agg['log_revenue'] = np.log1p(df_agg['gross_revenue'])

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

    # --- Fill exog ---
    for ex in exog_cols:
        if ex in df_agg.columns and ex in ['is_mega_sale_day', 'is_payday']:
            df_agg[ex] = df_agg[ex].fillna(0).astype(int)
        elif ex in df_agg.columns:
            mean_val = df_agg[ex][df_agg[ex] > 0].mean() or 0.0
            df_agg[ex] = df_agg[ex].fillna(mean_val).astype(float)

    target_col = 'log_revenue'

    # --- Interaction features ---
    if all(col in df_agg.columns for col in ['is_mega_sale_day', 'is_payday']):
        df_agg['is_event_day'] = ((df_agg['is_mega_sale_day'] == 1) | (df_agg['is_payday'] == 1)).astype(int)
    if all(col in df_agg.columns for col in ['avg_discount_rate', 'is_mega_sale_day']):
        df_agg['discount_on_event'] = df_agg['avg_discount_rate'] * df_agg['is_event_day']

    # Train/test chronological split
    split_date = df_agg['date'].max() - pd.Timedelta(days=test_size_days - 1)
    df_train = df_agg[df_agg['date'] < split_date].copy()
    df_test = df_agg[df_agg['date'] >= split_date].copy()

    logging.info(f"[{platform}] Data points | train: {len(df_train)} | test: {len(df_test)} (target test_days={test_size_days})")

    mae = mse = rmse = np.inf
    y_pred_test = pd.Series(dtype=float)
    forecast_90_days = np.repeat(0.0, forecast_horizon)

    # ---------- XGBoost & LightGBM ----------
    if model_name in ['XGBoost', 'LightGBM']:
        try:
            # ⚙️ Platform-specific lag/rolling config
            lag_periods = [1, 7, 14, 30, 60] if platform == 'Shopee' else [1, 7, 14]
            rolling_window = 14 if platform == 'Shopee' else 7

            df_full = build_time_series_features(df_agg, target_col, lag_periods=lag_periods, rolling_window=rolling_window)
            df_train_ml = df_full[df_full['date'] < split_date]
            df_test_ml = df_full[df_full['date'] >= split_date]

            features = df_train_ml.columns.drop(
                [target_col, 'date', 'platform_key', 'platform_name', 'total_items_sold'],
                errors='ignore'
            ).tolist()

            X_all, y_all = df_train_ml[features], df_train_ml[target_col]
            val_size = max(1, int(len(X_all) * 0.1))
            X_train, y_train_ml = X_all.iloc[:-val_size], y_all.iloc[:-val_size]
            X_valid, y_valid_ml = X_all.iloc[-val_size:], y_all.iloc[-val_size:]
            X_test, y_test_actual = df_test_ml[features], df_test_ml[target_col]

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
            y_pred_test = pd.Series(model.predict(X_test), index=y_test_actual.index)

            # --- Save test & forecast for XGBoost/LightGBM ---
            # Recursive 90-day forecast (option 1): one-step-ahead iterative predictions
            try:
                # Prepare rolling buffer of last observed target values (to compute lags/rolling)
                max_lag = max([int(c.split('_')[-1]) for c in features if f"{target_col}_lag_" in c] + [1])
                rolling_window = 14  # same as used in feature function
                # get last max_lag + rolling_window values from df_full target column
                hist_targets = list(df_full[target_col].iloc[-(max_lag + rolling_window):].values)
                # If not enough history, pad with zeros
                if len(hist_targets) < (max_lag + rolling_window):
                    pad = [0.0] * ((max_lag + rolling_window) - len(hist_targets))
                    hist_targets = pad + hist_targets

                # Start from the last row in df_full as template for date/exog
                last_row = df_full.iloc[-1].copy()
                future_preds = []
                last_values = hist_targets.copy()  # rolling buffer

                for step in range(forecast_horizon):
                    # build new feature row
                    new_date = last_row['date'] + pd.Timedelta(days=1)
                    new_row = last_row.copy()
                    new_row['date'] = new_date

                    # update calendar features
                    new_row['year'] = new_date.year
                    new_row['month'] = new_date.month
                    new_row['dayofweek'] = new_date.dayofweek
                    new_row['dayofyear'] = new_date.dayofyear
                    new_row['weekofyear'] = int(new_date.isocalendar()[1])
                    new_row['quarter'] = (new_date.month - 1) // 3 + 1
                    new_row['is_weekend'] = int(new_date.dayofweek >= 5)

                    # update Fourier features (same logic as feature_engineer)
                    t = 2 * np.pi * new_row['dayofyear'] / 366
                    for k in range(1, 6):
                        new_row[f'fourier_sin_{k}'] = np.sin(k * t)
                        new_row[f'fourier_cos_{k}'] = np.cos(k * t)

                    # set exogenous continuous features to last known (could be enhanced if you have forecasts)
                    for ex in ['avg_paid_price', 'avg_original_price', 'avg_discount_rate']:
                        if ex in new_row.index:
                            # use last observed value from df_agg
                            new_row[ex] = float(df_agg[ex].iloc[-1]) if ex in df_agg.columns else 0.0

                    # update lag features using last_values buffer
                    for col in features:
                        if f"{target_col}_lag_" in col:
                            try:
                                lag_n = int(col.split('_')[-1])
                                # last_values holds history with most recent at the end
                                new_row[col] = last_values[-lag_n]
                            except Exception:
                                new_row[col] = 0.0

                    # update rolling mean/std using last_values
                    try:
                        last_window = np.array(last_values[-rolling_window:])
                        new_row[f'{target_col}_rolling_mean_{rolling_window}'] = float(np.nanmean(last_window))
                        new_row[f'{target_col}_rolling_std_{rolling_window}'] = float(np.nanstd(last_window))
                    except Exception:
                        new_row[f'{target_col}_rolling_mean_{rolling_window}'] = 0.0
                        new_row[f'{target_col}_rolling_std_{rolling_window}'] = 0.0

                    # Build X vector and predict
                    X_future_row = pd.DataFrame([new_row[features]])
                    try:
                        pred = float(model.predict(X_future_row)[0])
                        if np.isnan(pred) or np.isinf(pred):
                            pred = 0.0
                    except Exception:
                        pred = 0.0

                    # append prediction and update buffers
                    future_preds.append(max(0.0, pred))
                    last_values.append(pred)
                    # keep buffer length
                    if len(last_values) > (max_lag + rolling_window):
                        last_values.pop(0)

                    # update last_row to new_row with target set to pred for next iteration
                    last_row = new_row.copy()
                    last_row[target_col] = pred

                forecast_90_days = np.array(future_preds)

            except Exception as e:
                logging.warning(f"[{platform} - {model_name}] recursive forecast failed: {e}")
                forecast_90_days = np.repeat(float(y_test_actual.mean() if len(y_test_actual)>0 else 0.0), forecast_horizon)

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
                auto_mod = auto_arima(df_train[target_col], exogenous=exog_train, seasonal=True, m=7,
                                      suppress_warnings=True, error_action='ignore', max_order=10)
                order = auto_mod.order
                seasonal_order = auto_mod.seasonal_order
            else:
                order = (1, 1, 1)
                seasonal_order = (1, 0, 1, 7)
                logging.info("pmdarima not available — using fallback SARIMAX order=(1,1,1) seasonal=(1,0,1,7)")

            logging.info(f"SARIMAX order={order}, seasonal_order={seasonal_order}")

            model = SARIMAX(df_train[target_col], order=order, seasonal_order=seasonal_order, exog=exog_train,
                            enforce_stationarity=False, enforce_invertibility=False)
            model_fit = model.fit(disp=False)

            # compute exog_future
            exog_future = pd.DataFrame(index=range(forecast_horizon))
            last_agg = df_agg.tail(1)

            for col in exog_cols:
                if col in df_agg.columns:
                    if col in ['is_mega_sale_day', 'is_payday']:
                        exog_future[col] = np.repeat(0.0, forecast_horizon)
                    else:
                        last_val = last_agg[col].iloc[0] if col in last_agg.columns and not last_agg[col].isna().all() else 0.0
                        exog_future[col] = np.repeat(last_val, forecast_horizon)
                else:
                    exog_future[col] = np.repeat(0.0, forecast_horizon)

            exog_pred_all = pd.concat([exog_test.reset_index(drop=True), exog_future.reset_index(drop=True)], ignore_index=True)

            pred_start = len(df_train[target_col])
            pred_end = pred_start + len(df_test[target_col]) + forecast_horizon - 1

            all_preds = model_fit.predict(start=pred_start, end=pred_end, exog=exog_pred_all)
            y_pred_test = all_preds.iloc[:len(df_test[target_col])]
            y_pred_test.index = df_test.index
            forecast_90_days = all_preds.iloc[len(df_test[target_col]):].values

        except Exception as e:
            logging.error(f"SARIMAX failed: {e}")
            # fallback save placeholders
            y_pred_test = pd.Series(np.repeat(0.0, len(df_test)), index=df_test.index)
            forecast_90_days = np.repeat(0.0, forecast_horizon)

            return {'platform': platform, 'model': model_name, 'mae': np.inf, 'mse': np.inf, 'rmse': np.inf}

    # ---------- Prophet ----------
    elif model_name == 'Prophet':
        try:
            df_prophet_train = df_train.rename(columns={'date': 'ds', target_col: 'y'})[['ds', 'y'] + [c for c in exog_cols if c in df_train.columns]]
            df_prophet_test = df_test.rename(columns={'date': 'ds', target_col: 'y'})[['ds', 'y'] + [c for c in exog_cols if c in df_test.columns]]

            if df_prophet_train.empty:
                raise ValueError("Empty prophet training frame.")

            m = Prophet(daily_seasonality=True, weekly_seasonality=True, yearly_seasonality=False)
            for col in [c for c in exog_cols if c in df_prophet_train.columns]:
                m.add_regressor(col)

            m.fit(df_prophet_train)

            test_dates = df_prophet_test['ds']
            future_dates = pd.date_range(start=test_dates.max() + pd.Timedelta(days=1), periods=forecast_horizon, freq='D')
            future_df = pd.concat([df_prophet_test.drop(columns='y', errors='ignore'), pd.DataFrame({'ds': future_dates})], ignore_index=True, sort=False)

            for col in [c for c in exog_cols if c in df_prophet_train.columns]:
                last_test_val = df_prophet_test[col].iloc[-1] if col in df_prophet_test.columns and not df_prophet_test[col].isna().all() else 0.0
                if col in ['is_mega_sale_day', 'is_payday']:
                    future_df.loc[future_df['ds'].isin(future_dates), col] = 0.0
                else:
                    future_df.loc[future_df['ds'].isin(future_dates), col] = last_test_val
                future_df[col] = future_df[col].fillna(0.0)

            forecast_df = m.predict(future_df)
            y_pred_test = forecast_df.loc[forecast_df['ds'].isin(df_prophet_test['ds']), 'yhat'].reset_index(drop=True)

            if len(y_pred_test) == len(df_prophet_test):
                y_pred_test.index = df_test.index
            else:
                y_pred_test = pd.Series(y_pred_test.values, index=df_test.index[:len(y_pred_test)])

            forecast_90_days = forecast_df.loc[forecast_df['ds'].isin(future_dates), 'yhat'].values
            if len(forecast_90_days) != forecast_horizon:
                forecast_90_days = np.repeat(y_pred_test.iloc[-1] if len(y_pred_test) > 0 else df_train[target_col].iloc[-1], forecast_horizon)

        except Exception as e:
            logging.error(f"Prophet failed: {e}")
            y_pred_test = pd.Series(np.repeat(0.0, len(df_test)), index=df_test.index)
            forecast_90_days = np.repeat(0.0, forecast_horizon)

            return {'platform': platform, 'model': model_name, 'mae': np.inf, 'mse': np.inf, 'rmse': np.inf}

    # ---------- SNaive ----------
    elif model_name == 'SNaive':
        try:
            season_len = min(7, max(1, len(df_train) // 2))
            y_pred_test, forecast_90_days = seasonal_naive_forecast(df_train[target_col], df_test[target_col], season_length=season_len, horizon=forecast_horizon)
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
        if target_col in df_test.columns:
            y_test_actual = df_test[target_col]
        # Ensure y_pred_test index aligns with y_test_actual
        if isinstance(y_pred_test, pd.Series):
            if len(y_pred_test) == len(y_test_actual):
                y_pred_test = y_pred_test.astype(float)
                y_pred_test.index = y_test_actual.index
            else:
                y_pred_test = pd.Series(y_pred_test.values[:len(y_test_actual)], index=y_test_actual.index)
        else:
            y_pred_test = pd.Series(np.repeat(0.0, len(y_test_actual)), index=y_test_actual.index)

        # Clip negative predictions to zero
        y_pred_test[y_pred_test < 0] = 0
        forecast_90_days = np.array(forecast_90_days)
        forecast_90_days[forecast_90_days < 0] = 0
    except Exception as e:
        logging.warning(f"Postprocessing predictions failed: {e}")
        y_pred_test = pd.Series(np.repeat(0.0, len(y_test_actual)), index=y_test_actual.index)
        forecast_90_days = np.repeat(0.0, forecast_horizon)

    y_test_log_actual = y_test_actual.copy()

    # Evaluate metrics (if possible)
    try:
        try:
            # Safely convert test data and predictions back
            y_pred_test = np.expm1(y_pred_test)
            forecast_90_days = np.expm1(forecast_90_days)
            y_test_linear_actual = np.expm1(y_test_log_actual)
            logging.info(f"[{platform}] Log-transform applied — median(log_revenue): {df_agg['log_revenue'].median():.2f}")
        except Exception as e:
            logging.warning(f"Inverse log transform failed: {e}")
            
        mae = calculate_mae(y_test_linear_actual, y_pred_test)
        mse = calculate_mse(y_test_linear_actual, y_pred_test)
        rmse = calculate_rmse(y_test_linear_actual, y_pred_test)
    except Exception as e:
        logging.warning(f"Metric calculation failed: {e}")
        mae = mse = rmse = np.inf

    # Save test CSV via helper (redundant but safe)
    try:
        test_results_df = pd.DataFrame({
            'date': pd.to_datetime(df_test['date']),
            'actual': np.array(y_test_linear_actual).astype(float),
            'prediction': np.array(y_pred_test).astype(float)
        }).fillna(0.0)
        
        save_test_data_to_csv(test_results_df, f"{platform}_{model_name}")
        save_plot(test_results_df, f"{platform} - {model_name}", 'TEST_VS_ACTUAL')
    except Exception as e:
        logging.error(f"[{platform} - {model_name}] Test save/plot failed: {e}")

    # Build forecast_plot_df with aligned lengths: history_dates + future_dates
    try:
        # --- Forecast CSV ---
        future_dates = pd.date_range(start=df_test['date'].max() + pd.Timedelta(days=1), periods=forecast_horizon, freq='D')
        forecast_df_to_save = pd.DataFrame({
            'date': future_dates,
            'forecasted_gross_revenue': np.nan_to_num(forecast_90_days, nan=0.0)
        })

        csv_file_name = f"{platform.lower()}_{model_name.lower()}_forecast_{forecast_horizon}_days.csv"
        csv_path = os.path.join('app/Analytics/csv_files', csv_file_name)
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        forecast_df_to_save.to_csv(csv_path, index=False)
        logging.info(f"✅ Saved CSV: {csv_file_name}")

        # --- Forecast Plot (History + Future) ---
        history_dates = pd.concat([df_train['date'], df_test['date']]).reset_index(drop=True)
        history_values_log = pd.concat([df_train[target_col], df_test[target_col]]).reset_index(drop=True)
        history_values_linear = np.expm1(history_values_log.values)

        future_dates_series = pd.Series(future_dates)
        
        full_dates = pd.concat([history_dates, future_dates_series], ignore_index=True)
        full_history = np.concatenate([history_values_linear, np.repeat(np.nan, forecast_horizon)])
        full_forecast = np.concatenate([np.repeat(np.nan, len(history_values_log)), np.nan_to_num(forecast_90_days, nan=0.0)])

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
