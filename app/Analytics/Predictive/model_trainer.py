# app/Analytics/Predictive/model_trainer.py

import os
import sys
import logging
import warnings
from datetime import datetime

import numpy as np
import pandas as pd
import joblib

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

    # For full horizon
    # full_last_season = last_season
    # forecast_90_days = np.tile(full_last_season, (horizon // season_length) + 1)[:horizon]
    forecast_90_days = np.tile(last_season, (horizon // season_length) + 1)[:horizon]
    return test_pred, forecast_90_days


# ---- main train & forecast function ----
def train_and_forecast_model(
    df: pd.DataFrame,
    platform: str,
    model_name: str,
    target_col: str = 'gross_revenue',
    exog_cols: list = ['is_mega_sale_day', 'is_payday', 'avg_paid_price', 'avg_original_price', 'avg_discount_rate'],
    test_size_days: int = 365,          # changed per user request
    forecast_horizon: int = 90
) -> dict:
    """
    Train, evaluate, and forecast using multiple model types.
    Returns a dict with metrics for summary.
    """

    logging.info(f"--- Training {model_name} for {platform} ---")

    # Ensure date column
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)

    # Safe numeric conversion for price features
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
        if ex in df_agg.columns:
            df_agg[ex] = df_agg[ex].fillna(0)

    # --- Info summary ---
    total_days = (last_sale_date - first_sale_date).days + 1
    days_with_sales = (df_agg[target_col] > 0).sum()
    days_no_sales = total_days - days_with_sales
    pct_no_sales = (days_no_sales / total_days * 100) if total_days > 0 else 0
    logging.info(
        f"[{platform}] Period: {first_sale_date.date()} → {last_sale_date.date()} | "
        f"Total days: {total_days} | With sales: {days_with_sales} | "
        f"No sales: {days_no_sales} ({pct_no_sales:.1f}% no-sales days)"
    )

    # Fill missing exogenous values (zero for flags, mean for continuous)
    # For simplicity fill all exog cols with 0, then override continuous with mean
    """for ex in exog_cols:
        if ex in df_agg.columns:
            df_agg[ex] = df_agg[ex].fillna(0)"""

    # Train/test chronological split
    split_date = df_agg['date'].max() - pd.Timedelta(days=test_size_days - 1)
    # df_train = df_agg[df_agg['date'] < split_date].copy().reset_index(drop=True)
    # df_test = df_agg[df_agg['date'] >= split_date].copy().reset_index(drop=True)
    df_train = df_agg[df_agg['date'] < split_date].copy()
    df_test = df_agg[df_agg['date'] >= split_date].copy()

    logging.info(f"[{platform}] Data points | train: {len(df_train)} | test: {len(df_test)} (target test_days={test_size_days})")

    # default outputs in case of failures
    """mae = mse = rmse = np.inf
    y_test_actual = df_test[target_col] if target_col in df_test.columns else pd.Series(dtype=float)
    y_pred_test = pd.Series(dtype=float)
    forecast_90_days = np.repeat(0.0, forecast_horizon)"""
    mae = mse = rmse = np.inf
    y_pred_test = pd.Series(dtype=float)
    forecast_90_days = np.repeat(0.0, forecast_horizon)
    # y_test_actual = df_test[target_col]

    # ---------- XGBoost & LightGBM ----------
    if model_name in ['XGBoost', 'LightGBM']:
        try:
            df_full = build_time_series_features(df_agg, target_col, lag_periods=[7, 14, 28], rolling_window=7)
            df_train_ml = df_full[df_full['date'] < split_date]
            df_test_ml = df_full[df_full['date'] >= split_date]

            features = df_train_ml.columns.drop(
                [target_col, 'date', 'platform_key', 'platform_name', 'total_items_sold'],
                errors='ignore'
            ).tolist()

            X_all = df_train_ml[features]
            y_all = df_train_ml[target_col]
            val_size = max(1, int(len(X_all) * 0.1))
            X_train, y_train_ml = X_all.iloc[:-val_size], y_all.iloc[:-val_size]
            X_valid, y_valid_ml = X_all.iloc[-val_size:], y_all.iloc[-val_size:]
            X_test = df_test_ml[features]
            y_test_actual = df_test_ml[target_col]

            tree_estimators = 40 # Default for Lazada
            if platform == 'Shopee':
                # Shopee performed best at 300 estimators
                tree_estimators = 100

            # XGBoost (version adaptive)
            if model_name == 'XGBoost':
                import xgboost as xgb
                
                # 1. Initialize the model
                model = XGBRegressor(n_estimators=tree_estimators, learning_rate=0.05, random_state=42)

                try:
                    version_str = xgb.__version__
                    version_nums = tuple(int(v) for v in version_str.split(".")[:2])
                    logging.info(f"XGBoost Version Detected: {version_str}")

                    # Default fit parameters, assuming v3.x or v1.x standard
                    fit_params = {
                        'eval_set': [(X_valid, y_valid_ml)],
                        'verbose': False
                    }
                    
                    # Flag to track if successful fit occurred
                    fit_successful = False

                    # --- PHASE 1: Try Modern Callbacks (v2.x and v3.x+) ---
                    if version_nums >= (2, 0):
                        try:
                            # Import is localized since 'callbacks' is a feature of the modern API
                            from xgboost.callback import EarlyStopping
                            early_stop_callback = EarlyStopping(rounds=25, save_best=True)

                            fit_params['callbacks'] = [early_stop_callback]
                            
                            # Check for v2.x specific parameter name 'evals'
                            if version_nums < (3, 0):
                                # v2.x used 'evals' instead of 'eval_set'
                                fit_params['evals'] = fit_params.pop('eval_set') 
                            
                            logging.info(f"[{platform} - XGBoost] Attempting fit with 'callbacks'.")
                            model.fit(X_train, y_train_ml, **fit_params)
                            fit_successful = True

                        except (TypeError, ImportError) as e_callback:
                            # Catches your current warning: 'unexpected keyword argument 'callbacks''
                            logging.warning(f"[{platform} - XGBoost] 'callbacks' failed ({e_callback}). Falling back to 'early_stopping_rounds'.")
                            
                    # --- PHASE 2: Try Legacy early_stopping_rounds (v1.x and fallback for others) ---
                    if not fit_successful:
                        try:
                            # Re-set standard params if they were modified for v2.x 'evals'
                            fit_params = {
                                'eval_set': [(X_valid, y_valid_ml)],
                                'verbose': False,
                                'early_stopping_rounds': 25 # Legacy parameter
                            }

                            logging.info(f"[{platform} - XGBoost] Attempting fit with 'early_stopping_rounds' (legacy).")
                            model.fit(X_train, y_train_ml, **fit_params)
                            fit_successful = True

                        except TypeError as e_legacy:
                            # Catches the error if 'early_stopping_rounds' is ALSO rejected (strict v3.0+)
                            logging.warning(f"[{platform} - XGBoost] Final fallback: Early stopping not supported ({e_legacy}), fitting normally.")
                            model.fit(X_train, y_train_ml)
                            fit_successful = True # Fit occurred, just without early stopping

                except Exception as e:
                    # General catch-all for any other failure during version check or setup
                    logging.warning(f"[{platform} - XGBoost] General fitting fallback due to: {e}")
                    model.fit(X_train, y_train_ml)

            # LightGBM (fallback-safe)
            else:
                # Use platform-specific optimal estimators (50 for all tree models now)
                model = LGBMRegressor(n_estimators=tree_estimators, learning_rate=0.05, random_state=42)
                try:
                    model.fit(
                        X_train, y_train_ml,
                        eval_set=[(X_valid, y_valid_ml)],
                        # Use the common parameter name first
                        early_stopping_rounds=25
                    )
                except TypeError as e:
                    # Catches if 'early_stopping_rounds' is rejected
                    logging.warning(f"[{platform} - LightGBM] Early stopping fit failed due to: {e}. Fitting normally.")
                    model.fit(X_train, y_train_ml)
                except Exception as e:
                    logging.warning(f"[{platform} - LightGBM] fallback fit due to: {e}")
                    model.fit(X_train, y_train_ml)

            y_pred_test = pd.Series(model.predict(X_test), index=y_test_actual.index)

            # Save model
            os.makedirs("app/Analytics/models", exist_ok=True)
            joblib.dump(model, f"app/Analytics/models/{platform}_{model_name}.pkl")
            logging.info(f"💾 Saved model to app/Analytics/models/{platform}_{model_name}.pkl")

        except Exception as e:
            logging.error(f"[{platform} - {model_name}] failed: {e}")
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

            # create exog for prediction: combine exog_test then exog_future (use last-7 mean or zeros)
            future_exog_rows = []
            if len(exog_test) > 0:
                # exog_test as-is
                pass
            # compute exog_future: for each col, repeat last-7 mean
            exog_future = pd.DataFrame(index=range(forecast_horizon))
            last_agg = df_agg.tail(1)
            
            for col in exog_cols:
                if col in df_agg.columns:
                    if col in ['is_mega_sale_day', 'is_payday']:
                        # Flags: Assume no mega sale/payday in the next 90 days unless manually specified
                        exog_future[col] = np.repeat(0.0, forecast_horizon)
                    else:
                        # Price features: Use the last observed value
                        last_val = last_agg[col].iloc[0] if col in last_agg.columns and not last_agg[col].isna().all() else 0.0
                        exog_future[col] = np.repeat(last_val, forecast_horizon)
                else:
                    exog_future[col] = np.repeat(0.0, forecast_horizon)

            # now build exog_pred_all matching the prediction indices (test + forecast)
            exog_pred_all = pd.concat([exog_test.reset_index(drop=True), exog_future.reset_index(drop=True)], ignore_index=True)

            # predict for test + horizon
            pred_start = len(df_train[target_col])
            pred_end = pred_start + len(df_test[target_col]) + forecast_horizon - 1

            all_preds = model_fit.predict(start=pred_start, end=pred_end, exog=exog_pred_all)
            # slice for y_pred_test and forecast
            y_pred_test = all_preds.iloc[:len(df_test[target_col])]
            y_pred_test.index = df_test.index  # align index
            forecast_90_days = all_preds.iloc[len(df_test[target_col]):].values

        except Exception as e:
            logging.error(f"SARIMAX failed: {e}")
            return {'platform': platform, 'model': model_name, 'mae': np.inf, 'mse': np.inf, 'rmse': np.inf}

    # ---------- Prophet ----------
    elif model_name == 'Prophet':
        try:
            df_prophet_train = df_train.rename(columns={'date': 'ds', target_col: 'y'})[['ds', 'y'] + [c for c in exog_cols if c in df_train.columns]]
            df_prophet_test = df_test.rename(columns={'date': 'ds', target_col: 'y'})[['ds', 'y'] + [c for c in exog_cols if c in df_test.columns]]

            if df_prophet_train.empty:
                raise ValueError("Empty prophet training frame.")

            model = Prophet(daily_seasonality=True, weekly_seasonality=True, yearly_seasonality=False)
            for col in [c for c in exog_cols if c in df_prophet_train.columns]:
                model.add_regressor(col)

            model.fit(df_prophet_train)

            # build future frame: test rows (for in-sample test predictions) + future dates for horizon
            test_dates = df_prophet_test['ds']
            future_dates = pd.date_range(start=test_dates.max() + pd.Timedelta(days=1), periods=forecast_horizon, freq='D')
            # prepare DF for predict: keep exog for historical test, and fill exog for future using last-7 mean
            future_df = pd.concat([df_prophet_test.drop(columns='y', errors='ignore'), pd.DataFrame({'ds': future_dates})], ignore_index=True, sort=False)

            for col in [c for c in exog_cols if c in df_prophet_train.columns]:
                # Use the last observed value from the test set for imputation
                last_test_val = df_prophet_test[col].iloc[-1] if col in df_prophet_test.columns and not df_prophet_test[col].isna().all() else 0.0

                if col in ['is_mega_sale_day', 'is_payday']:
                    # Flags: Set future to 0
                    future_df.loc[future_df['ds'].isin(future_dates), col] = 0.0
                else:
                    # Price features: Use last observed value for future
                    future_df.loc[future_df['ds'].isin(future_dates), col] = last_test_val
                    
                # Fill any remaining NaNs in the historical/test portion of future_df
                future_df[col] = future_df[col].fillna(0.0)

            forecast_df = model.predict(future_df)
            # extract test predictions (matching test dates)
            y_pred_test = forecast_df.loc[forecast_df['ds'].isin(df_prophet_test['ds']), 'yhat'].reset_index(drop=True)
            # align index
            if len(y_pred_test) == len(df_prophet_test):
                y_pred_test.index = df_test.index
            else:
                # fallback: use y_test_actual's index if mismatch
                y_pred_test = pd.Series(y_pred_test.values, index=df_test.index[:len(y_pred_test)])

            # future forecasts
            forecast_90_days = forecast_df.loc[forecast_df['ds'].isin(future_dates), 'yhat'].values
            if len(forecast_90_days) != forecast_horizon:
                # fallback to repeating last y_pred_test
                forecast_90_days = np.repeat(y_pred_test.iloc[-1] if len(y_pred_test) > 0 else df_train[target_col].iloc[-1], forecast_horizon)

        except Exception as e:
            logging.error(f"Prophet failed: {e}")
            return {'platform': platform, 'model': model_name, 'mae': np.inf, 'mse': np.inf, 'rmse': np.inf}

    # ---------- SNaive ----------
    elif model_name == 'SNaive':
        try:
            season_len = min(7, max(1, len(df_train) // 2))
            y_pred_test, forecast_90_days = seasonal_naive_forecast(df_train[target_col], df_test[target_col], season_length=season_len, horizon=forecast_horizon)
        except Exception as e:
            logging.error(f"SNaive failed: {e}")
            return {'platform': platform, 'model': model_name, 'mae': np.inf, 'mse': np.inf, 'rmse': np.inf}

    # ---------- Postprocess predictions ----------
    # Convert to Series and align
    try:
        # Ensure y_test_actual is set (for models that changed it)
        if target_col in df_test.columns:
            y_test_actual = df_test[target_col]
        # Ensure y_pred_test index aligns with y_test_actual
        if isinstance(y_pred_test, pd.Series):
            # align by index if lengths match, otherwise reset index
            if len(y_pred_test) == len(y_test_actual):
                y_pred_test = y_pred_test.astype(float)
                y_pred_test.index = y_test_actual.index
            else:
                # attempt to coerce to same length
                y_pred_test = pd.Series(y_pred_test.values[:len(y_test_actual)], index=y_test_actual.index)
        else:
            y_pred_test = pd.Series(np.repeat(0.0, len(y_test_actual)), index=y_test_actual.index)
    except Exception as e:
        logging.warning(f"Postprocessing predictions failed: {e}")
        y_pred_test = pd.Series(np.repeat(0.0, len(y_test_actual)), index=y_test_actual.index)
        forecast_90_days = np.repeat(0.0, forecast_horizon)

    # Clip negative predictions to zero
    try:
        y_pred_test[y_pred_test < 0] = 0
        forecast_90_days = np.array(forecast_90_days)
        forecast_90_days[forecast_90_days < 0] = 0
    except Exception:
        pass

    # Evaluate metrics (if possible)
    try:
        mae = calculate_mae(y_test_actual, y_pred_test)
        mse = calculate_mse(y_test_actual, y_pred_test)
        rmse = calculate_rmse(y_test_actual, y_pred_test)
    except Exception as e:
        logging.warning(f"Metric calculation failed: {e}")
        mae = mse = rmse = np.inf

    # Save test CSV
    try:
        test_results_df = pd.DataFrame({
            'date': df_test['date'].values,
            'actual': y_test_actual.values,
            'prediction': y_pred_test.values
        })
        save_test_data_to_csv(test_results_df, f"{platform}_{model_name}")
    except Exception as e:
        logging.warning(f"Saving test CSV failed: {e}")

    # Build forecast_plot_df with aligned lengths: history_dates + future_dates
    try:
        history_dates = pd.concat([df_train['date'], df_test['date']]).reset_index(drop=True)
        history_values = pd.concat([df_train[target_col], df_test[target_col]]).reset_index(drop=True)

        future_dates = pd.date_range(start=df_test['date'].max() + pd.Timedelta(days=1), periods=forecast_horizon, freq='D')
        future_dates_series = pd.Series(future_dates)

        # 1. Create a DataFrame for the raw 90-day forecast
        forecast_df_to_save = pd.DataFrame({
            'date': future_dates,
            'forecasted_gross_revenue': np.array(forecast_90_days)
        })
        
        # 2. Define path and save the CSV
        csv_file_name = f"{platform.lower()}_{model_name.lower()}_forecast_{forecast_horizon}_days.csv"
        csv_path = os.path.join('app/Analytics/csv_files', csv_file_name)
        
        # Ensure the directory exists
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        
        forecast_df_to_save.to_csv(csv_path, index=False)
        logging.info(f"✅ Saved CSV: {csv_file_name}")

        full_dates = pd.concat([history_dates, future_dates_series], ignore_index=True)
        full_history = np.concatenate([history_values.values, np.repeat(np.nan, forecast_horizon)])
        full_forecast = np.concatenate([np.repeat(np.nan, len(history_values)), np.array(forecast_90_days)])

        forecast_plot_df = pd.DataFrame({
            'date': full_dates,
            'history': full_history,
            'forecast': full_forecast
        })

        # Save plots
        save_plot(test_results_df, f"{platform} - {model_name}", 'TEST_VS_ACTUAL')
        save_plot(forecast_plot_df, f"{platform} - {model_name}", '90_DAY_FORECAST')

    except Exception as e:
        logging.warning(f"Plot generation failed: {e}")

    logging.info(f"✅ {platform} {model_name} — MAE: {mae:.2f}, RMSE: {rmse:.2f}")
    return {'platform': platform, 'model': model_name, 'mae': mae, 'mse': mse, 'rmse': rmse}
