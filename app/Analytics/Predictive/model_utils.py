# Analytics/Predictive_Modeling/model_utils.py
import pandas as pd
import joblib
from prophet import Prophet
from sklearn.metrics import mean_absolute_percentage_error

# --- PERSISTENCE UTILITIES ---

def save_model(model, filepath):
    """Saves the trained model to disk."""
    joblib.dump(model, filepath)
    print(f"Model successfully saved to {filepath}")

def load_model(filepath):
    """Loads a trained model from disk."""
    return joblib.load(filepath)

# --- EVALUATION UTILITY ---

def evaluate_model(y_true, y_pred, model_name="Model"):
    """Calculates Mean Absolute Percentage Error (MAPE)."""
    mape = mean_absolute_percentage_error(y_true, y_pred)
    print(f"{model_name} MAPE: {mape:.4f}")
    return mape

# --- PROPHET UTILITIES ---

def train_prophet_model(df, platform_name):
    """Initializes and trains a Prophet model for a specific platform."""
    
    platform_df = df[df['platform_name'] == platform_name].copy()
    prophet_data = platform_df.reset_index()[['date', 'gross_revenue', 'is_mega_sale_day', 'is_payday']].rename(
        columns={'date': 'ds', 'gross_revenue': 'y'}
    )
    
    m = Prophet(
        seasonality_mode='multiplicative',
        yearly_seasonality=True, 
        weekly_seasonality=True
    )
    
    # Add key features from DIM_TIME as extra regressors
    m.add_regressor('is_mega_sale_day')
    m.add_regressor('is_payday')
    
    m.fit(prophet_data)
    return m, prophet_data

def generate_prophet_forecast(model, historical_df, platform_name, horizon=90):
    """Generates the future dataframe and runs the prediction."""
    
    future = model.make_future_dataframe(periods=horizon)
    
    # Get the regressor data (is_mega_sale_day, is_payday)
    platform_hist = historical_df[historical_df['platform_name'] == platform_name].reset_index()
    
    # Merge future dates with the regressors
    future_df = pd.merge(
        future, 
        platform_hist[['date', 'is_mega_sale_day', 'is_payday']].rename(columns={'date': 'ds'}), 
        on='ds', 
        how='left'
    )
    
    # CRITICAL: Fill future mega sales/paydays. Assuming 0 unless explicitly known.
    future_df[['is_mega_sale_day', 'is_payday']] = future_df[['is_mega_sale_day', 'is_payday']].fillna(0)
    
    # Predict
    forecast = model.predict(future_df)
    
    # Format output
    forecast_result = forecast[['ds', 'yhat']].rename(columns={'ds': 'date', 'yhat': 'predicted_gross_revenue_prophet'})
    forecast_result['platform_name'] = platform_name
    return forecast_result