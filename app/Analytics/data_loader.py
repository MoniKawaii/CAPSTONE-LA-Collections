# Analytics/Predictive_Modeling/data_loader.py

import sys
import os
import pandas as pd
from supabase import create_client, Client
from supabase.client import ClientOptions
import json

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..')) 

# Add the project root to the system path
if project_root not in sys.path:
    sys.path.append(project_root)

# Import the config file (assuming it contains SUPABASE_URL and SUPABASE_KEY)
try:
    from config import SUPABASE_URL, SUPABASE_KEY
except ImportError:
    print("Error: config.py not found or missing SUPABASE_URL/SUPABASE_KEY.")
    sys.exit(1)

MAX_ROWS = 50000

def get_supabase_client() -> Client:
    """Initializes and returns the Supabase client."""
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY, options=ClientOptions(schema="la_collections",))
    
    return supabase

def _process_dataframe(data: list) -> pd.DataFrame:
    """Standard processing steps for data frames returned by Supabase RPCs."""
    df = pd.DataFrame(data)

    if df.empty:
        print("Warning: RPC returned an empty dataset. Check the function output in the SQL Editor.")
        return pd.DataFrame() 

    # Convert the BOOLEAN columns (returned from RPC) to integer (0 or 1) 
    # for compatibility with XGBoost
    for col in ['is_mega_sale_day', 'is_payday']:
        if col in df.columns:
            # Downcasting is safe here as it converts boolean True/False to 1/0
            df[col] = df[col].astype(int) 

    # CRITICAL FIX: Convert 'date' to datetime and set as index
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').reset_index(drop=True)
    
    return df

# --- ORIGINAL FUNCTION (FOR PREDICTIVE MODELING) ---
def load_base_sales_data(start_date='2020-09-19'):
    """
    Executes the PostgreSQL RPC function to retrieve joined sales data for the predictive model.
    (UPDATED to use get_factor_analysis_data which includes pricing features)
    """
    
    supabase = get_supabase_client()
    
    try:
        # CRITICAL CHANGE: Call the new, feature-rich function
        print("Executing Supabase RPC function: get_factor_analysis_data (for model)...")
        response = (
            supabase.rpc(
            'get_factor_analysis_data',  
            {'start_date_param': start_date}
            )
            .range(0, MAX_ROWS - 1)
            .execute()
            )
        print("[DEBUG] Raw RPC keys:", response.data[0].keys())

        data = response.data
        
    except Exception as e:
        print(f"Supabase Query Error: {e}")
        return pd.DataFrame() 
        
    df = _process_dataframe(data)
    
    print(f"[SUCCESS] Predictive model data loading complete. Loaded {len(df)} rows.")
    return df

# --- NEW FUNCTION (FOR DESCRIPTIVE ANALYSIS / EXECUTIVE SUMMARY) ---
def load_descriptive_analysis_data(start_date='2020-09-19'):
    """
    Executes the PostgreSQL RPC function to retrieve detailed sales data for descriptive analysis.
    """
    
    supabase = get_supabase_client()
    
    try:
        print("Executing Supabase RPC function: get_descriptive_analysis_data (for analysis)...")
        response = (
            supabase.rpc(
            'get_descriptive_analysis_data', 
            {'start_date_param': start_date}
            )
            .range(0, MAX_ROWS - 1)
            .execute()
            )

        data = response.data
        
    except Exception as e:
        print(f"Supabase Query Error: {e}")
        return pd.DataFrame() 
        
    df = _process_dataframe(data)
    
    print(f"[SUCCESS] Descriptive analysis data loading complete. Loaded {len(df)} rows.")
    return df