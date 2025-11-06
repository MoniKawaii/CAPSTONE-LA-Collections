# Analytics/Predictive_Modeling/data_loader.py (UPDATED)

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

# Now, the normal import should work
from config import SUPABASE_URL, SUPABASE_KEY

MAX_ROWS = 35641

def get_supabase_client() -> Client:
    """Initializes and returns the Supabase client."""
    # Ensure you have 'pip install supabase'
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY, options=ClientOptions(schema="la_collections",))
    
    return supabase

def _process_dataframe(data: list) -> pd.DataFrame:
    """Standard processing steps for data frames returned by Supabase RPCs."""
    df = pd.DataFrame(data)

    if df.empty:
        print("Warning: RPC returned an empty dataset. Check the function output in the SQL Editor.")
        return pd.DataFrame() 

    # Convert the BOOLEAN columns (returned from RPC) to integer (0 or 1) 
    # for compatibility with Python analysis/model feature expectations.
    df['is_mega_sale_day'] = df['is_mega_sale_day'].astype(int)
    df['is_payday'] = df['is_payday'].astype(int)

    # Final cleanup and indexing
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    
    # Sort by platform and date for time-series feature creation
    df = df.sort_values(by=['platform_key', 'date'])
    
    return df

# --- ORIGINAL FUNCTION (FOR PREDICTIVE MODELING) ---
def load_base_sales_data(start_date='2020-11-07'):
    """
    Executes the PostgreSQL RPC function to retrieve joined sales data for the predictive model.
    """
    
    supabase = get_supabase_client()
    
    try:
        print("Executing Supabase RPC function: get_sales_forecast_data (for model)...")
        response = (
            supabase.rpc(
            'get_sales_forecast_data', 
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
    
    print(f"[SUCCESS] Predictive model data loading complete. Loaded {len(df)} rows.")
    return df

# --- NEW FUNCTION (FOR DESCRIPTIVE ANALYSIS / EXECUTIVE SUMMARY) ---
def load_descriptive_analysis_data(start_date='2020-11-07'):
    """
    Executes the PostgreSQL RPC function to retrieve detailed sales data for descriptive analysis.
    
    This uses a separate RPC function that returns additional columns (total_discount, total_orders).
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