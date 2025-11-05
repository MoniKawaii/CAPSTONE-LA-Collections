# Analytics/Predictive_Modeling/data_loader.py
import sys
import os
import pandas as pd
import psycopg2

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..', '..')) 

# Add the project root to the system path
if project_root not in sys.path:
    sys.path.append(project_root)

# Now, the normal import should work
from config import DB_USER, DB_PASSWORD, DB_HOST, DB_NAME, DB_PORT

def get_db_connection():
    """Establishes and returns a database connection to Supabase (PostgreSQL)."""
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )
    return conn

def load_base_sales_data(start_date='2020-11-07'):
    """Extracts Fact Sales Aggregate and key dimension attributes."""
    conn = get_db_connection()
    
    query = f"""
    SELECT 
        dt.date, 
        fsa.platform_key, 
        dp.platform_name,
        fsa.gross_revenue, 
        fsa.total_items_sold,
        dt.is_mega_sale_day, 
        dt.is_payday
    FROM fact_sales_aggregate fsa
    JOIN dim_time dt ON fsa.time_key = dt.time_key
    JOIN dim_platform dp ON fsa.platform_key = dp.platform_key
    WHERE dt.date >= '{start_date}'
    ORDER BY fsa.platform_key, dt.date;
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    # Clean up and set index
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    
    return df