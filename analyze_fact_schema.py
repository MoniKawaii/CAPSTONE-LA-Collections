import pandas as pd
import numpy as np

def analyze_fact_orders_schema():
    """Analyze fact_orders table for PostgreSQL schema creation"""
    
    # Load the fact_orders table
    df = pd.read_csv('app/Transformed/fact_orders.csv')
    
    print('=== FACT_ORDERS TABLE SCHEMA ANALYSIS ===')
    print(f'Total records: {len(df)}')
    print(f'Total columns: {len(df.columns)}')
    print()
    
    print('Column names and types:')
    for col in df.columns:
        dtype = df[col].dtype
        null_count = df[col].isnull().sum()
        null_pct = (null_count / len(df)) * 100
        
        # Sample non-null values
        non_null_sample = df[col].dropna().head(3).tolist()
        
        print(f'{col}:')
        print(f'  - dtype: {dtype}')
        print(f'  - nulls: {null_count} ({null_pct:.1f}%)')
        print(f'  - sample: {non_null_sample}')
        print()
    
    print('=== DATA RANGES FOR NUMERIC COLUMNS ===')
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        if not df[col].isnull().all():
            print(f'{col}:')
            print(f'  - min: {df[col].min()}')
            print(f'  - max: {df[col].max()}')
            print(f'  - mean: {df[col].mean():.2f}')
            print()
    
    print('=== STRING COLUMN MAX LENGTHS ===')
    string_cols = df.select_dtypes(include=['object']).columns
    for col in string_cols:
        if not df[col].isnull().all():
            max_len = df[col].astype(str).str.len().max()
            avg_len = df[col].astype(str).str.len().mean()
            print(f'{col}: max_length={max_len}, avg_length={avg_len:.1f}')

if __name__ == "__main__":
    analyze_fact_orders_schema()