import pandas as pd
from app.supabase_client import supabase
from app.mappings import MAPPINGS
from datetime import datetime

def clean_date_format(date_str):
    """Clean and standardize date formats, exclude date ranges"""
    if pd.isna(date_str):
        return None
    
    date_str = str(date_str).strip()
    
    # Exclude date ranges (e.g., "2024-05-01~2024-05-31") - return None to filter out
    if '~' in date_str:
        return None  # This will cause the row to be excluded
    
    # Handle DD/MM/YYYY format (e.g., "01/05/2024")
    if '/' in date_str and len(date_str.split('/')) == 3:
        parts = date_str.split('/')
        if len(parts[0]) == 2:  # DD/MM/YYYY format
            day, month, year = parts
            date_str = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
    
    # Try to parse and return in YYYY-MM-DD format
    try:
        parsed_date = pd.to_datetime(date_str)
        return parsed_date.strftime('%Y-%m-%d')
    except:
        return None

def transform(df: pd.DataFrame, platform: str) -> pd.DataFrame:
    mapping = MAPPINGS.get(platform)
    if not mapping:
        raise ValueError(f"No mapping defined for platform: {platform}")

    df = df.rename(columns=mapping)
    df = df[list(mapping.values())]
    df["platform"] = platform
    
    # Clean date column if it exists
    if 'date' in df.columns:
        df['date'] = df['date'].apply(clean_date_format)
        # Remove rows with invalid dates
        df = df.dropna(subset=['date'])
    
    # Clean percentage values - remove % symbol and convert to decimal
    for col in df.columns:
        if df[col].dtype == 'object':
            # Check if column contains percentage values
            if df[col].astype(str).str.contains('%').any():
                # Remove % and convert to decimal (e.g., "9.47%" -> 0.0947)
                df[col] = df[col].astype(str).str.replace('%', '').astype(float) / 100
    
    return df

def load(df: pd.DataFrame, table: str = "ecommerce_metrics"):
    records = df.to_dict(orient="records")
    response = supabase.table(table).insert(records).execute()
    return response

def process_csv_file(file, platform: str, save_to_db: bool = False):
    """
    Takes an uploaded CSV file, transforms it for the platform,
    and optionally loads it into Supabase.
    Returns the processed DataFrame and operation details.
    """
    try:
        # Read CSV into DataFrame
        df = pd.read_csv(file)

        # Transform according to platform
        df = transform(df, platform)

        if save_to_db:
            # Load into Supabase
            response = load(df)
            return {
                "status": "success", 
                "inserted": len(df), 
                "supabase_response": response,
                "dataframe": df
            }
        else:
            # Just return the DataFrame
            return {
                "status": "success", 
                "rows_processed": len(df),
                "dataframe": df
            }

    except Exception as e:
        return {"status": "error", "detail": str(e), "dataframe": None}
