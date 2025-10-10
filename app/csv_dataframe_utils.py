"""
Helper functions to work with processed CSV data as DataFrames
"""
import pandas as pd
from app.csv_etl import process_csv_file

def get_dataframe_from_csv(file_path: str, platform: str) -> pd.DataFrame:
    """
    Process a CSV file and return the cleaned DataFrame
    
    Args:
        file_path (str): Path to the CSV file
        platform (str): Platform name ("Lazada" or "Shopee")
    
    Returns:
        pd.DataFrame: Processed DataFrame with cleaned data
    """
    try:
        with open(file_path, 'r') as file:
            result = process_csv_file(file, platform, save_to_db=False)
        
        if result["status"] == "success":
            return result["dataframe"]
        else:
            print(f"Error processing CSV: {result['detail']}")
            return pd.DataFrame()
            
    except Exception as e:
        print(f"File error: {e}")
        return pd.DataFrame()

def get_dataframe_summary(df: pd.DataFrame) -> dict:
    """
    Get summary information about the processed DataFrame
    
    Args:
        df (pd.DataFrame): The processed DataFrame
    
    Returns:
        dict: Summary information
    """
    if df.empty:
        return {"message": "DataFrame is empty"}
    
    return {
        "shape": df.shape,
        "columns": list(df.columns),
        "date_range": {
            "start": df['date'].min() if 'date' in df.columns else None,
            "end": df['date'].max() if 'date' in df.columns else None
        },
        "total_sales": df['total_sales_value'].sum() if 'total_sales_value' in df.columns else None,
        "total_orders": df['total_orders'].sum() if 'total_orders' in df.columns else None,
        "sample_data": df.head(3).to_dict('records')
    }

if __name__ == "__main__":
    # Process CSV and get DataFrame
    df = get_dataframe_from_csv("data/samplelazada.csv", "Lazada")
    
    # Get summary
    summary = get_dataframe_summary(df)
    print("DataFrame Summary:")
    print(f"Shape: {summary['shape']}")
    print(f"Date range: {summary['date_range']['start']} to {summary['date_range']['end']}")
    print(f"Total sales: ${summary['total_sales']:,.2f}")
    print(f"Total orders: {summary['total_orders']:,}")