import pandas as pd
from app.supabase_client import supabase
from app.mappings import MAPPINGS

def transform(df: pd.DataFrame, platform: str) -> pd.DataFrame:
    mapping = MAPPINGS.get(platform)
    if not mapping:
        raise ValueError(f"No mapping defined for platform: {platform}")

    df = df.rename(columns=mapping)
    df = df[list(mapping.values())]
    df["platform"] = platform
    return df

def load(df: pd.DataFrame, table: str = "ecommerce_metrics"):
    records = df.to_dict(orient="records")
    response = supabase.table(table).insert(records).execute()
    return response

def process_csv_file(file, platform: str, table: str = "ecommerce_metrics"):
    """
    Takes an uploaded CSV file, transforms it for the platform,
    and loads it into Supabase.
    """
    try:
        # Read CSV into DataFrame
        df = pd.read_csv(file)

        # Transform according to platform
        df = transform(df, platform)

        # Load into Supabase
        response = load(df, table)

        return {"status": "success", "inserted": len(df), "supabase_response": response}

    except Exception as e:
        return {"status": "error", "detail": str(e)}
