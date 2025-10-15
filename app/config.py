"""
LA Collections Configuration
Database schemas, API configurations, and empty DataFrames
"""

import pandas as pd
import os
import json
from datetime import datetime

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("Warning: python-dotenv not installed. Environment variables from .env file will not be loaded automatically.")

# =============================================================================
# KEYS & ACCESS TOKENS
# =============================================================================

def load_lazada_tokens():
    """
    Load Lazada tokens and credentials exclusively from environment variables
    (e.g., from GitHub Secrets or a local .env file).
    
    Returns:
        dict: Lazada tokens and app credentials
    """
    
    # NOTE: The local file reading logic (os.path.join, json.load) IS REMOVED.
    
    # 1. Load dynamic tokens (these will come from your rotation script / secrets)
    access_token = os.getenv("LAZADA_ACCESS_TOKEN", "")
    refresh_token = os.getenv("LAZADA_REFRESH_TOKEN", "")
    
    # 2. Load metadata fields (use int() for numeric fields)
    # The default value should be 604800 (1 week) as per original config, but 
    # reading from ENV allows your rotation script to update it.
    try:
        expires_in = int(os.getenv("LAZADA_EXPIRES_IN", 604800))
    except ValueError:
        expires_in = 604800 # Fallback to default if ENV var is invalid
        
    account_platform = os.getenv("LAZADA_ACCOUNT_PLATFORM", "seller_center")
    created_at = os.getenv("LAZADA_CREATED_AT", None) # Timestamp of when token was acquired
    
    # 3. Load static credentials
    app_key = os.getenv("LAZADA_APP_KEY", "")
    app_secret = os.getenv("LAZADA_APP_SECRET", "")
    
    # 4. Construct the final tokens dictionary
    tokens = {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "expires_in": expires_in,
        "account_platform": account_platform,
        "created_at": created_at,
        "app_key": app_key,
        "app_secret": app_secret
    }
    
    return tokens

# Load LAZADA tokens
LAZADA_TOKENS = load_lazada_tokens()

# =============================================================================
# ENV CONNECTIONS
# =============================================================================

DATABASE_URL = os.getenv("DATABASE_URL", os.getenv("SUPABASE_DB_URL", "postgresql://user:password@localhost:5432/la_collections"))
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", os.getenv("SUPABASE_ANON_KEY", ""))
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")

# Next.js frontend environment variables
NEXT_PUBLIC_SUPABASE_URL = os.getenv("NEXT_PUBLIC_SUPABASE_URL", "")
NEXT_PUBLIC_SUPABASE_ANON_KEY = os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY", "")

# Additional environment variables for database components
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "la_collections")
DB_USER = os.getenv("DB_USER", "user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")

# Shopee API credentials
SHOPEE_PARTNER_ID = os.getenv("SHOPEE_PARTNER_ID", "")
SHOPEE_PARTNER_KEY = os.getenv("SHOPEE_PARTNER_KEY", "")
SHOPEE_ACCESS_TOKEN = os.getenv("SHOPEE_ACCESS_TOKEN", "")
SHOPEE_REFRESH_TOKEN = os.getenv("SHOPEE_REFRESH_TOKEN", "")
SHOPEE_SHOP_ID = os.getenv("SHOPEE_SHOP_ID", "")

# Application environment
APP_ENV = os.getenv("APP_ENV", "development")
DEBUG = os.getenv("DEBUG", "True").lower() in ("true", "1", "yes")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# =============================================================================
# BASE URLS FOR API CALLING
# =============================================================================

LAZADA_API_URL = "https://api.lazada.com.ph/rest"
LAZADA_AUTH_URL = "https://auth.lazada.com/oauth/authorize"
SHOPEE_API_URL = "https://partner.shopeemobile.com"

# =============================================================================
# TABLE STRUCTURE - DIMENSIONAL TABLES (Based on LA_Collections_Schema.sql)
# =============================================================================

# Platform Dimension
DIM_PLATFORM_COLUMNS = [
    'platform_key', 'platform_name', 'platform_region'
]

# Time Dimension
DIM_TIME_COLUMNS = [
    'time_key', 'date', 'year', 'quarter', 'month', 'month_name', 
    'week', 'day', 'day_of_week', 'day_of_the_year', 'is_weekend', 
    'is_payday', 'is_mega_sale_day'
]

# Customer Dimension
DIM_CUSTOMER_COLUMNS = [
    'customer_key', 'platform_customer_id', 'customer_city', 
    'buyer_segment', 'total_orders', 'customer_since', 
    'last_order_date', 'platform_key'
]

# Product Dimension
DIM_PRODUCT_COLUMNS = [
    'product_key', 'product_item_id', 'product_name', 'product_sku_base',
    'product_category', 'product_status', 'product_price', 
    'product_rating', 'platform_key'
]

# Product Variant Dimension
DIM_PRODUCT_VARIANT_COLUMNS = [
    'product_variant_key', 'product_key', 'platform_sku_id', 'variant_sku',
    'variant_attribute_1', 'variant_attribute_2', 'variant_attribute_3', 'platform_key'
]

# Order Dimension
DIM_ORDER_COLUMNS = [
    'order_key', 'platform_order_id', 'order_status', 'order_date',
    'updated_at', 'price_total', 'total_item_count', 'payment_method',
    'shipping_city', 'platform_key'
]

# =============================================================================
# TABLE STRUCTURE - FACT TABLES (Based on LA_Collections_Schema.sql)
# =============================================================================

# Fact Orders Table
FACT_ORDERS_COLUMNS = [
    'order_item_key', 'order_key', 'product_key', 'product_variant_key', 'time_key',
    'customer_key', 'platform_key', 'item_quantity', 'paid_price',
    'original_unit_price', 'voucher_platform_amount', 'voucher_seller_amount',
    'shipping_fee_paid_by_buyer'
]

# Fact Traffic Table
FACT_TRAFFIC_COLUMNS = [
    'traffic_event_key', 'time_key', 'platform_key', 'clicks', 'impressions'
]

# Fact Sales Aggregate Table
FACT_SALES_AGGREGATE_COLUMNS = [
    'time_key', 'platform_key', 'customer_key', 'product_key',
    'total_orders', 'total_items_sold', 'gross_revenue', 'total_discounts',
    'net_sales', 'shipping_revenue'
]

# =============================================================================
# EMPTY DATAFRAMES
# =============================================================================

# Dimensional Tables
EMPTY_DIM_PLATFORM = pd.DataFrame(columns=DIM_PLATFORM_COLUMNS)
EMPTY_DIM_TIME = pd.DataFrame(columns=DIM_TIME_COLUMNS)
EMPTY_DIM_CUSTOMER = pd.DataFrame(columns=DIM_CUSTOMER_COLUMNS)
EMPTY_DIM_PRODUCT = pd.DataFrame(columns=DIM_PRODUCT_COLUMNS)
EMPTY_DIM_PRODUCT_VARIANT = pd.DataFrame(columns=DIM_PRODUCT_VARIANT_COLUMNS)
EMPTY_DIM_ORDER = pd.DataFrame(columns=DIM_ORDER_COLUMNS)

# Fact Tables
EMPTY_FACT_ORDERS = pd.DataFrame(columns=FACT_ORDERS_COLUMNS)
EMPTY_FACT_TRAFFIC = pd.DataFrame(columns=FACT_TRAFFIC_COLUMNS)
EMPTY_FACT_SALES_AGGREGATE = pd.DataFrame(columns=FACT_SALES_AGGREGATE_COLUMNS)

# =============================================================================
# ALL EMPTY DATAFRAMES DICTIONARY
# =============================================================================

EMPTY_DATAFRAMES = {
    # Dimensional Tables
    'dim_platform': EMPTY_DIM_PLATFORM,
    'dim_time': EMPTY_DIM_TIME,
    'dim_customer': EMPTY_DIM_CUSTOMER,
    'dim_product': EMPTY_DIM_PRODUCT,
    'dim_product_variant': EMPTY_DIM_PRODUCT_VARIANT,
    'dim_order': EMPTY_DIM_ORDER,
    
    # Fact Tables
    'fact_orders': EMPTY_FACT_ORDERS,
    'fact_traffic': EMPTY_FACT_TRAFFIC,
    'fact_sales_aggregate': EMPTY_FACT_SALES_AGGREGATE
}

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_empty_dataframe(table_name):
    """
    Get an empty DataFrame for a specific table
    
    Args:
        table_name (str): Name of the table
        
    Returns:
        pd.DataFrame: Empty DataFrame with the table structure
    """
    return EMPTY_DATAFRAMES.get(table_name, pd.DataFrame()).copy()

def get_all_table_names():
    """
    Get list of all available table names
    
    Returns:
        list: List of table names
    """
    return list(EMPTY_DATAFRAMES.keys())

def get_table_columns(table_name):
    """
    Get column names for a specific table
    
    Args:
        table_name (str): Name of the table
        
    Returns:
        list: List of column names
    """
    df = get_empty_dataframe(table_name)
    return df.columns.tolist()

def create_sample_time_data(start_date='2024-01-01', periods=365):
    """
    Create sample time dimension data based on LA_Collections_Schema
    
    Args:
        start_date (str): Start date in YYYY-MM-DD format
        periods (int): Number of days to generate
        
    Returns:
        pd.DataFrame: Time dimension DataFrame with sample data
    """
    df = get_empty_dataframe('dim_time')
    dates = pd.date_range(start_date, periods=periods)
    
    for i, date in enumerate(dates):
        time_key = int(date.strftime('%Y%m%d'))
        quarter = f"Q{(date.month - 1) // 3 + 1}"
        
        # Determine if it's payday (15th and last day of month)
        is_payday = date.day == 15 or date.day == date.days_in_month
        
        # Determine if it's mega sale day (11.11, 12.12, etc.)
        is_mega_sale = (date.month == 11 and date.day == 11) or \
                      (date.month == 12 and date.day == 12) or \
                      (date.month == 3 and date.day == 3) or \
                      (date.month == 6 and date.day == 6) or \
                      (date.month == 9 and date.day == 9)
        
        df.loc[i] = [
            time_key,                    # time_key
            date.date(),                 # date (convert to date only, not datetime)
            date.year,                   # year
            quarter,                     # quarter
            date.month,                  # month
            date.strftime('%B'),         # month_name
            date.isocalendar()[1],       # week
            date.day,                    # day
            date.dayofweek,             # day_of_week
            date.dayofyear,             # day_of_the_year
            date.dayofweek >= 5,        # is_weekend
            is_payday,                  # is_payday
            is_mega_sale                # is_mega_sale_day
        ]
    
    return df

def validate_config():
    """
    Validate the configuration settings
    
    Returns:
        dict: Validation results
    """
    results = {
        'tokens_valid': bool(LAZADA_TOKENS.get('access_token')),
        'dataframes_created': len(EMPTY_DATAFRAMES),
        'table_count': {
            'dimensional': 5,  # dim_platform, dim_time, dim_customer, dim_product, dim_order
            'fact': 3,         # fact_orders, fact_traffic, fact_sales_aggregate
            'total': len(EMPTY_DATAFRAMES)
        },
        'api_urls_configured': bool(LAZADA_API_URL and SHOPEE_API_URL),
        'schema_compliance': 'LA_Collections_Schema.sql',
        'environment_config': {
            'app_env': APP_ENV,
            'debug_mode': DEBUG,
            'database_configured': bool(DATABASE_URL and "postgresql://" in DATABASE_URL and DATABASE_URL != "postgresql://user:password@localhost:5432/la_collections"),
            'supabase_configured': bool(SUPABASE_URL and SUPABASE_KEY),
            'supabase_service_role_configured': bool(SUPABASE_SERVICE_ROLE_KEY),
            'next_public_configured': bool(NEXT_PUBLIC_SUPABASE_URL and NEXT_PUBLIC_SUPABASE_ANON_KEY),
            'shopee_configured': bool(SHOPEE_PARTNER_ID and SHOPEE_PARTNER_KEY),
            'lazada_tokens_loaded': bool(LAZADA_TOKENS.get('access_token')),
            'lazada_app_configured': bool(LAZADA_TOKENS.get('app_key') and LAZADA_TOKENS.get('app_secret'))
        }
    }
    return results

# =============================================================================
# LAZADA TO UNIFIED FIELD MAPPINGS
# =============================================================================

LAZADA_TO_UNIFIED_MAPPING = {
    # --- Dim_Product (Sources from root object) ---
    "item_id": "product_item_id",
    "attributes.name": "product_name",
    "primary_category": "product_category", # RAW FIELD: Used as input for get_category_name() function
    "statuses[]": "product_status", # gets the statuses at 0 index
    "skus[].price": "product_price",      # DERIVED: Base price calculated via extract_price_from_skus()
    "skus[].SellerSku": "product_sku_base", # DERIVED: Base SKU extracted via extract_base_sku()
    # product_rating is not currently mapped from source and is set to None. SO have it NULL for now
    "platform_key": "platform_key",
    
    # --- Dim_Product_Variant (Sources from skus[] array object) ---
    "skus[].SkuId": "platform_sku_id",
    "skus[].SellerSku": "variant_sku",
    "skus[].price": "variant_price", # NOTE: This data is extracted but NOT saved to dim_product_variant per new schema
    "skus[].saleProp": "variant_attribute_1_2_3", # DERIVED: Extracted from values in saleProp map
    "skus[].Variation1": "variant_attribute_1",  # FALLBACK: Used if saleProp is empty
    
    # --- Dim_Order ---
    "id": "order_key",  # not pulled from API, generated internally incremental
    "order_id": "platform_order_id", 
    "statuses": "order_status",  # Get status in index 0 for the order status
    "created_at": "order_date",  # Convert to date only (YYYY-MM-DD)
    "updated_at": "updated_at",  # Convert to date only (YYYY-MM-DD)  
    "price": "price_total",
    "items_count": "total_item_count",
    
    # --- Dim_Customer ---
    "customer_key": "customer_key",  # Generated internally incremental
    "platform_customer_id": "platform_customer_id",  # Generated: 'LZ' + first_char + last_char of first_name + first2_phone + last2_phone
    "address_shipping.city": "customer_city",  # From address_shipping.city
    "address_shipping.phone": "customer_phone",  # From address_shipping.phone (for platform_customer_id generation)
    "buyer_segment": "buyer_segment",  # Calculated: 'New Buyer' or 'Returning Buyer'
    "total_orders": "total_orders",  # Calculated: Count of orders per platform_customer_id
    "customer_since": "customer_since",  # Calculated: Earliest order_date for platform_customer_id
    "last_order_date": "last_order_date",  # Calculated: Latest order_date for platform_customer_id
    "platform_key": "platform_key",  # Always 1 for Lazada
    
    # --- Fact_Orders ---
    "order_item_key": "order_item_key",  # Generated internally incremental
    "order_key": "order_key",  # Foreign key to dim_order connected by order_id
    "product_key": "product_key",  # Foreign key to dim_product
    "product_variant_key": "product_variant_key",  # Foreign key to dim_product_variant
    "time_key": "time_key",  # Foreign key to dim_time based on order_date
    "customer_key": "customer_key",  # Foreign key to dim_customer
    "platform_key": "platform_key",  # Always 1 for Lazada
    "item_quantity": "item_quantity",  # From order_items[].quantity
    "paid_price": "paid_price",  # From order_items[].paid_price or
    "price": "original_unit_price",  # From order_items[].price
    "voucher_platform": "voucher_platform_amount",  # From order_items[].voucher_platform
    "voucher_selleramount": "voucher_seller_amount",  # From order_items[].voucher_seller
    "shipping_fee": "shipping_fee_paid_by_buyer",  # From order_items[].shipping_fee 
    
}

# =============================================================================
# DATA TYPE MAPPINGS
# =============================================================================

COLUMN_DATA_TYPES = {
    'dim_platform': {
        'platform_key': 'int',
        'platform_name': 'str',
        'platform_region': 'str'
    },
    'dim_time': {
        'time_key': 'int',
        'date': 'datetime64[D]',  # Date only, not datetime
        'year': 'int',
        'quarter': 'str',
        'month': 'int',
        'month_name': 'str',
        'week': 'int',
        'day': 'int',
        'day_of_week': 'int',
        'day_of_the_year': 'int',
        'is_weekend': 'bool',
        'is_payday': 'bool',
        'is_mega_sale_day': 'bool'
    },
    'dim_customer': {
        'customer_key': 'float64',  # Changed to float64 to handle NaN for empty DataFrame
        'platform_customer_id': 'str',
        'customer_city': 'str',
        'buyer_segment': 'str',
        'total_orders': 'int',
        'customer_since': 'datetime64[D]',  # Date only, not datetime
        'last_order_date': 'datetime64[D]',  # Date only, not datetime
        'platform_key': 'int'
    },
    'dim_product': {
        'product_key': 'float64',  # Changed to float64 to handle NaN for empty DataFrame
        'product_item_id': 'str',
        'product_name': 'str',
        'product_sku_base': 'str',
        'product_category': 'str',
        'product_status': 'str',
        'product_price': 'float64',  # Decimal equivalent in pandas
        'product_rating': 'float64',  # Decimal equivalent in pandas
        'platform_key': 'int'
    },
    'dim_product_variant': {
        'product_variant_key': 'float64',  # Changed to float64 to handle NaN for empty DataFrame
        'product_key': 'float64',  # Changed to float64 to match the .1 suffix pattern
        'platform_sku_id': 'str',
        'variant_sku': 'str',
        'variant_attribute_1': 'str',
        'variant_attribute_2': 'str',
        'variant_attribute_3': 'str',
        'platform_key': 'int'
    },
    'dim_order': {
        'order_key': 'float64',  # Changed to float64 to handle NaN for empty DataFrame    
        'platform_order_id': 'str',
        'order_status': 'str',
        'order_date': 'datetime64[D]',  # Date only, not datetime
        'updated_at': 'datetime64[D]',  # Date only, not datetime
        'price_total': 'float64',  # Decimal equivalent in pandas
        'total_item_count': 'int',
        'payment_method': 'str',
        'shipping_city': 'str',
        'platform_key': 'int'
    },
    'fact_orders': {
        'order_item_key': 'float64',  # Changed to float64 to handle NaN for empty DataFrame
        'order_key': 'float64',
        'product_key': 'float64',
        'product_variant_key': 'float64',
        'time_key': 'int',
        'customer_key': 'float64',
        'platform_key': 'int',
        'item_quantity': 'int',
        'paid_price': 'float64',  # Decimal equivalent in pandas
        'original_unit_price': 'float64',  # Decimal equivalent in pandas
        'voucher_platform_amount': 'float64',  # Decimal equivalent in pandas
        'voucher_seller_amount': 'float64',  # Decimal equivalent in pandas
        'shipping_fee_paid_by_buyer': 'float64'  # Decimal equivalent in pandas
    },
    'fact_traffic': {
        'traffic_event_key': 'float64',  # Bigint equivalent in pandas
        'time_key': 'int',
        'platform_key': 'int',
        'clicks': 'int',
        'impressions': 'int'
    },
    'fact_sales_aggregate': {
        'time_key': 'int',
        'platform_key': 'int',
        'customer_key': 'float64',
        'product_key': 'float64', 
        'total_orders': 'int',
        'total_items_sold': 'int',
        'gross_revenue': 'float64',  # Decimal equivalent in pandas
        'total_discounts': 'float64',  # Decimal equivalent in pandas
        'net_sales': 'float64',  # Decimal equivalent in pandas
        'shipping_revenue': 'float64'  # Decimal equivalent in pandas
    }
}

def apply_data_types(df, table_name):
    """
    Apply appropriate data types to DataFrame columns matching the SQL schema exactly
    
    Args:
        df (pd.DataFrame): DataFrame to apply types to
        table_name (str): Name of the table
        
    Returns:
        pd.DataFrame: DataFrame with proper data types
    """
    if table_name in COLUMN_DATA_TYPES:
        for col, dtype in COLUMN_DATA_TYPES[table_name].items():
            if col in df.columns:
                try:
                    # Special handling for date columns (convert from datetime to date)
                    if dtype == 'datetime64[D]':
                        # First convert to datetime if it's not already
                        df[col] = pd.to_datetime(df[col])
                        # Then convert to date only (removes time component)
                        df[col] = df[col].dt.date
                        # Convert back to datetime64[D] for pandas compatibility
                        df[col] = pd.to_datetime(df[col]).dt.date
                    else:
                        df[col] = df[col].astype(dtype)
                except (ValueError, TypeError) as e:
                    print(f"Warning: Could not convert column '{col}' to type '{dtype}': {e}")
                    pass  # Skip if conversion fails
    return df

if __name__ == "__main__":
    # Test the configuration
    print("LA Collections Configuration - Schema Compliant")
    print("=" * 60)
    
    validation = validate_config()
    print(f"Tokens Valid: {validation['tokens_valid']}")
    print(f"DataFrames Created: {validation['dataframes_created']}")
    print(f"API URLs Configured: {validation['api_urls_configured']}")
    print(f"Schema Compliance: {validation['schema_compliance']}")
    print(f"Table Count: {validation['table_count']}")
    
    print(f"\nEnvironment Configuration:")
    env_config = validation['environment_config']
    print(f"  App Environment: {env_config['app_env']}")
    print(f"  Debug Mode: {env_config['debug_mode']}")
    print(f"  Database Configured: {env_config['database_configured']}")
    print(f"  Supabase Configured: {env_config['supabase_configured']}")
    print(f"  Supabase Service Role: {env_config['supabase_service_role_configured']}")
    print(f"  Next.js Frontend: {env_config['next_public_configured']}")
    print(f"  Shopee API Configured: {env_config['shopee_configured']}")
    print(f"  Lazada Tokens Loaded: {env_config['lazada_tokens_loaded']}")
    print(f"  Lazada App Configured: {env_config['lazada_app_configured']}")
    
    print("\nAvailable Tables (LA_Collections_Schema.sql):")
    for table_name in get_all_table_names():
        columns = get_table_columns(table_name)
        print(f"  {table_name}: {len(columns)} columns")
    
    print("\nSample Time Dimension (first 3 rows):")
    sample_time = create_sample_time_data(periods=3)
    print(sample_time[['time_key', 'date', 'quarter', 'month_name', 'is_weekend', 'is_payday', 'is_mega_sale_day']])


