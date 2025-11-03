"""
LA Collections Configuration
Database schemas, API configurations, and empty DataFrames
"""

import pandas as pd
import os
import json
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# =============================================================================
# KEYS & ACCESS TOKENS
# =============================================================================

def load_lazada_tokens():
    """
    Load Lazada tokens from lazada_tokens.json file
    
    Returns:
        dict: Lazada tokens and app credentials
    """
    tokens_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'lazada', 'lazada_tokens.json')
    
    # Default empty tokens
    tokens = {
        "access_token": "",
        "refresh_token": "",
        "expires_in": 604800,
        "account_platform": "seller_center",
        "created_at": None
    }
    
    # Load from JSON file if it exists
    if os.path.exists(tokens_file):
        try:
            with open(tokens_file, 'r') as f:
                file_tokens = json.load(f)
                tokens.update(file_tokens)
        except (json.JSONDecodeError, IOError) as e:
            print(f"Warning: Could not load Lazada tokens from {tokens_file}: {e}")
    
    # Add app credentials from environment variables
    tokens.update({
        "app_key": os.getenv("LAZADA_APP_KEY", ""),
        "app_secret": os.getenv("LAZADA_APP_SECRET", "")
    })
    
    return tokens

# Load LAZADA tokens
LAZADA_TOKENS = load_lazada_tokens()

def load_shopee_tokens():
    """
    Load Shopee tokens from shopee_tokens.json file
    
    Returns:
        dict: Shopee tokens and app credentials
    """
    tokens_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'tokens', 'shopee_tokens.json')
    
    # Default empty tokens
    tokens = {
        "access_token": "",
        "refresh_token": "",
        "expire_in": 14400,
        "timestamp": None
    }
    
    # Load from JSON file if it exists
    if os.path.exists(tokens_file):
        try:
            with open(tokens_file, 'r') as f:
                file_tokens = json.load(f)
                tokens.update(file_tokens)
        except Exception as e:
            print(f"Warning: Could not load Shopee tokens: {e}")
    
    # Add app credentials from environment variables
    tokens.update({
        "partner_id": int(os.getenv("SHOPEE_PARTNER_ID", 0)),
        "partner_key": os.getenv("SHOPEE_PARTNER_KEY", ""),
        "shop_id": int(os.getenv("SHOPEE_SHOP_ID", 0))
    })
    
    return tokens

# Load SHOPEE tokens
SHOPEE_TOKENS = load_shopee_tokens()

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
SHOPEE_BASE_URL = "https://partner.shopeemobile.com"

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
    'customer_key', 'platform_customer_id', 
    'buyer_segment', 'total_orders', 'customer_since', 
    'last_order_date', 'platform_key'
]

# Product Dimension
DIM_PRODUCT_COLUMNS = [
    'product_key', 'product_item_id', 'product_name',
    'product_category', 'product_status', 
    'product_rating', 'platform_key'
]

# Product Variant Dimension
DIM_PRODUCT_VARIANT_COLUMNS = [
    'product_variant_key', 'product_key', 'platform_sku_id', 'canonical_sku',
    'scent', 'volume', 'current_price', 'original_price',
    'created_at', 'last_updated', 'platform_key'
]

# Order Dimension
DIM_ORDER_COLUMNS = [
    'orders_key', 'platform_order_id', 'order_status', 'order_date',
    'updated_at', 'price_total', 'total_item_count', 'payment_method',
    'shipping_city', 'platform_key'
]

# =============================================================================
# TABLE STRUCTURE - FACT TABLES (Based on LA_Collections_Schema.sql)
# =============================================================================

# Fact Orders Table
FACT_ORDERS_COLUMNS = [
    'order_item_key', 'orders_key', 'product_key', 'product_variant_key', 'time_key',
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
    'sales_summary_key', 'time_key', 'platform_key', 'buyer_segment',
    'total_orders', 'successful_orders', 'cancelled_orders', 'returned_orders',
    'total_items_sold', 'gross_revenue', 'shipping_revenue', 'total_discounts',
    'unique_customers', 'created_at', 'updated_at'
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
        'shopee_tokens_valid': bool(SHOPEE_TOKENS.get('access_token')),
        'dataframes_created': len(EMPTY_DATAFRAMES),
        'table_count': {
            'dimensional': 5,  # dim_platform, dim_time, dim_customer, dim_product, dim_order
            'fact': 3,         # fact_orders, fact_traffic, fact_sales_aggregate
            'total': len(EMPTY_DATAFRAMES)
        },
        'api_urls_configured': bool(LAZADA_API_URL and SHOPEE_BASE_URL),
        'schema_compliance': 'LA_Collections_Schema.sql',
        'environment_config': {
            'app_env': APP_ENV,
            'debug_mode': DEBUG,
            'database_configured': bool(DATABASE_URL and "postgresql://" in DATABASE_URL and DATABASE_URL != "postgresql://user:password@localhost:5432/la_collections"),
            'supabase_configured': bool(SUPABASE_URL and SUPABASE_KEY),
            'supabase_service_role_configured': bool(SUPABASE_SERVICE_ROLE_KEY),
            'next_public_configured': bool(NEXT_PUBLIC_SUPABASE_URL and NEXT_PUBLIC_SUPABASE_ANON_KEY),
            'shopee_configured': bool(SHOPEE_TOKENS.get('partner_id') and SHOPEE_TOKENS.get('partner_key')),
            'shopee_tokens_loaded': bool(SHOPEE_TOKENS.get('access_token')),
            'lazada_tokens_loaded': bool(LAZADA_TOKENS.get('access_token')),
            'lazada_app_configured': bool(LAZADA_TOKENS.get('app_key') and LAZADA_TOKENS.get('app_secret'))
        }
    }
    return results

# =============================================================================
# LAZADA TO UNIFIED FIELD MAPPINGS
# =============================================================================

LAZADA_TO_UNIFIED_MAPPING = {
    # --- Dim_Product ---
    "item_id": "product_item_id",
    "name": "product_name",
    "primary_category_name": "product_category",
    "status": "product_status",
    "price": "product_price",  # Base price
    "product_rating": "product_rating",
    "platform_key": "platform_key",  # Always 1 for Lazada
    
    # --- Product Reviews (from lazada_productreview_raw.json) ---
    "review.item_id": "product_item_id",  # Match reviews to products by item_id
    "review.rating": "rating",  # Individual review rating
    "review.review_comment": "review_comment",  # Review content
    "review.review_time": "review_time",  # Review timestamp
    "review.buyer_name": "buyer_name",  # Reviewer name
    "review.product_title": "product_title",  # Product name from review
    "review.id": "review_id",  # Unique review identifier
    
    # --- Dim_Product_Variant (from skus array) ---
    "SkuId": "platform_sku_id",  # From skus[].SkuId
    "SellerSku": "variant_sku",  # From skus[].SellerSku
    "Variation1": "variant_attribute_1",  
    "Variation2": "variant_attribute_2",  
    "Variation3": "variant_attribute_3",  
    
    # --- Dim_Order ---
    "id": "orders_key",  # not pulled from API, generated internally incremental
    "order_id": "platform_order_id", 
    "statuses": "order_status",  # Get status in index 0 for the order status
    "created_at": "order_date",  # Convert to date only (YYYY-MM-DD)
    "updated_at": "updated_at",  # Convert to date only (YYYY-MM-DD)  
    "price": "price_total",
    "items_count": "total_item_count",
    "payment_method": "payment_method",
    "shipping_address.city": "shipping_city",
    
    
    # --- Dim_Customer ---
    "customer_key": "customer_key",  # Generated internally incremental
    "platform_customer_id": "platform_customer_id",  # Generated: 'LZ' + first_char + last_char of first_name + first2_phone + last2_phone
    "address_shipping.phone": "customer_phone",  # From address_shipping.phone (for platform_customer_id generation)
    "buyer_segment": "buyer_segment",  # Calculated: 'New Buyer' or 'Returning Buyer'
    "total_orders": "total_orders",  # Calculated: Count of orders per platform_customer_id
    "customer_since": "customer_since",  # Calculated: Earliest order_date for platform_customer_id
    "last_order_date": "last_order_date",  # Calculated: Latest order_date for platform_customer_id
    "platform_key": "platform_key",  # Always 1 for Lazada
}

# =============================================================================
# SHOPEE TO UNIFIED FIELD MAPPINGS
# =============================================================================

SHOPEE_TO_UNIFIED_MAPPING = {
    # --- Dim_Product (from shopee_products_raw.json and shopee_productitem_raw.json) ---
    "item_id": "product_item_id",
    "item_name": "product_name",
    "category_id": "product_category",  # Will be mapped to category name from shopee_productcategory_raw.json
    "item_status": "product_status",
    "price_info.current_price": "product_price",  # Base price from model list
    "rating_star": "product_rating",
    "platform_key": "platform_key",  # Always 2 for Shopee
    "item_sku": "product_sku_base",  # From shopee_productitem_raw.json
    
    # --- Dim_Product_Variant (from shopee_product_variant_raw.json) ---
    "model_id": "platform_sku_id",  # From model_list[].model_id
    "model_sku": "variant_sku",  # From model_list[].model_sku
    "model_name": "variant_name",  # From model_list[].model_name
    "tier_index.0": "variant_attribute_1",  # From tier_index[0]
    "tier_index.1": "variant_attribute_2",  # From tier_index[1]
    "tier_index.2": "variant_attribute_3",  # From tier_index[2]
    "price_info.current_price": "variant_price",  # From model_list[].price_info.current_price
    "normal_stock": "variant_stock",  # From model_list[].normal_stock
    
    # --- Category Mapping (from shopee_productcategory_raw.json) ---
    "category_name": "category_name",  # Direct mapping from category file
    "parent_category_id": "parent_category_id",
    
    # --- Product Reviews (from shopee_productreview_raw.json) ---
    "review_id": "review_id",
    "product_item_id": "product_item_id",
    "rating": "rating",
    "comment": "comment",
    "review_time": "review_time",
    "reviewer_name": "reviewer_name",
    "review_type": "review_type",
    
    # --- Dim_Order ---
    "id": "orders_key",  # not pulled from API, generated internally incremental
    "order_sn": "platform_order_id", 
    "order_status": "order_status",  # Shopee order status enum
    "create_time": "order_date",  # Convert from Unix timestamp to date only (YYYY-MM-DD)
    "update_time": "updated_at",  # Convert from Unix timestamp to date only (YYYY-MM-DD)  
    "total_amount": "price_total",
    "item_list": "total_item_count",  # Count of items in item_list array
    "payment_method": "payment_method",
    "recipient_address.city": "shipping_city",
    
    # --- Dim_Customer ---
    "customer_key": "customer_key",  # Generated internally incremental
    "platform_customer_id": "platform_customer_id",  # Generated: 'SP' + first_char + last_char of buyer_username + first2_phone + last2_phone
    "recipient_address.phone": "customer_phone",  # From recipient_address.phone (for platform_customer_id generation)
    "buyer_username": "buyer_username",  # For platform_customer_id generation
    "buyer_segment": "buyer_segment",  # Calculated: 'New Buyer' or 'Returning Buyer'
    "total_orders": "total_orders",  # Calculated: Count of orders per platform_customer_id
    "customer_since": "customer_since",  # Calculated: Earliest order_date for platform_customer_id
    "last_order_date": "last_order_date",  # Calculated: Latest order_date for platform_customer_id
    "platform_key": "platform_key",  # Always 2 for Shopee
    
    # --- Fact_Orders (from item_list) ---
    "item_id": "product_item_id",
    "model_id": "product_variant_id",
    "model_quantity_purchased": "item_quantity",
    "model_original_price": "original_unit_price",
    "model_discounted_price": "paid_price",
    "voucher_absorbed_by_seller": "voucher_seller_amount",
    "voucher_absorbed_by_shopee": "voucher_platform_amount",
    "actual_shipping_fee": "shipping_fee_paid_by_buyer",
}

# =============================================================================
# ORDER STATUS STANDARDIZATION MAPPING
# =============================================================================

ORDER_STATUS_MAPPING = {
    'CONFIRMED': 'COMPLETED',
    'CONFIRM': 'COMPLETED',
    'TO_CONFIRM_RECEIVE': 'COMPLETED',
    'DELIVERED': 'COMPLETED',
    'DELIVERY': 'COMPLETED',
    'SHIPPED': 'SHIPPED',
    'PENDING': 'PENDING',
    'CANCELLED': 'CANCELLED',
    'CANCELED': 'CANCELLED',
    'RETURNED': 'RETURNED',
    'RETURN': 'RETURNED',
    'REFUNDED': 'REFUNDED',
    'REFUND': 'REFUNDED',
    'PROCESSING': 'PROCESSING',
    'READY_TO_SHIP': 'READY_TO_SHIP',
    'UNPAID': 'UNPAID',
    'FAILED': 'FAILED'
}

# =============================================================================
# PAYMENT METHOD STANDARDIZATION MAPPING
# =============================================================================

PAYMENT_METHOD_MAPPING = {
    'CREDIT': 'MIXEDCARD',
    'DEBIT': 'MIXEDCARD',
    'CREDIT CARD': 'MIXEDCARD',
    'DEBIT CARD': 'MIXEDCARD',
    'CREDIT/DEBIT': 'MIXEDCARD',
    'CARD': 'MIXEDCARD',
    'CASH ON DELIVERY': 'COD',
    'CASH_ON_DELIVERY': 'COD',
    'COD': 'COD',
    'GCASH_PP': 'GCASH',
    'PAYMAYA_PP': 'PAYMAYA',
    'WALLET_PAYMAYA2C2P': 'PAYMAYA',
    'BANK_TRANSFER': 'BANK TRANSFER',
    'BPI FOR PAYMENT': 'BANK TRANSFER',
    'BDO_IPP': 'BANK TRANSFER',
    'MARIBANK': 'BANK TRANSFER',
    'LAZSAVE_WALLET': 'PLATFORM WALLET BALANCE',
    'ONLINE BANKING': 'ONLINE BANKING',
    'ONLINE_BANKING': 'ONLINE BANKING',
    'WALLET': 'WALLET',
    'SPAYLATER':'PAYLATER',
    'PAYLATER':'PAYLATER',
    'PAY_LATER':'PAYLATER',
    'SHOPEEPAY BALANCE': 'PLATFORM WALLET BALANCE',
    'PAYMENT_ACCOUNT': 'PLATFORM WALLET BALANCE',
    'QRPH': 'QRPH',
    'GOOGLE PAY': 'GOOGLE PAY',
    'PURE_ZERO_PRICE': 'PURE ZERO PRICE',
    'ONLINE / OFFLINE PAYMENT': 'ONLINE / OFFLINE PAYMENT',
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
        'customer_key': 'float64',  # Changed to float64 to handle decimal points (1.1, 1.2, etc.)
        'platform_customer_id': 'str',
        'buyer_segment': 'str',
        'total_orders': 'int',
        'customer_since': 'datetime64[D]',  # Date only, not datetime
        'last_order_date': 'datetime64[D]',  # Date only, not datetime
        'platform_key': 'int'
    },
    'dim_product': {
        'product_key': 'float64',   # Changed to float64 for decimal keys (1.1, 2.1, etc.)
        'product_item_id': 'str',
        'product_name': 'str',
        'product_category': 'str',
        'product_status': 'str',
        'product_rating': 'float64',  # Decimal equivalent in pandas
        'platform_key': 'int'
    },
    'dim_product_variant': {
        'product_variant_key': 'float64',  # Changed to float64 for decimal keys (1.1, 2.2, etc.)
        'product_key': 'float64',  # Changed to float64 to match product table
        'platform_sku_id': 'str',
        'canonical_sku': 'str',
        'scent': 'str',
        'volume': 'str',
        'current_price': 'float64',  # Variant-level current selling price
        'original_price': 'float64',  # Variant-level original/list price
        'created_at': 'datetime64[ns]',  # Timestamp when record was created
        'last_updated': 'datetime64[ns]',  # Timestamp when record was last modified
        'platform_key': 'int'
    },
    'dim_order': {
        'orders_key': 'float64',
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
        'order_item_key': 'str',
        'orders_key': 'float64',
        'product_key': 'float64',
        'product_variant_key': 'float64',
        'time_key': 'float64',
        'customer_key': 'float64',  # Changed to float64 to match dim_customer (1.1, 1.2, etc.)
        'platform_key': 'int',
        'item_quantity': 'int',
        'paid_price': 'float64',  # Decimal equivalent in pandas
        'original_unit_price': 'float64',  # Decimal equivalent in pandas
        'voucher_platform_amount': 'float64',  # Decimal equivalent in pandas
        'voucher_seller_amount': 'float64',  # Decimal equivalent in pandas
        'shipping_fee_paid_by_buyer': 'float64'  # Decimal equivalent in pandas
    },
    'fact_traffic': {
        'traffic_event_key': 'int64',  # Bigint equivalent in pandas
        'time_key': 'int',
        'platform_key': 'int',
        'clicks': 'int',
        'impressions': 'int'
    },
    'fact_sales_aggregate': {
        'sales_summary_key': 'int',  # Serial/auto-increment handled at DB level
        'time_key': 'int',
        'platform_key': 'int',
        'buyer_segment': 'str',
        'total_orders': 'int',
        'successful_orders': 'int',
        'cancelled_orders': 'int',
        'returned_orders': 'int',
        'total_items_sold': 'int',
        'gross_revenue': 'float64',  # Decimal equivalent in pandas
        'shipping_revenue': 'float64',  # Decimal equivalent in pandas
        'total_discounts': 'float64',  # Decimal equivalent in pandas
        'unique_customers': 'int',
        'created_at': 'datetime64[D]',  # Date only, not datetime
        'updated_at': 'datetime64[D]'  # Date only, not datetime
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

# =============================================================================
# PLATFORM MAPPING HELPER FUNCTIONS
# =============================================================================

def get_platform_mapping(platform_name):
    """
    Get the appropriate field mapping for a platform
    
    Args:
        platform_name (str): Either 'lazada' or 'shopee'
        
    Returns:
        dict: Field mapping dictionary
    """
    if platform_name.lower() == 'lazada':
        return LAZADA_TO_UNIFIED_MAPPING
    elif platform_name.lower() == 'shopee':
        return SHOPEE_TO_UNIFIED_MAPPING
    else:
        raise ValueError(f"Unknown platform: {platform_name}. Must be 'lazada' or 'shopee'")

def get_platform_key(platform_name):
    """
    Get the platform key for a platform
    
    Args:
        platform_name (str): Either 'lazada' or 'shopee'
        
    Returns:
        int: Platform key (1 for Lazada, 2 for Shopee)
    """
    if platform_name.lower() == 'lazada':
        return 1
    elif platform_name.lower() == 'shopee':
        return 2
    else:
        raise ValueError(f"Unknown platform: {platform_name}. Must be 'lazada' or 'shopee'")

def get_staging_filename(platform_name, data_type):
    """
    Get the standardized staging filename for a platform and data type
    
    Args:
        platform_name (str): Either 'lazada' or 'shopee'
        data_type (str): Type of data (e.g., 'products', 'orders', 'order_items', etc.)
        
    Returns:
        str: Standardized filename
    """
    platform_prefix = platform_name.lower()
    
    # Map data types to standard filenames
    filename_mapping = {
        'products': f'{platform_prefix}_products_raw.json',
        'productitem': f'{platform_prefix}_productitem_raw.json',
        'product_variant': f'{platform_prefix}_product_variant_raw.json',
        'productcategory': f'{platform_prefix}_productcategory_raw.json',
        'productreview': f'{platform_prefix}_productreview_raw.json',
        'orders': f'{platform_prefix}_orders_raw.json',
        'order_items': f'{platform_prefix}_multiple_order_items_raw.json',
        'traffic': f'{platform_prefix}_reportoverview_raw.json',
        'product_details': f'{platform_prefix}_productitem_raw.json',
        'reviews': f'{platform_prefix}_productreview_raw.json',
        'review_history': f'{platform_prefix}_reviewhistorylist_raw.json'
    }
    
    return filename_mapping.get(data_type, f'{platform_prefix}_{data_type}_raw.json')

def get_all_platforms():
    """
    Get list of all supported platforms
    
    Returns:
        list: List of platform names
    """
    return ['lazada', 'shopee']

def get_platform_tokens(platform_name):
    """
    Get tokens for a specific platform
    
    Args:
        platform_name (str): Either 'lazada' or 'shopee'
        
    Returns:
        dict: Platform tokens
    """
    if platform_name.lower() == 'lazada':
        return LAZADA_TOKENS
    elif platform_name.lower() == 'shopee':
        return SHOPEE_TOKENS
    else:
        raise ValueError(f"Unknown platform: {platform_name}. Must be 'lazada' or 'shopee'")

def get_platform_api_url(platform_name):
    """
    Get API URL for a specific platform
    
    Args:
        platform_name (str): Either 'lazada' or 'shopee'
        
    Returns:
        str: Platform API URL
    """
    if platform_name.lower() == 'lazada':
        return LAZADA_API_URL
    elif platform_name.lower() == 'shopee':
        return SHOPEE_BASE_URL
    else:
        raise ValueError(f"Unknown platform: {platform_name}. Must be 'lazada' or 'shopee'")

# =============================================================================
# MAIN EXECUTION
# =============================================================================

if __name__ == "__main__":
    # Test the configuration
    print("LA Collections Configuration - Schema Compliant")
    print("=" * 60)
    
    validation = validate_config()
    print(f"Lazada Tokens Valid: {validation['tokens_valid']}")
    print(f"Shopee Tokens Valid: {validation['shopee_tokens_valid']}")
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
    print(f"  Shopee Tokens Loaded: {env_config['shopee_tokens_loaded']}")
    print(f"  Lazada Tokens Loaded: {env_config['lazada_tokens_loaded']}")
    print(f"  Lazada App Configured: {env_config['lazada_app_configured']}")
    
    print("\nSupported Platforms:")
    for platform in get_all_platforms():
        platform_key = get_platform_key(platform)
        print(f"  {platform.capitalize()}: platform_key={platform_key}")
    
    print("\nAvailable Tables (LA_Collections_Schema.sql):")
    for table_name in get_all_table_names():
        columns = get_table_columns(table_name)
        print(f"  {table_name}: {len(columns)} columns")
    
    print("\nSample Time Dimension (first 3 rows):")
    sample_time = create_sample_time_data(periods=3)
    print(sample_time[['time_key', 'date', 'quarter', 'month_name', 'is_weekend', 'is_payday', 'is_mega_sale_day']])


