"""
Order Dimension Harmonization Script
Maps Lazada and Shopee order data from raw JSON files to the standardized dimensional model

Data Sources:
- lazada_orders_raw.json 
- lazada_multiple_order_items_raw.json
- shopee_orders_raw.json

Target Schema: Dim_Order table structure
"""

import pandas as pd
import json
import os
import sys
from datetime import datetime

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import get_empty_dataframe, LAZADA_TO_UNIFIED_MAPPING, SHOPEE_TO_UNIFIED_MAPPING, apply_data_types, ORDER_STATUS_MAPPING, PAYMENT_METHOD_MAPPING

def load_lazada_orders():
    """
    Load Lazada order data from raw JSON files
    
    Returns:
        tuple: (orders_data, order_items_data) as lists of dictionaries
    """
    staging_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'Staging')
    
    # Load orders raw data
    orders_file = os.path.join(staging_dir, 'lazada_orders_raw.json')
    order_items_file = os.path.join(staging_dir, 'lazada_multiple_order_items_raw.json')
    
    orders_data = []
    order_items_data = []
    
    if os.path.exists(orders_file):
        with open(orders_file, 'r', encoding='utf-8') as f:
            orders_data = json.load(f)
        print(f"✅ Loaded {len(orders_data)} orders from lazada_orders_raw.json")
    else:
        print(f"⚠️ File not found: {orders_file}")
    
    if os.path.exists(order_items_file):
        with open(order_items_file, 'r', encoding='utf-8') as f:
            order_items_data = json.load(f)
        print(f"✅ Loaded {len(order_items_data)} order items from lazada_multiple_order_items_raw.json")
    else:
        print(f"⚠️ File not found: {order_items_file}")
    
    return orders_data, order_items_data

def load_shopee_orders():
    """
    Load Shopee order data from raw JSON files
    
    Returns:
        list: orders_data as list of dictionaries
    """
    staging_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'Staging')
    
    # Load orders raw data
    orders_file = os.path.join(staging_dir, 'shopee_orders_raw.json')
    
    orders_data = []
    
    if os.path.exists(orders_file):
        with open(orders_file, 'r', encoding='utf-8') as f:
            orders_data = json.load(f)
        print(f"✅ Loaded {len(orders_data)} orders from shopee_orders_raw.json")
    else:
        print(f"⚠️ File not found: {orders_file}")
    
    return orders_data

def standardize_order_status(status):
    """
    Standardize order status to ALL CAPS format
    Maps common status values to standardized format
    
    Args:
        status (str): Raw order status from API
        
    Returns:
        str: Standardized order status in ALL CAPS
    """
    if not status:
        return ''
    
    # Convert to uppercase and strip whitespace
    standardized = str(status).upper().strip()
    
    # Check if the standardized status is in our mapping (from config.py)
    if standardized in ORDER_STATUS_MAPPING:
        return ORDER_STATUS_MAPPING[standardized]
    
    # Return as-is if not in mapping (already uppercase)
    return standardized

def standardize_payment_method(payment_method):
    """
    Standardize payment method to ALL CAPS format
    Maps common payment method values to standardized format
    
    Args:
        payment_method (str): Raw payment method from API
        
    Returns:
        str: Standardized payment method in ALL CAPS
    """
    if not payment_method:
        return ''
    
    # Convert to uppercase and strip whitespace
    standardized = str(payment_method).upper().strip()
    
    # Check if the standardized payment method is in our mapping (from config.py)
    if standardized in PAYMENT_METHOD_MAPPING:
        return PAYMENT_METHOD_MAPPING[standardized]
    
    # Return as-is if not in mapping (already uppercase)
    return standardized

def extract_order_status(statuses_array):
    """
    Extract order status from statuses array (get status at index 0)
    
    Args:
        statuses_array (list): List of status objects
        
    Returns:
        str: Order status or empty string
    """
    if not statuses_array or len(statuses_array) == 0:
        return ''
    
    # Get status from first element (index 0)
    first_status = statuses_array[0]
    if isinstance(first_status, dict):
        return first_status.get('status', '')
    return str(first_status)

def parse_date_to_date_only(date_string):
    """
    Convert datetime string to date only (YYYY-MM-DD)
    
    Args:
        date_string (str): Date string from Lazada API
        
    Returns:
        str: Date in YYYY-MM-DD format or None
    """
    if not date_string:
        return None
    
    try:
        # Parse the datetime string and extract date only
        dt = pd.to_datetime(date_string)
        return dt.date()
    except (ValueError, TypeError):
        print(f"Warning: Could not parse date: {date_string}")
        return None

def extract_shipping_city(address_shipping):
    """
    Extract shipping city from address_shipping object
    Replace asterisked cities with "N/A"
    
    Args:
        address_shipping (dict): Shipping address object
        
    Returns:
        str: City name or "N/A" for asterisked values
    """
    if not address_shipping or not isinstance(address_shipping, dict):
        return ''
    
    city = address_shipping.get('city', '')
    # Replace asterisked cities with "N/A"
    if city and '*' in city:
        return 'N/A'
    return city

def harmonize_order_record(order_data, source_file):
    """
    Harmonize a single order record from Lazada format to dimensional model
    Uses LAZADA_TO_UNIFIED_MAPPING from config.py
    
    Args:
        order_data (dict): Raw order data from Lazada API
        source_file (str): Source file identifier ('orders' or 'order_items')
        
    Returns:
        dict: Harmonized order record
    """
    # Initialize harmonized record with None values
    harmonized_record = {}
    
    # Apply field mappings from config.py
    for lazada_field, unified_field in LAZADA_TO_UNIFIED_MAPPING.items():
        if unified_field in ['orders_key', 'platform_order_id', 'order_status', 'order_date', 
                            'updated_at', 'price_total', 'total_item_count', 'payment_method', 
                            'shipping_city', 'platform_key']:
            
            if lazada_field == 'order_id':
                harmonized_record['platform_order_id'] = str(order_data.get('order_id', ''))
            
            elif lazada_field == 'statuses':
                # Extract order status from statuses array (index 0)
                order_status = extract_order_status(order_data.get('statuses', []))
                harmonized_record['order_status'] = standardize_order_status(order_status)
            
            elif lazada_field == 'created_at':
                harmonized_record['order_date'] = parse_date_to_date_only(order_data.get('created_at'))
            
            elif lazada_field == 'updated_at':
                harmonized_record['updated_at'] = parse_date_to_date_only(order_data.get('updated_at'))
            
            elif lazada_field == 'price':
                # PRICE_MAPPING_FIX_APPLIED - Enhanced price mapping with validation
                price_total = None
                
                # Try multiple price sources for robustness
                price_sources = ['price', 'item_price', 'total_amount']
                
                for price_field in price_sources:
                    if price_field in order_data and order_data[price_field] is not None:
                        try:
                            price_value = order_data[price_field]
                            
                            # Handle string prices (e.g., "350.00")
                            if isinstance(price_value, str):
                                price_value = price_value.strip()
                                if price_value and price_value != '0.00':
                                    price_total = float(price_value)
                                    break
                            
                            # Handle numeric prices
                            elif isinstance(price_value, (int, float)):
                                if price_value > 0:
                                    price_total = float(price_value)
                                    break
                                    
                        except (ValueError, TypeError):
                            continue
                
                # Price validation and logging
                if price_total is None:
                    print(f"⚠️  No valid price found for order {order_data.get('order_id', 'unknown')}")
                    print(f"   Available price fields: {[f'{k}: {v}' for k, v in order_data.items() if 'price' in k.lower() or 'amount' in k.lower()]}")
                
                harmonized_record['price_total'] = price_total
            
            elif lazada_field == 'items_count':
                harmonized_record['total_item_count'] = order_data.get('items_count', 0)
            
            elif lazada_field == 'payment_method':
                payment_method = order_data.get('payment_method', '')
                harmonized_record['payment_method'] = standardize_payment_method(payment_method)
            
            elif lazada_field == 'shipping_address.city':
                # Extract shipping city from shipping_address and handle asterisks
                shipping_address = order_data.get('shipping_address', order_data.get('address_shipping', {}))
                shipping_city = ''
                if isinstance(shipping_address, dict):
                    shipping_city = shipping_address.get('city', '')
                    # Replace asterisked cities with "N/A"
                    if shipping_city and '*' in shipping_city:
                        shipping_city = 'N/A'
                harmonized_record['shipping_city'] = shipping_city
    
    # Set fields that are not in mapping but required
    harmonized_record['orders_key'] = None  # Will be generated as surrogate key
    harmonized_record['platform_key'] = 1  # Lazada platform key
    
    # Ensure all required columns exist with default values if missing
    required_columns = ['orders_key', 'platform_order_id', 'order_status', 'order_date', 
                       'updated_at', 'price_total', 'total_item_count', 'payment_method', 
                       'shipping_city', 'platform_key']
    
    for col in required_columns:
        if col not in harmonized_record:
            if col in ['orders_key']:
                harmonized_record[col] = None
            elif col in ['platform_order_id', 'order_status', 'payment_method', 'shipping_city']:
                harmonized_record[col] = ''
            elif col in ['price_total']:
                harmonized_record[col] = None
            elif col in ['total_item_count', 'platform_key']:
                harmonized_record[col] = 0
            elif col in ['order_date', 'updated_at']:
                harmonized_record[col] = None
    
    return harmonized_record

def harmonize_shopee_order_record(order_data):
    """
    Harmonize a single order record from Shopee format to dimensional model
    Uses SHOPEE_TO_UNIFIED_MAPPING from config.py
    
    Args:
        order_data (dict): Raw order data from Shopee API
        
    Returns:
        dict: Harmonized order record
    """
    # Initialize harmonized record with None values
    harmonized_record = {}
    
    # Apply field mappings from config.py
    for shopee_field, unified_field in SHOPEE_TO_UNIFIED_MAPPING.items():
        if unified_field in ['orders_key', 'platform_order_id', 'order_status', 'order_date', 
                            'updated_at', 'price_total', 'total_item_count', 'payment_method', 
                            'shipping_city', 'platform_key']:
            
            if shopee_field == 'order_sn':
                harmonized_record['platform_order_id'] = str(order_data.get('order_sn', ''))
            
            elif shopee_field == 'order_status':
                order_status = order_data.get('order_status', '')
                harmonized_record['order_status'] = standardize_order_status(order_status)
            
            elif shopee_field == 'create_time':
                # Convert Unix timestamps to date only format
                order_date = None
                if 'create_time' in order_data:
                    try:
                        order_date = datetime.fromtimestamp(order_data['create_time']).date()
                    except (ValueError, TypeError, OSError):
                        order_date = None
                harmonized_record['order_date'] = order_date
            
            elif shopee_field == 'update_time':
                updated_at = None
                if 'update_time' in order_data:
                    try:
                        updated_at = datetime.fromtimestamp(order_data['update_time']).date()
                    except (ValueError, TypeError, OSError):
                        updated_at = None
                harmonized_record['updated_at'] = updated_at
            
            elif shopee_field == 'total_amount':
                # Convert total_amount to float
                price_total = None
                if 'total_amount' in order_data:
                    try:
                        price_total = float(order_data['total_amount'])
                    except (ValueError, TypeError):
                        price_total = None
                harmonized_record['price_total'] = price_total
            
            elif shopee_field == 'item_list':
                # Count items in item_list
                total_item_count = 0
                item_list = order_data.get('item_list', [])
                if isinstance(item_list, list):
                    total_item_count = len(item_list)
                harmonized_record['total_item_count'] = total_item_count
            
            elif shopee_field == 'payment_method':
                payment_method = order_data.get('payment_method', '')
                harmonized_record['payment_method'] = standardize_payment_method(payment_method)
            
            elif shopee_field == 'recipient_address.city':
                # Extract shipping city from recipient_address and handle asterisks
                shipping_city = ''
                recipient_address = order_data.get('recipient_address', {})
                if isinstance(recipient_address, dict):
                    shipping_city = recipient_address.get('city', '')
                    # Replace asterisked cities with "N/A"
                    if shipping_city and '*' in shipping_city:
                        shipping_city = 'N/A'
                harmonized_record['shipping_city'] = shipping_city
    
    # Set fields that are not in mapping but required
    harmonized_record['orders_key'] = None  # Will be generated as surrogate key
    harmonized_record['platform_key'] = 2  # Shopee platform key
    
    # Ensure all required columns exist with default values if missing
    required_columns = ['orders_key', 'platform_order_id', 'order_status', 'order_date', 
                       'updated_at', 'price_total', 'total_item_count', 'payment_method', 
                       'shipping_city', 'platform_key']
    
    for col in required_columns:
        if col not in harmonized_record:
            if col in ['orders_key']:
                harmonized_record[col] = None
            elif col in ['platform_order_id', 'order_status', 'payment_method', 'shipping_city']:
                harmonized_record[col] = ''
            elif col in ['price_total']:
                harmonized_record[col] = None
            elif col in ['total_item_count', 'platform_key']:
                harmonized_record[col] = 0
            elif col in ['order_date', 'updated_at']:
                harmonized_record[col] = None
    
    return harmonized_record

def harmonize_dim_order():
    """
    Main function to harmonize Lazada and Shopee order data into dimensional model
    
    Process:
    1. Load all data into DataFrames
    2. Perform cleansing and standardization
    3. Sort by order_date ascending
    4. Generate incremental orders_key with platform-specific decimals
    
    Returns:
        pd.DataFrame: Harmonized order dimension table
    """
    print("🔄 Starting Order Dimension Harmonization (Lazada + Shopee)...")
    
    # Get empty DataFrame with proper structure
    dim_order_df = get_empty_dataframe('dim_order')
    print(f"📋 Target schema: {list(dim_order_df.columns)}")
    
    # Load raw data from both platforms
    print("\n📥 Loading Lazada data...")
    orders_data, order_items_data = load_lazada_orders()
    
    print("\n📥 Loading Shopee data...")
    shopee_orders_data = load_shopee_orders()
    
    # Process all data into a single DataFrame for unified processing
    all_orders = []
    
    # Process Lazada orders
    print(f"\n🔄 Processing Lazada orders...")
    lazada_orders_dict = {}
    
    # Process lazada_orders_raw.json (primary data source)
    for order in orders_data:
        order_id = str(order.get('order_id', ''))
        if order_id and order_id not in lazada_orders_dict:
            harmonized = harmonize_order_record(order, 'orders')
            harmonized['raw_platform_order_id'] = order_id  # Keep original for deduplication
            lazada_orders_dict[order_id] = harmonized
    
    # Process lazada_multiple_order_items_raw.json (supplement missing orders)
    for order in order_items_data:
        order_id = str(order.get('order_id', ''))
        if order_id and order_id not in lazada_orders_dict:
            harmonized = harmonize_order_record(order, 'order_items')
            harmonized['raw_platform_order_id'] = order_id  # Keep original for deduplication
            lazada_orders_dict[order_id] = harmonized
    
    all_orders.extend(list(lazada_orders_dict.values()))
    print(f"✅ Processed {len(lazada_orders_dict)} Lazada orders")
    
    # Process Shopee orders
    print(f"\n🔄 Processing Shopee orders...")
    shopee_orders_dict = {}
    
    for order in shopee_orders_data:
        order_sn = str(order.get('order_sn', ''))
        
        if order_sn and order_sn not in shopee_orders_dict:
            harmonized = harmonize_shopee_order_record(order)
            harmonized['raw_platform_order_id'] = order_sn  # Keep original for deduplication
            shopee_orders_dict[order_sn] = harmonized
    
    all_orders.extend(list(shopee_orders_dict.values()))
    print(f"✅ Processed {len(shopee_orders_dict)} Shopee orders")
    
    # Convert to DataFrame for unified processing
    if all_orders:
        print(f"\n🔄 Creating unified DataFrame with {len(all_orders)} orders...")
        df = pd.DataFrame(all_orders)
        
        # Step 1: Data Cleansing and Standardization
        print("🧹 Performing data cleansing and standardization...")
        
        # Handle missing/null order dates - set to minimum date for sorting
        min_date = pd.to_datetime('1900-01-01').date()
        df['order_date'] = df['order_date'].fillna(min_date)
        
        # Ensure platform_order_id is string and not empty
        df['platform_order_id'] = df['platform_order_id'].astype(str)
        df = df[df['platform_order_id'] != '']
        
        # Standardize order_status and payment_method to uppercase
        df['order_status'] = df['order_status'].fillna('').astype(str).str.upper().str.strip()
        df['payment_method'] = df['payment_method'].fillna('').astype(str).str.upper().str.strip()
        
        # Clean shipping_city - replace asterisked values with "N/A"
        df['shipping_city'] = df['shipping_city'].fillna('').astype(str).str.strip()
        df.loc[df['shipping_city'].str.contains(r'\*', na=False), 'shipping_city'] = 'N/A'
        
        # Handle price_total - ensure numeric
        df['price_total'] = pd.to_numeric(df['price_total'], errors='coerce')
        
        # Handle total_item_count - ensure integer
        df['total_item_count'] = pd.to_numeric(df['total_item_count'], errors='coerce').fillna(0).astype(int)
        
        # Remove duplicates based on platform and order ID
        initial_count = len(df)
        df = df.drop_duplicates(subset=['platform_key', 'raw_platform_order_id'], keep='first')
        if len(df) < initial_count:
            print(f"   🔄 Removed {initial_count - len(df)} duplicate orders")
        
        # Step 2: Sort by order_date ascending (earliest first)
        print("📅 Sorting by order_date ascending...")
        df = df.sort_values(['order_date', 'platform_key', 'platform_order_id'], ascending=[True, True, True])
        df = df.reset_index(drop=True)
        
        # Step 3: Generate incremental orders_key with platform-specific decimals
        print("🔢 Generating incremental orders_key with platform decimals...")
        
        # Create base incremental key (1, 2, 3, ...)
        df['base_key'] = range(1, len(df) + 1)
        
        # Add platform-specific decimal: +0.1 for Lazada (platform_key=1), +0.2 for Shopee (platform_key=2)
        df['orders_key'] = df['base_key'] + (df['platform_key'] * 0.1)
        
        # Remove helper columns
        df = df.drop(columns=['raw_platform_order_id', 'base_key'])
        
        # Step 4: Apply proper data types according to schema
        print("🔄 Applying data types...")
        df = apply_data_types(df, 'dim_order')
        
        # Step 5: Ensure correct column order according to DIM_ORDER_COLUMNS
        print("🔄 Ensuring correct column order...")
        target_columns = ['orders_key', 'platform_order_id', 'order_status', 'order_date', 
                         'updated_at', 'price_total', 'total_item_count', 'payment_method', 
                         'shipping_city', 'platform_key']
        
        # Reorder columns to match schema
        df = df[target_columns]
        
        # Final DataFrame
        dim_order_df = df
        
        print(f"\n✅ Harmonized {len(dim_order_df)} total orders")
        print(f"📊 Data Summary:")
        print(f"   - Total orders: {len(dim_order_df)}")
        
        # Count by platform
        lazada_count = len(dim_order_df[dim_order_df['platform_key'] == 1])
        shopee_count = len(dim_order_df[dim_order_df['platform_key'] == 2])
        print(f"   - Lazada orders: {lazada_count} (keys: {lazada_count} with .1 decimals)")
        print(f"   - Shopee orders: {shopee_count} (keys: {shopee_count} with .2 decimals)")
        
        print(f"   - Orders with valid dates: {len(dim_order_df[dim_order_df['order_date'] != min_date])}")
        print(f"   - Orders with prices: {len(dim_order_df[dim_order_df['price_total'].notna()])}")
        print(f"   - Unique order statuses: {dim_order_df['order_status'].nunique()}")
        print(f"   - Unique payment methods: {dim_order_df['payment_method'].nunique()}")
        
        # Show order_key range by platform
        print(f"\n🔢 Orders Key Ranges:")
        if lazada_count > 0:
            lazada_df = dim_order_df[dim_order_df['platform_key'] == 1]
            print(f"   - Lazada keys: {lazada_df['orders_key'].min():.1f} to {lazada_df['orders_key'].max():.1f}")
        if shopee_count > 0:
            shopee_df = dim_order_df[dim_order_df['platform_key'] == 2]
            print(f"   - Shopee keys: {shopee_df['orders_key'].min():.1f} to {shopee_df['orders_key'].max():.1f}")
        
        # Show sample of data from each platform (sorted by date)
        print("\n📋 Sample of earliest Lazada orders (by date):")
        lazada_df = dim_order_df[dim_order_df['platform_key'] == 1]
        if not lazada_df.empty:
            sample_cols = ['orders_key', 'platform_order_id', 'order_status', 'order_date', 'price_total', 'total_item_count']
            available_cols = [col for col in sample_cols if col in lazada_df.columns]
            print(lazada_df[available_cols].head(3).to_string(index=False))
        
        print("\n📋 Sample of earliest Shopee orders (by date):")
        shopee_df = dim_order_df[dim_order_df['platform_key'] == 2]
        if not shopee_df.empty:
            sample_cols = ['orders_key', 'platform_order_id', 'order_status', 'order_date', 'price_total', 'total_item_count']
            available_cols = [col for col in sample_cols if col in shopee_df.columns]
            print(shopee_df[available_cols].head(3).to_string(index=False))
        
        # Show order status distribution by platform
        print(f"\n📊 Order Status Distribution by Platform:")
        for platform_key, platform_name in [(1, 'Lazada'), (2, 'Shopee')]:
            platform_df = dim_order_df[dim_order_df['platform_key'] == platform_key]
            if not platform_df.empty:
                print(f"\n{platform_name}:")
                status_counts = platform_df['order_status'].value_counts()
                for status, count in status_counts.head(5).items():
                    print(f"   {status}: {count}")
        
        # Show date range
        valid_dates_df = dim_order_df[dim_order_df['order_date'] != min_date]
        if not valid_dates_df.empty:
            print(f"\n📅 Date Range:")
            print(f"   - Earliest order: {valid_dates_df['order_date'].min()}")
            print(f"   - Latest order: {valid_dates_df['order_date'].max()}")
            
        
    else:
        print("⚠️ No order data found to harmonize")
    
    return dim_order_df

def save_harmonized_orders(df, output_path=None):
    """
    Save harmonized order data to CSV
    
    Args:
        df (pd.DataFrame): Harmonized order DataFrame
        output_path (str): Output file path (optional)
    """
    if output_path is None:
        transformed_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'Transformed')
        os.makedirs(transformed_dir, exist_ok=True)
        output_path = os.path.join(transformed_dir, 'dim_order.csv')
    
    df.to_csv(output_path, index=False)
    print(f"💾 Saved harmonized order data to: {output_path}")
    return output_path

if __name__ == "__main__":
    # Run the harmonization process
    harmonized_df = harmonize_dim_order()
    
    if not harmonized_df.empty:
        output_file = save_harmonized_orders(harmonized_df)
        print(f"\n🎉 Order dimension harmonization completed successfully!")
        print(f"📁 Output file: {output_file}")
    else:
        print("❌ No data was harmonized. Please check your source files.")
        
    # Display mappings used
    print(f"\n🔗 Field mappings used:")
    
    print(f"\nLazada mappings:")
    lazada_order_mappings = {
        'order_id': 'platform_order_id',
        'statuses[0]': 'order_status',
        'created_at': 'order_date', 
        'updated_at': 'updated_at',
        'price': 'price_total',
        'items_count': 'total_item_count',
        'payment_method': 'payment_method',
        'address_shipping.city': 'shipping_city'
    }
    for lazada_field, unified_field in lazada_order_mappings.items():
        print(f"   {lazada_field} → {unified_field}")
    
    print(f"\nShopee mappings:")
    shopee_order_mappings = {
        'order_sn': 'platform_order_id',
        'order_status': 'order_status',
        'create_time': 'order_date', 
        'update_time': 'updated_at',
        'total_amount': 'price_total',
        'len(item_list)': 'total_item_count',
        'payment_method': 'payment_method',
        'recipient_address.city': 'shipping_city'
    }
    for shopee_field, unified_field in shopee_order_mappings.items():
        print(f"   {shopee_field} → {unified_field}")

def validate_price_mapping():
    """
    Integrated price mapping validation with comprehensive analysis
    Validates price completeness for both Lazada and Shopee orders
    """
    print(f"\n🔍 VALIDATING PRICE MAPPING COMPLETENESS...")
    
    try:
        # Re-read the saved data for validation
        transformed_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'Transformed')
        dim_order_path = os.path.join(transformed_dir, 'dim_order.csv')
        if not os.path.exists(dim_order_path):
            print(f"❌ dim_order.csv not found at {dim_order_path}")
            return False
            
        validation_df = pd.read_csv(dim_order_path)
        
        # Overall price statistics
        total_orders = len(validation_df)
        valid_prices = validation_df['price_total'].notna().sum()
        missing_prices = validation_df['price_total'].isna().sum()
        
        print(f"📊 Price Mapping Results:")
        print(f"  Total orders: {total_orders:,}")
        print(f"  Orders with valid prices: {valid_prices:,}")
        print(f"  Orders with missing prices: {missing_prices:,}")
        
        completion_rate = (valid_prices / total_orders * 100) if total_orders > 0 else 0
        print(f"  Price mapping completion rate: {completion_rate:.1f}%")
        
        # Platform-specific validation
        print(f"\n📊 Price Mapping by Platform:")
        platform_results = {}
        
        for platform_key, platform_name in [(1, 'Lazada'), (2, 'Shopee')]:
            platform_orders = validation_df[validation_df['platform_key'] == platform_key]
            
            if len(platform_orders) > 0:
                platform_total = len(platform_orders)
                platform_valid = platform_orders['price_total'].notna().sum()
                platform_rate = (platform_valid / platform_total * 100)
                platform_results[platform_name] = platform_rate
                
                print(f"  {platform_name}: {platform_valid:,}/{platform_total:,} ({platform_rate:.1f}%)")
                
                if platform_rate < 95.0:
                    print(f"    ⚠️  {platform_name} price mapping below threshold")
        
        # Validation thresholds and results
        if completion_rate >= 98.0:
            print(f"✅ PASS: Price mapping meets quality threshold (≥98%)")
            return True
        elif completion_rate >= 95.0:
            print(f"⚠️  WARNING: Price mapping below optimal threshold (95-98%)")
            return True
        else:
            print(f"❌ FAIL: Price mapping below acceptable threshold (<95%)")
            return False
            
    except Exception as e:
        print(f"❌ Error during price validation: {e}")
        return False

if __name__ == "__main__":
    # Run the harmonization process
    harmonized_df = harmonize_dim_order()
    
    if not harmonized_df.empty:
        output_file = save_harmonized_orders(harmonized_df)
        print(f"\n🎉 Order dimension harmonization completed successfully!")
        print(f"📁 Output file: {output_file}")
        
        # Run integrated price validation
        validate_price_mapping()
    else:
        print("❌ No data was harmonized. Please check your source files.")
        
    # Display mappings used
    print(f"\n🔗 Field mappings used:")
    
    print(f"\nLazada mappings:")
    lazada_order_mappings = {
        'order_id': 'platform_order_id',
        'statuses[0]': 'order_status',
        'created_at': 'order_date', 
        'updated_at': 'updated_at',
        'price': 'price_total',
        'items_count': 'total_item_count',
        'payment_method': 'payment_method',
        'address_shipping.city': 'shipping_city'
    }
    for lazada_field, unified_field in lazada_order_mappings.items():
        print(f"   {lazada_field} → {unified_field}")
    
    print(f"\nShopee mappings:")
    shopee_order_mappings = {
        'order_sn': 'platform_order_id',
        'order_status': 'order_status',
        'create_time': 'order_date', 
        'update_time': 'updated_at',
        'total_amount': 'price_total',
        'len(item_list)': 'total_item_count',
        'payment_method': 'payment_method',
        'recipient_address.city': 'shipping_city'
    }

