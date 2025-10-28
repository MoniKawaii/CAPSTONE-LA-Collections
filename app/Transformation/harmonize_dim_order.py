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
from config import get_empty_dataframe, LAZADA_TO_UNIFIED_MAPPING, SHOPEE_TO_UNIFIED_MAPPING, apply_data_types

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
    
    Args:
        address_shipping (dict): Shipping address object
        
    Returns:
        str: City name or empty string
    """
    if not address_shipping or not isinstance(address_shipping, dict):
        return ''
    
    return address_shipping.get('city', '')

def harmonize_order_record(order_data, source_file):
    """
    Harmonize a single order record from Lazada format to dimensional model
    
    Args:
        order_data (dict): Raw order data from Lazada API
        source_file (str): Source file identifier ('orders' or 'order_items')
        
    Returns:
        dict: Harmonized order record
    """
    # Extract order status from statuses array (index 0)
    order_status = extract_order_status(order_data.get('statuses', []))
    
    # Convert dates to date only format
    order_date = parse_date_to_date_only(order_data.get('created_at'))
    updated_at = parse_date_to_date_only(order_data.get('updated_at'))
    
    # Extract shipping city
    shipping_city = extract_shipping_city(order_data.get('address_shipping', {}))
    
    # Convert price to float
    price_total = None
    if 'price' in order_data:
        try:
            price_total = float(order_data['price'])
        except (ValueError, TypeError):
            price_total = None
    
    # Map using LAZADA_TO_UNIFIED_MAPPING structure
    harmonized_record = {
        'orders_key': None,  # Will be generated as surrogate key
        'platform_order_id': str(order_data.get('order_id', '')),
        'order_status': order_status,
        'order_date': order_date,
        'updated_at': updated_at,
        'price_total': price_total,
        'total_item_count': order_data.get('items_count', 0),
        'payment_method': order_data.get('payment_method', ''),
        'shipping_city': shipping_city
    }
    
    return harmonized_record

def harmonize_shopee_order_record(order_data):
    """
    Harmonize a single order record from Shopee format to dimensional model
    
    Args:
        order_data (dict): Raw order data from Shopee API
        
    Returns:
        dict: Harmonized order record
    """
    # Extract order status (Shopee uses order_status directly)
    order_status = order_data.get('order_status', '')
    
    # Convert Unix timestamps to date only format
    order_date = None
    if 'create_time' in order_data:
        try:
            order_date = datetime.fromtimestamp(order_data['create_time']).date()
        except (ValueError, TypeError, OSError):
            order_date = None
    
    updated_at = None
    if 'update_time' in order_data:
        try:
            updated_at = datetime.fromtimestamp(order_data['update_time']).date()
        except (ValueError, TypeError, OSError):
            updated_at = None
    
    # Extract shipping city from recipient_address
    shipping_city = ''
    recipient_address = order_data.get('recipient_address', {})
    if isinstance(recipient_address, dict):
        shipping_city = recipient_address.get('city', '')
    
    # Convert total_amount to float
    price_total = None
    if 'total_amount' in order_data:
        try:
            price_total = float(order_data['total_amount'])
        except (ValueError, TypeError):
            price_total = None
    
    # Count items in item_list
    total_item_count = 0
    item_list = order_data.get('item_list', [])
    if isinstance(item_list, list):
        total_item_count = len(item_list)
    
    # Map using SHOPEE_TO_UNIFIED_MAPPING structure
    harmonized_record = {
        'orders_key': None,  # Will be generated as surrogate key
        'platform_order_id': str(order_data.get('order_sn', '')),
        'order_status': order_status,
        'order_date': order_date,
        'updated_at': updated_at,
        'price_total': price_total,
        'total_item_count': total_item_count,
        'payment_method': order_data.get('payment_method', ''),
        'shipping_city': shipping_city
    }
    
    return harmonized_record

def harmonize_dim_order():
    """
    Main function to harmonize Lazada and Shopee order data into dimensional model
    
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
    
    # Combine all order data with platform tracking
    all_orders = []
    
    # Process Lazada orders
    print(f"\n🔄 Processing Lazada orders...")
    lazada_orders_dict = {}
    
    # Process lazada_orders_raw.json
    for order in orders_data:
        order_id = str(order.get('order_id', ''))
        if order_id and order_id not in lazada_orders_dict:
            harmonized = harmonize_order_record(order, 'orders')
            harmonized['platform_key'] = 1  # Lazada platform key
            lazada_orders_dict[order_id] = harmonized
    
    # Process lazada_multiple_order_items_raw.json (supplement missing orders)
    for order in order_items_data:
        order_id = str(order.get('order_id', ''))
        if order_id and order_id not in lazada_orders_dict:
            harmonized = harmonize_order_record(order, 'order_items')
            harmonized['platform_key'] = 1  # Lazada platform key
            lazada_orders_dict[order_id] = harmonized
        # Note: if order exists, we keep the one from orders_raw.json as it's more complete
    
    all_orders.extend(list(lazada_orders_dict.values()))
    print(f"✅ Processed {len(lazada_orders_dict)} Lazada orders")
    
    # Process Shopee orders
    print(f"\n🔄 Processing Shopee orders...")
    shopee_orders_dict = {}
    
    for order in shopee_orders_data:
        order_sn = str(order.get('order_sn', ''))
        if order_sn and order_sn not in shopee_orders_dict:
            harmonized = harmonize_shopee_order_record(order)
            harmonized['platform_key'] = 2  # Shopee platform key
            shopee_orders_dict[order_sn] = harmonized
    
    all_orders.extend(list(shopee_orders_dict.values()))
    print(f"✅ Processed {len(shopee_orders_dict)} Shopee orders")
    
    # Convert to DataFrame
    if all_orders:
        dim_order_df = pd.DataFrame(all_orders)
        
        # Generate surrogate keys (orders_key) - unified across both platforms
        dim_order_df['orders_key'] = range(1, len(dim_order_df) + 1)
        
        # Apply proper data types according to schema
        dim_order_df = apply_data_types(dim_order_df, 'dim_order')
        
        print(f"\n✅ Harmonized {len(dim_order_df)} total orders")
        print(f"📊 Data Summary:")
        print(f"   - Total orders: {len(dim_order_df)}")
        print(f"   - Lazada orders: {len(dim_order_df[dim_order_df['platform_key'] == 1])}")
        print(f"   - Shopee orders: {len(dim_order_df[dim_order_df['platform_key'] == 2])}")
        print(f"   - Orders with dates: {len(dim_order_df[dim_order_df['order_date'].notna()])}")
        print(f"   - Orders with prices: {len(dim_order_df[dim_order_df['price_total'].notna()])}")
        print(f"   - Unique order statuses: {dim_order_df['order_status'].nunique()}")
        print(f"   - Unique payment methods: {dim_order_df['payment_method'].nunique()}")
        
        # Show sample of data from each platform
        print("\n📋 Sample of Lazada orders:")
        lazada_df = dim_order_df[dim_order_df['platform_key'] == 1]
        if not lazada_df.empty:
            sample_cols = ['orders_key', 'platform_order_id', 'order_status', 'order_date', 'price_total', 'total_item_count', 'platform_key']
            available_cols = [col for col in sample_cols if col in lazada_df.columns]
            print(lazada_df[available_cols].head(3).to_string(index=False))
        
        print("\n📋 Sample of Shopee orders:")
        shopee_df = dim_order_df[dim_order_df['platform_key'] == 2]
        if not shopee_df.empty:
            sample_cols = ['orders_key', 'platform_order_id', 'order_status', 'order_date', 'price_total', 'total_item_count', 'platform_key']
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

