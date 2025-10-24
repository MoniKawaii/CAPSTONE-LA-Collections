"""
Order Dimension Harmonization Script - Multi-Platform (Lazada & Shopee)
Maps order data from both Lazada and Shopee raw JSON files to the standardized dimensional model

Data Sources:
Lazada:
- lazada_orders_raw.json 
- lazada_multiple_order_items_raw.json

Shopee:
- shopee_orders_raw.json
- shopee_multiple_order_items_raw.json

Target Schema: Dim_Order table structure with platform_key (1=Lazada, 2=Shopee)
"""

import pandas as pd
import json
import os
import sys
from datetime import datetime

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import get_empty_dataframe, LAZADA_TO_UNIFIED_MAPPING, SHOPEE_TO_UNIFIED_MAPPING, apply_data_types


def load_orders_raw(platform='lazada'):
    """
    Load order data from raw JSON files for specified platform
    
    Args:
        platform (str): 'lazada' or 'shopee'
        
    Returns:
        tuple: (orders_data, order_items_data) as lists of dictionaries
    """
    staging_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'Staging')
    
    # Define file names based on platform
    orders_file = os.path.join(staging_dir, f'{platform}_orders_raw.json')
    order_items_file = os.path.join(staging_dir, f'{platform}_multiple_order_items_raw.json')
    
    orders_data = []
    order_items_data = []
    
    if os.path.exists(orders_file):
        with open(orders_file, 'r', encoding='utf-8') as f:
            orders_data = json.load(f)
        print(f"✅ Loaded {len(orders_data)} orders from {platform}_orders_raw.json")
    else:
        print(f"⚠️ File not found: {orders_file}")
    
    if os.path.exists(order_items_file):
        with open(order_items_file, 'r', encoding='utf-8') as f:
            order_items_data = json.load(f)
        print(f"✅ Loaded {len(order_items_data)} order items from {platform}_multiple_order_items_raw.json")
    else:
        print(f"⚠️ File not found: {order_items_file}")
    
    return orders_data, order_items_data


def load_lazada_orders():
    """
    Load Lazada order data from raw JSON files (legacy function)
    
    Returns:
        tuple: (orders_data, order_items_data) as lists of dictionaries
    """
    return load_orders_raw('lazada')
    
    return orders_data, order_items_data

def extract_order_status(statuses_array, platform='lazada'):
    """
    Extract order status from statuses array (Lazada) or direct field (Shopee)
    
    Args:
        statuses_array: List of status objects (Lazada) or string (Shopee)
        platform (str): 'lazada' or 'shopee'
        
    Returns:
        str: Order status or empty string
    """
    if platform == 'lazada':
        if not statuses_array or len(statuses_array) == 0:
            return ''
        
        # Get status from first element (index 0)
        first_status = statuses_array[0]
        if isinstance(first_status, dict):
            return first_status.get('status', '')
        return str(first_status)
    
    elif platform == 'shopee':
        # Shopee uses order_status field directly
        if isinstance(statuses_array, str):
            return statuses_array
        return ''
    
    return ''


def parse_date_to_date_only(date_value, platform='lazada'):
    """
    Convert datetime string or Unix timestamp to date only (YYYY-MM-DD)
    
    Args:
        date_value: Date string (Lazada) or Unix timestamp (Shopee)
        platform (str): 'lazada' or 'shopee'
        
    Returns:
        str: Date in YYYY-MM-DD format or None
    """
    if not date_value:
        return None
    
    try:
        if platform == 'lazada':
            # Parse the datetime string and extract date only
            dt = pd.to_datetime(date_value)
            return dt.date()
        
        elif platform == 'shopee':
            # Convert Unix timestamp to date
            dt = datetime.fromtimestamp(date_value)
            return dt.date()
    
    except (ValueError, TypeError, OSError):
        print(f"Warning: Could not parse date: {date_value}")
        return None


def extract_shipping_city(address_data, platform='lazada'):
    """
    Extract shipping city from address object
    
    Args:
        address_data (dict): Shipping address object
        platform (str): 'lazada' or 'shopee'
        
    Returns:
        str: City name or empty string
    """
    if not address_data or not isinstance(address_data, dict):
        return ''
    
    if platform == 'lazada':
        return address_data.get('city', '')
    elif platform == 'shopee':
        # Shopee uses recipient_address with city field
        return address_data.get('city', '')
    
    return ''


def harmonize_order_record(order_data, source_file, platform='lazada'):
    """
    Harmonize a single order record from platform format to dimensional model
    
    Args:
        order_data (dict): Raw order data from platform API
        source_file (str): Source file identifier ('orders' or 'order_items')
        platform (str): 'lazada' or 'shopee'
        
    Returns:
        dict: Harmonized order record
    """
    if platform == 'lazada':
        # Extract order status from statuses array (index 0)
        order_status = extract_order_status(order_data.get('statuses', []), platform='lazada')
        
        # Convert dates to date only format
        order_date = parse_date_to_date_only(order_data.get('created_at'), platform='lazada')
        updated_at = parse_date_to_date_only(order_data.get('updated_at'), platform='lazada')
        
        # Extract shipping city
        shipping_city = extract_shipping_city(order_data.get('address_shipping', {}), platform='lazada')
        
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
            'shipping_city': shipping_city,
            'platform_key': 1  # Lazada = 1
        }
    
    elif platform == 'shopee':
        # Extract order status (direct field in Shopee)
        order_status = extract_order_status(order_data.get('order_status', ''), platform='shopee')
        
        # Convert Unix timestamps to date
        order_date = parse_date_to_date_only(order_data.get('create_time'), platform='shopee')
        updated_at = parse_date_to_date_only(order_data.get('update_time'), platform='shopee')
        
        # Extract shipping city from recipient_address
        shipping_city = extract_shipping_city(order_data.get('recipient_address', {}), platform='shopee')
        
        # Convert price - Shopee uses total_amount in cents
        price_total = None
        total_amount = order_data.get('total_amount')
        if total_amount:
            try:
                price_total = float(total_amount) / 100  # Convert from cents
            except (ValueError, TypeError):
                price_total = None
        
        # Count items from item_list
        item_count = len(order_data.get('item_list', []))
        
        # Map using SHOPEE_TO_UNIFIED_MAPPING structure
        harmonized_record = {
            'orders_key': None,  # Will be generated as surrogate key
            'platform_order_id': str(order_data.get('order_sn', '')),  # Shopee uses order_sn
            'order_status': order_status,
            'order_date': order_date,
            'updated_at': updated_at,
            'price_total': price_total,
            'total_item_count': item_count,
            'payment_method': order_data.get('payment_method', ''),
            'shipping_city': shipping_city,
            'platform_key': 2  # Shopee = 2
        }
    
    else:
        raise ValueError(f"Unsupported platform: {platform}")
    
    return harmonized_record


def harmonize_dim_order():
    """
    Main function to harmonize order data from both Lazada and Shopee into dimensional model
    
    Returns:
        pd.DataFrame: Harmonized order dimension table
    """
    print("🔄 Starting Order Dimension Harmonization (Multi-Platform)...")
    
    # Get empty DataFrame with proper structure
    dim_order_df = get_empty_dataframe('dim_order')
    print(f"📋 Target schema: {list(dim_order_df.columns)}")
    
    # Combine all order data and deduplicate by order_id
    all_orders = {}
    
    # Process Lazada orders
    print("\n📦 Processing Lazada orders...")
    lazada_orders_data, lazada_order_items_data = load_orders_raw('lazada')
    
    if lazada_orders_data or lazada_order_items_data:
        # Process lazada_orders_raw.json
        for order in lazada_orders_data:
            order_id = str(order.get('order_id', ''))
            if order_id and order_id not in all_orders:
                harmonized = harmonize_order_record(order, 'orders', platform='lazada')
                all_orders[order_id] = harmonized
        
        # Process lazada_multiple_order_items_raw.json (supplement missing orders)
        for order in lazada_order_items_data:
            order_id = str(order.get('order_id', ''))
            if order_id and order_id not in all_orders:
                harmonized = harmonize_order_record(order, 'order_items', platform='lazada')
                all_orders[order_id] = harmonized
        
        print(f"   ✓ Processed {len([o for o in all_orders.values() if o['platform_key'] == 1])} Lazada orders")
    else:
        print("   ⚠️ No Lazada order data found")
    
    # Process Shopee orders
    print("\n🛍️ Processing Shopee orders...")
    shopee_orders_data, shopee_order_items_data = load_orders_raw('shopee')
    
    if shopee_orders_data or shopee_order_items_data:
        # Process shopee_orders_raw.json
        for order in shopee_orders_data:
            order_sn = str(order.get('order_sn', ''))
            if order_sn and order_sn not in all_orders:
                harmonized = harmonize_order_record(order, 'orders', platform='shopee')
                all_orders[order_sn] = harmonized
        
        # Process shopee_multiple_order_items_raw.json (supplement missing orders)
        for order in shopee_order_items_data:
            order_sn = str(order.get('order_sn', ''))
            if order_sn and order_sn not in all_orders:
                harmonized = harmonize_order_record(order, 'order_items', platform='shopee')
                all_orders[order_sn] = harmonized
        
        print(f"   ✓ Processed {len([o for o in all_orders.values() if o['platform_key'] == 2])} Shopee orders")
    else:
        print("   ⚠️ No Shopee order data found")
    
    # Convert to DataFrame
    if all_orders:
        orders_list = list(all_orders.values())
        dim_order_df = pd.DataFrame(orders_list)
        
        # Generate surrogate keys (orders_key)
        dim_order_df['orders_key'] = range(1, len(dim_order_df) + 1)
        
        # Platform key is already set in harmonize_order_record
        
        # Apply proper data types according to schema
        dim_order_df = apply_data_types(dim_order_df, 'dim_order')
        
        print(f"\n✅ Harmonized {len(dim_order_df)} orders from both platforms")
        print(f"\n📊 Data Summary by Platform:")
        lazada_count = len(dim_order_df[dim_order_df['platform_key'] == 1])
        shopee_count = len(dim_order_df[dim_order_df['platform_key'] == 2])
        print(f"   • Lazada orders: {lazada_count}")
        print(f"   • Shopee orders: {shopee_count}")
        print(f"   • Total orders: {len(dim_order_df)}")
        
        print(f"\n📊 Additional Metrics:")
        print(f"   • Orders with dates: {len(dim_order_df[dim_order_df['order_date'].notna()])}")
        print(f"   • Orders with prices: {len(dim_order_df[dim_order_df['price_total'].notna()])}")
        print(f"   • Unique order statuses: {dim_order_df['order_status'].nunique()}")
        print(f"   • Unique payment methods: {dim_order_df['payment_method'].nunique()}")
        
        # Show sample of data
        print("\n📋 Sample of harmonized data (both platforms):")
        sample_cols = ['orders_key', 'platform_order_id', 'order_status', 'order_date', 'price_total', 'total_item_count', 'platform_key']
        available_cols = [col for col in sample_cols if col in dim_order_df.columns]
        print(dim_order_df[available_cols].head(5).to_string(index=False))
        
        # Show order status distribution by platform
        print(f"\n📊 Order Status Distribution:")
        status_counts = dim_order_df['order_status'].value_counts()
        for status, count in status_counts.head(10).items():
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
    print("=" * 70)
    print("🚀 Multi-Platform Order Harmonization (Lazada & Shopee)")
    print("=" * 70)
    
    # Run the harmonization process
    harmonized_df = harmonize_dim_order()
    
    if not harmonized_df.empty:
        output_file = save_harmonized_orders(harmonized_df)
        print(f"\n🎉 Order dimension harmonization completed successfully!")
        print(f"📁 Output file: {output_file}")
        print(f"\n✅ This dimension includes data from BOTH Lazada and Shopee platforms")
    else:
        print("❌ No data was harmonized. Please check your source files.")
        print("   Expected files:")
        print("   • lazada_orders_raw.json")
        print("   • lazada_multiple_order_items_raw.json")
        print("   • shopee_orders_raw.json")
        print("   • shopee_multiple_order_items_raw.json")
        
    # Display mapping used
    print(f"\n🔗 Field mappings:")
    print(f"\n   Lazada → Unified:")
    order_mappings = {
        'order_id': 'platform_order_id',
        'statuses[0]': 'order_status',
        'created_at': 'order_date', 
        'updated_at': 'updated_at',
        'price': 'price_total',
        'items_count': 'total_item_count',
        'payment_method': 'payment_method',
        'address_shipping.city': 'shipping_city'
    }
    for lazada_field, unified_field in order_mappings.items():
        print(f"      {lazada_field} → {unified_field}")
    
    print(f"\n   Shopee → Unified:")
    shopee_mappings = {
        'order_sn': 'platform_order_id',
        'order_status': 'order_status',
        'create_time': 'order_date',
        'update_time': 'updated_at',
        'total_amount': 'price_total',
        'item_list.length': 'total_item_count',
        'payment_method': 'payment_method',
        'recipient_address.city': 'shipping_city'
    }
    for shopee_field, unified_field in shopee_mappings.items():
        print(f"      {shopee_field} → {unified_field}")
