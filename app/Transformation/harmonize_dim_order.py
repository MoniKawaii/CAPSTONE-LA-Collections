"""
Order Dimension Harmonization Script
Maps Lazada order data from raw JSON files to the standardized dimensional model

Data Sources:
- lazada_orders_raw.json 
- lazada_multiple_order_items_raw.json

Target Schema: Dim_Order table structure
"""

import pandas as pd
import json
import os
import sys
from datetime import datetime

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import get_empty_dataframe, LAZADA_TO_UNIFIED_MAPPING, apply_data_types

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
        'shipping_city': shipping_city,
        'platform_key': None  # Lazada is always platform_key 1, Shopee is 2
    }
    
    return harmonized_record

def harmonize_dim_order():
    """
    Main function to harmonize Lazada order data into dimensional model
    
    Returns:
        pd.DataFrame: Harmonized order dimension table
    """
    print("🔄 Starting Order Dimension Harmonization...")
    
    # Get empty DataFrame with proper structure
    dim_order_df = get_empty_dataframe('dim_order')
    print(f"📋 Target schema: {list(dim_order_df.columns)}")
    
    # Load raw data
    orders_data, order_items_data = load_lazada_orders()
    
    # Combine all order data and deduplicate by order_id
    all_orders = {}
    
    # Process lazada_orders_raw.json
    for order in orders_data:
        order_id = str(order.get('order_id', ''))
        if order_id and order_id not in all_orders:
            harmonized = harmonize_order_record(order, 'orders')
            all_orders[order_id] = harmonized
    
    # Process lazada_multiple_order_items_raw.json (supplement missing orders)
    for order in order_items_data:
        order_id = str(order.get('order_id', ''))
        if order_id and order_id not in all_orders:
            harmonized = harmonize_order_record(order, 'order_items')
            all_orders[order_id] = harmonized
        # Note: if order exists, we keep the one from orders_raw.json as it's more complete
    
    # Convert to DataFrame
    if all_orders:
        orders_list = list(all_orders.values())
        dim_order_df = pd.DataFrame(orders_list)
        
        # Generate surrogate keys (orders_key)
        dim_order_df['orders_key'] = range(1, len(dim_order_df) + 1)
        
        # Add platform_key for Lazada (always 1)
        dim_order_df['platform_key'] = 1
        
        # Apply proper data types according to schema
        dim_order_df = apply_data_types(dim_order_df, 'dim_order')
        
        print(f"✅ Harmonized {len(dim_order_df)} orders")
        print(f"📊 Data Summary:")
        print(f"   - Total orders: {len(dim_order_df)}")
        print(f"   - Orders with dates: {len(dim_order_df[dim_order_df['order_date'].notna()])}")
        print(f"   - Orders with prices: {len(dim_order_df[dim_order_df['price_total'].notna()])}")
        print(f"   - Unique order statuses: {dim_order_df['order_status'].nunique()}")
        print(f"   - Unique payment methods: {dim_order_df['payment_method'].nunique()}")
        
        # Show sample of data
        print("\n📋 Sample of harmonized data:")
        sample_cols = ['orders_key', 'platform_order_id', 'order_status', 'order_date', 'price_total', 'total_item_count']
        available_cols = [col for col in sample_cols if col in dim_order_df.columns]
        print(dim_order_df[available_cols].head(3).to_string(index=False))
        
        # Show order status distribution
        print(f"\n📊 Order Status Distribution:")
        status_counts = dim_order_df['order_status'].value_counts()
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
        
    # Display mapping used
    print(f"\n🔗 Field mappings used:")
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
        print(f"   {lazada_field} → {unified_field}")
