"""
Harmonize Fact Orders from Lazada and Shopee API Response to Unified Schema
============================================================================

This module processes Lazada and Shopee order line items and harmonizes them into the Fact_Orders table.
The fact table contains one record per order line item (product within an order).

Key Requirements:
- Uses orders_key from Dim_Order (foreign key relationship)
- Each order can have multiple line items (multiple products)
- Creates individual records for each order item unit
- Maps platform-specific fields to unified schema

Field Mappings for Fact_Orders:
- order_item_key: Generated unique identifier for each line item
- orders_key: Foreign key to Dim_Order.orders_key (matched by platform_order_id)
- product_key: Foreign key to Dim_Product.product_key (matched by product_item_id)
- product_variant_key: Foreign key to Dim_Product_Variant (matched by variant_sku)
- time_key: Foreign key to Dim_Time (derived from order_date, format YYYYMMDD)
- customer_key: Foreign key to Dim_Customer (matched by platform_customer_id)
- platform_key: Always 1 for Lazada, 2 for Shopee
- item_quantity: Always 1 for individual line items (each record represents one unit)
- paid_price: Revenue for this individual line item (Lazada: paid_price, Shopee: model_discounted_price)
- original_unit_price: Non-discounted price per unit (Lazada: item_price, Shopee: model_original_price)
- voucher_platform_amount: Platform voucher amount (Shopee: voucher_absorbed_by_shopee)
- voucher_seller_amount: Seller voucher amount (Shopee: voucher_absorbed_by_seller)
- shipping_fee_paid_by_buyer: Shipping fee for this line item (Lazada: shipping_amount, Shopee: actual_shipping_fee)
"""

import pandas as pd
import numpy as np
from datetime import datetime, date
import json
import os
import sys

# Add the parent directory to sys.path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import (
    FACT_ORDERS_COLUMNS, 
    DIM_ORDER_COLUMNS,
    LAZADA_TO_UNIFIED_MAPPING,
    SHOPEE_TO_UNIFIED_MAPPING,
    COLUMN_DATA_TYPES,
    get_empty_dataframe,
    apply_data_types
)


def load_lazada_orders_raw():
    """Load raw Lazada orders from JSON file"""
    json_path = os.path.join(os.path.dirname(__file__), '..', 'Staging', 'lazada_orders_raw.json')
    
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            orders_data = json.load(f)
        print(f"✓ Loaded {len(orders_data)} raw Lazada orders")
        return orders_data
    except FileNotFoundError:
        print(f"❌ File not found: {json_path}")
        return []
    except json.JSONDecodeError as e:
        print(f"❌ Error parsing JSON: {e}")
        return []


def load_lazada_order_items_raw():
    """Load raw Lazada order items from JSON file"""
    json_path = os.path.join(os.path.dirname(__file__), '..', 'Staging', 'lazada_multiple_order_items_raw.json')
    
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            order_items_data = json.load(f)
        print(f"✓ Loaded {len(order_items_data)} raw Lazada order item records")
        return order_items_data
    except FileNotFoundError:
        print(f"❌ File not found: {json_path}")
        return []
    except json.JSONDecodeError as e:
        print(f"❌ Error parsing JSON: {e}")
        return []


def load_shopee_orders_raw():
    """Load raw Shopee orders from JSON file"""
    json_path = os.path.join(os.path.dirname(__file__), '..', 'Staging', 'shopee_orders_raw.json')
    
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            orders_data = json.load(f)
        print(f"✓ Loaded {len(orders_data)} raw Shopee orders")
        return orders_data
    except FileNotFoundError:
        print(f"❌ File not found: {json_path}")
        return []
    except json.JSONDecodeError as e:
        print(f"❌ Error parsing JSON: {e}")
        return []


def load_dim_order():
    """Load the harmonized Dim_Order table to get orders_key mappings"""
    csv_path = os.path.join(os.path.dirname(__file__), '..', 'Transformed', 'dim_order.csv')
    
    try:
        dim_order_df = pd.read_csv(csv_path)
        print(f"✓ Loaded {len(dim_order_df)} records from Dim_Order")
        return dim_order_df
    except FileNotFoundError:
        print(f"❌ File not found: {csv_path}")
        print("❌ Please run harmonize_dim_order.py first to generate dim_order.csv")
        return pd.DataFrame(columns=DIM_ORDER_COLUMNS)


def load_dim_customer():
    """Load the harmonized Dim_Customer table to get customer_key mappings"""
    csv_path = os.path.join(os.path.dirname(__file__), '..', 'Transformed', 'dim_customer.csv')
    
    try:
        dim_customer_df = pd.read_csv(csv_path)
        print(f"✓ Loaded {len(dim_customer_df)} records from Dim_Customer")
        return dim_customer_df
    except FileNotFoundError:
        print(f"❌ File not found: {csv_path}")
        print("❌ Please run harmonize_dim_customer.py first to generate dim_customer.csv")
        return pd.DataFrame()


def load_dim_product():
    """Load the harmonized Dim_Product table to get product_key mappings"""
    csv_path = os.path.join(os.path.dirname(__file__), '..', 'Transformed', 'dim_product.csv')
    
    try:
        dim_product_df = pd.read_csv(csv_path)
        print(f"✓ Loaded {len(dim_product_df)} records from Dim_Product")
        return dim_product_df
    except FileNotFoundError:
        print(f"❌ File not found: {csv_path}")
        print("❌ Please run harmonize_dim_product.py first to generate dim_product.csv")
        return pd.DataFrame()


def load_dim_product_variant():
    """Load the harmonized Dim_Product_Variant table to get variant_key mappings"""
    csv_path = os.path.join(os.path.dirname(__file__), '..', 'Transformed', 'dim_product_variant.csv')
    
    try:
        dim_variant_df = pd.read_csv(csv_path)
        # Rename to match fact table column name
        dim_variant_df = dim_variant_df.rename(columns={'variant_key': 'product_variant_key'})
        print(f"✓ Loaded {len(dim_variant_df)} records from Dim_Product_Variant")
        return dim_variant_df
    except FileNotFoundError:
        print(f"❌ File not found: {csv_path}")
        print("❌ Please run harmonize_dim_product.py first to generate dim_product_variant.csv")
        return pd.DataFrame()


def generate_time_key(date_string):
    """
    Convert date string to time_key format (YYYYMMDD)
    
    Args:
        date_string (str): Date in format "2022-10-05 21:02:52 +0800"
        
    Returns:
        int: Time key in format YYYYMMDD (e.g., 20221005)
    """
    try:
        # Parse the datetime string and extract date only
        dt = datetime.strptime(date_string.split(' ')[0], '%Y-%m-%d')
        time_key = int(dt.strftime('%Y%m%d'))
        return time_key
    except (ValueError, AttributeError, IndexError):
        print(f"⚠️ Warning: Could not parse date '{date_string}', using default")
        return 20220101  # Default fallback


def generate_platform_customer_id(first_name, phone):
    """
    Generate platform_customer_id using same logic as dim_customer
    to ensure proper foreign key relationship
    """
    import re
    
    def clean_name(name):
        if not name or len(name) < 2:
            return "XX"
        clean_name = re.sub(r'\*', '', str(name))
        if len(clean_name) >= 2:
            return clean_name[0].upper() + clean_name[-1].upper()
        elif len(clean_name) == 1:
            return clean_name[0].upper() + "X"
        else:
            return "XX"
    
    def extract_phone_digits(phone):
        if not phone:
            return "00", "00"
        digits = re.sub(r'[^\d]', '', str(phone))
        if len(digits) >= 4:
            return digits[:2], digits[-2:]
        elif len(digits) >= 2:
            return digits[:2], digits[:2]
        else:
            return "00", "00"
    
    name_chars = clean_name(first_name)
    first_2, last_2 = extract_phone_digits(phone)
    
    platform_customer_id = f"LZ{name_chars}{first_2}{last_2}"
    return platform_customer_id


def generate_shopee_platform_customer_id(buyer_username, phone):
    """
    Generate platform_customer_id for Shopee using same logic as dim_customer
    to ensure proper foreign key relationship
    """
    import re
    
    def clean_username(username):
        if not username or len(username) < 2:
            return "XX"
        clean_username = re.sub(r'\*', '', str(username))
        if len(clean_username) >= 2:
            return clean_username[0].upper() + clean_username[-1].upper()
        elif len(clean_username) == 1:
            return clean_username[0].upper() + "X"
        else:
            return "XX"
    
    def extract_phone_digits(phone):
        if not phone:
            return "00", "00"
        digits = re.sub(r'[^\d]', '', str(phone))
        if len(digits) >= 4:
            return digits[:2], digits[-2:]
        elif len(digits) >= 2:
            return digits[:2], digits[:2]
        else:
            return "00", "00"
    
    username_chars = clean_username(buyer_username)
    first_2, last_2 = extract_phone_digits(phone)
    
    platform_customer_id = f"SP{username_chars}{first_2}{last_2}"
    return platform_customer_id


def generate_time_key_from_timestamp(timestamp):
    """
    Convert Unix timestamp to time_key format (YYYYMMDD)
    
    Args:
        timestamp (int): Unix timestamp
        
    Returns:
        int: Time key in format YYYYMMDD (e.g., 20221005)
    """
    try:
        dt = datetime.fromtimestamp(timestamp)
        time_key = int(dt.strftime('%Y%m%d'))
        return time_key
    except (ValueError, TypeError, OSError):
        print(f"⚠️ Warning: Could not parse timestamp '{timestamp}', using default")
        return 20220101  # Default fallback


def extract_order_items_from_lazada(order_items_data, orders_data, dim_order_df, dim_customer_df, dim_product_df, dim_variant_df):
    """
    Extract and harmonize fact order records from Lazada order items
    Creates one record per individual order item (no aggregation)
    
    Args:
        order_items_data (list): Raw Lazada order items from API  
        orders_data (list): Raw Lazada orders from API (for customer info)
        dim_order_df (DataFrame): Harmonized dimension order table
        dim_customer_df (DataFrame): Harmonized dimension customer table
        dim_product_df (DataFrame): Harmonized dimension product table
        dim_variant_df (DataFrame): Harmonized dimension product variant table
        
    Returns:
        DataFrame: Harmonized fact orders records (one per order item)
    """
    fact_orders_records = []
    
    # Create lookup dictionaries for foreign key mapping
    order_key_lookup = dict(zip(
        dim_order_df['platform_order_id'].astype(str), 
        dim_order_df['orders_key']
    )) if not dim_order_df.empty else {}
    
    customer_key_lookup = dict(zip(
        dim_customer_df['platform_customer_id'].astype(str),
        dim_customer_df['customer_key']
    )) if not dim_customer_df.empty else {}
    
    product_key_lookup = dict(zip(
        dim_product_df['product_item_id'].astype(str),
        dim_product_df['product_key']
    )) if not dim_product_df.empty else {}
    
    variant_key_lookup = dict(zip(
        dim_variant_df['platform_sku_id'].astype(str),
        dim_variant_df['product_variant_key']
    )) if not dim_variant_df.empty else {}
    
    # Create orders lookup for customer info (order_id -> customer info)
    orders_customer_lookup = {}
    for order in orders_data:
        order_id = str(order.get('order_id', ''))
        address_shipping = order.get('address_shipping', {})
        orders_customer_lookup[order_id] = {
            'first_name': address_shipping.get('first_name', ''),
            'phone': address_shipping.get('phone', '')
        }
    
    print(f"📊 Created customer lookup for {len(orders_customer_lookup)} orders")
    
    order_item_key_counter = 1
    
    for order_record in order_items_data:
        try:
            platform_order_id = str(order_record.get('order_id', ''))
            order_items = order_record.get('order_items', [])
            
            # Get orders_key from dimension table
            orders_key = order_key_lookup.get(platform_order_id)
            if orders_key is None:
                print(f"⚠️ Warning: Could not find orders_key for order_id {platform_order_id}")
                continue
            
            # Get customer info from orders lookup
            customer_info = orders_customer_lookup.get(platform_order_id, {})
            first_name = customer_info.get('first_name', '')
            phone = customer_info.get('phone', '')
            platform_customer_id = generate_platform_customer_id(first_name, phone)
            customer_key = customer_key_lookup.get(platform_customer_id)
            
            # Debug customer key lookup for the first few records
            if order_item_key_counter <= 3:
                print(f"🔍 Debug order {platform_order_id}:")
                print(f"   - first_name: '{first_name}'")
                print(f"   - phone: '{phone}'")
                print(f"   - generated platform_customer_id: '{platform_customer_id}'")
                print(f"   - found customer_key: {customer_key}")
                if customer_key is None:
                    # Show some sample customer IDs to help debug
                    sample_customers = list(customer_key_lookup.keys())[:5]
                    print(f"   - Sample customer IDs in lookup: {sample_customers}")
            
            if customer_key is None:
                print(f"⚠️ Warning: Could not find customer_key for platform_customer_id '{platform_customer_id}' in order {platform_order_id}")
                continue  # Skip records where we can't find the customer
            
            # Create one fact record per individual order item (no grouping/aggregation)
            for item in order_items:
                try:
                    # Generate time_key from order creation date
                    time_key = generate_time_key(item.get('created_at', ''))
                    
                    # Get product_key from item_id (match product_item_id from products)
                    product_key = None
                    item_name = item.get('name', '')
                    item_id = str(item.get('item_id', ''))
                    
                    # First try direct item_id match
                    if item_id and item_id in product_key_lookup:
                        product_key = product_key_lookup[item_id]
                    
                    # If not found, try to find product by name match
                    if product_key is None and not dim_product_df.empty and item_name:
                        import re
                        escaped_name = re.escape(item_name[:50])
                        name_match = dim_product_df[dim_product_df['product_name'].str.contains(escaped_name, case=False, na=False)]
                        if not name_match.empty:
                            product_key = name_match.iloc[0]['product_key']
                    
                    # Skip records without valid product_key
                    if product_key is None:
                        if order_item_key_counter <= 3:
                            print(f"⚠️ Warning: Could not find product_key for item '{item_name}' (item_id: {item_id})")
                        continue
                    
                    # Get product_variant_key from sku_id
                    sku_id = str(item.get('sku_id', ''))
                    product_variant_key = variant_key_lookup.get(sku_id)
                    
                    # Create individual fact record for this order item
                    fact_record = {
                        'order_item_key': f"OI{order_item_key_counter:08d}",  # Generate unique key
                        'orders_key': orders_key,
                        'product_key': product_key,  # Should not be None due to earlier check
                        'product_variant_key': product_variant_key if product_variant_key else None,
                        'time_key': time_key,
                        'customer_key': customer_key,  # Should not be None due to earlier check
                        'platform_key': 1,  # Always 1 for Lazada
                        'item_quantity': 1,  # Always 1 for individual line items
                        'paid_price': float(item.get('paid_price', 0.0)),  # Individual item price
                        'original_unit_price': float(item.get('item_price', 0.0)),
                        'voucher_platform_amount': float(item.get('voucher_platform', 0.0)),
                        'voucher_seller_amount': float(item.get('voucher_seller', 0.0)),
                        'shipping_fee_paid_by_buyer': float(item.get('shipping_amount', 0.0))
                    }
                    
                    fact_orders_records.append(fact_record)
                    order_item_key_counter += 1
                    
                except Exception as e:
                    print(f"⚠️ Error processing item in order {platform_order_id}: {e}")
                    continue
                
        except Exception as e:
            print(f"⚠️ Error processing order {order_record.get('order_id', 'unknown')}: {e}")
            continue
    
    # Create DataFrame 
    fact_orders_df = pd.DataFrame(fact_orders_records, columns=FACT_ORDERS_COLUMNS)
    
    # Apply data type conversions
    if len(fact_orders_df) > 0:
        data_types = COLUMN_DATA_TYPES.get('fact_orders', {})
        for column, dtype in data_types.items():
            if column in fact_orders_df.columns:
                try:
                    if dtype == 'float64':
                        # Handle NaN values for float columns
                        fact_orders_df[column] = pd.to_numeric(fact_orders_df[column], errors='coerce').fillna(0.0)
                    elif dtype == 'int':
                        # Handle NaN values for int columns  
                        fact_orders_df[column] = pd.to_numeric(fact_orders_df[column], errors='coerce').fillna(0).astype('int')
                    else:
                        fact_orders_df[column] = fact_orders_df[column].astype(dtype)
                except Exception as e:
                    print(f"⚠️ Warning: Could not convert {column} to {dtype}: {e}")
    
    return fact_orders_df


def extract_order_items_from_shopee(orders_data, dim_order_df, dim_customer_df, dim_product_df, dim_variant_df, order_item_key_counter_start=1):
    """
    Extract and harmonize fact order records from Shopee orders
    Creates one record per individual order item (no aggregation)
    
    Args:
        orders_data (list): Raw Shopee orders from API (includes item_list)
        dim_order_df (DataFrame): Harmonized dimension order table
        dim_customer_df (DataFrame): Harmonized dimension customer table
        dim_product_df (DataFrame): Harmonized dimension product table
        dim_variant_df (DataFrame): Harmonized dimension product variant table
        order_item_key_counter_start (int): Starting counter for order_item_key
        
    Returns:
        DataFrame: Harmonized fact orders records (one per order item)
    """
    fact_orders_records = []
    
    # Create lookup dictionaries for foreign key mapping
    order_key_lookup = dict(zip(
        dim_order_df['platform_order_id'].astype(str), 
        dim_order_df['orders_key']
    )) if not dim_order_df.empty else {}
    
    customer_key_lookup = dict(zip(
        dim_customer_df['platform_customer_id'].astype(str),
        dim_customer_df['customer_key']
    )) if not dim_customer_df.empty else {}
    
    product_key_lookup = dict(zip(
        dim_product_df['product_item_id'].astype(str),
        dim_product_df['product_key']
    )) if not dim_product_df.empty else {}
    
    variant_key_lookup = dict(zip(
        dim_variant_df['platform_sku_id'].astype(str),
        dim_variant_df['product_variant_key']
    )) if not dim_variant_df.empty else {}
    
    print(f"📊 Processing Shopee orders with item lists...")
    
    order_item_key_counter = order_item_key_counter_start
    
    for order in orders_data:
        try:
            platform_order_id = str(order.get('order_sn', ''))
            item_list = order.get('item_list', [])
            
            # Get orders_key from dimension table
            orders_key = order_key_lookup.get(platform_order_id)
            if orders_key is None:
                print(f"⚠️ Warning: Could not find orders_key for order_sn {platform_order_id}")
                continue
            
            # Get customer info from order
            buyer_username = order.get('buyer_username', '')
            recipient_address = order.get('recipient_address', {})
            phone = recipient_address.get('phone', '') if isinstance(recipient_address, dict) else ''
            
            platform_customer_id = generate_shopee_platform_customer_id(buyer_username, phone)
            customer_key = customer_key_lookup.get(platform_customer_id)
            
            # Debug customer key lookup for the first few records
            if order_item_key_counter <= order_item_key_counter_start + 2:
                print(f"🔍 Debug Shopee order {platform_order_id}:")
                print(f"   - buyer_username: '{buyer_username}'")
                print(f"   - phone: '{phone}'")
                print(f"   - generated platform_customer_id: '{platform_customer_id}'")
                print(f"   - found customer_key: {customer_key}")
                if customer_key is None:
                    # Show some sample customer IDs to help debug
                    sample_customers = list(customer_key_lookup.keys())[:5]
                    print(f"   - Sample customer IDs in lookup: {sample_customers}")
            
            if customer_key is None:
                print(f"⚠️ Warning: Could not find customer_key for platform_customer_id '{platform_customer_id}' in order {platform_order_id}")
                continue  # Skip records where we can't find the customer
            
            # Get order creation time for time_key
            create_time = order.get('create_time')
            time_key = generate_time_key_from_timestamp(create_time) if create_time else 20220101
            
            # Create one fact record per individual order item
            for item in item_list:
                try:
                    # Get product_key from item_id
                    item_id = str(item.get('item_id', ''))
                    product_key = product_key_lookup.get(item_id)
                    
                    # Debug: Show what's available in first few iterations
                    if product_key is None and order_item_key_counter <= order_item_key_counter_start + 5:
                        print(f"⚠️ Warning: Could not find product_key for item_id {item_id}")
                        # Show sample of available product IDs (Shopee only)
                        shopee_products = [pid for pid in product_key_lookup.keys() if pid not in ['', 'nan']]
                        if shopee_products:
                            print(f"   Available Shopee product_item_ids (sample): {shopee_products[:5]}")
                        else:
                            print(f"   No Shopee products found in dim_product. Please run harmonize_dim_product.py first.")
                    
                    # Skip records without valid product_key
                    if product_key is None:
                        continue
                    
                    # Get product_variant_key from model_id
                    model_id = str(item.get('model_id', ''))
                    product_variant_key = variant_key_lookup.get(model_id)
                    
                    # Get item quantity
                    item_quantity = item.get('model_quantity_purchased', 1)
                    
                    # Get pricing information
                    model_discounted_price = float(item.get('model_discounted_price', 0.0))
                    model_original_price = float(item.get('model_original_price', 0.0))
                    
                    # Get voucher amounts
                    voucher_absorbed_by_seller = float(item.get('voucher_absorbed_by_seller', 0.0))
                    voucher_absorbed_by_shopee = float(item.get('voucher_absorbed_by_shopee', 0.0))
                    
                    # Get shipping fee (at order level, distribute across items)
                    actual_shipping_fee = float(order.get('actual_shipping_fee', 0.0))
                    shipping_per_item = actual_shipping_fee / len(item_list) if len(item_list) > 0 else 0.0
                    
                    # Create individual fact record for this order item
                    fact_record = {
                        'order_item_key': f"OI{order_item_key_counter:08d}",  # Generate unique key
                        'orders_key': orders_key,
                        'product_key': product_key,
                        'product_variant_key': product_variant_key if product_variant_key else None,
                        'time_key': time_key,
                        'customer_key': customer_key,
                        'platform_key': 2,  # Always 2 for Shopee
                        'item_quantity': item_quantity,
                        'paid_price': model_discounted_price,
                        'original_unit_price': model_original_price,
                        'voucher_platform_amount': voucher_absorbed_by_shopee,
                        'voucher_seller_amount': voucher_absorbed_by_seller,
                        'shipping_fee_paid_by_buyer': shipping_per_item
                    }
                    
                    fact_orders_records.append(fact_record)
                    order_item_key_counter += 1
                    
                except Exception as e:
                    print(f"⚠️ Error processing item in Shopee order {platform_order_id}: {e}")
                    continue
                
        except Exception as e:
            print(f"⚠️ Error processing Shopee order {order.get('order_sn', 'unknown')}: {e}")
            continue
    
    # Create DataFrame 
    fact_orders_df = pd.DataFrame(fact_orders_records, columns=FACT_ORDERS_COLUMNS)
    
    # Apply data type conversions
    if len(fact_orders_df) > 0:
        data_types = COLUMN_DATA_TYPES.get('fact_orders', {})
        for column, dtype in data_types.items():
            if column in fact_orders_df.columns:
                try:
                    if dtype == 'float64':
                        # Handle NaN values for float columns
                        fact_orders_df[column] = pd.to_numeric(fact_orders_df[column], errors='coerce').fillna(0.0)
                    elif dtype == 'int':
                        # Handle NaN values for int columns  
                        fact_orders_df[column] = pd.to_numeric(fact_orders_df[column], errors='coerce').fillna(0).astype('int')
                    else:
                        fact_orders_df[column] = fact_orders_df[column].astype(dtype)
                except Exception as e:
                    print(f"⚠️ Warning: Could not convert {column} to {dtype}: {e}")
    
    return fact_orders_df


def harmonize_fact_orders():
    """
    Main function to harmonize Fact Orders from Lazada and Shopee API response
    
    Returns:
        DataFrame: Harmonized fact orders table
    """
    print("🚀 Starting Fact Orders harmonization for Lazada and Shopee...")
    
    # Load Lazada source data
    print("\n📥 Loading Lazada data...")
    lazada_order_items_data = load_lazada_order_items_raw()
    lazada_orders_data = load_lazada_orders_raw()
    
    # Load Shopee source data
    print("\n📥 Loading Shopee data...")
    shopee_orders_data = load_shopee_orders_raw()
    
    # Load all dimension tables for foreign key relationships
    dim_order_df = load_dim_order()
    if dim_order_df.empty:
        print("❌ No Dim_Order data available - required for orders_key mapping")
        return get_empty_dataframe('fact_orders')
    
    dim_customer_df = load_dim_customer()
    if dim_customer_df.empty:
        print("⚠️ No Dim_Customer data available - customer_key will be NULL")
    
    dim_product_df = load_dim_product()  
    if dim_product_df.empty:
        print("⚠️ No Dim_Product data available - product_key will be NULL")
    else:
        # Check if we have products from both platforms
        lazada_products = len(dim_product_df[dim_product_df['platform_key'] == 1])
        shopee_products = len(dim_product_df[dim_product_df['platform_key'] == 2])
        print(f"📊 Loaded products: {lazada_products} Lazada, {shopee_products} Shopee")
        if shopee_products == 0 and shopee_orders_data:
            print("⚠️ WARNING: No Shopee products found in dim_product! Run harmonize_dim_product.py first.")
        
    dim_variant_df = load_dim_product_variant()
    if dim_variant_df.empty:
        print("⚠️ No Dim_Product_Variant data available - product_variant_key will be NULL")
    else:
        # Check if we have variants from both platforms
        lazada_variants = len(dim_variant_df[dim_variant_df['platform_key'] == 1])
        shopee_variants = len(dim_variant_df[dim_variant_df['platform_key'] == 2])
        print(f"📊 Loaded variants: {lazada_variants} Lazada, {shopee_variants} Shopee")
    
    # Process Lazada orders
    print("\n🔄 Processing Lazada order items...")
    lazada_fact_df = pd.DataFrame()
    if lazada_order_items_data and lazada_orders_data:
        lazada_fact_df = extract_order_items_from_lazada(
            lazada_order_items_data, lazada_orders_data, dim_order_df, dim_customer_df, dim_product_df, dim_variant_df
        )
        print(f"✅ Processed {len(lazada_fact_df)} Lazada fact order records")
    else:
        print("⚠️ No Lazada order data available")
    
    # Process Shopee orders
    print("\n🔄 Processing Shopee order items...")
    shopee_fact_df = pd.DataFrame()
    if shopee_orders_data:
        # Start Shopee counter after Lazada records
        shopee_start_counter = len(lazada_fact_df) + 1 if not lazada_fact_df.empty else 1
        shopee_fact_df = extract_order_items_from_shopee(
            shopee_orders_data, dim_order_df, dim_customer_df, dim_product_df, dim_variant_df, shopee_start_counter
        )
        print(f"✅ Processed {len(shopee_fact_df)} Shopee fact order records")
    else:
        print("⚠️ No Shopee order data available")
    
    # Combine both platforms
    fact_orders_df = pd.concat([lazada_fact_df, shopee_fact_df], ignore_index=True)
    
    print(f"\n✅ Successfully harmonized {len(fact_orders_df)} total fact order records")
    print(f"   - Lazada records: {len(lazada_fact_df)}")
    print(f"   - Shopee records: {len(shopee_fact_df)}")
    print(f"📊 Data shape: {fact_orders_df.shape}")
    
    if len(fact_orders_df) > 0:
        print("\n📋 Sample Lazada records:")
        lazada_sample = fact_orders_df[fact_orders_df['platform_key'] == 1]
        if not lazada_sample.empty:
            print(lazada_sample.head(2).to_string(index=False))
        
        print("\n📋 Sample Shopee records:")
        shopee_sample = fact_orders_df[fact_orders_df['platform_key'] == 2]
        if not shopee_sample.empty:
            print(shopee_sample.head(2).to_string(index=False))
        
        print(f"\n📈 Summary Statistics:")
        print(f"   • Total Revenue: ${fact_orders_df['paid_price'].sum():.2f}")
        print(f"   • Lazada Revenue: ${fact_orders_df[fact_orders_df['platform_key'] == 1]['paid_price'].sum():.2f}")
        print(f"   • Shopee Revenue: ${fact_orders_df[fact_orders_df['platform_key'] == 2]['paid_price'].sum():.2f}")
        print(f"   • Total Items: {fact_orders_df['item_quantity'].sum():,}")
        print(f"   • Average Item Quantity: {fact_orders_df['item_quantity'].mean():.1f}")
        print(f"   • Average Unit Price: ${fact_orders_df['original_unit_price'].mean():.2f}")
        print(f"   • Date Range: {fact_orders_df['time_key'].min()} to {fact_orders_df['time_key'].max()}")
        
        # Foreign key relationship statistics
        print(f"\n🔗 Foreign Key Coverage:")
        print(f"   • Orders with valid orders_key: {fact_orders_df['orders_key'].notna().sum()} / {len(fact_orders_df)}")
        print(f"   • Orders with valid customer_key: {fact_orders_df['customer_key'].notna().sum()} / {len(fact_orders_df)}")
        print(f"   • Orders with valid product_key: {fact_orders_df['product_key'].notna().sum()} / {len(fact_orders_df)}")
        print(f"   • Orders with valid product_variant_key: {fact_orders_df['product_variant_key'].notna().sum()} / {len(fact_orders_df)}")
    
    return fact_orders_df


def save_fact_orders(df, filename='fact_orders.csv'):
    """
    Save harmonized fact orders to CSV file
    
    Args:
        df (DataFrame): Harmonized fact orders dataframe
        filename (str): Output filename
    """
    if df.empty:
        print("⚠️ No data to save")
        return
        
    output_path = os.path.join(os.path.dirname(__file__), '..', 'Transformed', filename)
    
    try:
        df.to_csv(output_path, index=False)
        print(f"✅ Fact Orders saved to: {output_path}")
        print(f"📁 Records saved: {len(df)}")
    except Exception as e:
        print(f"❌ Error saving file: {e}")


if __name__ == "__main__":
    # Run the harmonization
    fact_orders_df = harmonize_fact_orders()
    
    # Save results
    if not fact_orders_df.empty:
        save_fact_orders(fact_orders_df)
    else:
        print("❌ No data to save")