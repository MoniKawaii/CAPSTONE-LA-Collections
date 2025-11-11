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


def load_shopee_payment_details_raw():
    """Load Shopee payment details from existing split files"""
    staging_dir = os.path.join(os.path.dirname(__file__), '..', 'Staging')
    
    # Load from the two existing files as requested
    file1_path = os.path.join(staging_dir, 'shopee_paymentdetail_raw.json')
    file2_path = os.path.join(staging_dir, 'shopee_paymentdetail_2_raw.json')
    
    payment_details = []
    
    # Load from both files
    for file_path in [file1_path, file2_path]:
        if os.path.exists(file_path):
            print(f"Loading {file_path}")
            try:
                with open(file_path, 'r', encoding='utf-8') as file:
                    data = json.load(file)
                    if isinstance(data, list):
                        payment_details.extend(data)
                    elif 'data' in data:
                        payment_details.extend(data['data'])
                    else:
                        print(f"⚠️ Unexpected data structure in {file_path}")
            except Exception as e:
                print(f"❌ Error loading {file_path}: {e}")
        else:
            print(f"⚠️ File not found: {file_path}")
    
    print(f"Loaded {len(payment_details)} payment detail records")
    
    # Create lookup dictionary by order_sn
    payment_lookup = {}
    for detail in payment_details:
        order_sn = detail.get('order_sn')
        if order_sn:
            payment_lookup[order_sn] = detail
    
    return payment_lookup


def load_dimension_lookups():
    """Load dimension tables from CSV files for key lookups - ONLY COMPLETED ORDERS"""
    transformed_dir = os.path.join(os.path.dirname(__file__), '..', 'Transformed')
    dim_lookups = {}
    
    # Load customer lookup
    customer_df = pd.read_csv(os.path.join(transformed_dir, 'dim_customer.csv'))
    dim_lookups['customer'] = dict(zip(customer_df['platform_customer_id'].astype(str), customer_df['customer_key']))
    
    # Load product lookup
    product_df = pd.read_csv(os.path.join(transformed_dir, 'dim_product.csv'))
    dim_lookups['product'] = dict(zip(product_df['product_item_id'].astype(str), product_df['product_key']))
    
    # Load order lookup - FILTER TO ONLY COMPLETED ORDERS
    order_df = pd.read_csv(os.path.join(transformed_dir, 'dim_order.csv'))
    completed_orders_df = order_df[order_df['order_status'] == 'COMPLETED'].copy()
    dim_lookups['order'] = dict(zip(completed_orders_df['platform_order_id'].astype(str), completed_orders_df['orders_key']))
    
    # Also create a price lookup for COMPLETED orders to ensure price consistency
    dim_lookups['order_prices'] = dict(zip(completed_orders_df['platform_order_id'].astype(str), completed_orders_df['price_total']))
    
    # Load product variant lookup
    variant_df = pd.read_csv(os.path.join(transformed_dir, 'dim_product_variant.csv'))
    # Use platform_sku_id for variant lookup
    dim_lookups['product_variant'] = dict(zip(variant_df['platform_sku_id'].astype(str), variant_df['product_variant_key']))
    
    print(f"Loaded dimension lookups (COMPLETED ORDERS ONLY):")
    print(f"  - Customers: {len(dim_lookups['customer'])}")
    print(f"  - Products: {len(dim_lookups['product'])}")
    print(f"  - Orders (COMPLETED): {len(dim_lookups['order'])} of {len(order_df)} total")
    print(f"  - Product Variants: {len(dim_lookups['product_variant'])}")
    
    return dim_lookups, variant_df


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


def generate_shopee_platform_customer_id(buyer_user_id, buyer_username=None, phone=None, order_sn=None, create_time=None):
    """
    Generate platform_customer_id for Shopee using buyer_user_id from API
    Must match logic in harmonize_dim_customer.py for foreign key relationship
    """
    import re
    
    # Use buyer_user_id if available AND not 0 (preferred method - raw value)
    if buyer_user_id is not None and buyer_user_id != 0:
        return str(buyer_user_id)
    
    # For buyer_user_id=0 (anonymous customers), use anonymous customer placeholder
    # The dimension table now includes an anonymous customer placeholder for these cases
    if buyer_user_id == 0:
        return "0"  # This matches the anonymous Shopee customer in dim_customer
    
    # Fallback to old method if buyer_user_id not available (should not happen for Shopee)
    return None  # Skip records without valid buyer_user_id


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


def extract_order_items_from_lazada(order_items_data, orders_data, dim_lookups, variant_df):
    """
    Extract and harmonize fact order records from Lazada order items
    Creates one record per individual order item (no aggregation)
    ONLY PROCESSES COMPLETED ORDERS from dim_order
    
    Args:
        order_items_data (list): Raw Lazada order items from API  
        orders_data (list): Raw Lazada orders from API (for customer info)
        dim_lookups (dict): Dictionary of dimension key lookups
        
    Returns:
        DataFrame: Harmonized fact orders records (one per order item)
    """
    fact_orders_records = []
    
    # Get lookup dictionaries from dim_lookups
    order_key_lookup = dim_lookups.get('order', {})  # Only contains COMPLETED orders
    order_prices_lookup = dim_lookups.get('order_prices', {})
    customer_key_lookup = dim_lookups.get('customer', {})
    product_key_lookup = dim_lookups.get('product', {})
    variant_key_lookup = dim_lookups.get('product_variant', {})
    
    # Build buyer_id lookup from order_items_data (same logic as dim_customer)
    buyer_id_lookup = {}
    for order in order_items_data:
        order_id = str(order.get('order_id', ''))
        order_items = order.get('order_items', [])
        if order_items and len(order_items) > 0:
            buyer_id = order_items[0].get('buyer_id')
            if buyer_id:
                buyer_id_lookup[order_id] = str(buyer_id)
    
    # Create orders lookup for customer info (order_id -> customer info)
    orders_customer_lookup = {}
    for order in orders_data:
        order_id = str(order.get('order_id', ''))
        address_shipping = order.get('address_shipping', {})
        orders_customer_lookup[order_id] = {
            'first_name': address_shipping.get('first_name', ''),
            'phone': address_shipping.get('phone', '')
        }
    
    print(f"📊 Created buyer_id lookup for {len(buyer_id_lookup)} orders")
    print(f"📊 Created customer lookup for {len(orders_customer_lookup)} orders")
    
    order_item_key_counter = 1
    
    for order_record in order_items_data:
        try:
            platform_order_id = str(order_record.get('order_id', ''))
            order_items = order_record.get('order_items', [])
            
            # Get orders_key from dimension table (only COMPLETED orders are in lookup)
            orders_key = order_key_lookup.get(platform_order_id)
            if orders_key is None:
                # Skip non-COMPLETED orders - they're not in the lookup
                continue
            
            # Get customer info from orders lookup
            customer_info = orders_customer_lookup.get(platform_order_id, {})
            first_name = customer_info.get('first_name', '')
            phone = customer_info.get('phone', '')
            
            # Use buyer_id from lookup if available, otherwise generate platform_customer_id
            buyer_id = buyer_id_lookup.get(platform_order_id)
            if buyer_id:
                platform_customer_id = buyer_id
            else:
                platform_customer_id = generate_platform_customer_id(first_name, phone)
                
            customer_key = customer_key_lookup.get(platform_customer_id)
            
            # Debug customer key lookup for the first few records
            if order_item_key_counter <= 3:
                print(f"🔍 Debug order {platform_order_id}:")
                print(f"   - buyer_id from lookup: {buyer_id}")
                print(f"   - first_name: '{first_name}'")
                print(f"   - phone: '{phone}'")
                print(f"   - platform_customer_id: '{platform_customer_id}'")
                print(f"   - found customer_key: {customer_key}")
                if customer_key is None:
                    # Show some sample customer IDs to help debug
                    sample_customers = list(customer_key_lookup.keys())[:5]
                    print(f"   - Sample customer IDs in lookup: {sample_customers}")
            
            if customer_key is None:
                # Use anonymous customer fallback for failed lookups
                anonymous_platform_customer_id = "ANONYMOUS_LAZADA"
                customer_key = customer_key_lookup.get(anonymous_platform_customer_id)
                
                if customer_key is None:
                    print(f"❌ ERROR: Anonymous Lazada customer not found in dimension table!")
                    continue  # Skip if even anonymous customer is missing
                    
                print(f"🔄 Using anonymous customer fallback for order {platform_order_id} (original platform_customer_id: '{platform_customer_id}')")
                # Update platform_customer_id to reflect the fallback used
                platform_customer_id = anonymous_platform_customer_id
            
            # Create one fact record per individual order item (no grouping/aggregation)
            for item in order_items:
                try:
                    # Generate time_key from order creation date
                    time_key = generate_time_key(item.get('created_at', ''))
                    
                    # Get product_key from item_id (match product_item_id from products)
                    # For Lazada: Extract product_item_id from SKU field (format: "1856947959-1664459645508-0")
                    sku = item.get('sku', '')
                    product_item_id = sku.split('-')[0] if sku and '-' in sku else ''
                    product_key = None
                    
                    # Try product_item_id lookup from SKU
                    if product_item_id and product_item_id in product_key_lookup:
                        product_key = product_key_lookup.get(product_item_id)
                    
                    # If not found in products table, try to find via variants table
                    if product_key is None:
                        # Try sku_id lookup in variants table
                        sku_id = str(item.get('sku_id', ''))
                        if sku_id in variant_key_lookup:
                            # Found in variants - get the associated product_key
                            variant_row = variant_df[variant_df['platform_sku_id'] == sku_id]
                            if not variant_row.empty:
                                product_key = variant_row.iloc[0]['product_key']
                                if order_item_key_counter <= 3:
                                    print(f"🎯 FOUND via variants: Lazada item {product_item_id} (sku_id: {sku_id}) -> product_key: {product_key}")
                    
                    # If still not found, skip the item
                    if product_key is None:
                        if order_item_key_counter <= 3:
                            print(f"⚠️ Skipping Lazada item without product_key for product_item_id '{product_item_id}' (from sku '{sku}') in order {platform_order_id}")
                            print(f"   Available product IDs: {list(product_key_lookup.keys())[:5]}")
                        continue
                    
                    # Get product_variant_key from sku_id
                    sku_id = str(item.get('sku_id', ''))
                    product_variant_key = variant_key_lookup.get(sku_id)
                    
                    # If no direct variant match, look for DEFAULT variant for this product
                    if not product_variant_key and product_key:
                        # First try to find if this product has exactly one variant
                        product_variants = variant_df[variant_df['product_key'] == product_key]
                        if len(product_variants) == 1:
                            # If product has exactly one variant, use it
                            product_variant_key = product_variants.iloc[0]['product_variant_key']
                        else:
                            # Look for DEFAULT variant for this product
                            default_variant = variant_df[
                                (variant_df['product_key'] == product_key) & 
                                (variant_df['platform_sku_id'].str.startswith('DEFAULT_', na=False))
                            ]
                            if not default_variant.empty:
                                product_variant_key = default_variant.iloc[0]['product_variant_key']
                                print(f"🔄 Using DEFAULT variant {product_variant_key} for product {product_key}")
                    
                    # Ensure product_variant_key is never None
                    if not product_variant_key:
                        product_variant_key = 0.0
                    
                    # Create individual fact record for this order item (without order_item_key for now)
                    fact_record = {
                        'order_item_key': None,  # Will be generated after sorting
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
                    # Remove order_item_key_counter increment - will be done after sorting
                    
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


def extract_order_items_from_shopee(orders_data, payment_details_data, dim_lookups, variant_df):
    """
    Extract and harmonize fact order records from Shopee orders with payment details
    Creates one record per individual order item (no aggregation)
    ONLY PROCESSES COMPLETED ORDERS from dim_order
    
    Args:
        orders_data (list): Raw Shopee orders from API (includes item_list)
        payment_details_data (list): Raw Shopee payment details from API
        dim_lookups (dict): Dictionary of dimension key lookups
        variant_df (DataFrame): Harmonized dimension product variant table
        
    Returns:
        DataFrame: Harmonized fact orders records (one per order item)
    """
    fact_orders_records = []
    
    # Get lookup dictionaries from dim_lookups
    order_key_lookup = dim_lookups.get('order', {})  # Only contains COMPLETED orders
    order_prices_lookup = dim_lookups.get('order_prices', {})
    customer_key_lookup = dim_lookups.get('customer', {})
    product_key_lookup = dim_lookups.get('product', {})
    variant_key_lookup = dim_lookups.get('product_variant', {})
    
    # Create payment details lookup by order_sn
    payment_lookup = {}
    if isinstance(payment_details_data, dict):
        payment_lookup = payment_details_data
    else:
        for payment_detail in payment_details_data:
            order_sn = payment_detail.get('order_sn', '')
            if order_sn:
                payment_lookup[order_sn] = payment_detail
    
    print(f"📊 Processing Shopee orders with payment details...")
    print(f"📊 Created lookup for {len(payment_lookup)} payment records")
    
    order_item_key_counter = 1
    
    for order in orders_data:
        try:
            platform_order_id = str(order.get('order_sn', ''))
            item_list = order.get('item_list', [])
            
            # Get orders_key from dimension table (only COMPLETED orders are in lookup)
            orders_key = order_key_lookup.get(platform_order_id)
            if orders_key is None:
                # Skip non-COMPLETED orders - they're not in the lookup
                continue
            
            # Get customer info from order
            buyer_user_id = order.get('buyer_user_id')  # Primary ID from API
            buyer_username = order.get('buyer_username', '')
            recipient_address = order.get('recipient_address', {})
            phone = recipient_address.get('phone', '') if isinstance(recipient_address, dict) else ''
            
            platform_customer_id = generate_shopee_platform_customer_id(
                buyer_user_id=buyer_user_id,
                buyer_username=buyer_username,
                phone=phone,
                order_sn=platform_order_id,
                create_time=order.get('create_time')
            )
            
            # Skip orders with anonymous customers (buyer_user_id=0) since they're not in dimension table
            if platform_customer_id is None:
                if order_item_key_counter <= 3:
                    print(f"🔍 Skipping anonymous customer order {platform_order_id} (buyer_user_id={buyer_user_id})")
                continue
                
            customer_key = customer_key_lookup.get(platform_customer_id)
            
            # Debug customer key lookup for the first few records
            if order_item_key_counter <= 3:
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
            
            # Get payment details for this order
            payment_detail = payment_lookup.get(platform_order_id, {})
            order_income = payment_detail.get('order_income', {})
            payment_items = order_income.get('items', [])
            
            # Create item lookup from payment details for more accurate pricing
            payment_item_lookup = {}
            for payment_item in payment_items:
                item_id = str(payment_item.get('item_id', ''))
                model_id = str(payment_item.get('model_id', ''))
                key = f"{item_id}_{model_id}"
                payment_item_lookup[key] = payment_item
            
            # Get order-level shipping fee from payment details
            order_shipping_fee = float(order_income.get('actual_shipping_fee', 0.0))
            
            # Create one fact record per individual order item
            for item in item_list:
                try:
                    # Get product_key from item_id
                    item_id = str(item.get('item_id', ''))
                    product_key = product_key_lookup.get(item_id)
                    
                    # If not found in products table, try to find via variants table
                    if product_key is None:
                        # Try item_id or model_id lookup in variants table
                        model_id = str(item.get('model_id', ''))
                        
                        # Check both item_id and model_id in variants
                        if item_id in variant_key_lookup:
                            variant_row = variant_df[variant_df['platform_sku_id'] == item_id]
                            if not variant_row.empty:
                                product_key = variant_row.iloc[0]['product_key']
                                if order_item_key_counter <= 3:
                                    print(f"🎯 FOUND via variants: Shopee item_id {item_id} -> product_key: {product_key}")
                        elif model_id != '0' and model_id in variant_key_lookup:
                            variant_row = variant_df[variant_df['platform_sku_id'] == model_id]
                            if not variant_row.empty:
                                product_key = variant_row.iloc[0]['product_key']
                                if order_item_key_counter <= 3:
                                    print(f"🎯 FOUND via variants: Shopee model_id {model_id} -> product_key: {product_key}")
                    
                    # Debug: Show what's available in first few iterations
                    if product_key is None and order_item_key_counter <= 6:
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
                    
                    # Get product_variant_key from model_id or item_id
                    model_id = str(item.get('model_id', ''))
                    item_id = str(item.get('item_id', ''))
                    
                    # For Shopee: if model_id is 0, use item_id for variant lookup (base products)
                    if model_id == '0' or not model_id:
                        product_variant_key = variant_key_lookup.get(item_id)
                        
                        # If no direct variant match by item_id, use fallback logic
                        if not product_variant_key and product_key:
                            # Find variants for this specific product
                            product_variants = variant_df[variant_df['product_key'] == product_key]
                            if len(product_variants) == 1:
                                # If product has exactly one variant, use it
                                product_variant_key = product_variants.iloc[0]['product_variant_key']
                            else:
                                # Look for DEFAULT variant for this product
                                default_variant = variant_df[
                                    (variant_df['product_key'] == product_key) & 
                                    (variant_df['platform_sku_id'].str.startswith('DEFAULT_', na=False))
                                ]
                                if not default_variant.empty:
                                    product_variant_key = default_variant.iloc[0]['product_variant_key']
                                    print(f"🔄 Using DEFAULT variant {product_variant_key} for Shopee product {product_key}")
                    else:
                        product_variant_key = variant_key_lookup.get(model_id)
                        
                        # If no direct variant match by model_id, use fallback logic  
                        if not product_variant_key and product_key:
                            # Find variants for this specific product
                            product_variants = variant_df[variant_df['product_key'] == product_key]
                            if len(product_variants) == 1:
                                # If product has exactly one variant, use it
                                product_variant_key = product_variants.iloc[0]['product_variant_key']
                            else:
                                # Look for DEFAULT variant for this product
                                default_variant = variant_df[
                                    (variant_df['product_key'] == product_key) & 
                                    (variant_df['platform_sku_id'].str.startswith('DEFAULT_', na=False))
                                ]
                                if not default_variant.empty:
                                    product_variant_key = default_variant.iloc[0]['product_variant_key']
                                    print(f"🔄 Using DEFAULT variant {product_variant_key} for Shopee product {product_key}")
                    
                    # Ensure product_variant_key is never None
                    if not product_variant_key:
                        product_variant_key = 0.0
                    
                    # Get item quantity
                    item_quantity = item.get('model_quantity_purchased', 1)
                    
                    # Get pricing information from payment details if available
                    payment_item_key = f"{item_id}_{model_id}"
                    payment_item = payment_item_lookup.get(payment_item_key, {})
                    
                    if payment_item:
                        # Use payment detail pricing (more accurate) - LINE TOTALS (price × quantity)
                        original_unit_price = float(payment_item.get('selling_price', 0.0))  # Total line revenue before any discounts
                        
                        # Get all discount components
                        coin_discount = float(payment_item.get('discount_from_coin', 0.0))  # Shopee coins discount
                        voucher_platform_amount = float(payment_item.get('discount_from_voucher_shopee', 0.0))  # Platform voucher discount
                        voucher_seller_amount = float(payment_item.get('discount_from_voucher_seller', 0.0))    # Seller voucher discount
                        seller_discount = float(payment_item.get('seller_discount', 0.0))  # Direct seller discount
                        shopee_discount = float(payment_item.get('shopee_discount', 0.0))  # Shopee platform discount
                        
                        # Final paid price = original - all discounts
                        # discounted_price already has some discounts applied, but we need the final customer-paid amount
                        discounted_price = float(payment_item.get('discounted_price', 0.0))
                        
                        # Calculate final customer paid amount by subtracting all remaining discounts from discounted_price
                        # Note: discounted_price typically has seller/shopee discounts already applied
                        # We need to subtract coin discounts and voucher discounts that are applied at checkout
                        paid_price = discounted_price - coin_discount - voucher_platform_amount - voucher_seller_amount
                    else:
                        # Fallback to basic order data - convert to line totals
                        unit_original = float(item.get('model_original_price', 0.0))
                        unit_discounted = float(item.get('model_discounted_price', 0.0))
                        original_unit_price = unit_original * item_quantity  # Convert to line total
                        paid_price = unit_discounted * item_quantity         # Convert to line total
                        voucher_platform_amount = float(item.get('voucher_absorbed_by_shopee', 0.0))
                        voucher_seller_amount = float(item.get('voucher_absorbed_by_seller', 0.0))
                    
                    # Calculate weighted shipping fee based on paid_price proportion
                    # Formula: (order_shipping_fee / sum_of_all_paid_prices) * current_item_paid_price
                    total_paid_across_items = sum(
                        float(payment_item_lookup.get(f"{i.get('item_id', '')}_{i.get('model_id', '')}", {}).get('discounted_price', 0.0))
                        for i in item_list
                    )
                    if total_paid_across_items > 0:
                        shipping_per_item = (order_shipping_fee / total_paid_across_items) * paid_price
                    else:
                        shipping_per_item = order_shipping_fee / len(item_list) if len(item_list) > 0 else 0.0
                    
                    # Create individual fact record for this order item (without order_item_key for now)
                    fact_record = {
                        'order_item_key': None,  # Will be generated after sorting
                        'orders_key': orders_key,
                        'product_key': product_key,
                        'product_variant_key': product_variant_key if product_variant_key else None,
                        'time_key': time_key,
                        'customer_key': customer_key,
                        'platform_key': 2,  # Always 2 for Shopee
                        'item_quantity': item_quantity,
                        'paid_price': paid_price,
                        'original_unit_price': original_unit_price,
                        'voucher_platform_amount': voucher_platform_amount,
                        'voucher_seller_amount': voucher_seller_amount,
                        'shipping_fee_paid_by_buyer': shipping_per_item
                    }
                    
                    fact_orders_records.append(fact_record)
                    # Remove order_item_key_counter increment - will be done after sorting
                    
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
    
    # Load dimension lookups from CSV files
    print("\n📥 Loading dimension lookups...")
    dim_lookups, variant_df = load_dimension_lookups()
    
    # Load Lazada source data
    print("\n📥 Loading Lazada data...")
    lazada_order_items_data = load_lazada_order_items_raw()
    lazada_orders_data = load_lazada_orders_raw()
    
    # Load Shopee source data
    print("\n📥 Loading Shopee data...")
    shopee_orders_data = load_shopee_orders_raw()
    shopee_payment_details_data = load_shopee_payment_details_raw()
    
    # Process Lazada orders
    print("\n🔄 Processing Lazada order items...")
    lazada_fact_df = pd.DataFrame()
    if lazada_order_items_data and lazada_orders_data:
        lazada_fact_df = extract_order_items_from_lazada(
            lazada_order_items_data, lazada_orders_data, dim_lookups, variant_df
        )
        print(f"✅ Processed {len(lazada_fact_df)} Lazada fact order records")
    else:
        print("⚠️ No Lazada order data available")
    
    # Process Shopee orders
    print("\n🔄 Processing Shopee order items...")
    shopee_fact_df = pd.DataFrame()
    if shopee_orders_data:
        shopee_fact_df = extract_order_items_from_shopee(
            shopee_orders_data, shopee_payment_details_data, dim_lookups, variant_df
        )
        print(f"✅ Processed {len(shopee_fact_df)} Shopee fact order records")
    else:
        print("⚠️ No Shopee order data available")
    
    # Combine both platforms
    fact_orders_df = pd.concat([lazada_fact_df, shopee_fact_df], ignore_index=True)
    
    # Sort by orders_key to group related orders together
    if len(fact_orders_df) > 0:
        fact_orders_df = fact_orders_df.sort_values('orders_key').reset_index(drop=True)
        
        # Generate order_item_key based on platform_key after sorting
        def generate_order_item_key(row, counter):
            if row['platform_key'] == 1:  # Lazada
                return f"L{counter:08d}"
            elif row['platform_key'] == 2:  # Shopee  
                return f"S{counter:08d}"
            else:
                return f"O{counter:08d}"  # Fallback
        
        # Apply order_item_key generation
        fact_orders_df['order_item_key'] = [
            generate_order_item_key(row, i+1) 
            for i, (_, row) in enumerate(fact_orders_df.iterrows())
        ]
        
        print(f"\n✅ Generated sequential order_item_keys after sorting by orders_key")
    
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
        
        # Integrated validation
        print(f"\n🔍 VALIDATING FACT ORDERS...")
        total_records = len(fact_orders_df)
        total_revenue = fact_orders_df['paid_price'].sum()
        
        platform_stats = fact_orders_df.groupby('platform_key').agg({
            'order_item_key': 'count',
            'paid_price': 'sum'
        }).round(2)
        
        print(f"📊 Fact Orders Validation:")
        print(f"  Total records: {total_records:,}")
        print(f"  Total revenue: ${total_revenue:,.2f}")
        print(f"  Platform breakdown:")
        
        for platform_key, stats in platform_stats.iterrows():
            platform_name = "Lazada" if platform_key == 1 else "Shopee"
            print(f"    {platform_name}: {int(stats['order_item_key']):,} records, ${stats['paid_price']:,.2f}")
            
        # Check for missing foreign keys
        null_checks = {
            'orders_key': fact_orders_df['orders_key'].isna().sum(),
            'customer_key': fact_orders_df['customer_key'].isna().sum(),
            'product_key': fact_orders_df['product_key'].isna().sum(),
            'product_variant_key': fact_orders_df['product_variant_key'].isna().sum()
        }
        
        print(f"🔗 Foreign Key Validation:")
        for key, null_count in null_checks.items():
            status = "✅" if null_count == 0 else f"❌ {null_count:,} missing"
            print(f"  {key}: {status}")
            
        if all(count == 0 for count in null_checks.values()):
            print(f"✅ All foreign key relationships intact")
        else:
            print(f"⚠️  Some foreign key issues detected")
    else:
        print("❌ No data to save")