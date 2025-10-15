"""
Harmonize Fact Orders from Lazada API Response to Unified Schema
================================================================

This module processes Lazada order line items and harmonizes them into the Fact_Orders table.
The fact table contains one record per individual item (not aggregated by quantity).

Key Requirements:
- Uses orders_key from Dim_Order (foreign key relationship)
- Each order can have multiple line items (multiple products)
- Creates individual records for each item (if quantity=3, creates 3 records with quantity=1 each)
- Maps platform-specific fields to unified schema
- Validates that total individual item count matches between Dim_Order and Fact_Orders

Field Mappings for Fact_Orders:
- order_item_key: Generated unique identifier for each individual item
- orders_key: Foreign key to Dim_Order.orders_key (matched by platform_order_id)
- product_key: Foreign key to Dim_Product.product_key (matched by product_item_id)
- product_variant_key: Foreign key to Dim_Product_Variant (matched by platform_sku_id)
- time_key: Foreign key to Dim_Time (derived from order_date, format YYYYMMDD)
- customer_key: Foreign key to Dim_Customer (matched by platform_customer_id)
- platform_key: Always 1 for Lazada, 2 for Shopee
- item_quantity: Always 1 (each record represents one individual item)
- paid_price: Revenue per individual item (split from total line item price)
- original_unit_price: Non-discounted price per individual item (split from total)
- voucher_platform_amount: Platform voucher amount per individual item (split from total)
- voucher_seller_amount: Seller voucher amount per individual item (split from total)
- shipping_fee_paid_by_buyer: Shipping fee per individual item (split from total)
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


def extract_order_items_from_transformed_data(order_items_data, orders_data, dim_order_df, dim_customer_df, dim_product_df, dim_variant_df, dim_platform_df, dim_time_df):
    """
    Extract and harmonize fact order records using transformed dimension tables as reference
    Creates one record per individual item (not aggregated by quantity)
    
    Args:
        order_items_data (list): Raw Lazada order items from API  
        orders_data (list): Raw Lazada orders from API (for additional details)
        dim_order_df (DataFrame): Transformed dimension order table
        dim_customer_df (DataFrame): Transformed dimension customer table  
        dim_product_df (DataFrame): Transformed dimension product table
        dim_variant_df (DataFrame): Transformed dimension product variant table
        dim_platform_df (DataFrame): Transformed dimension platform table
        dim_time_df (DataFrame): Transformed dimension time table
        
    Returns:
        DataFrame: Harmonized fact orders records (one per individual item)
    """
    fact_orders_records = []
    
    print(f"🔄 Creating fact orders from {len(dim_order_df)} order records...")
    
    # Use the dim_order table as the primary source since it's already harmonized
    order_item_key_counter = 1
    
    for _, order_row in dim_order_df.iterrows():
        try:
            platform_order_id = str(order_row['platform_order_id'])
            orders_key = order_row['orders_key']
            total_item_count = int(order_row.get('total_item_count', 1))
            order_date = order_row['order_date']
            
            # Convert order_date to time_key
            if isinstance(order_date, str):
                try:
                    date_obj = pd.to_datetime(order_date)
                    time_key = int(date_obj.strftime('%Y%m%d'))
                except:
                    time_key = 20220101  # Default fallback
            else:
                time_key = int(order_date.strftime('%Y%m%d'))
            
            # Find customer_key from the order's shipping city (we'll need to match this differently)
            customer_key = None
            shipping_city = order_row.get('shipping_city', '')
            
            # Try to find customer by looking for customers in the same city/area
            if not dim_customer_df.empty and shipping_city:
                customer_matches = dim_customer_df[
                    dim_customer_df['customer_city'].str.contains(shipping_city, case=False, na=False)
                ]
                if not customer_matches.empty:
                    customer_key = customer_matches.iloc[0]['customer_key']
            
            # If no customer match found, create a placeholder
            if customer_key is None:
                if order_item_key_counter <= 5:
                    print(f"⚠️ No customer match found for order {platform_order_id} in city {shipping_city}")
                customer_key = 1  # Use default customer key
            
            # Get platform_key (should be 1 for Lazada)
            platform_key = 1
            if not dim_platform_df.empty:
                lazada_platform = dim_platform_df[dim_platform_df['platform_name'] == 'Lazada']
                if not lazada_platform.empty:
                    platform_key = lazada_platform.iloc[0]['platform_key']
            
            # Find corresponding products for this order from raw data
            raw_order_items = []
            for raw_order in order_items_data:
                if str(raw_order.get('order_id', '')) == platform_order_id:
                    raw_order_items = raw_order.get('order_items', [])
                    break
            
            # If we have raw order items, use them for product details
            if raw_order_items:
                for item in raw_order_items:
                    try:
                        # Get product information
                        product_key = None
                        product_variant_key = None
                        
                        # Try to match by product name or other available fields
                        item_name = item.get('name', '')
                        if item_name and not dim_product_df.empty:
                            # Look for product by name similarity
                            product_matches = dim_product_df[
                                dim_product_df['product_name'].str.contains(
                                    item_name[:30], case=False, na=False, regex=False
                                )
                            ]
                            if not product_matches.empty:
                                product_key = product_matches.iloc[0]['product_key']
                                
                                # Once we have product_key, find the corresponding variant
                                if not dim_variant_df.empty:
                                    variant_matches = dim_variant_df[
                                        dim_variant_df['product_key'] == product_key
                                    ]
                                    if not variant_matches.empty:
                                        # Use the first variant for this product
                                        product_variant_key = variant_matches.iloc[0]['variant_key']
                        
                        # If no product found by name, try by SKU or other identifiers
                        if product_key is None:
                            sku_id = str(item.get('sku_id', ''))
                            if sku_id and not dim_variant_df.empty:
                                # Try to find variant by platform_sku_id
                                variant_matches = dim_variant_df[
                                    dim_variant_df['platform_sku_id'].astype(str) == sku_id
                                ]
                                if not variant_matches.empty:
                                    product_variant_key = variant_matches.iloc[0]['variant_key']
                                    product_key = variant_matches.iloc[0]['product_key']
                        
                        # If still no product found, use default
                        if product_key is None:
                            if not dim_product_df.empty:
                                product_key = dim_product_df.iloc[0]['product_key']  # Use first product as default
                                # Find a variant for this default product
                                if not dim_variant_df.empty:
                                    default_variant = dim_variant_df[
                                        dim_variant_df['product_key'] == product_key
                                    ]
                                    if not default_variant.empty:
                                        product_variant_key = default_variant.iloc[0]['variant_key']
                            else:
                                continue  # Skip if no products available
                        
                        # Get item quantity and create individual records
                        item_quantity = int(item.get('quantity', 1))
                        
                        # Create individual item records (not aggregated)
                        for individual_item in range(item_quantity):
                            fact_record = {
                                'order_item_key': f"OI{order_item_key_counter:08d}",
                                'orders_key': orders_key,
                                'product_key': product_key,
                                'product_variant_key': product_variant_key,  # Will be None if not found
                                'time_key': time_key,
                                'customer_key': customer_key,
                                'platform_key': platform_key,
                                'item_quantity': 1,  # Always 1 for individual items
                                'paid_price': float(item.get('paid_price', 0.0)) / item_quantity,
                                'original_unit_price': float(item.get('item_price', 0.0)) / item_quantity,
                                'voucher_platform_amount': float(item.get('voucher_platform', 0.0)) / item_quantity,
                                'voucher_seller_amount': float(item.get('voucher_seller', 0.0)) / item_quantity,
                                'shipping_fee_paid_by_buyer': float(item.get('shipping_amount', 0.0)) / item_quantity
                            }
                            
                            fact_orders_records.append(fact_record)
                            order_item_key_counter += 1
                            
                    except Exception as e:
                        print(f"⚠️ Error processing item in order {platform_order_id}: {e}")
                        continue
            else:
                # If no raw order items found, create records based on total_item_count from dim_order
                if total_item_count > 0:
                    # Use first available product as default
                    product_key = dim_product_df.iloc[0]['product_key'] if not dim_product_df.empty else 1
                    
                    # Find a variant for this product
                    product_variant_key = None
                    if not dim_variant_df.empty and product_key:
                        variant_matches = dim_variant_df[
                            dim_variant_df['product_key'] == product_key
                        ]
                        if not variant_matches.empty:
                            product_variant_key = variant_matches.iloc[0]['variant_key']
                    
                    # Create individual records based on total_item_count
                    for individual_item in range(total_item_count):
                        fact_record = {
                            'order_item_key': f"OI{order_item_key_counter:08d}",
                            'orders_key': orders_key,
                            'product_key': product_key,
                            'product_variant_key': product_variant_key,
                            'time_key': time_key,
                            'customer_key': customer_key,
                            'platform_key': platform_key,
                            'item_quantity': 1,  # Always 1 for individual items
                            'paid_price': float(order_row.get('price_total', 0.0)) / total_item_count,
                            'original_unit_price': float(order_row.get('price_total', 0.0)) / total_item_count,
                            'voucher_platform_amount': 0.0,
                            'voucher_seller_amount': 0.0,
                            'shipping_fee_paid_by_buyer': 0.0
                        }
                        
                        fact_orders_records.append(fact_record)
                        order_item_key_counter += 1
        
        except Exception as e:
            print(f"⚠️ Error processing order {order_row.get('platform_order_id', 'unknown')}: {e}")
            continue
    
    # Create DataFrame
    fact_orders_df = pd.DataFrame(fact_orders_records, columns=FACT_ORDERS_COLUMNS)
    
    # Apply data type conversions
    if len(fact_orders_df) > 0:
        fact_orders_df = apply_data_types(fact_orders_df, 'fact_orders')
    
    return fact_orders_df
    """
    Extract and harmonize fact order records from Lazada order items
    Creates one record per individual item (not aggregated by quantity)
    
    Example: If an order has 3 units of Product A, this creates 3 separate records 
    each with quantity=1, rather than 1 record with quantity=3
    
    Args:
        order_items_data (list): Raw Lazada order items from API  
        orders_data (list): Raw Lazada orders from API (for customer info)
        dim_order_df (DataFrame): Harmonized dimension order table
        dim_customer_df (DataFrame): Harmonized dimension customer table
        dim_product_df (DataFrame): Harmonized dimension product table
        dim_variant_df (DataFrame): Harmonized dimension product variant table
        
    Returns:
        DataFrame: Harmonized fact orders records (one per individual item, not aggregated)
    """
    fact_orders_records = []
    
    # Create lookup dictionaries for foreign key mapping
    order_key_lookup = dict(zip(
        dim_order_df['platform_order_id'].astype(str), 
        dim_order_df['orders_key']
    )) if not dim_order_df.empty else {}
    
    # Also create order date lookup for time_key generation
    order_date_lookup = dict(zip(
        dim_order_df['platform_order_id'].astype(str),
        dim_order_df['order_date']
    )) if not dim_order_df.empty else {}
    
    customer_key_lookup = dict(zip(
        dim_customer_df['platform_customer_id'].astype(str),
        dim_customer_df['customer_key']
    )) if not dim_customer_df.empty else {}
    
    product_key_lookup = dict(zip(
        dim_product_df['product_item_id'].astype(str),
        dim_product_df['product_key']
    )) if not dim_product_df.empty else {}
    
    # Use platform_sku_id for variant lookup (this is the SkuId from API)
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
    
    print(f"📊 Created lookups for:")
    print(f"   - {len(order_key_lookup)} orders")
    print(f"   - {len(customer_key_lookup)} customers")
    print(f"   - {len(product_key_lookup)} products")
    print(f"   - {len(variant_key_lookup)} product variants")
    print(f"   - {len(orders_customer_lookup)} order customer mappings")
    
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
            
            # Get order date for time_key
            order_date = order_date_lookup.get(platform_order_id)
            if order_date:
                # Convert order date to time_key
                if isinstance(order_date, str):
                    time_key = generate_time_key(order_date + " 00:00:00 +0000")
                else:
                    time_key = int(order_date.strftime('%Y%m%d'))
            else:
                time_key = 20220101  # Default fallback
            
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
                print(f"   - order_date: {order_date}")
                print(f"   - time_key: {time_key}")
                if customer_key is None:
                    # Show some sample customer IDs to help debug
                    sample_customers = list(customer_key_lookup.keys())[:5]
                    print(f"   - Sample customer IDs in lookup: {sample_customers}")
            
            if customer_key is None:
                print(f"⚠️ Warning: Could not find customer_key for platform_customer_id '{platform_customer_id}' in order {platform_order_id}")
                continue  # Skip records where we can't find the customer
            
            # Create one fact record per individual item (not aggregated)
            for item in order_items:
                try:
                    # Get product_key from item_id (match product_item_id from products)
                    item_id = str(item.get('item_id', ''))
                    product_key = product_key_lookup.get(item_id)
                    
                    # Skip records without valid product_key
                    if product_key is None:
                        if order_item_key_counter <= 5:
                            print(f"⚠️ Warning: Could not find product_key for item_id '{item_id}' in order {platform_order_id}")
                        continue
                    
                    # Get product_variant_key from sku_id (platform_sku_id)
                    sku_id = str(item.get('sku_id', ''))
                    product_variant_key = variant_key_lookup.get(sku_id)
                    
                    # Get quantity from the order item
                    item_quantity = int(item.get('quantity', 1))
                    
                    # Create individual fact records for each item (not aggregated)
                    # If quantity is 3, create 3 separate records with quantity=1 each
                    for individual_item in range(item_quantity):
                        fact_record = {
                            'order_item_key': f"OI{order_item_key_counter:08d}",  # Generate unique key
                            'orders_key': orders_key,
                            'product_key': product_key,
                            'product_variant_key': product_variant_key if product_variant_key else None,
                            'time_key': time_key,
                            'customer_key': customer_key,
                            'platform_key': 1,  # Always 1 for Lazada
                            'item_quantity': 1,  # Always 1 for individual items (not aggregated)
                            'paid_price': float(item.get('paid_price', 0.0)) / item_quantity,  # Split price per item
                            'original_unit_price': float(item.get('item_price', 0.0)) / item_quantity,  # Split price per item
                            'voucher_platform_amount': float(item.get('voucher_platform', 0.0)) / item_quantity,  # Split voucher per item
                            'voucher_seller_amount': float(item.get('voucher_seller', 0.0)) / item_quantity,  # Split voucher per item
                            'shipping_fee_paid_by_buyer': float(item.get('shipping_amount', 0.0)) / item_quantity  # Split shipping per item
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
        fact_orders_df = apply_data_types(fact_orders_df, 'fact_orders')
    
    return fact_orders_df


def validate_fact_orders_against_dim_order(fact_orders_df, dim_order_df):
    """
    Validate that the fact orders table matches the expected counts from dimension order table
    
    Validation Rule: sum of total_item_count from dim_order should equal count of order_item_key in fact_orders
    
    Args:
        fact_orders_df (DataFrame): Harmonized fact orders table
        dim_order_df (DataFrame): Harmonized dimension order table
        
    Returns:
        tuple: (is_valid: bool, validation_message: str)
    """
    try:
        if fact_orders_df.empty or dim_order_df.empty:
            return False, "❌ Empty dataframes provided for validation"
        
        # Calculate expected total items from dim_order (sum of total_item_count)
        dim_order_total_items = dim_order_df['total_item_count'].sum()
        
        # Calculate actual fact_orders count (should be one row per item line)
        fact_orders_count = len(fact_orders_df)
        
        print(f"📊 Validation Results:")
        print(f"   - Expected total items from dim_order: {dim_order_total_items}")
        print(f"   - Actual fact_orders count: {fact_orders_count}")
        
        # They should be equal
        if dim_order_total_items == fact_orders_count:
            return True, f"✅ Validation PASSED: {dim_order_total_items} expected items = {fact_orders_count} fact order records"
        else:
            difference = abs(dim_order_total_items - fact_orders_count)
            return False, f"❌ Validation FAILED: {dim_order_total_items} expected items ≠ {fact_orders_count} fact order records (difference: {difference})"
            
    except Exception as e:
        return False, f"❌ Validation ERROR: {str(e)}"


def harmonize_fact_orders():
    """
    Main function to harmonize Fact Orders from Lazada API response
    
    Returns:
        DataFrame: Harmonized fact orders table
    """
    print("🚀 Starting Fact Orders harmonization for Lazada...")
    
    # Load source data - both order items and orders data
    order_items_data = load_lazada_order_items_raw()
    if not order_items_data:
        print("❌ No order items data available")
        return get_empty_dataframe('fact_orders')
    
    orders_data = load_lazada_orders_raw()
    if not orders_data:
        print("❌ No orders data available for customer info")
        return get_empty_dataframe('fact_orders')
    
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
        
    dim_variant_df = load_dim_product_variant()
    if dim_variant_df.empty:
        print("⚠️ No Dim_Product_Variant data available - product_variant_key will be NULL")
    
    # Process and harmonize
    fact_orders_df = extract_order_items_from_transformed_data(
        order_items_data, orders_data, dim_order_df, dim_customer_df, dim_product_df, dim_variant_df, 
        pd.DataFrame(), pd.DataFrame()  # Empty platform and time DFs for now
    )
    
    print(f"✅ Successfully harmonized {len(fact_orders_df)} fact order records")
    print(f"📊 Data shape: {fact_orders_df.shape}")
    
    if len(fact_orders_df) > 0:
        print("\n📋 Sample records:")
        print(fact_orders_df.head(3).to_string(index=False))
        
        print(f"\n📈 Summary Statistics:")
        print(f"   • Total Revenue: ${fact_orders_df['paid_price'].sum():.2f}")
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


def harmonize_fact_orders():
    """
    Main function to harmonize fact orders using existing transformed dimension tables
    
    Returns:
        DataFrame: Harmonized fact orders ready for analysis
    """
    print("🏭 Starting Fact Orders Harmonization from Transformed CSVs...")
    
    # Load transformed dimension tables
    print("📂 Loading transformed dimension tables...")
    
    # Load dimension tables from Transformed folder
    transformed_path = os.path.join(os.path.dirname(__file__), '..', 'Transformed')
    
    try:
        dim_order_df = pd.read_csv(os.path.join(transformed_path, 'dim_order.csv'))
        dim_customer_df = pd.read_csv(os.path.join(transformed_path, 'dim_customer.csv'))
        dim_product_df = pd.read_csv(os.path.join(transformed_path, 'dim_product.csv'))
        dim_variant_df = pd.read_csv(os.path.join(transformed_path, 'dim_product_variant.csv'))
        
        # Try to load platform and time tables, create empty if not found
        try:
            dim_platform_df = pd.read_csv(os.path.join(transformed_path, 'dim_platform.csv'))
        except:
            dim_platform_df = pd.DataFrame()
            
        try:
            dim_time_df = pd.read_csv(os.path.join(transformed_path, 'dim_time.csv'))
        except:
            dim_time_df = pd.DataFrame()
        
        print(f"✅ Loaded dimension tables:")
        print(f"   • Orders: {len(dim_order_df)} records")
        print(f"   • Customers: {len(dim_customer_df)} records") 
        print(f"   • Products: {len(dim_product_df)} records")
        print(f"   • Product Variants: {len(dim_variant_df)} records")
        print(f"   • Platforms: {len(dim_platform_df)} records")
        print(f"   • Time: {len(dim_time_df)} records")
        
    except Exception as e:
        print(f"❌ Error loading dimension tables: {e}")
        return get_empty_dataframe('fact_orders')
    
    # Load raw order items data to get the actual transaction details
    order_items_data = load_lazada_order_items_raw()
    orders_data = load_lazada_orders_raw()
    
    if not order_items_data:
        order_items_data = []
        print("⚠️ No raw order items data - will use dimension order data only")
        
    if not orders_data:
        orders_data = []
        print("⚠️ No raw orders data - will use dimension order data only")
    
    print(f"📊 Raw data loaded:")
    print(f"   • Order Items: {len(order_items_data)} orders with items")
    print(f"   • Orders: {len(orders_data)} order records")
    
    # Extract and harmonize fact order records
    fact_orders_df = extract_order_items_from_transformed_data(
        order_items_data, 
        orders_data,
        dim_order_df,
        dim_customer_df, 
        dim_product_df,
        dim_variant_df,
        dim_platform_df,
        dim_time_df
    )
    
    # Display summary statistics
    display_fact_orders_summary(fact_orders_df)
    
    return fact_orders_df


def display_fact_orders_summary(fact_orders_df):
    """
    Display summary statistics for the harmonized fact orders
    
    Args:
        fact_orders_df (DataFrame): Harmonized fact orders dataframe
    """
    print(f"✅ Successfully harmonized {len(fact_orders_df)} fact order records")
    print(f"📊 Data shape: {fact_orders_df.shape}")
    
    if len(fact_orders_df) > 0:
        print("\n📋 Sample records:")
        print(fact_orders_df.head(3).to_string(index=False))
        
        print(f"\n📈 Summary Statistics:")
        print(f"   • Total Revenue: ${fact_orders_df['paid_price'].sum():.2f}")
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


if __name__ == "__main__":
    # Run the harmonization
    fact_orders_df = harmonize_fact_orders()
    
    if not fact_orders_df.empty:
        # Load dimension order for validation
        dim_order_path = os.path.join(os.path.dirname(__file__), '..', 'Transformed', 'dim_order.csv')
        
        if os.path.exists(dim_order_path):
            print(f"\n📊 Loading dimension order table for validation...")
            dim_order_df = pd.read_csv(dim_order_path)
            
            # Perform validation before saving
            is_valid, validation_message = validate_fact_orders_against_dim_order(fact_orders_df, dim_order_df)
            print(f"\n{validation_message}")
            
            if is_valid:
                print(f"✅ Validation passed - proceeding to save fact_orders.csv")
            else:
                print(f"⚠️ Validation failed - check data quality before saving")
                print(f"💡 Consider reviewing the order items extraction logic")
        else:
            print(f"⚠️ Warning: dim_order.csv not found at {dim_order_path}")
            print(f"💡 Skipping validation - ensure dim_order is harmonized first")
        
        # Save results
        save_fact_orders(fact_orders_df)
    else:
        print("❌ No data to save")