"""
Harmonize Customer Dimension from Lazada API Response to Unified Schema
======================================================================

This module processes Lazada and Shopee orders and extracts customer information to create
the Dim_Customer dimension table with proper customer segmentation and analytics.

Key Requirements from Schema:
- customer_key: Internal surrogate ID (sequential)
- platform_customer_id: Generated synthetic ID (Lazada doesn't provide this)
- buyer_segment: Calculated as 'New Buyer' or 'Returning Buyer'
- total_orders: Count of orders per platform_customer_id
- customer_since: Earliest order_date for each platform_customer_id
- last_order_date: Latest order_date for each platform_customer_id
- platform_key: Always 1 for Lazada, 2 for Shopee

Platform Customer ID Generation Logic:
Since Lazada doesn't provide platform_customer_id, we generate it using:
'LZ' + first_char(first_name) + last_char(first_name) + first2_digits(phone) + last2_digits(phone)

For Shopee:
- Uses buyer_user_id directly from API when available and not 0
- When buyer_user_id = 0 (missing/anonymous): Generates unique synthetic ID
  Format: "SP0_{order_suffix}_{timestamp}" to prevent aggregating different customers
- Falls back to username+phone format if buyer_user_id is None

Example: 
- first_name: "Antonio", phone: "639123456789"
- platform_customer_id: "LZAO1289" must be toUppercase match the example above.
"""

import pandas as pd
import numpy as np
from datetime import datetime, date
import json
import os
import sys
import re

# Add the parent directory to sys.path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import (
    DIM_CUSTOMER_COLUMNS,
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
        print(f"⚠️ File not found: {json_path}")
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
        print(f"⚠️ File not found: {json_path}")
        return []
    except json.JSONDecodeError as e:
        print(f"❌ Error parsing JSON: {e}")
        return []


def load_orders_raw(platform='lazada'):
    """
    Load raw orders from JSON file for specified platform
    
    Args:
        platform (str): 'lazada' or 'shopee'
        
    Returns:
        list: Orders data
    """
    if platform == 'lazada':
        return load_lazada_orders_raw()
    elif platform == 'shopee':
        return load_shopee_orders_raw()
    else:
        print(f"❌ Unknown platform: {platform}")
        return []


def clean_name(name):
    """
    Clean and extract characters from masked name
    
    Args:
        name (str): Masked name like "A**********a" or "c*****************n"
        
    Returns:
        str: Cleaned name characters
    """
    if not name or len(name) < 2:
        return "XX"  # Default if name is too short
    
    # Remove asterisks and get first and last character
    clean_name = re.sub(r'\*', '', str(name))
    if len(clean_name) >= 2:
        return clean_name[0].upper() + clean_name[-1].upper()
    elif len(clean_name) == 1:
        return clean_name[0].upper() + "X"
    else:
        return "XX"


def extract_phone_digits(phone):
    """
    Extract first 2 and last 2 digits from phone number
    
    Args:
        phone (str): Phone number like "63*********91"
        
    Returns:
        tuple: (first_2_digits, last_2_digits)
    """
    if not phone:
        return "00", "00"
    
    # Extract only digits
    digits = re.sub(r'[^\d]', '', str(phone))
    
    if len(digits) >= 4:
        return digits[:2], digits[-2:]
    elif len(digits) >= 2:
        return digits[:2], digits[:2]  # Use first 2 for both if less than 4 digits
    else:
        return "00", "00"


def generate_platform_customer_id(first_name, phone):
    """
        ACTUALLY just retrieve the buyer_id from the lazada_multiple_order_items_raw.json
    """
    name_chars = clean_name(first_name)
    first_2, last_2 = extract_phone_digits(phone)
    
    platform_customer_id = f"LZ{name_chars}{first_2}{last_2}"
    return platform_customer_id


def generate_shopee_platform_customer_id(buyer_user_id, buyer_username=None, phone=None, order_sn=None, create_time=None):
    """
    Generate platform_customer_id using Shopee buyer_user_id from API
    For buyer_user_id=0, generates unique synthetic ID based on order_sn + timestamp
    Falls back to username+phone format if buyer_user_id is not available
    
    Args:
        buyer_user_id (int/str): Shopee buyer_user_id from API (preferred)
        buyer_username (str): Shopee buyer username (fallback)
        phone (str): Customer phone number (fallback)
        order_sn (str): Order number for generating unique ID when buyer_user_id=0
        create_time (int): Unix timestamp for generating unique ID when buyer_user_id=0
        
    Returns:
        str: platform_customer_id - raw buyer_user_id or synthetic format
    """
    # Use buyer_user_id if available AND not 0 (preferred method - raw value)
    # buyer_user_id=0 indicates missing/anonymous customer data
    if buyer_user_id is not None and buyer_user_id != 0:
        return str(buyer_user_id)
    
    # Special handling for buyer_user_id=0: Generate unique synthetic ID
    # This prevents aggregating all anonymous orders into a single customer
    if buyer_user_id == 0 and order_sn and create_time:
        # Create unique ID using: last 6 digits of order_sn + last 6 digits of timestamp
        # Extract only numeric characters to avoid letters
        order_digits = ''.join(filter(str.isdigit, str(order_sn)))
        order_suffix = order_digits[-6:] if len(order_digits) >= 6 else order_digits.zfill(6)
        time_suffix = str(create_time)[-6:] if create_time else "000000"
        return f"{order_suffix}{time_suffix}"
    
    # Fallback to old method if buyer_user_id not available
    if buyer_username and phone:
        name_chars = clean_name(buyer_username)
        first_2, last_2 = extract_phone_digits(phone)
        return f"SP{name_chars}{first_2}{last_2}"
    
    # Last resort fallback
    return f"SP_UNKNOWN_{hash(str(buyer_username or '') + str(phone or '') + str(order_sn or ''))}"


def extract_customers_from_lazada_orders(orders_data):
    """
    Extract unique customers from Lazada orders and calculate metrics
    Uses buyer_id directly as platform_customer_id for better accuracy
    
    Args:
        orders_data (list): Raw Lazada orders from API
        
    Returns:
        DataFrame: Harmonized customer dimension records
    """
    # First, create a lookup of order_id -> buyer_id from order items
    print("📋 Building buyer_id lookup from lazada_multiple_order_items_raw.json...")
    staging_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'Staging')
    order_items_file = os.path.join(staging_dir, 'lazada_multiple_order_items_raw.json')
    
    buyer_id_lookup = {}
    if os.path.exists(order_items_file):
        with open(order_items_file, 'r', encoding='utf-8') as f:
            order_items_data = json.load(f)
        
        for order in order_items_data:
            order_id = str(order.get('order_id', ''))
            order_items = order.get('order_items', [])
            if order_items and len(order_items) > 0:
                buyer_id = order_items[0].get('buyer_id')
                if buyer_id:
                    buyer_id_lookup[order_id] = str(buyer_id)
        
        print(f"   ✓ Built lookup for {len(buyer_id_lookup)} orders with buyer_id")
    
    customer_records = []
    customer_order_tracking = {}  # Track orders per customer for analytics
    
    for order in orders_data:
        try:
            # Extract customer information from shipping address
            shipping_address = order.get('address_shipping', {})
            first_name = order.get('customer_first_name', '')
            phone = shipping_address.get('phone', '')
            city = shipping_address.get('city', '')
            
            # Get buyer_id from lookup, fallback to generated ID
            order_id = str(order.get('order_id', ''))
            buyer_id = buyer_id_lookup.get(order_id)
            
            # Use buyer_id directly as platform_customer_id if available
            if buyer_id:
                platform_customer_id = buyer_id
            else:
                platform_customer_id = generate_platform_customer_id(first_name, phone)
            
            # Extract order date for customer analytics
            created_at = order.get('created_at', '')
            order_date = None
            if created_at:
                try:
                    order_date = datetime.strptime(created_at.split(' ')[0], '%Y-%m-%d').date()
                except ValueError:
                    order_date = None
            
            # Track customer orders for analytics
            if platform_customer_id not in customer_order_tracking:
                customer_order_tracking[platform_customer_id] = {
                    'first_name': first_name,
                    'city': city,
                    'phone': phone,
                    'order_dates': [],
                    'order_count': 0
                }
            
            if order_date:
                customer_order_tracking[platform_customer_id]['order_dates'].append(order_date)
            customer_order_tracking[platform_customer_id]['order_count'] += 1
            
        except Exception as e:
            print(f"⚠️ Error processing order {order.get('order_id', 'unknown')}: {e}")
            continue
    
    # Generate customer dimension records
    for platform_customer_id, customer_info in customer_order_tracking.items():
        try:
            order_dates = customer_info['order_dates']
            total_orders = customer_info['order_count']
            
            # Calculate customer_since and last_order_date
            customer_since = min(order_dates) if order_dates else None
            last_order_date = max(order_dates) if order_dates else None
            
            # Determine buyer segment
            buyer_segment = "New Buyer" if total_orders == 1 else "Returning Buyer"
            
            customer_record = {
                'customer_key': None,  # Will be generated centrally in main function
                'platform_customer_id': platform_customer_id,
                'buyer_segment': buyer_segment,
                'total_orders': total_orders,
                'customer_since': customer_since,
                'last_order_date': last_order_date,
                'platform_key': 1  # Always 1 for Lazada
            }
            
            customer_records.append(customer_record)
            
        except Exception as e:
            print(f"⚠️ Error creating customer record for {platform_customer_id}: {e}")
            continue
    
    # Create DataFrame and apply data types
    customers_df = pd.DataFrame(customer_records)
    
    # Apply data type conversions using the config function
    if len(customers_df) > 0:
        customers_df = apply_data_types(customers_df, 'dim_customer')
    
    return customers_df


def extract_customers_from_shopee_orders(orders_data):
    """
    Extract unique customers from Shopee orders and calculate metrics
    
    Args:
        orders_data (list): Raw Shopee orders from API
        
    Returns:
        DataFrame: Harmonized customer dimension records
    """
    customer_records = []
    customer_order_tracking = {}  # Track orders per customer for analytics
    
    for order in orders_data:
        try:
            # Extract customer information from order and recipient_address
            recipient_address = order.get('recipient_address', {})
            buyer_user_id = order.get('buyer_user_id')  # Primary ID from API
            buyer_username = order.get('buyer_username', '')
            phone = recipient_address.get('phone', '')
            city = recipient_address.get('city', '')
            order_sn = order.get('order_sn', '')  # For unique ID generation when buyer_user_id=0
            create_time = order.get('create_time')  # For unique ID generation when buyer_user_id=0
            
            # Generate platform_customer_id using buyer_user_id (preferred)
            # If buyer_user_id=0, generates unique synthetic ID to avoid aggregation
            platform_customer_id = generate_shopee_platform_customer_id(
                buyer_user_id=buyer_user_id,
                buyer_username=buyer_username,
                phone=phone,
                order_sn=order_sn,
                create_time=create_time
            )
            
            # Extract order date for customer analytics (Shopee uses Unix timestamp)
            create_time = order.get('create_time')
            order_date = None
            if create_time:
                try:
                    order_date = datetime.fromtimestamp(create_time).date()
                except (ValueError, TypeError, OSError):
                    order_date = None
            
            # Track customer orders for analytics
            if platform_customer_id not in customer_order_tracking:
                customer_order_tracking[platform_customer_id] = {
                    'buyer_username': buyer_username,
                    'city': city,
                    'phone': phone,
                    'order_dates': [],
                    'order_count': 0
                }
            
            if order_date:
                customer_order_tracking[platform_customer_id]['order_dates'].append(order_date)
            customer_order_tracking[platform_customer_id]['order_count'] += 1
            
        except Exception as e:
            print(f"⚠️ Error processing order {order.get('order_sn', 'unknown')}: {e}")
            continue
    
    # Generate customer dimension records
    for platform_customer_id, customer_info in customer_order_tracking.items():
        try:
            order_dates = customer_info['order_dates']
            total_orders = customer_info['order_count']
            
            # Calculate customer_since and last_order_date
            customer_since = min(order_dates) if order_dates else None
            last_order_date = max(order_dates) if order_dates else None
            
            # Determine buyer segment
            buyer_segment = "New Buyer" if total_orders == 1 else "Returning Buyer"
            
            customer_record = {
                'customer_key': None,  # Will be generated centrally in main function
                'platform_customer_id': platform_customer_id,
                'buyer_segment': buyer_segment,
                'total_orders': total_orders,
                'customer_since': customer_since,
                'last_order_date': last_order_date,
                'platform_key': 2  # Always 2 for Shopee
            }
            
            customer_records.append(customer_record)
            
        except Exception as e:
            print(f"⚠️ Error creating customer record for {platform_customer_id}: {e}")
            continue
    
    # Create DataFrame
    customers_df = pd.DataFrame(customer_records)
    
    return customers_df


def harmonize_dim_customer():
    """
    Main function to harmonize Lazada and Shopee customer data into dimensional model
    
    Process:
    1. Load all customer data into DataFrames
    2. Perform cleansing and standardization
    3. Sort by customer_since ascending
    4. Generate incremental customer_key with platform-specific decimals
    
    Returns:
        pd.DataFrame: Harmonized customer dimension table
    """
    print("� Starting Customer Dimension Harmonization (Lazada + Shopee)...")
    
    # Get empty DataFrame with proper structure
    dim_customer_df = get_empty_dataframe('dim_customer')
    print(f"📋 Target schema: {list(dim_customer_df.columns)}")
    
    all_customers = []
    
    # Process Lazada customers
    print("\n� Processing Lazada customers...")
    lazada_orders = load_lazada_orders_raw()
    if lazada_orders:
        lazada_customers = extract_customers_from_lazada_orders(lazada_orders)
        if not lazada_customers.empty:
            all_customers.append(lazada_customers)
            print(f"   ✓ Extracted {len(lazada_customers)} Lazada customer records")
    else:
        print("   ⚠️ No Lazada orders available")
    
    # Process Shopee customers
    print("\n� Processing Shopee customers...")
    shopee_orders = load_shopee_orders_raw()
    if shopee_orders:
        shopee_customers = extract_customers_from_shopee_orders(shopee_orders)
        if not shopee_customers.empty:
            all_customers.append(shopee_customers)
            print(f"   ✓ Extracted {len(shopee_customers)} Shopee customer records")
    else:
        print("   ⚠️ No Shopee orders available")
    
    # Combine all customer data
    if not all_customers:
        print("❌ No customer data from any platform")
        return get_empty_dataframe('dim_customer')
    
    print(f"\n🔄 Creating unified DataFrame with {sum(len(df) for df in all_customers)} customers...")
    customers_df = pd.concat(all_customers, ignore_index=True)
    
    # Step 1: Data Cleansing and Standardization
    print("🧹 Performing data cleansing and standardization...")
    
    # Handle missing/null customer_since dates - set to minimum date for sorting
    min_date = pd.to_datetime('1900-01-01').date()
    customers_df['customer_since'] = customers_df['customer_since'].fillna(min_date)
    
    # Ensure platform_customer_id is string and not empty
    customers_df['platform_customer_id'] = customers_df['platform_customer_id'].astype(str)
    customers_df = customers_df[customers_df['platform_customer_id'] != '']
    
    # Standardize buyer_segment
    customers_df['buyer_segment'] = customers_df['buyer_segment'].fillna('').astype(str).str.strip()
    
    # Handle total_orders - ensure integer
    customers_df['total_orders'] = pd.to_numeric(customers_df['total_orders'], errors='coerce').fillna(0).astype(int)
    
    # Remove duplicates based on platform and customer ID
    initial_count = len(customers_df)
    customers_df = customers_df.drop_duplicates(subset=['platform_key', 'platform_customer_id'], keep='first')
    final_count = len(customers_df)
    if initial_count != final_count:
        print(f"📝 Removed {initial_count - final_count} duplicate customers")
    
    # Step 2: Sort by customer_since ascending
    print("📅 Sorting by customer_since ascending...")
    customers_df = customers_df.sort_values(['customer_since', 'platform_key', 'platform_customer_id'], ascending=[True, True, True])
    customers_df = customers_df.reset_index(drop=True)
    
    # Step 3: Generate incremental customer_key with platform-specific decimals
    print("🔢 Generating incremental customer_key with platform decimals...")
    customer_key_counter = 10001  # Start from 10001
    
    # Generate customer_key with platform decimals
    customer_keys = []
    for _, row in customers_df.iterrows():
        platform_key = row['platform_key']
        if platform_key == 1:  # Lazada
            customer_key = f"{customer_key_counter}.1"
        elif platform_key == 2:  # Shopee
            customer_key = f"{customer_key_counter}.2"
        else:
            customer_key = f"{customer_key_counter}.0"  # Fallback
        
        customer_keys.append(float(customer_key))
        customer_key_counter += 1
    
    customers_df['customer_key'] = customer_keys
    
    # Step 4: Apply data types
    print("🔄 Applying data types...")
    customers_df = apply_data_types(customers_df, 'dim_customer')
    
    # Remove the temporary raw_platform_customer_id column if it exists
    if 'raw_platform_customer_id' in customers_df.columns:
        customers_df = customers_df.drop(columns=['raw_platform_customer_id'])
    
    print(f"\n✅ Harmonized {len(customers_df)} total customers")
    print(f"📊 Data Summary:")
    print(f"   - Total customers: {len(customers_df)}")
    
    # Platform breakdown
    lazada_count = len(customers_df[customers_df['platform_key'] == 1])
    shopee_count = len(customers_df[customers_df['platform_key'] == 2])
    print(f"   - Lazada customers: {lazada_count} (keys: {lazada_count} with .1 decimals)")
    print(f"   - Shopee customers: {shopee_count} (keys: {shopee_count} with .2 decimals)")
    
    # Date range
    if not customers_df['customer_since'].isna().all():
        valid_dates = customers_df[customers_df['customer_since'] != min_date]['customer_since']
        if len(valid_dates) > 0:
            print(f"   - Customers with valid dates: {len(valid_dates)}")
            print(f"   - Date range: {valid_dates.min()} to {valid_dates.max()}")
    
    # Customer key ranges
    lazada_keys = customers_df[customers_df['platform_key'] == 1]['customer_key']
    shopee_keys = customers_df[customers_df['platform_key'] == 2]['customer_key']
    if len(lazada_keys) > 0:
        print(f"   - Lazada keys: {lazada_keys.min()} to {lazada_keys.max()}")
    if len(shopee_keys) > 0:
        print(f"   - Shopee keys: {shopee_keys.min()} to {shopee_keys.max()}")
    
    # Sample records
    print(f"\n📋 Sample of earliest customers (by customer_since):")
    print(customers_df[['customer_key', 'platform_customer_id', 'buyer_segment', 'customer_since', 'platform_key']].head(3).to_string(index=False))
    
    # Buyer segment breakdown
    print(f"\n📈 Customer Analytics:")
    print(f"   Buyer Segments:")
    new_buyers = len(customers_df[customers_df['buyer_segment'] == 'New Buyer'])
    returning_buyers = len(customers_df[customers_df['buyer_segment'] == 'Returning Buyer'])
    print(f"   - New Buyers: {new_buyers}")
    print(f"   - Returning Buyers: {returning_buyers}")
    print(f"   - Average Orders per Customer: {customers_df['total_orders'].mean():.1f}")
    
    return customers_df
    
    return customers_df


def save_dim_customer(df, filename='dim_customer.csv'):
    """
    Save harmonized customer dimension to CSV file
    
    Args:
        df (DataFrame): Harmonized customer dataframe
        filename (str): Output filename
    """
    if df.empty:
        print("⚠️ No data to save")
        return
        
    output_path = os.path.join(os.path.dirname(__file__), '..', 'Transformed', filename)
    
    try:
        df.to_csv(output_path, index=False)
        print(f"✅ Customer Dimension saved to: {output_path}")
        print(f"📁 Records saved: {len(df)}")
    except Exception as e:
        print(f"❌ Error saving file: {e}")


if __name__ == "__main__":
    # Run the harmonization
    customers_df = harmonize_dim_customer()
    
    # Save results
    if not customers_df.empty:
        save_dim_customer(customers_df)
    else:
        print("❌ No data to save")

