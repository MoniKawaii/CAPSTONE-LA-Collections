"""
Harmonize Customer Dimension from Multi-Platform API Response to Unified Schema
================================================================================

This module processes BOTH Lazada and Shopee orders and extracts customer information 
to create the Dim_Customer dimension table with proper customer segmentation and analytics.

Key Requirements from Schema:
- customer_key: Internal surrogate ID (sequential)
- platform_customer_id: Generated synthetic ID (both platforms)
- customer_city: Derived from shipping address city
- buyer_segment: Calculated as 'New Buyer' or 'Returning Buyer'
- total_orders: Count of orders per platform_customer_id
- customer_since: Earliest order_date for each platform_customer_id
- last_order_date: Latest order_date for each platform_customer_id
- platform_key: 1 for Lazada, 2 for Shopee

Platform Customer ID Generation Logic:
LAZADA: 'LZ' + first_char(first_name) + last_char(first_name) + first2_digits(phone) + last2_digits(phone)
SHOPEE: 'SP' + first_char(buyer_username) + last_char(buyer_username) + first2_digits(phone) + last2_digits(phone)

Examples: 
- Lazada: "Antonio", phone: "639123456789" → "LZAo1289"
- Shopee: "buyer123", phone: "639123456789" → "SPb31289"
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


def load_orders_raw(platform='lazada'):
    """
    Load raw orders from JSON file for specified platform
    
    Args:
        platform (str): 'lazada' or 'shopee'
        
    Returns:
        list: Orders data
    """
    filename = f'{platform}_orders_raw.json'
    json_path = os.path.join(os.path.dirname(__file__), '..', 'Staging', filename)
    
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            orders_data = json.load(f)
        print(f"✓ Loaded {len(orders_data)} raw {platform.capitalize()} orders")
        return orders_data
    except FileNotFoundError:
        print(f"⚠️ File not found: {json_path}")
        return []
    except json.JSONDecodeError as e:
        print(f"❌ Error parsing JSON: {e}")
        return []


def load_lazada_orders_raw():
    """Load raw Lazada orders from JSON file (legacy function)"""
    return load_orders_raw('lazada')


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


def generate_platform_customer_id_lazada(order_number):
    """
    Generate platform-specific customer ID for Lazada
    Format: LZ{OrderNumber}
    
    Args:
        order_number (str): The Lazada order number
        
    Returns:
        str: Platform customer ID
    """
    return f"LZ{order_number}"


def generate_platform_customer_id_shopee(order_sn):
    """
    Generate platform-specific customer ID for Shopee
    Format: SP{OrderSN}
    
    Args:
        order_sn (str): The Shopee order serial number
        
    Returns:
        str: Platform customer ID
    """
    return f"SP{order_sn}"


def extract_customers_from_lazada_orders(orders_data):
    """
    Extract unique customers from Lazada order data
    
    Args:
        orders_data (list): Raw Lazada order data
        
    Returns:
        tuple: (DataFrame with customer information, dict tracking customers)
    """
    customers_dict = {}
    tracking = {'new_buyers': 0, 'returning_buyers': 0}
    
    for order in orders_data:
        # Skip if no customer info
        if 'customer_first_name' not in order:
            continue
            
        order_number = order.get('order_number', '')
        platform_customer_id = generate_platform_customer_id_lazada(order_number)
        
        # Use phone + name as unique key since emails can be missing
        first_name = order.get('customer_first_name', '').strip()
        phone = order.get('customer_phone', '').strip()
        
        unique_key = f"{first_name}_{phone}"
        
        # Track if this is a new or returning buyer
        if unique_key in customers_dict:
            tracking['returning_buyers'] += 1
            continue
        
        tracking['new_buyers'] += 1
        
        # Parse and format dates
        created_at_str = order.get('created_at', '')
        created_at = None
        if created_at_str:
            try:
                created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
            except (ValueError, AttributeError):
                pass
        
        customer_since = created_at.strftime('%Y-%m-%d') if created_at else None
        
        # Extract address components
        address = order.get('address_shipping', {})
        
        customers_dict[unique_key] = {
            'platform_customer_id': platform_customer_id,
            'platform_key': 1,  # Lazada
            'first_name': first_name,
            'last_name': order.get('customer_last_name', '').strip(),
            'email': order.get('customer_email', '').strip(),
            'phone': phone,
            'address_line1': address.get('address1', '').strip() if address else '',
            'city': address.get('city', '').strip() if address else '',
            'postal_code': address.get('post_code', '').strip() if address else '',
            'country': address.get('country', '').strip() if address else '',
            'customer_since': customer_since,
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        }
    
    return pd.DataFrame(list(customers_dict.values())), tracking


def extract_customers_from_shopee_orders(orders_data):
    """
    Extract unique customers from Shopee order data
    
    Args:
        orders_data (list): Raw Shopee order data
        
    Returns:
        tuple: (DataFrame with customer information, dict tracking customers)
    """
    customers_dict = {}
    tracking = {'new_buyers': 0, 'returning_buyers': 0}
    
    for order in orders_data:
        # Skip if no customer info
        if 'buyer_username' not in order:
            continue
            
        order_sn = order.get('order_sn', '')
        platform_customer_id = generate_platform_customer_id_shopee(order_sn)
        
        # Use buyer_username as unique key for Shopee
        buyer_username = order.get('buyer_username', '').strip()
        
        unique_key = f"shopee_{buyer_username}"
        
        # Track if this is a new or returning buyer
        if unique_key in customers_dict:
            tracking['returning_buyers'] += 1
            continue
        
        tracking['new_buyers'] += 1
        
        # Parse Shopee timestamps (Unix timestamps)
        create_time = order.get('create_time')
        created_at = None
        customer_since = None
        
        if create_time:
            try:
                created_at = datetime.fromtimestamp(create_time)
                customer_since = created_at.strftime('%Y-%m-%d')
            except (ValueError, TypeError, OSError):
                pass
        
        # Extract address components from recipient_address
        address = order.get('recipient_address', {})
        
        customers_dict[unique_key] = {
            'platform_customer_id': platform_customer_id,
            'platform_key': 2,  # Shopee
            'first_name': buyer_username,  # Shopee uses username instead of first/last name
            'last_name': '',
            'email': '',  # Shopee doesn't expose buyer email
            'phone': address.get('phone', '').strip() if address else '',
            'address_line1': address.get('full_address', '').strip() if address else '',
            'city': address.get('city', '').strip() if address else '',
            'postal_code': address.get('zipcode', '').strip() if address else '',
            'country': address.get('region', '').strip() if address else '',
            'customer_since': customer_since,
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        }
    
    return pd.DataFrame(list(customers_dict.values())), tracking


def harmonize_dim_customer():
    """
    Main function to harmonize Customer Dimension from both Lazada and Shopee API responses
    
    Returns:
        DataFrame: Harmonized customer dimension table with data from both platforms
    """
    print("🚀 Starting Customer Dimension harmonization for multi-platform...")
    
    all_customers = []
    overall_tracking = {
        'lazada': {'new_buyers': 0, 'returning_buyers': 0},
        'shopee': {'new_buyers': 0, 'returning_buyers': 0}
    }
    
    # Process Lazada orders
    print("\n📦 Processing Lazada orders...")
    lazada_orders = load_orders_raw('lazada')
    if lazada_orders:
        lazada_customers, lazada_tracking = extract_customers_from_lazada_orders(lazada_orders)
        all_customers.append(lazada_customers)
        overall_tracking['lazada'] = lazada_tracking
        print(f"   ✓ Extracted {len(lazada_customers)} unique Lazada customers")
    else:
        print("   ⚠️ No Lazada orders available")
    
    # Process Shopee orders
    print("\n🛍️ Processing Shopee orders...")
    shopee_orders = load_orders_raw('shopee')
    if shopee_orders:
        shopee_customers, shopee_tracking = extract_customers_from_shopee_orders(shopee_orders)
        all_customers.append(shopee_customers)
        overall_tracking['shopee'] = shopee_tracking
        print(f"   ✓ Extracted {len(shopee_customers)} unique Shopee customers")
    else:
        print("   ⚠️ No Shopee orders available")
    
    # Combine all customers
    if not all_customers:
        print("❌ No customer data available from any platform")
        return get_empty_dataframe('dim_customer')
    
    customers_df = pd.concat(all_customers, ignore_index=True)
    
    print(f"\n✅ Successfully harmonized {len(customers_df)} customer records from both platforms")
    print(f"📊 Data shape: {customers_df.shape}")
    
    if len(customers_df) > 0:
        print("\n📋 Sample records:")
        print(customers_df.head(3).to_string(index=False))
        
        print(f"\n📈 Customer Analytics:")
        print(f"   Platform Breakdown:")
        print(f"   • Lazada customers: {len(customers_df[customers_df['platform_key'] == 1])}")
        print(f"   • Shopee customers: {len(customers_df[customers_df['platform_key'] == 2])}")
        print(f"\n   Buyer Segments:")
        print(f"   • Lazada new buyers: {overall_tracking['lazada']['new_buyers']}")
        print(f"   • Lazada returning buyers: {overall_tracking['lazada']['returning_buyers']}")
        print(f"   • Shopee new buyers: {overall_tracking['shopee']['new_buyers']}")
        print(f"   • Shopee returning buyers: {overall_tracking['shopee']['returning_buyers']}")
        print(f"\n   Top Cities: {customers_df['city'].value_counts().head(3).to_dict()}")
        
        if not customers_df['customer_since'].isna().all():
            date_range = f"{customers_df['customer_since'].min()} to {customers_df['customer_since'].max()}"
            print(f"   • Customer Date Range: {date_range}")
    
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

