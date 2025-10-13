"""
Harmonize Customer Dimension from Lazada API Response to Unified Schema
======================================================================

This module processes Lazada orders and extracts customer information to create
the Dim_Customer dimension table with proper customer segmentation and analytics.

Key Requirements from Schema:
- customer_key: Internal surrogate ID (sequential)
- platform_customer_id: Generated synthetic ID (Lazada doesn't provide this)
- customer_city: Derived from shipping address city
- buyer_segment: Calculated as 'New Buyer' or 'Returning Buyer'
- total_orders: Count of orders per platform_customer_id
- customer_since: Earliest order_date for each platform_customer_id
- last_order_date: Latest order_date for each platform_customer_id
- platform_key: Always 1 for Lazada, 2 for Shopee

Platform Customer ID Generation Logic:
Since Lazada doesn't provide platform_customer_id, we generate it using:
'LZ' + first_char(first_name) + last_char(first_name) + first2_digits(phone) + last2_digits(phone)

Example: 
- first_name: "Antonio", phone: "639123456789"
- platform_customer_id: "LZAo1289"
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
    Generate platform_customer_id using Lazada naming convention
    
    Args:
        first_name (str): Customer first name
        phone (str): Customer phone number
        
    Returns:
        str: Generated platform_customer_id in format LZ{first}{last}{first2}{last2}
    """
    name_chars = clean_name(first_name)
    first_2, last_2 = extract_phone_digits(phone)
    
    platform_customer_id = f"LZ{name_chars}{first_2}{last_2}"
    return platform_customer_id


def extract_customers_from_orders(orders_data):
    """
    Extract unique customers from Lazada orders and calculate metrics
    
    Args:
        orders_data (list): Raw Lazada orders from API
        
    Returns:
        DataFrame: Harmonized customer dimension records
    """
    customer_records = []
    customer_order_tracking = {}  # Track orders per customer for analytics
    
    for order in orders_data:
        try:
            # Extract customer information from shipping address
            shipping_address = order.get('address_shipping', {})
            first_name = order.get('customer_first_name', '')
            phone = shipping_address.get('phone', '')
            city = shipping_address.get('city', '')
            
            # Generate platform_customer_id
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
    customer_key_counter = 1
    
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
                'customer_key': customer_key_counter,
                'platform_customer_id': platform_customer_id,
                'customer_city': customer_info['city'],
                'buyer_segment': buyer_segment,
                'total_orders': total_orders,
                'customer_since': customer_since,
                'last_order_date': last_order_date,
                'platform_key': 1  # Always 1 for Lazada
            }
            
            customer_records.append(customer_record)
            customer_key_counter += 1
            
        except Exception as e:
            print(f"⚠️ Error creating customer record for {platform_customer_id}: {e}")
            continue
    
    # Create DataFrame and apply data types
    customers_df = pd.DataFrame(customer_records, columns=DIM_CUSTOMER_COLUMNS)
    
    # Apply data type conversions using the config function
    if len(customers_df) > 0:
        customers_df = apply_data_types(customers_df, 'dim_customer')
    
    return customers_df


def harmonize_dim_customer():
    """
    Main function to harmonize Customer Dimension from Lazada API response
    
    Returns:
        DataFrame: Harmonized customer dimension table
    """
    print("🚀 Starting Customer Dimension harmonization for Lazada...")
    
    # Load source data
    orders_data = load_lazada_orders_raw()
    if not orders_data:
        print("❌ No orders data available")
        return get_empty_dataframe('dim_customer')
    
    # Process and harmonize
    customers_df = extract_customers_from_orders(orders_data)
    
    print(f"✅ Successfully harmonized {len(customers_df)} customer records")
    print(f"📊 Data shape: {customers_df.shape}")
    
    if len(customers_df) > 0:
        print("\n📋 Sample records:")
        print(customers_df.head(3).to_string(index=False))
        
        print(f"\n📈 Customer Analytics:")
        print(f"   • New Buyers: {len(customers_df[customers_df['buyer_segment'] == 'New Buyer'])}")
        print(f"   • Returning Buyers: {len(customers_df[customers_df['buyer_segment'] == 'Returning Buyer'])}")
        print(f"   • Average Orders per Customer: {customers_df['total_orders'].mean():.1f}")
        print(f"   • Top Cities: {customers_df['customer_city'].value_counts().head(3).to_dict()}")
        
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

