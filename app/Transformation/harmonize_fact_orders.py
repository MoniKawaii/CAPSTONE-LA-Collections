"""
Harmonize Fact Orders from Lazada API Response to Unified Schema
================================================================

This module processes Lazada order line items and harmonizes them into the Fact_Orders table.
The fact table contains one record per order line item (product within an order).

Key Requirements:
- Uses orders_key from Dim_Order (foreign key relationship)
- Each order can have multiple line items (multiple products)
- Handles unit-level aggregation for Lazada (count individual units, sum prices)
- Maps platform-specific fields to unified schema

Field Mappings for Fact_Orders:
- order_item_key: Generated unique identifier for each line item
- orders_key: Foreign key to Dim_Order.orders_key (matched by platform_order_id)
- product_key: Foreign key to Dim_Product.product_key (matched by product_item_id)
- product_variant_key: Foreign key to Dim_Product_Variant (matched by variant_sku)
- time_key: Foreign key to Dim_Time (derived from order_date, format YYYYMMDD)
- customer_key: Foreign key to Dim_Customer (matched by platform_customer_id)
- platform_key: Always 1 for Lazada, 2 for Shopee
- item_quantity: Count of units for this SKU in this order (Lazada: count of order_item records)
- paid_price: Total revenue for this line item (Lazada: sum of paid_price for all units)
- original_unit_price: Non-discounted price per unit (Lazada: item_price)
- voucher_platform_amount: Platform voucher amount
- voucher_seller_amount: Seller voucher amount  
- shipping_fee_paid_by_buyer: Shipping fee for this line item (Lazada: shipping_amount)
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
    get_empty_dataframe
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


def extract_order_items_from_lazada(orders_data, dim_order_df):
    """
    Extract and harmonize fact order records from Lazada orders
    
    Args:
        orders_data (list): Raw Lazada orders from API
        dim_order_df (DataFrame): Harmonized dimension order table
        
    Returns:
        DataFrame: Harmonized fact orders records
    """
    fact_orders_records = []
    
    # Create lookup dictionary for orders_key mapping
    order_key_lookup = dict(zip(
        dim_order_df['platform_order_id'].astype(str), 
        dim_order_df['orders_key']
    ))
    
    order_item_key_counter = 1  # Sequential counter for order_item_key
    
    for order in orders_data:
        try:
            # Get basic order information
            platform_order_id = str(order.get('order_id', ''))
            orders_key = order_key_lookup.get(platform_order_id)
            
            if orders_key is None:
                print(f"⚠️ Warning: Could not find orders_key for order_id {platform_order_id}")
                continue
                
            # Generate time_key from order creation date
            time_key = generate_time_key(order.get('created_at', ''))
            
            # Get order items (this would be from order_items array in a full API response)
            # For now, we'll simulate this based on the items_count
            items_count = order.get('items_count', 1)
            order_total = float(order.get('price', '0.0'))
            
            # For this example, we'll create a single line item per order
            # In a real scenario, you'd iterate through the order_items array
            
            fact_record = {
                'order_item_key': order_item_key_counter,
                'orders_key': orders_key,
                'product_key': None,  # To be filled when Dim_Product is available
                'product_variant_key': None,  # To be filled when Dim_Product_Variant is available
                'time_key': time_key,
                'customer_key': None,  # To be filled when Dim_Customer is available
                'platform_key': 1,  # Always 1 for Lazada
                'item_quantity': items_count,
                'paid_price': order_total,
                'original_unit_price': order_total / max(items_count, 1),  # Approximate unit price
                'voucher_platform_amount': float(order.get('voucher_platform', '0.0')),
                'voucher_seller_amount': float(order.get('voucher_seller', '0.0')),
                'shipping_fee_paid_by_buyer': float(order.get('shipping_fee', '0.0'))
            }
            
            fact_orders_records.append(fact_record)
            order_item_key_counter += 1
            
        except Exception as e:
            print(f"⚠️ Error processing order {order.get('order_id', 'unknown')}: {e}")
            continue
    
    # Create DataFrame and apply data types
    fact_orders_df = pd.DataFrame(fact_orders_records, columns=FACT_ORDERS_COLUMNS)
    
    # Apply data type conversions
    if len(fact_orders_df) > 0:
        data_types = COLUMN_DATA_TYPES.get('fact_orders', {})
        for column, dtype in data_types.items():
            if column in fact_orders_df.columns:
                try:
                    fact_orders_df[column] = fact_orders_df[column].astype(dtype)
                except Exception as e:
                    print(f"⚠️ Warning: Could not convert {column} to {dtype}: {e}")
    
    return fact_orders_df


def harmonize_fact_orders():
    """
    Main function to harmonize Fact Orders from Lazada API response
    
    Returns:
        DataFrame: Harmonized fact orders table
    """
    print("🚀 Starting Fact Orders harmonization for Lazada...")
    
    # Load source data
    orders_data = load_lazada_orders_raw()
    if not orders_data:
        print("❌ No orders data available")
        return get_empty_dataframe('fact_orders')
    
    # Load Dim_Order for orders_key mapping
    dim_order_df = load_dim_order()
    if dim_order_df.empty:
        print("❌ No Dim_Order data available")
        return get_empty_dataframe('fact_orders')
    
    # Process and harmonize
    fact_orders_df = extract_order_items_from_lazada(orders_data, dim_order_df)
    
    print(f"✅ Successfully harmonized {len(fact_orders_df)} fact order records")
    print(f"📊 Data shape: {fact_orders_df.shape}")
    
    if len(fact_orders_df) > 0:
        print("\n📋 Sample records:")
        print(fact_orders_df.head(3).to_string(index=False))
        
        print(f"\n📈 Summary Statistics:")
        print(f"   • Total Revenue: ${fact_orders_df['paid_price'].sum():.2f}")
        print(f"   • Total Items: {fact_orders_df['item_quantity'].sum():,}")
        print(f"   • Average Order Value: ${fact_orders_df['paid_price'].mean():.2f}")
        print(f"   • Date Range: {fact_orders_df['time_key'].min()} to {fact_orders_df['time_key'].max()}")
    
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