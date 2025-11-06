#!/usr/bin/env python3

import sys
sys.path.append('C:/Users/alyss/Desktop/CAPSTONE-LA-Collections')
import pandas as pd

print("🔍 Investigating missing orders in fact_orders harmonization...")

# Load the tables
dim_order_df = pd.read_csv('app/Transformed/dim_order.csv')
fact_orders_df = pd.read_csv('app/Transformed/fact_orders.csv')

# Focus on COMPLETED orders only
completed_orders = dim_order_df[dim_order_df['order_status'] == 'COMPLETED']
print(f"📊 Total COMPLETED orders in dim_order: {len(completed_orders)}")

# Check how many unique orders are in fact_orders
unique_fact_orders = fact_orders_df['orders_key'].nunique()
print(f"📊 Unique orders in fact_orders: {unique_fact_orders}")

print(f"📊 Missing orders: {len(completed_orders) - unique_fact_orders}")

# Break down by platform
completed_lazada = completed_orders[completed_orders['platform_key'] == 1]
completed_shopee = completed_orders[completed_orders['platform_key'] == 2]

fact_lazada = fact_orders_df[fact_orders_df['platform_key'] == 1]
fact_shopee = fact_orders_df[fact_orders_df['platform_key'] == 2]

unique_fact_lazada = fact_lazada['orders_key'].nunique()
unique_fact_shopee = fact_shopee['orders_key'].nunique()

print(f"\n📊 Platform breakdown:")
print(f"   Lazada:")
print(f"     - COMPLETED in dim_order: {len(completed_lazada)}")
print(f"     - Unique in fact_orders: {unique_fact_lazada}")
print(f"     - Missing: {len(completed_lazada) - unique_fact_lazada}")

print(f"   Shopee:")
print(f"     - COMPLETED in dim_order: {len(completed_shopee)}")
print(f"     - Unique in fact_orders: {unique_fact_shopee}")
print(f"     - Missing: {len(completed_shopee) - unique_fact_shopee}")

# Let's find specific orders that are missing
# Get the order keys from fact_orders
fact_order_keys = set(fact_orders_df['orders_key'].unique())

# Get all COMPLETED order keys from dim_order
completed_order_keys = set(completed_orders['orders_key'].tolist())

# Find missing order keys
missing_order_keys = completed_order_keys - fact_order_keys

print(f"\n🔍 Analysis of missing order keys:")
print(f"   - Missing order keys: {len(missing_order_keys)}")

if len(missing_order_keys) > 0:
    # Sample some missing orders
    sample_missing = list(missing_order_keys)[:5]
    print(f"   - Sample missing order keys: {sample_missing}")
    
    # Look up details of missing orders
    missing_orders_details = completed_orders[completed_orders['orders_key'].isin(sample_missing)]
    print(f"\n🔍 Sample missing orders details:")
    for _, order in missing_orders_details.iterrows():
        platform_name = "Lazada" if order['platform_key'] == 1 else "Shopee"
        print(f"     - Order key {order['orders_key']}: {platform_name} order {order['platform_order_id']}")
        print(f"       Status: {order['order_status']}, Date: {order['order_date']}")

print(f"\n💡 This confirms that the issue is in the fact_orders harmonization process!")
print(f"   - COMPLETED orders exist in dim_order but don't make it to fact_orders")
print(f"   - Need to debug why these {len(missing_order_keys)} orders are being skipped")