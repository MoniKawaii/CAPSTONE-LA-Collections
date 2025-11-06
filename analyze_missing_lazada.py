#!/usr/bin/env python3

import sys
sys.path.append('C:/Users/alyss/Desktop/CAPSTONE-LA-Collections')
import pandas as pd
import json

print("🔍 Analyzing missing Lazada orders in fact_orders...")

# Load raw Lazada order data
with open('app/Staging/lazada_orders_raw.json', 'r', encoding='utf-8') as f:
    lazada_orders = json.load(f)

print(f"📊 Total raw Lazada orders: {len(lazada_orders)}")

# Count completed orders
completed_orders = [order for order in lazada_orders if order.get('order_status') == 'completed']
print(f"📊 Completed Lazada orders: {len(completed_orders)}")

# Load harmonized fact orders
fact_orders_df = pd.read_csv('app/Transformed/fact_orders.csv')
lazada_fact_orders = fact_orders_df[fact_orders_df['platform_key'] == 1]
unique_lazada_orders = lazada_fact_orders['orders_key'].nunique()

print(f"📊 Harmonized Lazada orders in fact_orders: {unique_lazada_orders}")
print(f"📊 Missing Lazada orders: {len(completed_orders) - unique_lazada_orders}")

# Load customer dimension to check Lazada customers
dim_customer_df = pd.read_csv('app/Transformed/dim_customer.csv')
lazada_customers = dim_customer_df[dim_customer_df['platform_key'] == 1]
print(f"📊 Lazada customers in dimension: {len(lazada_customers)}")

# Check if we have anonymous Lazada customer
anonymous_lazada = lazada_customers[lazada_customers['buyer_segment'] == 'Anonymous']
print(f"📊 Anonymous Lazada customers: {len(anonymous_lazada)}")

if len(anonymous_lazada) > 0:
    print(f"✅ Anonymous Lazada customer exists: {anonymous_lazada.iloc[0]['platform_customer_id']}")
else:
    print("❌ No anonymous Lazada customer found!")

# Sample some completed orders that might be missing
print(f"\n🔍 Sample completed Lazada orders (first 5):")
for i, order in enumerate(completed_orders[:5]):
    print(f"   {i+1}. Order: {order.get('order_sn', 'N/A')}")
    print(f"      - Status: {order.get('order_status', 'N/A')}")
    print(f"      - Customer info available: {bool(order.get('customer_info'))}")
    if order.get('customer_info'):
        customer = order['customer_info']
        print(f"      - First name: '{customer.get('first_name', 'N/A')}'")
        print(f"      - Phone: '{customer.get('phone', 'N/A')}'")