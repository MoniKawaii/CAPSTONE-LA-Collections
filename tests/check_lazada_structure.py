#!/usr/bin/env python3

import sys
sys.path.append('C:/Users/alyss/Desktop/CAPSTONE-LA-Collections')
import pandas as pd
import json

print("🔍 Examining Lazada raw order structure...")

# Load raw Lazada order data
with open('app/Staging/lazada_orders_raw.json', 'r', encoding='utf-8') as f:
    lazada_orders = json.load(f)

print(f"📊 Total raw Lazada orders: {len(lazada_orders)}")

if len(lazada_orders) > 0:
    print(f"\n🔍 First Lazada order structure:")
    first_order = lazada_orders[0]
    print(f"   Keys: {list(first_order.keys())}")
    
    print(f"\n🔍 Sample order data:")
    for key, value in first_order.items():
        if isinstance(value, (list, dict)):
            print(f"   - {key}: {type(value).__name__} (length: {len(value) if hasattr(value, '__len__') else 'N/A'})")
        else:
            print(f"   - {key}: {value}")
    
    print(f"\n🔍 Checking if we have order items in multiple files:")
    
# Check if we have separate order items file
try:
    with open('app/Staging/lazada_multiple_order_items_raw.json', 'r', encoding='utf-8') as f:
        lazada_items = json.load(f)
    print(f"📊 Lazada order items file: {len(lazada_items)} items")
    
    if len(lazada_items) > 0:
        print(f"   Sample item keys: {list(lazada_items[0].keys())}")
        
except FileNotFoundError:
    print("❌ No lazada_multiple_order_items_raw.json found")

# Check how orders are getting into fact_orders if raw orders have no order_sn
print(f"\n🔍 Investigating how orders get processed...")

# Look at the transformation scripts to understand the mapping
fact_orders_df = pd.read_csv('app/Transformed/fact_orders.csv')
lazada_fact_orders = fact_orders_df[fact_orders_df['platform_key'] == 1]

print(f"📊 Sample fact order data (Lazada):")
sample_fact = lazada_fact_orders.head(3)
for col in sample_fact.columns:
    print(f"   - {col}: {sample_fact[col].tolist()}")
    
print(f"\n🔍 Unique order key patterns:")
unique_keys = lazada_fact_orders['orders_key'].unique()[:10]
print(f"   Sample order keys: {unique_keys}")

# This suggests the harmonization script is working with DIFFERENT Lazada data
# Let's check if there's another source or if the data structure changed