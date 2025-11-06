#!/usr/bin/env python3

import sys
sys.path.append('C:/Users/alyss/Desktop/CAPSTONE-LA-Collections')
import pandas as pd
import json
from collections import Counter

print("🔍 Deep analysis of Lazada order statuses...")

# Load raw Lazada order data
with open('app/Staging/lazada_orders_raw.json', 'r', encoding='utf-8') as f:
    lazada_orders = json.load(f)

print(f"📊 Total raw Lazada orders: {len(lazada_orders)}")

# Check all unique order statuses
order_statuses = [order.get('order_status') for order in lazada_orders]
status_counts = Counter(order_statuses)

print(f"\n📊 Lazada order status distribution:")
for status, count in status_counts.most_common():
    print(f"   - {status}: {count}")

# Load fact orders to see what's actually processed
fact_orders_df = pd.read_csv('app/Transformed/fact_orders.csv')
lazada_fact_orders = fact_orders_df[fact_orders_df['platform_key'] == 1]

print(f"\n📊 Fact orders analysis:")
print(f"   - Total fact orders: {len(fact_orders_df)}")
print(f"   - Lazada fact orders: {len(lazada_fact_orders)}")
print(f"   - Unique Lazada orders: {lazada_fact_orders['orders_key'].nunique()}")

# Check the mapping between raw orders and fact orders
print(f"\n🔍 Raw vs Fact order mapping:")
sample_fact_orders = lazada_fact_orders.head(5)['orders_key'].tolist()
print(f"   Sample fact order keys: {sample_fact_orders}")

# Find orders with different statuses that might be included
for status in status_counts.keys():
    if status and status != 'completed':
        status_orders = [o for o in lazada_orders if o.get('order_status') == status]
        print(f"\n🔍 Sample {status} orders (first 3):")
        for i, order in enumerate(status_orders[:3]):
            print(f"   {i+1}. Order SN: {order.get('order_sn', 'N/A')}")
            print(f"      - Status: {order.get('order_status', 'N/A')}")
            print(f"      - Created: {order.get('created_at', 'N/A')}")
            
        if len(status_orders) > 0:
            break

# Let's check the harmonization logic in loading_script to understand what gets processed
print(f"\n🔍 Checking which orders meet processing criteria...")

# Count orders that might be processable (non-null order_sn, etc.)
processable_orders = [o for o in lazada_orders if o.get('order_sn')]
print(f"   - Orders with order_sn: {len(processable_orders)}")

orders_with_items = [o for o in lazada_orders if o.get('order_items') and len(o.get('order_items', [])) > 0]
print(f"   - Orders with items: {len(orders_with_items)}")

# Check if we can find the missing ~593 orders by looking at the gap
total_expected = len(processable_orders)
total_harmonized = lazada_fact_orders['orders_key'].nunique()
missing_count = total_expected - total_harmonized

print(f"\n📊 Missing order calculation:")
print(f"   - Expected processable orders: {total_expected}")
print(f"   - Harmonized orders: {total_harmonized}")
print(f"   - Missing orders: {missing_count}")