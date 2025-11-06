#!/usr/bin/env python3

import sys
sys.path.append('C:/Users/alyss/Desktop/CAPSTONE-LA-Collections')
import pandas as pd
from collections import Counter

print("🔍 Analyzing dim_order table status values...")

# Load dim_order table
dim_order_df = pd.read_csv('app/Transformed/dim_order.csv')

print(f"📊 Total orders in dim_order: {len(dim_order_df)}")

# Check platform distribution
platform_counts = dim_order_df['platform_key'].value_counts()
print(f"\n📊 Platform distribution:")
for platform, count in platform_counts.items():
    platform_name = "Lazada" if platform == 1 else "Shopee" if platform == 2 else f"Platform {platform}"
    print(f"   - {platform_name} (key {platform}): {count}")

# Check status distribution
status_counts = dim_order_df['order_status'].value_counts()
print(f"\n📊 Order status distribution:")
for status, count in status_counts.items():
    print(f"   - '{status}': {count}")

# Focus on Lazada orders specifically
lazada_orders = dim_order_df[dim_order_df['platform_key'] == 1]
lazada_status_counts = lazada_orders['order_status'].value_counts()
print(f"\n📊 Lazada order status distribution:")
for status, count in lazada_status_counts.items():
    print(f"   - '{status}': {count}")

# Check for COMPLETED orders specifically
completed_orders = dim_order_df[dim_order_df['order_status'] == 'COMPLETED']
completed_lazada = completed_orders[completed_orders['platform_key'] == 1]
completed_shopee = completed_orders[completed_orders['platform_key'] == 2]

print(f"\n📊 COMPLETED orders analysis:")
print(f"   - Total COMPLETED orders: {len(completed_orders)}")
print(f"   - COMPLETED Lazada orders: {len(completed_lazada)}")
print(f"   - COMPLETED Shopee orders: {len(completed_shopee)}")

if len(completed_lazada) > 0:
    print(f"\n✅ SUCCESS: Lazada orders ARE being marked as COMPLETED!")
    print(f"   Sample COMPLETED Lazada order IDs: {completed_lazada['platform_order_id'].head().tolist()}")
else:
    print(f"\n❌ ISSUE: NO Lazada orders are marked as COMPLETED!")
    print(f"   This explains why fact_orders has missing Lazada orders")
    
# Show sample Lazada orders and their status progression
print(f"\n🔍 Sample Lazada orders in dim_order:")
sample_lazada = lazada_orders.head(5)
for _, order in sample_lazada.iterrows():
    print(f"   - Order {order['platform_order_id']}: status='{order['order_status']}'")

print(f"\n💡 Analysis Summary:")
print(f"   - If Lazada orders show as 'COMPLETED' → mapping is working correctly")
print(f"   - If Lazada orders show as 'CONFIRMED' → mapping failed")
print(f"   - If no COMPLETED Lazada orders → this is why fact_orders is missing ~593 Lazada orders")