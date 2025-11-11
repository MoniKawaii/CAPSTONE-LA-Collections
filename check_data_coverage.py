#!/usr/bin/env python3

import pandas as pd
import json
import os

print('🔍 Checking dimension lookups for filtering issues...')

# Check dim_order
try:
    dim_order_df = pd.read_csv('app/Transformed/dim_order.csv')
    print(f'dim_order.csv: {len(dim_order_df)} orders')
    print(f'Platforms in dim_order: {dim_order_df["platform_key"].value_counts().to_dict()}')
    print(f'Order statuses: {dim_order_df["order_status"].value_counts().head().to_dict()}')
    print(f'Date range: {dim_order_df["order_date"].min()} to {dim_order_df["order_date"].max()}')
    
    # Check how many have platform_order_id that would match our raw data
    print(f'Sample platform_order_ids from dim_order:')
    for platform in [1, 2]:
        platform_orders = dim_order_df[dim_order_df['platform_key'] == platform]
        if not platform_orders.empty:
            platform_name = 'Lazada' if platform == 1 else 'Shopee'
            print(f'  {platform_name}: {len(platform_orders)} orders')
            print(f'    Sample IDs: {platform_orders["platform_order_id"].head(3).tolist()}')
            
except Exception as e:
    print(f'Error loading dim_order: {e}')

# Check if there are raw orders not in dim_order
print(f'\n📊 Raw data counts:')

try:
    # Shopee raw data
    with open('app/Staging/shopee_multiple_order_items_raw.json', 'r', encoding='utf-8') as f:
        shopee_multiple = json.load(f)
    with open('app/Staging/shopee_orders_raw.json', 'r', encoding='utf-8') as f:
        shopee_orders = json.load(f)

    print(f'shopee_multiple_order_items_raw.json: {len(shopee_multiple)} items')
    print(f'shopee_orders_raw.json: {len(shopee_orders)} orders')

    # Get unique order_sn from both
    shopee_multiple_orders = set(str(item.get('order_sn', '')) for item in shopee_multiple)
    shopee_orders_orders = set(str(order.get('order_sn', '')) for order in shopee_orders)

    print(f'Unique order_sn in multiple_items: {len(shopee_multiple_orders)}')
    print(f'Unique order_sn in orders: {len(shopee_orders_orders)}')
    print(f'Orders in multiple_items but not in orders: {len(shopee_multiple_orders - shopee_orders_orders)}')
    print(f'Orders in orders but not in multiple_items: {len(shopee_orders_orders - shopee_multiple_orders)}')
    
    # Check Lazada too
    with open('app/Staging/lazada_multiple_order_items_raw.json', 'r', encoding='utf-8') as f:
        lazada_multiple = json.load(f)
    with open('app/Staging/lazada_orders_raw.json', 'r', encoding='utf-8') as f:
        lazada_orders = json.load(f)
    
    print(f'\nlazada_multiple_order_items_raw.json: {len(lazada_multiple)} items')
    print(f'lazada_orders_raw.json: {len(lazada_orders)} orders')
    
    lazada_multiple_orders = set(str(item.get('order_id', '')) for item in lazada_multiple)
    lazada_orders_orders = set(str(order.get('order_id', '')) for order in lazada_orders)
    
    print(f'Unique order_id in multiple_items: {len(lazada_multiple_orders)}')
    print(f'Unique order_id in orders: {len(lazada_orders_orders)}')
    
except Exception as e:
    print(f'Error loading raw data: {e}')

# Check if we can find the original fact_orders.csv to see what it had
try:
    if os.path.exists('app/Transformed/fact_orders.csv'):
        original_fact = pd.read_csv('app/Transformed/fact_orders.csv')
        print(f'\n📊 EXISTING fact_orders.csv: {len(original_fact)} records')
        if len(original_fact) > 0:
            print(f'Platform breakdown:')
            platform_counts = original_fact['platform_key'].value_counts()
            for platform, count in platform_counts.items():
                platform_name = 'Lazada' if platform == 1 else 'Shopee' if platform == 2 else f'Platform {platform}'
                print(f'  {platform_name}: {count:,} records')
        
except Exception as e:
    print(f'Error loading existing fact_orders: {e}')

print('\n✅ Analysis complete!')