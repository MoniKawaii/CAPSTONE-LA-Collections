#!/usr/bin/env python3

import pandas as pd

# Check dim_order for filtering
dim_order_df = pd.read_csv('app/Transformed/dim_order.csv')
print(f'📊 dim_order.csv analysis:')
print(f'Total orders: {len(dim_order_df)}')

platform_breakdown = dict(dim_order_df.groupby("platform_key").size())
print(f'Platform breakdown: {platform_breakdown}')

status_breakdown = dict(dim_order_df["order_status"].value_counts())
print(f'Status breakdown: {status_breakdown}')

# Critical insight: Check if we're only processing COMPLETED orders
completed_orders = dim_order_df[dim_order_df['order_status'] == 'COMPLETED']
print(f'\n🔍 COMPLETED orders only: {len(completed_orders)}')

completed_lazada = len(completed_orders[completed_orders["platform_key"] == 1])
completed_shopee = len(completed_orders[completed_orders["platform_key"] == 2])

print(f'  Lazada COMPLETED: {completed_lazada}')
print(f'  Shopee COMPLETED: {completed_shopee}')

# What if we process ALL orders instead of just completed?
all_orders = len(dim_order_df)
print(f'\n💡 Potential if we process ALL orders: {all_orders}')
print(f'  Current processing: {len(completed_orders)} (only COMPLETED)')
print(f'  Missing opportunity: {all_orders - len(completed_orders)} additional orders')

# Check what other statuses we're missing
other_statuses = dim_order_df[dim_order_df['order_status'] != 'COMPLETED']
print(f'\n📋 Non-COMPLETED orders we\'re skipping:')
for status, count in other_statuses['order_status'].value_counts().items():
    print(f'  {status}: {count} orders')