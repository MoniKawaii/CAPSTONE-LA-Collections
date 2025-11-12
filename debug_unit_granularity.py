#!/usr/bin/env python3

import json
import pandas as pd

# Load the specific order data to debug the unit-level granularity issue
with open('app/Staging/shopee_multiple_order_items_raw.json', 'r') as f:
    multiple_items_data = json.load(f)

# Load dimension order to get orders_key
dim_order = pd.read_csv('app/Transformed/dim_order.csv')

# Find the specific problematic order
problematic_orders = ['221019RJQ1CBAG', '230708E6R03S6G']

print("🔍 DEBUGGING UNIT-LEVEL GRANULARITY ISSUE")
print("=" * 60)

for target_order in problematic_orders:
    print(f"\n📋 Checking Order: {target_order}")
    print("-" * 40)
    
    # Find in dim_order
    dim_record = dim_order[dim_order['platform_order_id'] == target_order]
    if dim_record.empty:
        print(f"❌ Order {target_order} not found in dim_order")
        continue
        
    orders_key = dim_record.iloc[0]['orders_key']
    print(f"✓ Found in dim_order: orders_key = {orders_key}")
    
    # Find in multiple_items_data
    order_found = False
    for item_record in multiple_items_data:
        if item_record.get('order_sn') == target_order:
            order_found = True
            print(f"✓ Found in multiple_items_data:")
            print(f"  - order_sn: {item_record.get('order_sn')}")
            print(f"  - item_id: {item_record.get('item_id')}")
            print(f"  - model_id: {item_record.get('model_id')}")
            print(f"  - model_quantity_purchased: {item_record.get('model_quantity_purchased')}")
            print(f"  - model_original_price: {item_record.get('model_original_price')}")
            
    if not order_found:
        print(f"❌ Order {target_order} NOT found in multiple_items_data")
        
    # Check what records exist in current fact_orders
    fact_orders = pd.read_csv('app/Transformed/fact_orders.csv')
    fact_records = fact_orders[fact_orders['orders_key'] == orders_key]
    
    print(f"\n📊 Current fact_orders records for {target_order}:")
    if not fact_records.empty:
        for idx, row in fact_records.iterrows():
            print(f"  - order_item_key: {row['order_item_key']}, qty: {row['item_quantity']}, paid: ₱{row['paid_price']}")
    else:
        print(f"❌ No records found in fact_orders for {target_order}")

print(f"\n📈 SUMMARY")
print("=" * 60)

# Count all fact_orders records by quantity
fact_orders = pd.read_csv('app/Transformed/fact_orders.csv')
shopee_records = fact_orders[fact_orders['platform_key'] == 2]
qty_counts = shopee_records['item_quantity'].value_counts().sort_index()

print(f"Shopee fact_orders quantity distribution:")
for qty, count in qty_counts.items():
    print(f"  qty={qty}: {count} records ({count/len(shopee_records)*100:.1f}%)")

print(f"\nUnit-level granularity compliance: {(qty_counts.get(1, 0)/len(shopee_records)*100):.1f}%")