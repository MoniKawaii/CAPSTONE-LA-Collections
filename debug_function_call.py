#!/usr/bin/env python3

# Debug script to trace the exact processing path for problematic orders
import json
import pandas as pd
import os

# Add the app directory to the Python path
import sys
sys.path.append(os.path.join(os.getcwd(), 'app'))

# Import the harmonization function
from Transformation.harmonize_fact_orders import extract_order_items_from_shopee_multiple_items, load_dimension_lookups

# Load the data
print("🔄 Loading data...")

# Load dimension lookups
dim_lookups, variant_df = load_dimension_lookups()

# Load shopee data  
with open('app/Staging/shopee_multiple_order_items_raw.json', 'r') as f:
    shopee_orders_data = json.load(f)

# Load payment details using the actual load function
from Transformation.harmonize_fact_orders import load_shopee_payment_details
shopee_payment_details_data = load_shopee_payment_details()

# Load nested orders (empty for this test)
shopee_nested_orders_data = []

print(f"✓ Loaded {len(shopee_orders_data)} multiple order items")
print(f"✓ Loaded {len(shopee_payment_details_data)} payment details")

# Filter for specific problematic orders
target_orders = ['221019RJQ1CBAG', '230708E6R03S6G']
filtered_orders_data = [item for item in shopee_orders_data if item.get('order_sn') in target_orders]

print(f"\n🎯 Found {len(filtered_orders_data)} items for target orders")

for item in filtered_orders_data:
    print(f"  - Order {item.get('order_sn')}: item_id={item.get('item_id')}, qty={item.get('model_quantity_purchased')}")

# Run the transformation function with debug
print(f"\n🔄 Running transformation function with debug...")

# Temporarily modify the function to add debug for our target orders
print("⚠️ Note: We'll need to check the actual function implementation")
print("The issue might be that the unit-level granularity code has a bug or isn't being reached")

# For now, let's check what the function would return
try:
    result_df = extract_order_items_from_shopee_multiple_items(
        filtered_orders_data, 
        shopee_payment_details_data, 
        dim_lookups, 
        variant_df, 
        shopee_nested_orders_data
    )
    
    print(f"\n📊 Function returned {len(result_df)} records:")
    if not result_df.empty:
        for idx, row in result_df.iterrows():
            print(f"  - {row['order_item_key']}: orders_key={row['orders_key']}, qty={row['item_quantity']}, paid=₱{row['paid_price']}")
    
    # Check quantity distribution
    qty_counts = result_df['item_quantity'].value_counts().sort_index()
    print(f"\nQuantity distribution in results:")
    for qty, count in qty_counts.items():
        print(f"  qty={qty}: {count} records")
        
except Exception as e:
    print(f"❌ Error running function: {e}")
    import traceback
    traceback.print_exc()