#!/usr/bin/env python3

import pandas as pd
import sys
import os

# Add the app directory to the Python path
sys.path.append(os.path.join(os.getcwd(), 'app'))

# Import the harmonization function
from Transformation.harmonize_fact_orders import extract_order_items_from_shopee_multiple_items, load_dimension_lookups, load_shopee_payment_details

print("🔍 TESTING PAID PRICE CALCULATION FIX")
print("=" * 60)

# Load the data
print("🔄 Loading data...")

# Load dimension lookups
dim_lookups, variant_df = load_dimension_lookups()

# Load payment details
shopee_payment_details_data = load_shopee_payment_details()

# Load test orders data  
import json
with open('app/Staging/shopee_multiple_order_items_raw.json', 'r') as f:
    shopee_orders_data = json.load(f)

# Filter for specific test orders
target_orders = ['210511BGWNB0FQ', '221019RJQ1CBAG']
filtered_orders_data = [item for item in shopee_orders_data if item.get('order_sn') in target_orders]

print(f"✓ Found {len(filtered_orders_data)} test order items")

# Run the transformation function
print(f"\n🔄 Running transformation with FIXED paid price calculation...")

result_df = extract_order_items_from_shopee_multiple_items(
    filtered_orders_data, 
    shopee_payment_details_data, 
    dim_lookups, 
    variant_df, 
    []  # Empty nested orders
)

print(f"\n📊 RESULTS - Fixed Paid Price Calculation:")
print("=" * 60)

if not result_df.empty:
    # Group by orders_key for analysis
    for orders_key in result_df['orders_key'].unique():
        order_records = result_df[result_df['orders_key'] == orders_key]
        
        print(f"\nOrder Key: {orders_key}")
        print(f"Total Records: {len(order_records)}")
        
        for idx, row in order_records.iterrows():
            original = row['original_unit_price']
            voucher_platform = row['voucher_platform_amount']
            voucher_seller = row['voucher_seller_amount'] 
            paid = row['paid_price']
            
            # Calculate expected paid price
            expected_paid = original - voucher_platform - voucher_seller
            
            print(f"  Record {idx + 1}:")
            print(f"    Original Unit Price: ₱{original:.2f}")
            print(f"    Platform Voucher:    ₱{voucher_platform:.2f}")
            print(f"    Seller Voucher:      ₱{voucher_seller:.2f}")
            print(f"    Paid Price:          ₱{paid:.2f}")
            print(f"    Expected Paid:       ₱{expected_paid:.2f}")
            print(f"    Formula Check: ✅" if abs(paid - expected_paid) < 0.01 else f"    Formula Check: ❌ Difference: ₱{abs(paid - expected_paid):.2f}")
            
    # Overall validation
    print(f"\n📈 OVERALL VALIDATION:")
    print("=" * 40)
    
    # Check if paid_price = original_unit_price - voucher_platform_amount - voucher_seller_amount for all records
    result_df['calculated_paid'] = result_df['original_unit_price'] - result_df['voucher_platform_amount'] - result_df['voucher_seller_amount']
    result_df['price_diff'] = abs(result_df['paid_price'] - result_df['calculated_paid'])
    
    accurate_count = (result_df['price_diff'] < 0.01).sum()
    total_count = len(result_df)
    
    print(f"Records with accurate paid price calculation: {accurate_count}/{total_count}")
    print(f"Accuracy percentage: {(accurate_count/total_count)*100:.1f}%")
    
    if accurate_count == total_count:
        print("✅ PAID PRICE CALCULATION IS NOW CORRECT!")
    else:
        print(f"❌ {total_count - accurate_count} records still have incorrect paid price calculation")
        
        # Show problematic records
        problematic = result_df[result_df['price_diff'] >= 0.01]
        print("\nProblematic records:")
        for idx, row in problematic.iterrows():
            print(f"  Order Key {row['orders_key']}: Expected ₱{row['calculated_paid']:.2f}, Got ₱{row['paid_price']:.2f}")
    
else:
    print("❌ No records returned from transformation function")