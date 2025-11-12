#!/usr/bin/env python3

import pandas as pd
import sys
import os

# Add the app directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

def check_voucher_proration():
    """Check if the voucher proration is fixed for order 22110331NRG31Q"""
    
    print("🔍 Checking voucher proration fix for order 22110331NRG31Q...")
    
    # Load the latest fact orders data
    try:
        # Load from the harmonization result 
        from app.Transformation.harmonize_fact_orders import harmonize_fact_orders
        fact_df = harmonize_fact_orders()
        
        print(f"✓ Loaded {len(fact_df)} fact order records")
        
        # Load dim_order to get the orders_key for 22110331NRG31Q 
        dim_order_df = pd.read_csv('app/Transformed/dim_order.csv')
        target_order = dim_order_df[dim_order_df['platform_order_id'] == '22110331NRG31Q']
        
        if target_order.empty:
            print("❌ Order 22110331NRG31Q not found in dim_order")
            return
            
        orders_key = target_order.iloc[0]['orders_key']
        print(f"✓ Found orders_key: {orders_key}")
        
        # Get fact records for this order
        order_records = fact_df[fact_df['orders_key'] == orders_key].copy()
        order_records = order_records.sort_values('order_item_key')
        
        if order_records.empty:
            print("❌ No fact records found for this order")
            return
            
        print(f"✓ Found {len(order_records)} fact records for order 22110331NRG31Q")
        
        print("\n📊 VOUCHER PRORATION ANALYSIS:")
        print("=" * 60)
        
        for idx, record in order_records.iterrows():
            item_key = record['order_item_key']
            original_price = record['original_unit_price']
            seller_voucher = record['voucher_seller_amount']
            platform_voucher = record['voucher_platform_amount']
            paid_price = record['paid_price']
            
            calculated_paid_price = original_price - seller_voucher - platform_voucher
            
            print(f"Record {item_key}:")
            print(f"  Original Unit Price: ₱{original_price}")
            print(f"  Seller Voucher:      ₱{seller_voucher}")
            print(f"  Platform Voucher:    ₱{platform_voucher}")
            print(f"  Paid Price:          ₱{paid_price}")
            print(f"  Calculated:          ₱{calculated_paid_price}")
            print(f"  Match: {'✅' if abs(paid_price - calculated_paid_price) < 0.01 else '❌'}")
            print()
            
        # Summary
        total_seller_voucher = order_records['voucher_seller_amount'].sum()
        total_platform_voucher = order_records['voucher_platform_amount'].sum()
        avg_original_price = order_records['original_unit_price'].mean()
        avg_paid_price = order_records['paid_price'].mean()
        
        print("📈 SUMMARY:")
        print(f"  Total Units: {len(order_records)}")
        print(f"  Total Seller Voucher: ₱{total_seller_voucher:.2f}")
        print(f"  Total Platform Voucher: ₱{total_platform_voucher:.2f}")
        print(f"  Average Original Price: ₱{avg_original_price:.2f}")
        print(f"  Average Paid Price: ₱{avg_paid_price:.2f}")
        
        # Expected values
        expected_seller_voucher_per_unit = 10.0 / 3  # ₱10 / 3 units = ₱3.33
        expected_paid_price = 350.0 - expected_seller_voucher_per_unit  # ₱350 - ₱3.33 = ₱346.67
        
        print(f"\n🎯 EXPECTED VALUES:")
        print(f"  Expected Seller Voucher per unit: ₱{expected_seller_voucher_per_unit:.2f}")
        print(f"  Expected Paid Price per unit: ₱{expected_paid_price:.2f}")
        
        # Validation
        voucher_per_unit_correct = abs(order_records['voucher_seller_amount'].iloc[0] - expected_seller_voucher_per_unit) < 0.01
        paid_price_correct = abs(order_records['paid_price'].iloc[0] - expected_paid_price) < 0.01
        
        print(f"\n✅ VALIDATION RESULTS:")
        print(f"  Seller Voucher Proration: {'✅ FIXED' if voucher_per_unit_correct else '❌ STILL WRONG'}")
        print(f"  Paid Price Calculation: {'✅ CORRECT' if paid_price_correct else '❌ INCORRECT'}")
        
        if voucher_per_unit_correct and paid_price_correct:
            print(f"\n🎉 SUCCESS! Voucher proration is now working correctly!")
        else:
            print(f"\n⚠️ ISSUE: Voucher proration still needs fixing")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    check_voucher_proration()