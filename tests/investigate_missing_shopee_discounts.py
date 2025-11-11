#!/usr/bin/env python3
"""
Investigate missing Shopee discount components by examining payment detail structure
"""

import json
import pandas as pd
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

def investigate_missing_shopee_discounts():
    """Check payment details for additional discount components not captured"""
    print("🔍 INVESTIGATING MISSING SHOPEE DISCOUNT COMPONENTS")
    print("=" * 60)
    
    try:
        # Load payment details
        payment_files = [
            '../app/Staging/shopee_paymentdetail_raw.json',
            '../app/Staging/shopee_paymentdetail_2_raw.json'
        ]
        
        all_payment_details = []
        for file_path in payment_files:
            if os.path.exists(file_path):
                with open(file_path, 'r', encoding='utf-8') as f:
                    payment_data = json.load(f)
                    if isinstance(payment_data, list):
                        all_payment_details.extend(payment_data)
                    else:
                        all_payment_details.append(payment_data)
        
        print(f"✓ Loaded {len(all_payment_details)} payment detail records")
        
        # Analyze discount components in payment details
        print(f"\n🔍 ANALYZING PAYMENT DETAIL DISCOUNT STRUCTURE")
        print("-" * 50)
        
        discount_fields = []
        voucher_fields = []
        fee_fields = []
        
        # Sample a few payment records to see all available fields
        sample_count = 0
        for payment in all_payment_details[:10]:
            if sample_count >= 3:
                break
            
            print(f"\nSample Payment {sample_count + 1} - Order: {payment.get('order_sn', 'N/A')}")
            print("-" * 30)
            
            # Check buyer_payment_info
            buyer_info = payment.get('buyer_payment_info', {})
            if buyer_info:
                print("buyer_payment_info fields:")
                for key, value in buyer_info.items():
                    print(f"  {key}: {value}")
                    if 'voucher' in key.lower() or 'discount' in key.lower():
                        if key not in voucher_fields:
                            voucher_fields.append(key)
            
            # Check order_income structure
            order_income = payment.get('order_income', {})
            if order_income:
                print("\norder_income fields:")
                for key, value in order_income.items():
                    if key != 'items':
                        print(f"  {key}: {value}")
                        if 'discount' in key.lower() or 'voucher' in key.lower() or 'fee' in key.lower():
                            if key not in discount_fields:
                                discount_fields.append(key)
                
                # Check order_income.items for item-level discounts
                items = order_income.get('items', [])
                if items and len(items) > 0:
                    print("\norder_income.items[0] fields:")
                    item = items[0]
                    for key, value in item.items():
                        print(f"  {key}: {value}")
                        if 'discount' in key.lower() or 'voucher' in key.lower() or 'fee' in key.lower():
                            if key not in discount_fields:
                                discount_fields.append(key)
            
            sample_count += 1
        
        print(f"\n📊 IDENTIFIED DISCOUNT/VOUCHER FIELDS")
        print("-" * 40)
        print("Voucher fields in buyer_payment_info:")
        for field in voucher_fields:
            print(f"  • {field}")
        
        print("Discount/fee fields in order_income:")
        for field in discount_fields:
            print(f"  • {field}")
        
        # Now check what we're actually using vs what's available
        print(f"\n🔍 CURRENT HARMONIZATION VS AVAILABLE FIELDS")
        print("-" * 50)
        
        # Load fact orders to see what we currently capture
        fact_df = pd.read_csv('../app/Transformed/fact_orders.csv')
        fact_shopee = fact_df[fact_df['platform_key'] == 2]
        
        print("Currently captured in fact_orders:")
        print("  • voucher_platform_amount (from shopee_voucher?)")
        print("  • voucher_seller_amount (from seller_voucher?)")
        
        # Calculate total missing discounts
        total_vouchers_fact = fact_shopee['voucher_platform_amount'].sum() + fact_shopee['voucher_seller_amount'].sum()
        total_discount_apparent = (fact_shopee['original_unit_price'] - fact_shopee['paid_price']).sum()
        missing_discount = total_discount_apparent - total_vouchers_fact
        
        print(f"\nDiscount Analysis:")
        print(f"  • Total apparent discount: ₱{total_discount_apparent:,.2f}")
        print(f"  • Total captured vouchers: ₱{total_vouchers_fact:,.2f}")
        print(f"  • Missing discount amount: ₱{missing_discount:,.2f}")
        
        # Check specific payment details for additional fields
        print(f"\n🔍 CHECKING FOR ADDITIONAL DISCOUNT TYPES")
        print("-" * 50)
        
        additional_discounts = {}
        coins_discounts = {}
        fee_adjustments = {}
        
        for payment in all_payment_details[:100]:  # Check first 100 for patterns
            order_income = payment.get('order_income', {})
            items = order_income.get('items', [])
            
            for item in items:
                # Look for other discount types
                for key, value in item.items():
                    if ('discount' in key.lower() or 'promotion' in key.lower() or 
                        'coin' in key.lower() or 'credit' in key.lower()) and value != 0:
                        
                        if key not in ['discount_from_voucher_shopee', 'discount_from_voucher_seller']:
                            if key not in additional_discounts:
                                additional_discounts[key] = []
                            additional_discounts[key].append(float(value))
        
        if additional_discounts:
            print("Additional discount fields found:")
            for field, values in additional_discounts.items():
                if len(values) > 0:
                    total_value = sum(values)
                    avg_value = total_value / len(values)
                    print(f"  • {field}: {len(values)} instances, total ₱{total_value:.2f}, avg ₱{avg_value:.2f}")
        else:
            print("No additional discount fields found beyond vouchers")
        
        # Specific investigation of problem records
        print(f"\n🔍 INVESTIGATING SPECIFIC PROBLEMATIC RECORDS")
        print("-" * 50)
        
        # Get some specific orders with discrepancies
        discrepancy_records = fact_shopee[
            (fact_shopee['original_unit_price'] - fact_shopee['paid_price'] > 
             fact_shopee['voucher_platform_amount'] + fact_shopee['voucher_seller_amount'] + 1)
        ].head(5)
        
        for _, row in discrepancy_records.iterrows():
            order_key = row['orders_key']
            print(f"\nAnalyzing Order Key: {order_key}")
            
            apparent_discount = row['original_unit_price'] - row['paid_price']
            captured_vouchers = row['voucher_platform_amount'] + row['voucher_seller_amount']
            missing = apparent_discount - captured_vouchers
            
            print(f"  • Original: ₱{row['original_unit_price']:.2f}")
            print(f"  • Paid: ₱{row['paid_price']:.2f}")
            print(f"  • Apparent discount: ₱{apparent_discount:.2f}")
            print(f"  • Captured vouchers: ₱{captured_vouchers:.2f}")
            print(f"  • Missing discount: ₱{missing:.2f}")
        
        return missing_discount
        
    except Exception as e:
        print(f"❌ Error investigating missing discounts: {e}")
        return 0

if __name__ == "__main__":
    investigate_missing_shopee_discounts()