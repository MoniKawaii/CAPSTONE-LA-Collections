#!/usr/bin/env python3
"""
Check if discount_from_coin is the missing component in Shopee pricing
"""

import json
import pandas as pd
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

def check_missing_coin_discounts():
    """Check if discount_from_coin explains the missing pricing component"""
    print("🔍 CHECKING MISSING COIN DISCOUNTS")
    print("=" * 40)
    
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
        
        # Extract coin discount information
        coin_discounts = []
        
        for payment in all_payment_details:
            order_sn = payment.get('order_sn', '')
            order_income = payment.get('order_income', {})
            items = order_income.get('items', [])
            
            for item in items:
                item_id = item.get('item_id', '')
                model_id = item.get('model_id', 0)
                
                # Get coin discount
                discount_from_coin = item.get('discount_from_coin', 0)
                
                if discount_from_coin != 0:
                    coin_discounts.append({
                        'order_sn': order_sn,
                        'item_id': item_id,
                        'model_id': model_id,
                        'discount_from_coin': float(discount_from_coin),
                        'discounted_price': float(item.get('discounted_price', 0)),
                        'original_price': float(item.get('original_price', 0))
                    })
        
        if coin_discounts:
            coin_df = pd.DataFrame(coin_discounts)
            print(f"\n📊 COIN DISCOUNT ANALYSIS:")
            print(f"Items with coin discounts: {len(coin_df):,}")
            print(f"Total coin discount amount: ₱{coin_df['discount_from_coin'].sum():,.2f}")
            print(f"Average coin discount: ₱{coin_df['discount_from_coin'].mean():.2f}")
            print(f"Max coin discount: ₱{coin_df['discount_from_coin'].max():.2f}")
            
            # Show sample records
            print(f"\nSample coin discount records:")
            print(coin_df.head().to_string())
            
            return coin_df['discount_from_coin'].sum()
        else:
            print("No coin discounts found")
            return 0
        
    except Exception as e:
        print(f"❌ Error checking coin discounts: {e}")
        return 0

def check_all_missing_discounts():
    """Check all potential missing discount types"""
    print("\n🔍 CHECKING ALL MISSING DISCOUNT TYPES")
    print("=" * 50)
    
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
        
        # Collect all discount types
        discount_totals = {
            'discount_from_coin': 0,
            'seller_discount': 0,
            'shopee_discount': 0,
            'discount_from_voucher_shopee': 0,
            'discount_from_voucher_seller': 0,
            'payment_promotion': 0,
            'trade_in_discount': 0,
            'credit_card_promotion': 0
        }
        
        discount_counts = dict.fromkeys(discount_totals.keys(), 0)
        
        for payment in all_payment_details:
            # Check buyer_payment_info for order-level discounts
            buyer_info = payment.get('buyer_payment_info', {})
            order_income = payment.get('order_income', {})
            
            # Order-level discounts
            if 'trade_in_discount' in buyer_info:
                val = buyer_info.get('trade_in_discount', 0)
                if isinstance(val, (int, float)) and val != 0:
                    discount_totals['trade_in_discount'] += val
                    discount_counts['trade_in_discount'] += 1
            
            if 'credit_card_promotion' in buyer_info:
                val = buyer_info.get('credit_card_promotion', 0)
                if isinstance(val, (int, float)) and val != 0:
                    discount_totals['credit_card_promotion'] += val
                    discount_counts['credit_card_promotion'] += 1
            
            if 'payment_promotion' in order_income:
                val = order_income.get('payment_promotion', 0)
                if isinstance(val, (int, float)) and val != 0:
                    discount_totals['payment_promotion'] += val
                    discount_counts['payment_promotion'] += 1
            
            # Item-level discounts
            items = order_income.get('items', [])
            for item in items:
                for discount_type in ['discount_from_coin', 'seller_discount', 'shopee_discount',
                                    'discount_from_voucher_shopee', 'discount_from_voucher_seller']:
                    val = item.get(discount_type, 0)
                    if isinstance(val, (int, float)) and val != 0:
                        discount_totals[discount_type] += val
                        discount_counts[discount_type] += 1
        
        print("DISCOUNT COMPONENT BREAKDOWN:")
        print("-" * 40)
        
        total_all_discounts = 0
        for discount_type, total_amount in discount_totals.items():
            count = discount_counts[discount_type]
            if total_amount != 0:
                print(f"{discount_type}:")
                print(f"  Count: {count:,} instances")
                print(f"  Total: ₱{total_amount:,.2f}")
                print(f"  Average: ₱{total_amount/max(count,1):.2f}")
                total_all_discounts += total_amount
                print()
        
        print(f"TOTAL ALL DISCOUNT TYPES: ₱{total_all_discounts:,.2f}")
        
        # Compare with current fact table
        fact_df = pd.read_csv('../app/Transformed/fact_orders.csv')
        fact_shopee = fact_df[fact_df['platform_key'] == 2]
        
        current_vouchers = fact_shopee['voucher_platform_amount'].sum() + fact_shopee['voucher_seller_amount'].sum()
        apparent_discount = (fact_shopee['original_unit_price'] - fact_shopee['paid_price']).sum()
        
        print(f"\nCOMPARISON:")
        print(f"Current captured vouchers: ₱{current_vouchers:,.2f}")
        print(f"Apparent total discount: ₱{apparent_discount:,.2f}")
        print(f"All payment detail discounts: ₱{total_all_discounts:,.2f}")
        print(f"Still missing: ₱{apparent_discount - total_all_discounts:,.2f}")
        
        return discount_totals
        
    except Exception as e:
        print(f"❌ Error checking all discounts: {e}")
        return {}

if __name__ == "__main__":
    coin_total = check_missing_coin_discounts()
    all_discounts = check_all_missing_discounts()
    
    print(f"\n🎯 SUMMARY:")
    print(f"The missing ₱274,474 appears to be primarily from:")
    print(f"  • discount_from_coin: ₱{coin_total:,.2f}")
    
    if all_discounts:
        other_missing = sum(v for k, v in all_discounts.items() if k != 'discount_from_voucher_shopee' and k != 'discount_from_voucher_seller' and v > 0)
        print(f"  • Other discount types: ₱{other_missing - coin_total:,.2f}")