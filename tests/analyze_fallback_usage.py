#!/usr/bin/env python3
"""
Check if the fallback section in harmonize_fact_orders.py is being triggered
"""

import json
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

def analyze_fallback_usage():
    """Check if payment details are missing and fallback would be triggered"""
    try:
        print("Analyzing payment detail coverage...")
        
        # Load Shopee orders
        with open('../app/Staging/shopee_orders_raw.json', 'r', encoding='utf-8') as f:
            shopee_orders = json.load(f)
        
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
        
        print(f"Total Shopee orders: {len(shopee_orders)}")
        print(f"Total payment details: {len(all_payment_details)}")
        
        # Create payment lookup
        payment_lookup = {}
        item_payment_lookup = {}
        
        for payment in all_payment_details:
            order_sn = payment.get('order_sn', '')
            if order_sn:
                payment_lookup[order_sn] = payment
                
                # Create item-level lookup
                order_income = payment.get('order_income', {})
                if 'items' in order_income and isinstance(order_income['items'], list):
                    for item in order_income['items']:
                        item_id = item.get('item_id')
                        model_id = item.get('model_id')
                        if item_id and model_id:
                            key = f"{item_id}_{model_id}"
                            item_payment_lookup[key] = item
        
        print(f"Orders with payment details: {len(payment_lookup)}")
        print(f"Items with payment details: {len(item_payment_lookup)}")
        
        # Check coverage
        orders_with_payment = 0
        orders_without_payment = 0
        items_with_payment = 0
        items_without_payment = 0
        
        for order in shopee_orders:
            order_sn = order.get('order_sn', '')
            has_payment = order_sn in payment_lookup
            
            if has_payment:
                orders_with_payment += 1
            else:
                orders_without_payment += 1
                
            # Check items
            order_list = order.get('order_list', [])
            for item in order_list:
                item_id = item.get('item_id')
                model_id = item.get('model_id')
                if item_id and model_id:
                    key = f"{item_id}_{model_id}"
                    if key in item_payment_lookup:
                        items_with_payment += 1
                    else:
                        items_without_payment += 1
        
        print(f"\nCoverage analysis:")
        print(f"Orders with payment details: {orders_with_payment}")
        print(f"Orders without payment details: {orders_without_payment}")
        print(f"Order coverage: {orders_with_payment/(orders_with_payment+orders_without_payment)*100:.1f}%")
        
        print(f"\nItems with payment details: {items_with_payment}")
        print(f"Items without payment details: {items_without_payment}")
        if (items_with_payment + items_without_payment) > 0:
            print(f"Item coverage: {items_with_payment/(items_with_payment+items_without_payment)*100:.1f}%")
        
        # Show sample fallback case
        if orders_without_payment > 0:
            print(f"\nSample orders without payment details (would use fallback):")
            count = 0
            for order in shopee_orders:
                if count >= 3:
                    break
                order_sn = order.get('order_sn', '')
                if order_sn not in payment_lookup:
                    print(f"  - Order: {order_sn}")
                    order_list = order.get('order_list', [])
                    for item in order_list[:2]:  # Show first 2 items
                        print(f"    Item: {item.get('item_id')}_{item.get('model_id')}")
                        # Check what fields are available for voucher info
                        voucher_fields = [k for k in item.keys() if 'voucher' in k.lower()]
                        if voucher_fields:
                            print(f"    Available voucher fields: {voucher_fields}")
                    count += 1
        
    except Exception as e:
        print(f"Error analyzing fallback usage: {e}")

if __name__ == "__main__":
    analyze_fallback_usage()