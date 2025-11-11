#!/usr/bin/env python3
"""
Check what voucher fields are available in order items for fallback processing
"""

import json
import sys
import os

def check_fallback_voucher_fields():
    """Check what voucher fields exist in order items for fallback cases"""
    try:
        print("Checking voucher fields in order items...")
        
        # Load Shopee orders
        with open('../app/Staging/shopee_orders_raw.json', 'r', encoding='utf-8') as f:
            shopee_orders = json.load(f)
        
        # Load payment details to identify orders without payment details
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
        
        # Create payment lookup
        payment_lookup = {payment.get('order_sn', ''): payment for payment in all_payment_details}
        
        # Find orders without payment details and check their voucher fields
        voucher_fields_found = set()
        sample_items = []
        
        for order in shopee_orders[:1000]:  # Check first 1000 orders
            order_sn = order.get('order_sn', '')
            if order_sn not in payment_lookup:
                # This order would use fallback
                order_list = order.get('order_list', [])
                for item in order_list:
                    # Collect all fields that might contain voucher info
                    item_fields = list(item.keys())
                    voucher_fields = [field for field in item_fields if 'voucher' in field.lower()]
                    discount_fields = [field for field in item_fields if 'discount' in field.lower()]
                    
                    voucher_fields_found.update(voucher_fields)
                    voucher_fields_found.update(discount_fields)
                    
                    if len(sample_items) < 3:
                        sample_items.append({
                            'order_sn': order_sn,
                            'item_id': item.get('item_id'),
                            'model_id': item.get('model_id'),
                            'all_fields': item_fields,
                            'voucher_fields': voucher_fields,
                            'discount_fields': discount_fields
                        })
        
        print(f"Voucher-related fields found in order items:")
        for field in sorted(voucher_fields_found):
            print(f"  - {field}")
        
        print(f"\nSample order items that would use fallback:")
        for i, item in enumerate(sample_items, 1):
            print(f"\nSample {i}:")
            print(f"  Order: {item['order_sn']}")
            print(f"  Item: {item['item_id']}_{item['model_id']}")
            print(f"  Voucher fields: {item['voucher_fields']}")
            print(f"  Discount fields: {item['discount_fields']}")
            print(f"  Total fields: {len(item['all_fields'])}")
        
        # Check if the fallback fields exist
        fallback_fields = ['voucher_absorbed_by_shopee', 'voucher_absorbed_by_seller']
        print(f"\nFallback fields check:")
        for field in fallback_fields:
            exists = field in voucher_fields_found
            print(f"  - {field}: {'EXISTS' if exists else 'MISSING'}")
        
    except Exception as e:
        print(f"Error checking fallback voucher fields: {e}")

if __name__ == "__main__":
    check_fallback_voucher_fields()