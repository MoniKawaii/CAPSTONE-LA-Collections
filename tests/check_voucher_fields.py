import sys
import os
sys.path.append(os.path.dirname(__file__))

import json

print("🔍 Checking full payment detail structure for vouchers...")

staging_dir = os.path.join(os.path.dirname(__file__), '..', 'Staging')
payment_file = os.path.join(staging_dir, 'shopee_paymentdetail_raw.json')

with open(payment_file, 'r', encoding='utf-8') as f:
    data = json.load(f)

if data and len(data) > 0:
    sample_record = data[0]
    
    print(f"\n📋 Top level fields:")
    for key in sample_record.keys():
        print(f"   - {key}")
    
    # Check order_income structure
    if 'order_income' in sample_record:
        order_income = sample_record['order_income']
        print(f"\n📋 order_income fields:")
        for key in order_income.keys():
            print(f"   - {key}")
            
        # Check if items exist
        if 'items' in order_income:
            items = order_income['items']
            if items:
                print(f"\n📋 First item fields in order_income:")
                for key in items[0].keys():
                    print(f"   - {key}")
    
    # Check buyer_payment_info structure
    if 'buyer_payment_info' in sample_record:
        payment_info = sample_record['buyer_payment_info']
        print(f"\n📋 buyer_payment_info voucher fields:")
        voucher_fields = [k for k in payment_info.keys() if 'voucher' in k.lower()]
        for field in voucher_fields:
            print(f"   - {field}: {payment_info[field]}")
else:
    print("❌ No payment detail data found")