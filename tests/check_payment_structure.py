import sys
import os
sys.path.append(os.path.dirname(__file__))

import json

print("🔍 Checking payment detail file structure...")

staging_dir = os.path.join(os.path.dirname(__file__), '..', 'Staging')
payment_file = os.path.join(staging_dir, 'shopee_paymentdetail_raw.json')

with open(payment_file, 'r', encoding='utf-8') as f:
    data = json.load(f)

if data and len(data) > 0:
    sample_record = data[0]
    print(f"\n📋 Sample payment detail record structure:")
    print(f"Available fields: {list(sample_record.keys())}")
    print(f"\n📊 Sample values:")
    for key, value in sample_record.items():
        print(f"   - {key}: {value}")
        if len(str(value)) > 100:  # Limit long values
            break
else:
    print("❌ No payment detail data found")