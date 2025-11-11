import sys
import os
sys.path.append(os.path.dirname(__file__))

from harmonize_fact_orders import load_shopee_payment_details_raw

print("🔍 Testing Shopee payment details loading...")

payment_details = load_shopee_payment_details_raw()

print(f"\n📊 Payment Details Summary:")
print(f"   - Total payment records: {len(payment_details) if payment_details else 0}")

if payment_details:
    sample_keys = list(payment_details.keys())[:5]
    print(f"   - Sample order_sn keys: {sample_keys}")
    
    # Check one sample payment detail
    if sample_keys:
        sample_order = sample_keys[0] 
        sample_detail = payment_details[sample_order]
        print(f"\n📋 Sample payment detail for order {sample_order}:")
        print(f"   - order_sn: {sample_detail.get('order_sn')}")
        print(f"   - voucher_absorbed_by_shopee: {sample_detail.get('voucher_absorbed_by_shopee', 'N/A')}")
        print(f"   - voucher_absorbed_by_seller: {sample_detail.get('voucher_absorbed_by_seller', 'N/A')}")
        print(f"   - actual_shipping_fee: {sample_detail.get('actual_shipping_fee', 'N/A')}")
else:
    print("❌ No payment details found!")