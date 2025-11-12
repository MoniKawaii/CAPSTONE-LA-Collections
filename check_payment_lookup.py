import json

# Load order items and payment details
with open('app/Staging/shopee_multiple_order_items_raw.json', 'r', encoding='utf-8') as f:
    order_items = json.load(f)

with open('app/Staging/shopee_paymentdetail_raw.json', 'r', encoding='utf-8') as f:
    payment_details = json.load(f)

# Look for the problematic orders
problem_orders = ['2203150FA0SCN5', '2203150F8D5PR4', '2210019GJKRWKS']

# Build payment lookup like the transformation does
payment_item_lookup = {}
for payment in payment_details:
    for item in payment.get('payment_item_list', []):
        item_id = item.get('item_id', '')
        model_id = item.get('model_id', '0')
        payment_item_key = f"{item_id}_{model_id}"
        payment_item_lookup[payment_item_key] = item

print(f"Total payment_item_lookup entries: {len(payment_item_lookup)}")

for order_sn in problem_orders:
    print(f"\n=== Checking {order_sn} ===")
    for item in order_items:
        if item.get('order_sn') == order_sn:
            item_id = item.get('item_id')
            model_id = item.get('model_id', '0')
            payment_item_key = f"{item_id}_{model_id}"
            
            print(f"Item: {item_id}, Model: {model_id}")
            print(f"Payment key: {payment_item_key}")
            
            if payment_item_key in payment_item_lookup:
                payment_item = payment_item_lookup[payment_item_key]
                print(f"✅ Found in payment_item_lookup:")
                print(f"  - original_price: {payment_item.get('original_price', 0)}")
                print(f"  - discounted_price: {payment_item.get('discounted_price', 0)}")
                print(f"  - discount_from_voucher_shopee: {payment_item.get('discount_from_voucher_shopee', 0)}")
            else:
                print("❌ NOT found in payment_item_lookup (would use fallback)")
            break