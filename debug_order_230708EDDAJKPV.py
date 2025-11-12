import json

# Load the raw data for order 230708EDDAJKPV
print("🔍 Investigating order 230708EDDAJKPV (orders_key 28280.2)")

# Check multiple_order_items_raw.json
with open('app/Staging/shopee_multiple_order_items_raw.json', 'r') as f:
    multiple_items = json.load(f)

order_items = [item for item in multiple_items if item.get('order_sn') == '230708EDDAJKPV']
print(f"\n📋 Found {len(order_items)} items in multiple_order_items_raw.json:")
for item in order_items:
    print(f"  - item_id: {item.get('item_id')}, model_id: {item.get('model_id')}")
    print(f"    quantity: {item.get('model_quantity_purchased')}")
    print(f"    original_price: {item.get('model_original_price')}")
    print(f"    discounted_price: {item.get('model_discounted_price')}")

# Check payment details
with open('app/Staging/shopee_paymentdetail_raw.json', 'r') as f:
    payment_details = json.load(f)

payment_detail = None
for detail in payment_details:
    if detail.get('order_sn') == '230708EDDAJKPV':
        payment_detail = detail
        break

if payment_detail:
    print(f"\n💰 Payment details found:")
    order_income = payment_detail.get('order_income', {})
    print(f"  - order_seller_discount: {order_income.get('order_seller_discount', 0)}")
    print(f"  - voucher_from_shopee: {order_income.get('voucher_from_shopee', 0)}")
    print(f"  - buyer_paid_shipping_fee: {order_income.get('buyer_paid_shipping_fee', 0)}")
    
    items = order_income.get('items', [])
    print(f"\n📦 Payment items ({len(items)}):")
    for i, item in enumerate(items):
        print(f"  Item {i+1}:")
        print(f"    - item_id: {item.get('item_id')}")
        print(f"    - model_id: {item.get('model_id')}")
        print(f"    - original_price: {item.get('original_price')}")
        print(f"    - selling_price: {item.get('selling_price')}")
        print(f"    - discounted_price: {item.get('discounted_price')}")
        print(f"    - quantity_purchased: {item.get('quantity_purchased')}")
        print(f"    - seller_discount: {item.get('seller_discount')}")
        print(f"    - shopee_discount: {item.get('shopee_discount')}")
        print(f"    - discount_from_voucher_shopee: {item.get('discount_from_voucher_shopee')}")
else:
    print("\n❌ No payment details found for this order")