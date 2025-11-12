import json

# Load payment details
with open('app/Staging/shopee_paymentdetail_raw.json', 'r', encoding='utf-8') as f:
    payment_details = json.load(f)

# Look for the problematic orders
problem_orders = ['2203150FA0SCN5', '2203150F8D5PR4', '2210019GJKRWKS']

for order_sn in problem_orders:
    count = 0
    for payment in payment_details:
        if payment.get('order_sn') == order_sn:
            count += 1
            print(f'Found {order_sn} payment detail:')
            print(f'  - discount_from_voucher_shopee: {payment.get("discount_from_voucher_shopee", 0)}')
            print(f'  - discount_from_coin: {payment.get("discount_from_coin", 0)}')
            print(f'  - discount_from_voucher_seller: {payment.get("discount_from_voucher_seller", 0)}')
            print(f'  - discounted_price: {payment.get("discounted_price", 0)}')
            print(f'  - original_price: {payment.get("original_price", 0)}')
            break
    if count == 0:
        print(f'❌ Order {order_sn} NOT FOUND in payment_details!')