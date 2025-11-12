import json

# Load the Shopee order items
with open('app/Staging/shopee_multiple_order_items_raw.json', 'r', encoding='utf-8') as f:
    order_items = json.load(f)

# Look for the problematic orders
problem_orders = ['2203150FA0SCN5', '2203150F8D5PR4', '2210019GJKRWKS']

for order_sn in problem_orders:
    count = 0
    for item in order_items:
        if item.get('order_sn') == order_sn:
            count += 1
            item_id = item.get('item_id', 'N/A')
            model_id = item.get('model_id', 'N/A')
            print(f'Found {order_sn} item: {item_id} with model_id: {model_id}')
            print(f'  - model_original_price: {item.get("model_original_price", 0)}')
            print(f'  - model_discounted_price: {item.get("model_discounted_price", 0)}')
            print(f'  - voucher_absorbed_by_shopee: {item.get("voucher_absorbed_by_shopee", 0)}')
            print(f'  - voucher_absorbed_by_seller: {item.get("voucher_absorbed_by_seller", 0)}')
            break
    if count == 0:
        print(f'❌ Order {order_sn} NOT FOUND in order_items!')