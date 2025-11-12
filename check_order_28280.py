import pandas as pd

df = pd.read_csv('app/Transformed/fact_orders.csv')
records = df[df['orders_key'] == 28280.2]

print('Order 28280.2 paid_price validation:')
print('Formula: paid_price = original_unit_price - voucher_platform_amount - voucher_seller_amount')
print('')

for idx, row in records.iterrows():
    expected_paid = row['original_unit_price'] - row['voucher_platform_amount'] - row['voucher_seller_amount']
    actual_paid = row['paid_price']
    diff = actual_paid - expected_paid
    is_correct = abs(diff) < 0.01
    
    print(f'Record {row["order_item_key"]}:')
    print(f'  {row["original_unit_price"]} - {row["voucher_platform_amount"]} - {row["voucher_seller_amount"]} = Expected: {expected_paid}')
    print(f'  Actual paid_price: {actual_paid}')
    print(f'  Difference: {diff}')
    print(f'  Status: {"✓ Correct" if is_correct else "❌ WRONG"}')
    print('')

# Check if this order matches the 230708E6R03S6G sample order
dim_order = pd.read_csv('app/Transformed/dim_order.csv')
order_info = dim_order[dim_order['orders_key'] == 28280.2]
if not order_info.empty:
    order_sn = order_info.iloc[0]['platform_order_id']
    print(f'This corresponds to order_sn: {order_sn}')