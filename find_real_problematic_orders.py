"""
Find the order SNs for the actual problematic orders_keys
"""
import pandas as pd

# Load the saved fact orders
fact_orders_df = pd.read_csv('app/Transformed/fact_orders.csv')
dim_orders_df = pd.read_csv('app/Transformed/dim_order.csv')

print("Columns in dim_order:", dim_orders_df.columns.tolist())
print("Sample dim_order row:")
print(dim_orders_df.head(1))

# Get the actual problematic orders
problematic_orders = fact_orders_df[
    fact_orders_df['voucher_platform_amount'] + fact_orders_df['voucher_seller_amount'] > fact_orders_df['original_unit_price']
]

print(f"Found {len(problematic_orders)} problematic orders")

# Get the first 10 problematic orders_keys to investigate
sample_problematic_keys = problematic_orders['orders_key'].head(10).tolist()

print(f"\nSample problematic orders_keys: {sample_problematic_keys}")

# Map these to order SNs
for orders_key in sample_problematic_keys:
    order_row = dim_orders_df[dim_orders_df['orders_key'] == orders_key]
    if len(order_row) > 0:
        order_sn = order_row.iloc[0]['platform_order_id']
        platform_key = order_row.iloc[0]['platform_key']
        platform_name = "Shopee" if platform_key == 2 else "Lazada"
        print(f"orders_key {orders_key} -> order_sn: {order_sn} (platform: {platform_name})")
        
        # Also show the problematic fact record
        fact_record = problematic_orders[problematic_orders['orders_key'] == orders_key].iloc[0]
        print(f"  • paid_price: {fact_record['paid_price']}")
        print(f"  • original_unit_price: {fact_record['original_unit_price']}")
        print(f"  • voucher_platform_amount: {fact_record['voucher_platform_amount']}")
        print(f"  • voucher_seller_amount: {fact_record['voucher_seller_amount']}")
        total_vouchers = fact_record['voucher_platform_amount'] + fact_record['voucher_seller_amount']
        print(f"  • total_vouchers: {total_vouchers}")
        print()
    else:
        print(f"orders_key {orders_key} -> NOT FOUND in dim_order")