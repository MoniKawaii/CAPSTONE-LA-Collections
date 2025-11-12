"""
Check the final transformed data for our problematic orders to see what values are actually saved
"""
import pandas as pd

# Load the saved fact orders
fact_orders_df = pd.read_csv('app/Transformed/fact_orders.csv')

# Load dimension orders to map order SNs to orders_keys
dim_orders_df = pd.read_csv('app/Transformed/dim_order.csv')

# Debug order SNs
debug_orders = ['2203150FA0SCN5', '2203150F8D5PR4', '2210019GJKRWKS']

print("🔍 Checking final transformed data for problematic orders...")

for order_sn in debug_orders:
    # Get orders_key for this order_sn
    order_row = dim_orders_df[dim_orders_df['platform_order_id'] == order_sn]
    if len(order_row) == 0:
        print(f"❌ Order {order_sn} not found in dim_orders")
        continue
    
    orders_key = order_row.iloc[0]['orders_key']
    print(f"\n🔍 Order {order_sn} (orders_key: {orders_key}):")
    
    # Get fact records for this order
    fact_records = fact_orders_df[fact_orders_df['orders_key'] == orders_key]
    
    if len(fact_records) == 0:
        print(f"❌ No fact records found for orders_key {orders_key}")
        continue
    
    for idx, record in fact_records.iterrows():
        print(f"   • order_item_key: {record['order_item_key']}")
        print(f"   • paid_price: {record['paid_price']}")
        print(f"   • original_unit_price: {record['original_unit_price']}")
        print(f"   • voucher_platform_amount: {record['voucher_platform_amount']}")
        print(f"   • voucher_seller_amount: {record['voucher_seller_amount']}")
        total_vouchers = record['voucher_platform_amount'] + record['voucher_seller_amount']
        print(f"   • total_vouchers: {total_vouchers}")
        print(f"   • formula check (paid = orig - vouchers): {record['paid_price']} = {record['original_unit_price']} - {total_vouchers}")
        
        # Check if there's a mismatch
        expected_paid = record['original_unit_price'] - total_vouchers
        if abs(record['paid_price'] - expected_paid) > 0.01:
            print(f"   ❌ MISMATCH: Expected {expected_paid}, got {record['paid_price']}")
        else:
            print(f"   ✅ Formula correct")

print("\n🔍 Summary: Checking if these orders still show up in our validation query...")

# Run the same validation query as before
problematic_orders = fact_orders_df[
    fact_orders_df['voucher_platform_amount'] + fact_orders_df['voucher_seller_amount'] > fact_orders_df['original_unit_price']
]

print(f"\nTotal problematic records found: {len(problematic_orders)}")
if len(problematic_orders) > 0:
    print("Sample problematic records:")
    print(problematic_orders[['order_item_key', 'orders_key', 'paid_price', 'original_unit_price', 
                              'voucher_platform_amount', 'voucher_seller_amount']].head(10))
    
    # Check if our debug orders are still problematic
    debug_orders_keys = [dim_orders_df[dim_orders_df['platform_order_id'] == order_sn]['orders_key'].iloc[0] 
                        for order_sn in debug_orders 
                        if len(dim_orders_df[dim_orders_df['platform_order_id'] == order_sn]) > 0]
    
    problematic_debug = problematic_orders[problematic_orders['orders_key'].isin(debug_orders_keys)]
    print(f"\nDebug orders that are still problematic: {len(problematic_debug)}")
    if len(problematic_debug) > 0:
        print(problematic_debug[['order_item_key', 'orders_key', 'paid_price', 'original_unit_price', 
                                'voucher_platform_amount', 'voucher_seller_amount']])