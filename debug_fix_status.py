import pandas as pd

# Read the transformed data
df = pd.read_csv('app/Transformed/fact_orders.csv')

# Check for the specific problematic records
problem_orders = ["S00009410", "S00009411", "S00019284"]
problem_records = df[df['order_item_key'].isin(problem_orders)]

print("🔍 PROBLEMATIC RECORD DETAILS:")
for _, row in problem_records.iterrows():
    print(f"Order: {row['order_item_key']}")
    print(f"  Orders key: {row['orders_key']}")
    print(f"  Original: {row['original_unit_price']}")
    print(f"  Paid: {row['paid_price']}")
    print(f"  Platform vouchers: {row['voucher_platform_amount']}")
    print(f"  Seller vouchers: {row['voucher_seller_amount']}")
    print(f"  Total vouchers: {row['voucher_platform_amount'] + row['voucher_seller_amount']}")
    print(f"  Issue: {'Vouchers > Original' if row['voucher_platform_amount'] + row['voucher_seller_amount'] > row['original_unit_price'] else 'Paid = 0 but formula wrong'}")
    print()

print("🔍 EXPECTED BEHAVIOR:")
print("If discounted_price = 0 and vouchers > original_price:")
print("  -> paid_price should = original_price - vouchers (can be negative)")
print("  -> This would make the pricing formula work")
print()
print("🔍 CURRENT BEHAVIOR:")
print("All these records have paid_price = 0.0")
print("This suggests my validation logic isn't triggering or isn't working correctly")

# Let's also check if there are any negative paid_price records (which would indicate my fix worked)
negative_paid = df[df['paid_price'] < 0]
print(f"\n🔍 NEGATIVE PAID_PRICE RECORDS: {len(negative_paid)}")
if len(negative_paid) > 0:
    print("My validation logic IS working - some records now have negative paid_price")
    for _, row in negative_paid.head(5).iterrows():
        print(f"  Order: {row['order_item_key']}, Paid: {row['paid_price']}, Original: {row['original_unit_price']}, Total vouchers: {row['voucher_platform_amount'] + row['voucher_seller_amount']}")
else:
    print("No negative paid_price records found - validation logic may not be working")