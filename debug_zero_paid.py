import pandas as pd

# Read the transformed data
df = pd.read_csv('app/Transformed/fact_orders.csv')

# Check all records with paid_price = 0
zero_paid = df[df['paid_price'] == 0.0]

print(f"🔍 RECORDS WITH PAID_PRICE = 0:")
print(f"Total count: {len(zero_paid)}")
print(f"Platform breakdown:")
print(f"  Lazada: {len(zero_paid[zero_paid['platform_key'] == 1])}")
print(f"  Shopee: {len(zero_paid[zero_paid['platform_key'] == 2])}")
print()

print("🔍 SAMPLE ZERO PAID_PRICE RECORDS:")
for idx, row in zero_paid.head(10).iterrows():
    print(f"Order: {row['order_item_key']}, Platform: {'Lazada' if row['platform_key'] == 1 else 'Shopee'}")
    print(f"  Original: {row['original_unit_price']}, Paid: {row['paid_price']}")
    print(f"  Platform vouchers: {row['voucher_platform_amount']}")
    print(f"  Seller vouchers: {row['voucher_seller_amount']}")
    print(f"  Total vouchers: {row['voucher_platform_amount'] + row['voucher_seller_amount']}")
    print()

# Check if these are all Shopee records (suggesting payment detail issue)
print("🎯 KEY INSIGHT:")
if len(zero_paid[zero_paid['platform_key'] == 2]) == len(zero_paid):
    print("All zero paid_price records are from Shopee - this confirms the payment detail logic issue")
else:
    print(f"Mixed platforms with zero paid_price - need further investigation")