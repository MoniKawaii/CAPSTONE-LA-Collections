import pandas as pd

# Read the transformed fact_orders.csv
df = pd.read_csv('app/Transformed/fact_orders.csv')

# Check the pricing formula: paid_price = original_unit_price - total_vouchers
df['total_vouchers'] = df['voucher_platform_amount'] + df['voucher_seller_amount']
df['formula_check'] = df['original_unit_price'] - df['total_vouchers']
df['pricing_accurate'] = abs(df['paid_price'] - df['formula_check']) < 0.01

# Count records with excessive vouchers (where vouchers > original price)
excessive_vouchers = df[df['total_vouchers'] > df['original_unit_price']]

print("🔍 PRICING VALIDATION RESULTS:")
print(f"Total records: {len(df):,}")
print(f"Pricing formula accuracy: {df['pricing_accurate'].sum()}/{len(df)} ({df['pricing_accurate'].mean()*100:.2f}%)")
print(f"Records with excessive vouchers: {len(excessive_vouchers):,}")

if len(excessive_vouchers) > 0:
    print("\n❌ EXCESSIVE VOUCHER EXAMPLES:")
    for idx, row in excessive_vouchers.head(10).iterrows():
        print(f"  Order: {row['order_item_key']}, Original: {row['original_unit_price']}, Total Vouchers: {row['total_vouchers']:.2f}, Paid: {row['paid_price']}")
else:
    print("\n✅ NO EXCESSIVE VOUCHERS FOUND!")

print(f"\n📊 Voucher Statistics:")
print(f"  Platform vouchers - Mean: {df['voucher_platform_amount'].mean():.2f}, Max: {df['voucher_platform_amount'].max():.2f}")
print(f"  Seller vouchers - Mean: {df['voucher_seller_amount'].mean():.2f}, Max: {df['voucher_seller_amount'].max():.2f}")
print(f"  Total vouchers - Mean: {df['total_vouchers'].mean():.2f}, Max: {df['total_vouchers'].max():.2f}")