import pandas as pd

# Load the new fact_sales_aggregate
df = pd.read_csv('app/Transformed/fact_sales_aggregate.csv')
customers = pd.read_csv('app/Transformed/dim_customer.csv')
products = pd.read_csv('app/Transformed/dim_product.csv')

print('🎯 NEW FACT_SALES_AGGREGATE WITH IMPROVED GRANULARITY')
print('=' * 60)
print(f'📋 Total records: {len(df):,}')
print(f'📅 Date range: {df.time_key.min()} to {df.time_key.max()}')
print(f'👥 Unique customers: {df.customer_key.nunique():,}')
print(f'📦 Unique products: {df.product_key.nunique():,}')
print(f'💰 Total gross revenue: ${df.gross_revenue.sum():,.2f}')
print(f'💵 Total net sales: ${df.net_sales.sum():,.2f}')
print(f'💸 Total discounts: ${df.total_discounts.sum():,.2f}')

print('\n🔍 SAMPLE ANALYTICAL CAPABILITIES:')
print('=' * 40)

# Join with dimensions for richer analysis
enriched = df.merge(customers[['customer_key', 'buyer_segment', 'customer_city']], on='customer_key', how='left')
enriched = enriched.merge(products[['product_key', 'product_category', 'product_name']], on='product_key', how='left')

print('\n📊 Top 5 Products by Revenue:')
top_products = enriched.groupby('product_name')['gross_revenue'].sum().sort_values(ascending=False).head()
for i, (product, revenue) in enumerate(top_products.items(), 1):
    product_short = product[:40] + ('...' if len(product) > 40 else '')
    print(f'   {i}. {product_short}: ${revenue:,.2f}')

print('\n👥 Revenue by Customer Segment:')
segment_revenue = enriched.groupby('buyer_segment')['gross_revenue'].sum().sort_values(ascending=False)
for segment, revenue in segment_revenue.items():
    print(f'   {segment}: ${revenue:,.2f}')

print('\n🏪 Revenue by Product Category:')
category_revenue = enriched.groupby('product_category')['gross_revenue'].sum().sort_values(ascending=False)
for category, revenue in category_revenue.items():
    print(f'   {category}: ${revenue:,.2f}')

print('\n✅ Data Structure Validation:')
print(f'   All revenue matches: {abs(df.gross_revenue.sum() - 2711264.56) < 0.01}')
print(f'   All items match: {df.total_items_sold.sum() == 8461}')
print(f'   No null values: {df.isnull().sum().sum() == 0}')

print('\n🚀 READY FOR BUSINESS INTELLIGENCE!')
print('The new structure enables slicing by:')
print('- Time (daily granularity)')
print('- Customer segments and geography') 
print('- Product categories and individual products')
print('- Platform performance')
print('- Cross-dimensional analysis (e.g., customer behavior by product category)')