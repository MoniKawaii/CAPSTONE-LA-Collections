import pandas as pd

def analyze_sales_aggregate():
    """Analyze the generated fact_sales_aggregate table"""
    
    # Load the generated sales aggregate
    df = pd.read_csv('app/Transformed/fact_sales_aggregate.csv')
    
    print('=== FACT SALES AGGREGATE ANALYSIS ===')
    print(f'Total records: {len(df):,}')
    print(f'Columns: {list(df.columns)}')
    print()
    
    print('Sample records:')
    print(df.head(5).to_string())
    print()
    
    print('=== GRANULARITY BREAKDOWN ===')
    print(f'Unique combinations (Time x Platform x Customer x Product): {len(df):,}')
    print(f'- Unique dates: {df["time_key"].nunique():,}')
    print(f'- Unique platforms: {df["platform_key"].nunique():,}')
    print(f'- Unique customers: {df["customer_key"].nunique():,}')
    print(f'- Unique products: {df["product_key"].nunique():,}')
    print()
    
    print('=== PLATFORM BREAKDOWN ===')
    platform_summary = df.groupby('platform_key').agg({
        'total_orders': 'sum',
        'total_items_sold': 'sum', 
        'gross_revenue': 'sum',
        'total_discounts': 'sum',
        'net_sales': 'sum'
    }).round(2)
    print(platform_summary)
    print()
    
    print('=== TOP 5 CUSTOMERS BY REVENUE ===')
    customer_revenue = df.groupby('customer_key')['gross_revenue'].sum().sort_values(ascending=False)
    print(customer_revenue.head(5))
    print()
    
    print('=== TOP 5 PRODUCTS BY REVENUE ===')
    product_revenue = df.groupby('product_key')['gross_revenue'].sum().sort_values(ascending=False)
    print(product_revenue.head(5))

if __name__ == "__main__":
    analyze_sales_aggregate()