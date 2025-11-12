import pandas as pd
import os

def analyze_discrepancy():
    """Analyze discrepancy between fact_orders and sales_aggregate"""
    
    # Load both tables
    base_path = r'app\Transformed'
    fact_orders = pd.read_csv(os.path.join(base_path, 'fact_orders.csv'))
    sales_agg = pd.read_csv(os.path.join(base_path, 'fact_sales_aggregate.csv'))
    
    print("=== FACT ORDERS ANALYSIS ===")
    print(f"Total records: {len(fact_orders):,}")
    print(f"Total revenue (paid_price): ${fact_orders['paid_price'].sum():,.2f}")
    print(f"Total item_quantity: {fact_orders['item_quantity'].sum():,}")
    print(f"Unique orders: {fact_orders['orders_key'].nunique():,}")
    print(f"Unique customers: {fact_orders['customer_key'].nunique():,}")
    print(f"Unique products: {fact_orders['product_key'].nunique():,}")
    print(f"Date range: {fact_orders['time_key'].min()} to {fact_orders['time_key'].max()}")
    
    print("\n=== SALES AGGREGATE ANALYSIS ===")
    print(f"Total records: {len(sales_agg):,}")
    print(f"Total net_sales: ${sales_agg['net_sales'].sum():,.2f}")
    print(f"Total items_sold: {sales_agg['total_items_sold'].sum():,}")
    print(f"Sum of total_orders: {sales_agg['total_orders'].sum():,}")
    print(f"Unique customers: {sales_agg['customer_key'].nunique():,}")
    print(f"Unique products: {sales_agg['product_key'].nunique():,}")
    print(f"Date range: {sales_agg['time_key'].min()} to {sales_agg['time_key'].max()}")
    
    print("\n=== DISCREPANCY ANALYSIS ===")
    revenue_diff = abs(fact_orders['paid_price'].sum() - sales_agg['net_sales'].sum())
    items_diff = fact_orders['item_quantity'].sum() - sales_agg['total_items_sold'].sum()
    
    print(f"Revenue match: {revenue_diff < 0.01} (diff: ${revenue_diff:.2f})")
    print(f"Items match: {items_diff == 0} (diff: {items_diff})")
    print(f"Records vs items match: {len(fact_orders) == sales_agg['total_items_sold'].sum()}")
    
    # Sample comparison
    print("\n=== SAMPLE MANUAL AGGREGATION ===")
    manual_agg = fact_orders.groupby(['time_key', 'platform_key', 'customer_key', 'product_key']).agg({
        'orders_key': 'nunique',
        'item_quantity': 'sum', 
        'paid_price': 'sum'
    }).reset_index()
    
    print(f"Manual aggregation records: {len(manual_agg):,}")
    print(f"Manual total items: {manual_agg['item_quantity'].sum():,}")
    print(f"Manual total revenue: ${manual_agg['paid_price'].sum():,.2f}")
    print(f"Manual unique orders sum: {manual_agg['orders_key'].sum():,}")
    
    # Check for differences
    print(f"\nManual vs Sales_Agg comparison:")
    print(f"Records: {len(manual_agg)} vs {len(sales_agg)} (diff: {len(sales_agg) - len(manual_agg)})")
    print(f"Items: {manual_agg['item_quantity'].sum()} vs {sales_agg['total_items_sold'].sum()} (diff: {sales_agg['total_items_sold'].sum() - manual_agg['item_quantity'].sum()})")
    print(f"Revenue: {manual_agg['paid_price'].sum():.2f} vs {sales_agg['net_sales'].sum():.2f} (diff: {sales_agg['net_sales'].sum() - manual_agg['paid_price'].sum():.2f})")

if __name__ == "__main__":
    analyze_discrepancy()