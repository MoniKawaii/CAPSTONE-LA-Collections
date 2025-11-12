import pandas as pd
import os

def explain_orders_discrepancy():
    """Explain why total_orders sum is higher than unique orders"""
    
    # Load fact orders
    base_path = r'app\Transformed'
    fact_orders = pd.read_csv(os.path.join(base_path, 'fact_orders.csv'))
    
    print("=== UNDERSTANDING THE ORDERS COUNT DISCREPANCY ===")
    print()
    
    # 1. Check for orders with multiple products
    orders_with_multiple_products = fact_orders.groupby('orders_key')['product_key'].nunique().reset_index()
    orders_with_multiple_products.columns = ['orders_key', 'product_count']
    multi_product_orders = orders_with_multiple_products[orders_with_multiple_products['product_count'] > 1]
    
    print(f"1. ORDERS WITH MULTIPLE PRODUCTS:")
    print(f"   Total unique orders: {fact_orders['orders_key'].nunique():,}")
    print(f"   Orders with multiple products: {len(multi_product_orders):,}")
    print(f"   Orders with single product: {len(orders_with_multiple_products) - len(multi_product_orders):,}")
    
    if len(multi_product_orders) > 0:
        print(f"   Sample multi-product orders:")
        sample_multi = fact_orders[fact_orders['orders_key'].isin(multi_product_orders['orders_key'].head(3))]
        for order_key in sample_multi['orders_key'].unique()[:3]:
            order_items = sample_multi[sample_multi['orders_key'] == order_key]
            print(f"     Order {order_key}: {len(order_items)} products")
    
    # 2. Check for orders across multiple time periods
    orders_across_time = fact_orders.groupby('orders_key')['time_key'].nunique().reset_index()
    orders_across_time.columns = ['orders_key', 'time_count']
    multi_time_orders = orders_across_time[orders_across_time['time_count'] > 1]
    
    print(f"\n2. ORDERS ACROSS MULTIPLE TIME PERIODS:")
    print(f"   Orders spanning multiple days: {len(multi_time_orders):,}")
    if len(multi_time_orders) > 0:
        print(f"   Sample cross-time orders:")
        for order_key in multi_time_orders['orders_key'].head(3):
            order_items = fact_orders[fact_orders['orders_key'] == order_key]
            time_keys = order_items['time_key'].unique()
            print(f"     Order {order_key}: dates {time_keys}")
    
    # 3. Demonstrate the aggregation effect
    print(f"\n3. AGGREGATION EFFECT EXPLANATION:")
    print(f"   When we aggregate by Time x Customer x Product:")
    print(f"   - If Order ABC has 3 products → creates 3 aggregate records")
    print(f"   - Each record counts 1 order in total_orders")
    print(f"   - Sum of total_orders = 3 (but only 1 unique order)")
    
    # Calculate the exact multiplier effect
    sample_agg = fact_orders.groupby(['time_key', 'platform_key', 'customer_key', 'product_key']).agg({
        'orders_key': 'nunique'
    }).reset_index()
    
    theoretical_order_sum = sample_agg['orders_key'].sum()
    actual_unique_orders = fact_orders['orders_key'].nunique()
    
    print(f"\n4. MATHEMATICAL VALIDATION:")
    print(f"   Unique orders in fact_orders: {actual_unique_orders:,}")
    print(f"   Sum of orders in aggregated view: {theoretical_order_sum:,}")
    print(f"   Difference: {theoretical_order_sum - actual_unique_orders:,}")
    print(f"   This difference represents orders with multiple products!")
    
    # 5. Show specific examples
    print(f"\n5. CONCRETE EXAMPLES:")
    multi_product_sample = multi_product_orders.head(3)
    for _, row in multi_product_sample.iterrows():
        order_key = row['orders_key']
        product_count = row['product_count']
        order_details = fact_orders[fact_orders['orders_key'] == order_key]
        
        print(f"   Order {order_key}:")
        print(f"     - Products: {product_count}")
        print(f"     - Customer: {order_details['customer_key'].iloc[0]}")
        print(f"     - Date: {order_details['time_key'].iloc[0]}")
        print(f"     - In aggregate: appears in {product_count} records")
        print(f"     - Contributes {product_count} to total_orders sum")

if __name__ == "__main__":
    explain_orders_discrepancy()