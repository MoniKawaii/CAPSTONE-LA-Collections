import pandas as pd

def check_order_statuses():
    """Check order statuses in dim_order and relationship to fact_orders"""
    
    # Check order statuses in dim_order
    dim_order = pd.read_csv('app/Transformed/dim_order.csv')
    print('=== ORDER STATUS ANALYSIS ===')
    print('Available order statuses:')
    print(dim_order['order_status'].value_counts())
    print()
    
    # Check fact_orders relationship to orders
    fact_orders = pd.read_csv('app/Transformed/fact_orders.csv')
    print(f'Total fact_orders records: {len(fact_orders):,}')
    print(f'Unique orders_key in fact_orders: {fact_orders["orders_key"].nunique():,}')
    print(f'Unique orders_key in dim_order: {dim_order["orders_key"].nunique():,}')
    print()
    
    # Show COMPLETED orders specifically
    completed_orders = dim_order[dim_order['order_status'] == 'COMPLETED']
    print(f'COMPLETED orders: {len(completed_orders):,}')
    print('Platform breakdown for COMPLETED orders:')
    print(completed_orders['platform_key'].value_counts())

if __name__ == "__main__":
    check_order_statuses()