#!/usr/bin/env python3

from harmonize_fact_orders import load_dimension_lookups
import pandas as pd

def check_missing_orders():
    # Load dimension lookups
    dim_lookups, variant_df = load_dimension_lookups()
    order_lookup = dim_lookups['order']
    
    print(f'Total orders in lookup: {len(order_lookup)}')
    
    # Check specific early orders that should be COMPLETED
    early_orders = ['2009196MH5Q37C', '201004EJ2XU2MN', '201005H6A997Y3', '201006M8N0SE72', '201007NM38VY8N']
    
    print('\nChecking early orders in lookup:')
    for oid in early_orders:
        result = order_lookup.get(oid, "NOT FOUND")
        print(f'{oid}: {result}')
    
    # Check dim_order directly
    print('\nChecking dim_order CSV directly:')
    dim_order = pd.read_csv('../Transformed/dim_order.csv')
    
    for oid in early_orders:
        order_row = dim_order[dim_order['platform_order_id'] == oid]
        if len(order_row) > 0:
            status = order_row.iloc[0]['order_status']
            orders_key = order_row.iloc[0]['orders_key']
            print(f'{oid}: Status={status}, Key={orders_key}')
        else:
            print(f'{oid}: NOT FOUND in dim_order')
    
    print('\nFirst 10 orders in lookup:')
    sample_orders = list(order_lookup.items())[:10]
    for oid, key in sample_orders:
        print(f'{oid}: {key}')

if __name__ == "__main__":
    check_missing_orders()