"""
Sales Aggregate Grain Options Analysis
Compare different dimensional grains for business intelligence needs
"""

import pandas as pd
import os

def analyze_grain_options():
    """Analyze different grain options for sales aggregate"""
    
    # Load fact orders
    base_path = r'app\Transformed'
    fact_orders = pd.read_csv(os.path.join(base_path, 'fact_orders.csv'))
    
    print("🎯 DIMENSIONAL GRAIN OPTIONS ANALYSIS")
    print("=" * 60)
    
    print(f"📊 Source Data Overview:")
    print(f"   - Total fact records: {len(fact_orders):,}")
    print(f"   - Unique orders: {fact_orders['orders_key'].nunique():,}")
    print(f"   - Total revenue: ${fact_orders['paid_price'].sum():,.2f}")
    print(f"   - Total items: {fact_orders['item_quantity'].sum():,}")
    
    grains = [
        {
            'name': 'CURRENT: Time × Platform × Customer × Product',
            'group_by': ['time_key', 'platform_key', 'customer_key', 'product_key'],
            'description': 'Product-level analysis (current implementation)',
            'use_case': 'Product performance, customer product preferences'
        },
        {
            'name': 'OPTION 1: Time × Platform × Customer',
            'group_by': ['time_key', 'platform_key', 'customer_key'],
            'description': 'Customer-level analysis (no product split)',
            'use_case': 'Customer behavior, lifetime value, retention'
        },
        {
            'name': 'OPTION 2: Time × Platform × Product',
            'group_by': ['time_key', 'platform_key', 'product_key'],
            'description': 'Product-level analysis (no customer split)',
            'use_case': 'Product trends, inventory planning, pricing'
        },
        {
            'name': 'OPTION 3: Time × Platform',
            'group_by': ['time_key', 'platform_key'],
            'description': 'Platform-level analysis (high-level summary)',
            'use_case': 'Platform comparison, executive dashboards'
        },
        {
            'name': 'OPTION 4: Time × Order (Order-level)',
            'group_by': ['time_key', 'platform_key', 'orders_key'],
            'description': 'Order-level analysis (preserves order integrity)',
            'use_case': 'Order analysis, shipping, fulfillment'
        }
    ]
    
    for i, grain in enumerate(grains):
        print(f"\n{'-' * 60}")
        print(f"🔍 {grain['name']}")
        print(f"📝 Description: {grain['description']}")
        print(f"🎯 Best for: {grain['use_case']}")
        
        # Generate sample aggregation
        if grain['name'].startswith('OPTION 4'):
            # For order-level, we need different aggregation logic
            sample_agg = fact_orders.groupby(grain['group_by']).agg({
                'customer_key': 'first',  # One customer per order
                'product_key': 'nunique', # Count distinct products per order
                'item_quantity': 'sum',   # Total items in order
                'paid_price': 'sum'       # Total order value
            }).reset_index()
        else:
            sample_agg = fact_orders.groupby(grain['group_by']).agg({
                'orders_key': 'nunique',
                'item_quantity': 'sum',
                'paid_price': 'sum'
            }).reset_index()
        
        print(f"📊 Aggregation Results:")
        print(f"   - Records: {len(sample_agg):,}")
        print(f"   - Total revenue: ${sample_agg['paid_price'].sum():,.2f}")
        print(f"   - Total items: {sample_agg['item_quantity'].sum():,}")
        
        if 'orders_key' in sample_agg.columns:
            orders_sum = sample_agg['orders_key'].sum()
            unique_orders = fact_orders['orders_key'].nunique()
            print(f"   - Orders sum: {orders_sum:,}")
            print(f"   - Unique orders: {unique_orders:,}")
            print(f"   - Order integrity: {'✅' if orders_sum == unique_orders else '❌'}")
        
        # Show granularity reduction
        reduction_factor = len(fact_orders) / len(sample_agg)
        print(f"   - Compression: {reduction_factor:.1f}x (from {len(fact_orders):,} to {len(sample_agg):,})")

def recommend_grain():
    """Recommend the best grain based on business needs"""
    
    print(f"\n{'=' * 60}")
    print(f"💡 GRAIN RECOMMENDATIONS")
    print(f"{'=' * 60}")
    
    recommendations = [
        {
            'scenario': 'Product Performance Analytics',
            'grain': 'Time × Platform × Product',
            'why': 'No order count inflation, clear product metrics',
            'example': 'Which products sell best on weekends?'
        },
        {
            'scenario': 'Customer Behavior Analytics', 
            'grain': 'Time × Platform × Customer',
            'why': 'Customer-centric view, no product split complexity',
            'example': 'Customer lifetime value, retention rates'
        },
        {
            'scenario': 'Executive Dashboards',
            'grain': 'Time × Platform', 
            'why': 'High-level trends, clean numbers',
            'example': 'Daily/monthly sales by platform'
        },
        {
            'scenario': 'Order Operations Analytics',
            'grain': 'Time × Platform × Order',
            'why': 'Preserves order integrity, good for logistics',
            'example': 'Average order value, shipping analysis'
        },
        {
            'scenario': 'Comprehensive BI (Multiple Grains)',
            'grain': 'Multiple fact tables at different grains',
            'why': 'Different grains for different use cases',
            'example': 'fact_sales_daily, fact_sales_by_product, fact_sales_by_customer'
        }
    ]
    
    for rec in recommendations:
        print(f"\n🎯 {rec['scenario']}:")
        print(f"   📊 Recommended grain: {rec['grain']}")
        print(f"   💭 Why: {rec['why']}")
        print(f"   🔍 Example use: {rec['example']}")

if __name__ == "__main__":
    analyze_grain_options()
    recommend_grain()