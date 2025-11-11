"""
Detailed Analysis of Missing Lazada Order Items
===============================================

This script analyzes why Lazada order items are missing from fact_orders.csv
and investigates the revenue variance to identify root causes.
"""

import pandas as pd
import json
import os
import sys

# Add paths
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'app'))

def analyze_missing_lazada_items():
    """Detailed analysis of missing Lazada items"""
    
    base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    staging_path = os.path.join(base_path, 'app', 'Staging')
    transformed_path = os.path.join(base_path, 'app', 'Transformed')
    
    print("🔍 Loading data files...")
    
    # Load transformed data
    fact_orders_df = pd.read_csv(os.path.join(transformed_path, 'fact_orders.csv'))
    dim_order_df = pd.read_csv(os.path.join(transformed_path, 'dim_order.csv'))
    dim_customer_df = pd.read_csv(os.path.join(transformed_path, 'dim_customer.csv'))
    dim_product_df = pd.read_csv(os.path.join(transformed_path, 'dim_product.csv'))
    
    # Load raw data
    with open(os.path.join(staging_path, 'lazada_multiple_order_items_raw.json'), 'r', encoding='utf-8') as f:
        lazada_order_items_raw = json.load(f)
    
    with open(os.path.join(staging_path, 'lazada_orders_raw.json'), 'r', encoding='utf-8') as f:
        lazada_orders_raw = json.load(f)
    
    print(f"✓ Loaded {len(fact_orders_df)} fact order records")
    print(f"✓ Loaded {len(lazada_order_items_raw)} raw Lazada order items")
    print(f"✓ Loaded {len(lazada_orders_raw)} raw Lazada orders")
    
    # Get completed Lazada orders
    completed_lazada_orders = dim_order_df[
        (dim_order_df['platform_key'] == 1) & 
        (dim_order_df['order_status'] == 'COMPLETED')
    ]
    completed_order_ids = set(completed_lazada_orders['platform_order_id'].astype(str))
    
    print(f"\n📊 Order Status Analysis:")
    print(f"  - Total orders in dim_order: {len(dim_order_df[dim_order_df['platform_key'] == 1])}")
    print(f"  - Completed orders: {len(completed_lazada_orders)}")
    print(f"  - Completed order IDs: {len(completed_order_ids)}")
    
    # Analyze raw order items
    print(f"\n🔍 Analyzing raw order items...")
    
    total_raw_items = 0
    completed_raw_items = 0
    missing_products = []
    missing_customers = []
    price_issues = []
    raw_items_by_order = {}
    
    for order_record in lazada_order_items_raw:
        order_id = str(order_record.get('order_id', ''))
        order_items = order_record.get('order_items', [])
        
        total_raw_items += len(order_items)
        
        if order_id in completed_order_ids:
            completed_raw_items += len(order_items)
            raw_items_by_order[order_id] = order_items
            
            # Check individual items for issues
            for item in order_items:
                sku_id = str(item.get('sku_id', ''))
                item_price = float(item.get('item_price', 0))
                paid_price = float(item.get('paid_price', 0))
                
                # Check if product exists
                product_exists = len(dim_product_df[
                    (dim_product_df['platform_key'] == 1) & 
                    (dim_product_df['product_item_id'].astype(str) == sku_id)
                ]) > 0
                
                if not product_exists:
                    missing_products.append({
                        'order_id': order_id,
                        'sku_id': sku_id,
                        'item_price': item_price,
                        'paid_price': paid_price
                    })
                
                # Check for pricing issues
                if paid_price <= 0 or item_price <= 0:
                    price_issues.append({
                        'order_id': order_id,
                        'sku_id': sku_id,
                        'item_price': item_price,
                        'paid_price': paid_price
                    })
    
    print(f"  - Total raw items: {total_raw_items}")
    print(f"  - Raw items in completed orders: {completed_raw_items}")
    print(f"  - Missing products: {len(missing_products)}")
    print(f"  - Price issues: {len(price_issues)}")
    
    # Check customer mapping issues
    print(f"\n🔍 Analyzing customer mapping...")
    
    lazada_orders_dict = {str(order.get('order_id', '')): order for order in lazada_orders_raw}
    customer_issues = 0
    
    for order_id in completed_order_ids:
        if order_id in lazada_orders_dict:
            order = lazada_orders_dict[order_id]
            buyer_id = order.get('address_billing', {}).get('customer_id')
            
            if not buyer_id:
                customer_issues += 1
    
    print(f"  - Orders with customer issues: {customer_issues}")
    
    # Analyze fact table coverage
    lazada_fact_orders = fact_orders_df[fact_orders_df['platform_key'] == 1]
    fact_order_ids = set(
        dim_order_df[
            dim_order_df['orders_key'].isin(lazada_fact_orders['orders_key'])
        ]['platform_order_id'].astype(str)
    )
    
    print(f"\n📊 Coverage Analysis:")
    print(f"  - Orders in fact table: {len(fact_order_ids)}")
    print(f"  - Missing orders: {len(completed_order_ids) - len(fact_order_ids)}")
    print(f"  - Order coverage: {len(fact_order_ids) / len(completed_order_ids) * 100:.1f}%")
    
    # Count items in fact table
    fact_item_count = len(lazada_fact_orders)
    item_coverage = fact_item_count / completed_raw_items * 100
    
    print(f"  - Raw items (completed): {completed_raw_items}")
    print(f"  - Fact items: {fact_item_count}")
    print(f"  - Item coverage: {item_coverage:.1f}%")
    print(f"  - Missing items: {completed_raw_items - fact_item_count}")
    
    # Detailed missing product analysis
    print(f"\n📋 Missing Products Analysis (Top 10):")
    
    if missing_products:
        missing_revenue = sum(item['paid_price'] for item in missing_products)
        print(f"  - Missing products causing revenue loss: ${missing_revenue:,.2f}")
        
        # Group by sku_id to find most problematic products
        from collections import Counter
        missing_sku_counts = Counter(item['sku_id'] for item in missing_products)
        
        for sku_id, count in missing_sku_counts.most_common(10):
            revenue_loss = sum(
                item['paid_price'] for item in missing_products 
                if item['sku_id'] == sku_id
            )
            print(f"    SKU {sku_id}: {count} items, ${revenue_loss:.2f} revenue loss")
    
    # Revenue variance analysis
    print(f"\n💰 Revenue Variance Analysis:")
    
    # Calculate raw revenue for completed orders
    raw_revenue = 0
    for order_id in completed_order_ids:
        if order_id in raw_items_by_order:
            for item in raw_items_by_order[order_id]:
                raw_revenue += float(item.get('paid_price', 0))
    
    fact_revenue = lazada_fact_orders['paid_price'].sum()
    variance = abs(fact_revenue - raw_revenue)
    variance_pct = variance / raw_revenue * 100
    
    print(f"  - Raw revenue (completed orders): ${raw_revenue:,.2f}")
    print(f"  - Fact revenue: ${fact_revenue:,.2f}")
    print(f"  - Revenue variance: ${variance:,.2f} ({variance_pct:.2f}%)")
    
    # Break down variance sources
    missing_product_revenue = sum(item['paid_price'] for item in missing_products)
    explained_variance = missing_product_revenue / variance * 100 if variance > 0 else 0
    
    print(f"  - Missing product revenue: ${missing_product_revenue:,.2f}")
    print(f"  - Explained by missing products: {explained_variance:.1f}%")
    
    return {
        'total_raw_items': total_raw_items,
        'completed_raw_items': completed_raw_items,
        'fact_item_count': fact_item_count,
        'missing_products_count': len(missing_products),
        'missing_product_revenue': missing_product_revenue,
        'raw_revenue': raw_revenue,
        'fact_revenue': fact_revenue,
        'variance': variance
    }


def generate_recommendations(analysis_results):
    """Generate recommendations based on analysis results"""
    
    print(f"\n🎯 RECOMMENDATIONS:")
    print("="*50)
    
    coverage = analysis_results['fact_item_count'] / analysis_results['completed_raw_items'] * 100
    
    if coverage < 95:
        print(f"1. 📉 Item Coverage Issue ({coverage:.1f}%)")
        print(f"   - Missing {analysis_results['completed_raw_items'] - analysis_results['fact_item_count']} items")
        print(f"   - Primary cause: {analysis_results['missing_products_count']} missing product mappings")
        print(f"   - Revenue impact: ${analysis_results['missing_product_revenue']:,.2f}")
        print(f"   - Action: Review product harmonization to capture missing SKUs")
    
    variance_pct = analysis_results['variance'] / analysis_results['raw_revenue'] * 100
    
    if variance_pct > 5:
        print(f"\n2. 💰 Revenue Variance Issue ({variance_pct:.2f}%)")
        print(f"   - Revenue difference: ${analysis_results['variance']:,.2f}")
        print(f"   - Explained by missing products: {analysis_results['missing_product_revenue'] / analysis_results['variance'] * 100:.1f}%")
        print(f"   - Action: Focus on product mapping completeness")
    
    print(f"\n3. 🔧 Next Steps:")
    print(f"   - Run product harmonization with broader matching criteria")
    print(f"   - Check for missing SKU mappings in dim_product")
    print(f"   - Consider adding DEFAULT product category for unmapped items")
    print(f"   - Review customer mapping logic for anonymous handling")


if __name__ == "__main__":
    print("🚀 Starting Detailed Lazada Analysis")
    print("="*50)
    
    try:
        results = analyze_missing_lazada_items()
        generate_recommendations(results)
        
        print(f"\n✅ Analysis complete!")
        
    except Exception as e:
        print(f"❌ Error during analysis: {e}")
        import traceback
        traceback.print_exc()