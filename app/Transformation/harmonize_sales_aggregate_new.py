"""
Sales Aggregate Harmonization Script
Creates Fact_Sales_Aggregate table with proper data cube granularity:
- Grain: Time, Platform, Customer, Product
- Allows slicing by date, customer segment/city, product category, etc.
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os
import sys

# Add the app directory to Python path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import FACT_SALES_AGGREGATE_COLUMNS, COLUMN_DATA_TYPES, apply_data_types

def load_dimension_tables():
    """Load all required dimension and fact tables"""
    base_path = os.path.join(os.path.dirname(__file__), '..', 'Transformed')
    
    # Load dimension tables
    dim_time = pd.read_csv(os.path.join(base_path, 'dim_time.csv'))
    dim_customer = pd.read_csv(os.path.join(base_path, 'dim_customer.csv'))
    dim_product = pd.read_csv(os.path.join(base_path, 'dim_product.csv'))
    
    # Load fact orders
    fact_orders = pd.read_csv(os.path.join(base_path, 'fact_orders.csv'))
    
    print(f"✅ Loaded dimension tables:")
    print(f"   📅 Dim Time: {len(dim_time):,} records")
    print(f"   👥 Dim Customer: {len(dim_customer):,} records") 
    print(f"   📦 Dim Product: {len(dim_product):,} records")
    print(f"   📋 Fact Orders: {len(fact_orders):,} records")
    
    return dim_time, dim_customer, dim_product, fact_orders

def generate_sales_aggregate(dim_time, dim_customer, dim_product, fact_orders):
    """Generate sales aggregate with proper grain: Time x Platform x Customer x Product"""
    print(f"\n🔄 Generating Sales Aggregate with granularity: Time x Platform x Customer x Product")
    
    # Group fact_orders by the grain dimensions and aggregate metrics
    sales_agg = fact_orders.groupby([
        'time_key', 'platform_key', 'customer_key', 'product_key'
    ]).agg({
        'orders_key': 'nunique',
        'item_quantity': 'sum',
        'paid_price': 'sum',
        'voucher_platform_amount': 'sum',
        'voucher_seller_amount': 'sum',
        'shipping_fee_paid_by_buyer': 'sum'
    }).reset_index()
    
    # Rename columns to match schema
    sales_agg.columns = [
        'time_key', 'platform_key', 'customer_key', 'product_key',
        'total_orders', 'total_items_sold', 'gross_revenue', 
        'platform_discounts', 'seller_discounts', 'shipping_revenue'
    ]
    
    # Calculate derived metrics
    sales_agg['total_discounts'] = sales_agg['platform_discounts'] + sales_agg['seller_discounts']
    sales_agg['net_sales'] = sales_agg['gross_revenue'] - sales_agg['total_discounts']
    
    # Drop intermediate columns
    sales_agg = sales_agg.drop(['platform_discounts', 'seller_discounts'], axis=1)
    
    # Reorder columns to match schema
    sales_agg = sales_agg[FACT_SALES_AGGREGATE_COLUMNS]
    
    print(f"✅ Generated {len(sales_agg):,} sales aggregate records")
    print(f"   📊 Covering {sales_agg['time_key'].nunique():,} unique dates")
    print(f"   👥 Covering {sales_agg['customer_key'].nunique():,} unique customers")  
    print(f"   📦 Covering {sales_agg['product_key'].nunique():,} unique products")
    print(f"   🏪 Covering {sales_agg['platform_key'].nunique():,} platforms")
    
    return sales_agg

def validate_sales_aggregate(sales_agg, fact_orders):
    """Validate the sales aggregate against source data"""
    print(f"\n🔍 Validating Sales Aggregate...")
    
    # Validation 1: Total revenue should match
    source_revenue = fact_orders['paid_price'].sum()
    agg_revenue = sales_agg['gross_revenue'].sum()
    
    print(f"   💰 Revenue validation:")
    print(f"      Source total: ${source_revenue:,.2f}")
    print(f"      Aggregate total: ${agg_revenue:,.2f}")
    print(f"      Match: {'✅' if abs(source_revenue - agg_revenue) < 0.01 else '❌'}")
    
    # Validation 2: Total items should match
    source_items = fact_orders['item_quantity'].sum()
    agg_items = sales_agg['total_items_sold'].sum()
    
    print(f"   📦 Items validation:")
    print(f"      Source total: {source_items:,}")
    print(f"      Aggregate total: {agg_items:,}")
    print(f"      Match: {'✅' if source_items == agg_items else '❌'}")
    
    # Validation 3: Check for null values
    null_counts = sales_agg.isnull().sum()
    print(f"   🔍 Null value check:")
    for col, count in null_counts.items():
        if count > 0:
            print(f"      ⚠️  {col}: {count} nulls")
    if null_counts.sum() == 0:
        print(f"      ✅ No null values found")

def main():
    """Main execution function"""
    print("=" * 60)
    print("🚀 FACT SALES AGGREGATE HARMONIZATION")
    print("=" * 60)
    print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🎯 Grain: Time x Platform x Customer x Product")
    
    try:
        # Step 1: Load source data
        print(f"\n📂 Step 1: Loading source tables...")
        dim_time, dim_customer, dim_product, fact_orders = load_dimension_tables()
        
        # Step 2: Generate sales aggregate
        print(f"\n🔄 Step 2: Generating sales aggregate...")
        sales_agg = generate_sales_aggregate(dim_time, dim_customer, dim_product, fact_orders)
        
        # Step 3: Apply data types
        print(f"\n🔧 Step 3: Applying data types...")
        sales_agg = apply_data_types(sales_agg, 'fact_sales_aggregate')
        print(f"✅ Data types applied successfully")
        
        # Step 4: Validate results
        validate_sales_aggregate(sales_agg, fact_orders)
        
        # Step 5: Save to CSV
        output_path = os.path.join(os.path.dirname(__file__), '..', 'Transformed', 'fact_sales_aggregate.csv')
        sales_agg.to_csv(output_path, index=False)
        print(f"\n💾 Step 5: Saved to {output_path}")
        
        # Summary statistics
        print(f"\n📊 SUMMARY STATISTICS:")
        print(f"   📋 Total records: {len(sales_agg):,}")
        print(f"   📅 Date range: {sales_agg['time_key'].min()} to {sales_agg['time_key'].max()}")
        print(f"   💰 Total gross revenue: ${sales_agg['gross_revenue'].sum():,.2f}")
        print(f"   💸 Total discounts: ${sales_agg['total_discounts'].sum():,.2f}")
        print(f"   💵 Total net sales: ${sales_agg['net_sales'].sum():,.2f}")
        print(f"   📦 Total items sold: {sales_agg['total_items_sold'].sum():,}")
        print(f"   🚚 Total shipping revenue: ${sales_agg['shipping_revenue'].sum():,.2f}")
        
        print(f"\n✅ Sales aggregate harmonization completed successfully!")
        print(f"📅 Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        print(f"\n❌ Error during harmonization: {str(e)}")
        raise e

if __name__ == "__main__":
    main()
