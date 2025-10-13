"""
Validation script for fact_orders table
Checks for data quality and foreign key relationships
"""

import pandas as pd
import os

def main():
    # Load the fact_orders data
    fact_orders_path = os.path.join(os.path.dirname(__file__), '..', 'Transformed', 'fact_orders.csv')
    fact_orders_df = pd.read_csv(fact_orders_path)
    
    print("🔍 FACT ORDERS VALIDATION REPORT")
    print("=" * 50)
    
    # Basic data quality checks
    print(f"📊 Total Records: {len(fact_orders_df):,}")
    print(f"📊 Total Columns: {len(fact_orders_df.columns)}")
    
    # Check for NULL values in required foreign keys
    print("\n🔗 FOREIGN KEY VALIDATION:")
    required_fks = ['orders_key', 'customer_key', 'product_key', 'time_key', 'platform_key']
    
    for fk in required_fks:
        null_count = fact_orders_df[fk].isnull().sum()
        zero_count = (fact_orders_df[fk] == 0).sum()
        print(f"   • {fk}: {null_count:,} nulls, {zero_count:,} zeros")
    
    # Check data ranges
    print("\n📈 DATA RANGES:")
    print(f"   • Item Quantity: {fact_orders_df['item_quantity'].min()} to {fact_orders_df['item_quantity'].max()}")
    print(f"   • Paid Price: ${fact_orders_df['paid_price'].min():.2f} to ${fact_orders_df['paid_price'].max():.2f}")
    print(f"   • Original Unit Price: ${fact_orders_df['original_unit_price'].min():.2f} to ${fact_orders_df['original_unit_price'].max():.2f}")
    
    # Check for duplicate order_item_keys
    duplicate_keys = fact_orders_df['order_item_key'].duplicated().sum()
    print(f"\n🔑 DUPLICATE KEYS: {duplicate_keys:,}")
    
    # Summary by platform
    platform_summary = fact_orders_df.groupby('platform_key').agg({
        'order_item_key': 'count',
        'paid_price': 'sum',
        'item_quantity': 'sum'
    }).round(2)
    
    print("\n🏪 PLATFORM SUMMARY:")
    for platform_key, row in platform_summary.iterrows():
        platform_name = "Lazada" if platform_key == 1 else "Shopee"
        print(f"   • {platform_name}: {row['order_item_key']:,} records, ${row['paid_price']:,.2f} revenue, {row['item_quantity']:,} items")
    
    # Date range analysis
    time_keys = fact_orders_df['time_key'].unique()
    min_date = str(min(time_keys))
    max_date = str(max(time_keys))
    
    # Format dates for display
    min_formatted = f"{min_date[:4]}-{min_date[4:6]}-{min_date[6:8]}"
    max_formatted = f"{max_date[:4]}-{max_date[4:6]}-{max_date[6:8]}"
    
    print(f"\n📅 DATE RANGE: {min_formatted} to {max_formatted}")
    print(f"📅 UNIQUE DATES: {len(time_keys):,}")
    
    print("\n✅ VALIDATION COMPLETE!")

if __name__ == "__main__":
    main()