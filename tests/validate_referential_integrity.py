#!/usr/bin/env python3
"""
Validate fact_orders referential integrity with dimension tables
"""

import pandas as pd
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

def validate_referential_integrity():
    """Check referential integrity between fact and dimension tables"""
    print("🔍 REFERENTIAL INTEGRITY VALIDATION")
    print("=" * 50)
    
    try:
        # Load fact orders
        print("📊 Loading fact_orders.csv...")
        fact_df = pd.read_csv('../app/Transformed/fact_orders.csv')
        print(f"✓ Loaded {len(fact_df):,} fact order records")
        
        # Load dimension tables
        dim_tables = {}
        dim_files = {
            'dim_order': '../app/Transformed/dim_order.csv',
            'dim_product': '../app/Transformed/dim_product.csv',
            'dim_product_variant': '../app/Transformed/dim_product_variant.csv',
            'dim_customer': '../app/Transformed/dim_customer.csv',
            'dim_time': '../app/Transformed/dim_time.csv'
        }
        
        print("\n📊 Loading dimension tables...")
        for dim_name, file_path in dim_files.items():
            if os.path.exists(file_path):
                dim_tables[dim_name] = pd.read_csv(file_path)
                print(f"✓ {dim_name}: {len(dim_tables[dim_name]):,} records")
            else:
                print(f"⚠️  {dim_name}: File not found")
        
        integrity_issues = []
        
        # Check each foreign key relationship
        print(f"\n🔍 CHECKING FOREIGN KEY RELATIONSHIPS")
        print("-" * 40)
        
        # 1. Orders Key
        if 'dim_order' in dim_tables:
            print("📋 Checking orders_key...")
            fact_orders = set(fact_df['orders_key'].unique())
            dim_orders = set(dim_tables['dim_order']['orders_key'].unique())
            
            missing_orders = fact_orders - dim_orders
            if missing_orders:
                integrity_issues.append(f"Orders missing from dim_order: {len(missing_orders)} keys")
                print(f"❌ Missing orders in dimension: {len(missing_orders)}")
                # Show sample
                sample = list(missing_orders)[:5]
                print(f"   Sample missing: {sample}")
            else:
                print("✅ All orders exist in dim_order")
        
        # 2. Product Key  
        if 'dim_product' in dim_tables:
            print("📦 Checking product_key...")
            fact_products = set(fact_df['product_key'].unique())
            dim_products = set(dim_tables['dim_product']['product_key'].unique())
            
            missing_products = fact_products - dim_products
            if missing_products:
                integrity_issues.append(f"Products missing from dim_product: {len(missing_products)} keys")
                print(f"❌ Missing products in dimension: {len(missing_products)}")
                sample = list(missing_products)[:5]
                print(f"   Sample missing: {sample}")
            else:
                print("✅ All products exist in dim_product")
        
        # 3. Product Variant Key
        if 'dim_product_variant' in dim_tables:
            print("🔧 Checking product_variant_key...")
            fact_variants = set(fact_df['product_variant_key'].unique())
            dim_variants = set(dim_tables['dim_product_variant']['product_variant_key'].unique())
            
            missing_variants = fact_variants - dim_variants
            if missing_variants:
                integrity_issues.append(f"Variants missing from dim_product_variant: {len(missing_variants)} keys")
                print(f"❌ Missing variants in dimension: {len(missing_variants)}")
                sample = list(missing_variants)[:5]
                print(f"   Sample missing: {sample}")
            else:
                print("✅ All variants exist in dim_product_variant")
        
        # 4. Customer Key
        if 'dim_customer' in dim_tables:
            print("👤 Checking customer_key...")
            fact_customers = set(fact_df['customer_key'].unique())
            dim_customers = set(dim_tables['dim_customer']['customer_key'].unique())
            
            missing_customers = fact_customers - dim_customers
            if missing_customers:
                integrity_issues.append(f"Customers missing from dim_customer: {len(missing_customers)} keys")
                print(f"❌ Missing customers in dimension: {len(missing_customers)}")
                sample = list(missing_customers)[:5]
                print(f"   Sample missing: {sample}")
            else:
                print("✅ All customers exist in dim_customer")
        
        # 5. Time Key
        if 'dim_time' in dim_tables:
            print("📅 Checking time_key...")
            fact_times = set(fact_df['time_key'].unique())
            dim_times = set(dim_tables['dim_time']['time_key'].unique())
            
            missing_times = fact_times - dim_times
            if missing_times:
                integrity_issues.append(f"Times missing from dim_time: {len(missing_times)} keys")
                print(f"❌ Missing times in dimension: {len(missing_times)}")
                sample = list(missing_times)[:5]
                print(f"   Sample missing: {sample}")
            else:
                print("✅ All times exist in dim_time")
        
        # Platform key validation (should be 1 or 2)
        print("🏪 Checking platform_key...")
        invalid_platforms = ~fact_df['platform_key'].isin([1, 2])
        if invalid_platforms.any():
            integrity_issues.append(f"Invalid platform_key values: {invalid_platforms.sum()}")
            print(f"❌ Invalid platform keys: {invalid_platforms.sum()}")
        else:
            print("✅ All platform keys valid (1=Lazada, 2=Shopee)")
        
        # Summary
        print(f"\n📊 REFERENTIAL INTEGRITY SUMMARY")
        print("=" * 50)
        
        print(f"Fact table records: {len(fact_df):,}")
        print(f"Dimension tables checked: {len([d for d in dim_tables.keys()])}")
        print(f"Integrity issues found: {len(integrity_issues)}")
        
        if integrity_issues:
            print(f"\n❌ INTEGRITY ISSUES:")
            for i, issue in enumerate(integrity_issues, 1):
                print(f"  {i}. {issue}")
            
            return False
        else:
            print(f"\n✅ PERFECT REFERENTIAL INTEGRITY!")
            print("All foreign keys properly reference dimension tables")
            return True
    
    except Exception as e:
        print(f"❌ Error during referential integrity validation: {e}")
        return False

if __name__ == "__main__":
    success = validate_referential_integrity()
    sys.exit(0 if success else 1)