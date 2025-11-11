#!/usr/bin/env python3
"""
Post-Harmonization Validation
=============================

Run this after harmonization to ensure no data was lost and all mappings work correctly.
"""

import pandas as pd
import os
import sys

def validate_post_harmonization():
    """Validate that harmonization completed successfully without data loss"""
    print("🔍 POST-HARMONIZATION VALIDATION...")
    
    # Move to project root
    os.chdir('..')
    
    validation_results = {}
    
    # 1. Validate dim_order completeness
    print("\n📊 VALIDATING DIM_ORDER...")
    try:
        dim_order = pd.read_csv('app/Transformed/dim_order.csv')
        
        # Check price completeness
        total_orders = len(dim_order)
        valid_prices = dim_order['price_total'].notna().sum()
        missing_prices = dim_order['price_total'].isna().sum()
        completion_rate = (valid_prices / total_orders * 100) if total_orders > 0 else 0
        
        print(f"  Total orders: {total_orders:,}")
        print(f"  Valid prices: {valid_prices:,}")
        print(f"  Missing prices: {missing_prices:,}")
        print(f"  Completion rate: {completion_rate:.1f}%")
        
        validation_results['dim_order_price_completion'] = completion_rate >= 98.0
        
        if completion_rate >= 98.0:
            print("  ✅ Price completeness PASSED (≥98%)")
        else:
            print("  ❌ Price completeness FAILED (<98%)")
        
        # Check platform distribution
        platform_counts = dim_order['platform_key'].value_counts()
        print(f"  Lazada orders (platform_key=1): {platform_counts.get(1, 0):,}")
        print(f"  Shopee orders (platform_key=2): {platform_counts.get(2, 0):,}")
        
        validation_results['dim_order_exists'] = True
        
    except FileNotFoundError:
        print("  ❌ dim_order.csv not found")
        validation_results['dim_order_exists'] = False
        validation_results['dim_order_price_completion'] = False
    
    # 2. Validate fact_orders alignment
    print("\n📊 VALIDATING FACT_ORDERS...")
    try:
        fact_orders = pd.read_csv('app/Transformed/fact_orders.csv')
        
        # Count records
        total_fact_records = len(fact_orders)
        print(f"  Total fact records: {total_fact_records:,}")
        
        # Check revenue totals
        total_revenue = fact_orders['paid_price'].sum()
        print(f"  Total revenue: ₱{total_revenue:,.2f}")
        
        # Check platform distribution
        platform_revenue = fact_orders.groupby('platform_key')['paid_price'].agg(['count', 'sum'])
        if 1 in platform_revenue.index:
            print(f"  Lazada: {platform_revenue.loc[1, 'count']:,} records, ₱{platform_revenue.loc[1, 'sum']:,.2f}")
        if 2 in platform_revenue.index:
            print(f"  Shopee: {platform_revenue.loc[2, 'count']:,} records, ₱{platform_revenue.loc[2, 'sum']:,.2f}")
        
        # Compare with completed orders in dim_order
        if validation_results.get('dim_order_exists'):
            completed_orders = dim_order[dim_order['order_status'] == 'COMPLETED']
            completed_count = len(completed_orders)
            completed_revenue = completed_orders['price_total'].sum()
            
            print(f"\n  📊 COMPLETED ORDERS COMPARISON:")
            print(f"    dim_order COMPLETED: {completed_count:,} orders, ₱{completed_revenue:,.2f}")
            print(f"    fact_orders unique orders: {fact_orders['orders_key'].nunique():,}")
            
            # Check if fact_orders covers all completed orders
            coverage_rate = (fact_orders['orders_key'].nunique() / completed_count * 100) if completed_count > 0 else 0
            print(f"    Coverage rate: {coverage_rate:.1f}%")
            
            validation_results['fact_orders_coverage'] = coverage_rate >= 95.0
            
            if coverage_rate >= 95.0:
                print("    ✅ Fact orders coverage PASSED (≥95%)")
            else:
                print("    ❌ Fact orders coverage FAILED (<95%)")
        
        validation_results['fact_orders_exists'] = True
        
    except FileNotFoundError:
        print("  ❌ fact_orders.csv not found")
        validation_results['fact_orders_exists'] = False
        validation_results['fact_orders_coverage'] = False
    
    # 3. Validate dimension consistency
    print("\n📊 VALIDATING DIMENSION CONSISTENCY...")
    
    dimension_files = [
        'dim_customer.csv',
        'dim_product.csv',
        'dim_product_variant.csv'
    ]
    
    for dim_file in dimension_files:
        try:
            df = pd.read_csv(f'app/Transformed/{dim_file}')
            print(f"  ✅ {dim_file}: {len(df):,} records")
            validation_results[f'{dim_file}_exists'] = True
        except FileNotFoundError:
            print(f"  ❌ {dim_file}: Not found")
            validation_results[f'{dim_file}_exists'] = False
    
    # 4. Summary and recommendations
    print("\n" + "=" * 60)
    print("📊 VALIDATION SUMMARY")
    print("=" * 60)
    
    passed_validations = sum(1 for result in validation_results.values() if result)
    total_validations = len(validation_results)
    
    for validation_name, result in validation_results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"  {status}: {validation_name}")
    
    success_rate = (passed_validations / total_validations * 100) if total_validations > 0 else 0
    print(f"\n📈 Overall Success Rate: {success_rate:.1f}% ({passed_validations}/{total_validations})")
    
    if success_rate >= 90.0:
        print("🎉 HARMONIZATION VALIDATION PASSED!")
        print("   Your harmonization completed successfully with high data quality.")
        return True
    else:
        print("⚠️  HARMONIZATION VALIDATION FAILED!")
        print("   Some data quality issues were detected. Review the failed validations.")
        return False

if __name__ == "__main__":
    success = validate_post_harmonization()
    
    if success:
        print("\n🎯 RECOMMENDATIONS:")
        print("  ✅ Data quality is excellent - ready for analytics")
        print("  ✅ Run dashboard generation scripts")
        print("  ✅ Use data for business insights")
    else:
        print("\n🔧 NEXT STEPS:")
        print("  1. Review failed validation items above")
        print("  2. Check harmonization logs for errors")
        print("  3. Re-run harmonization if needed")
        print("  4. Use tests in this folder for debugging")
    
    exit(0 if success else 1)