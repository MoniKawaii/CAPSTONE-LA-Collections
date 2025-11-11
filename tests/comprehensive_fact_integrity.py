#!/usr/bin/env python3
"""
Comprehensive Fact Orders Integrity Validation
Performs detailed validation of fact_orders.csv data integrity
"""

import pandas as pd
import numpy as np
import sys
import os
from datetime import datetime

# Add parent directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

def validate_fact_integrity():
    """Comprehensive fact orders integrity validation"""
    print("🔍 COMPREHENSIVE FACT ORDERS INTEGRITY VALIDATION")
    print("=" * 60)
    
    try:
        # Load fact orders
        print("📊 Loading fact_orders.csv...")
        df = pd.read_csv('../app/Transformed/fact_orders.csv')
        print(f"✓ Loaded {len(df):,} fact order records")
        
        issues = []
        warnings = []
        
        # 1. Schema Validation
        print("\n🔍 1. SCHEMA VALIDATION")
        print("-" * 30)
        
        expected_columns = [
            'order_item_key', 'orders_key', 'product_key', 'product_variant_key',
            'time_key', 'customer_key', 'platform_key', 'item_quantity',
            'paid_price', 'original_unit_price', 'voucher_platform_amount',
            'voucher_seller_amount', 'shipping_fee_paid_by_buyer'
        ]
        
        missing_cols = [col for col in expected_columns if col not in df.columns]
        extra_cols = [col for col in df.columns if col not in expected_columns]
        
        if missing_cols:
            issues.append(f"Missing columns: {missing_cols}")
        if extra_cols:
            warnings.append(f"Extra columns: {extra_cols}")
        
        print(f"✓ Schema check: {len(expected_columns)} expected columns")
        if not missing_cols:
            print("✅ All required columns present")
        
        # 2. Data Type Validation
        print("\n🔍 2. DATA TYPE VALIDATION")
        print("-" * 30)
        
        # Check numeric columns
        numeric_cols = ['item_quantity', 'paid_price', 'original_unit_price', 
                       'voucher_platform_amount', 'voucher_seller_amount', 'shipping_fee_paid_by_buyer']
        
        for col in numeric_cols:
            if col in df.columns:
                non_numeric = pd.to_numeric(df[col], errors='coerce').isna().sum()
                if non_numeric > 0:
                    issues.append(f"{col}: {non_numeric} non-numeric values")
                else:
                    print(f"✓ {col}: All values numeric")
        
        # Check key columns are strings/objects
        key_cols = ['order_item_key', 'orders_key', 'product_key', 'product_variant_key', 'customer_key']
        for col in key_cols:
            if col in df.columns:
                null_keys = df[col].isna().sum()
                if null_keys > 0:
                    issues.append(f"{col}: {null_keys} null values")
                else:
                    print(f"✓ {col}: No null values")
        
        # 3. Business Logic Validation
        print("\n🔍 3. BUSINESS LOGIC VALIDATION")
        print("-" * 30)
        
        # Quantity validation
        if 'item_quantity' in df.columns:
            zero_qty = (df['item_quantity'] == 0).sum()
            negative_qty = (df['item_quantity'] < 0).sum()
            
            if negative_qty > 0:
                issues.append(f"Negative quantities: {negative_qty} records")
            if zero_qty > 0:
                warnings.append(f"Zero quantities: {zero_qty} records")
            
            print(f"✓ Quantity range: {df['item_quantity'].min()} to {df['item_quantity'].max()}")
        
        # Price validation
        price_cols = ['paid_price', 'original_unit_price']
        for col in price_cols:
            if col in df.columns:
                negative_prices = (df[col] < 0).sum()
                if negative_prices > 0:
                    issues.append(f"{col}: {negative_prices} negative values")
                
                print(f"✓ {col} range: ₱{df[col].min():.2f} to ₱{df[col].max():.2f}")
        
        # Paid vs Original price logic
        if 'paid_price' in df.columns and 'original_unit_price' in df.columns:
            paid_gt_original = (df['paid_price'] > df['original_unit_price']).sum()
            if paid_gt_original > 0:
                warnings.append(f"Paid price > Original price: {paid_gt_original} records")
            
            # Check discount patterns
            df['discount_amount'] = df['original_unit_price'] - df['paid_price']
            df['discount_pct'] = (df['discount_amount'] / df['original_unit_price'] * 100).fillna(0)
            
            print(f"✓ Discount patterns:")
            print(f"  - Records with discounts: {(df['discount_amount'] > 0).sum():,}")
            print(f"  - Average discount: {df['discount_pct'].mean():.1f}%")
            print(f"  - Max discount: {df['discount_pct'].max():.1f}%")
        
        # 4. Foreign Key Integrity
        print("\n🔍 4. FOREIGN KEY INTEGRITY")
        print("-" * 30)
        
        # Platform keys should be 1 or 2
        if 'platform_key' in df.columns:
            invalid_platforms = (~df['platform_key'].isin([1, 2])).sum()
            if invalid_platforms > 0:
                issues.append(f"Invalid platform_key values: {invalid_platforms}")
            else:
                print("✓ Platform keys valid (1=Lazada, 2=Shopee)")
        
        # Time key format validation (YYYYMMDD)
        if 'time_key' in df.columns:
            invalid_dates = 0
            for time_key in df['time_key'].unique():
                try:
                    datetime.strptime(str(time_key), '%Y%m%d')
                except:
                    invalid_dates += 1
            
            if invalid_dates > 0:
                issues.append(f"Invalid time_key formats: {invalid_dates}")
            else:
                print("✓ Time keys in valid YYYYMMDD format")
        
        # 5. Completeness Validation
        print("\n🔍 5. COMPLETENESS VALIDATION")
        print("-" * 30)
        
        # Check for null values in critical fields
        critical_fields = ['orders_key', 'product_key', 'item_quantity', 'paid_price']
        for field in critical_fields:
            if field in df.columns:
                null_count = df[field].isna().sum()
                if null_count > 0:
                    issues.append(f"{field}: {null_count} null values")
                else:
                    print(f"✓ {field}: Complete (no nulls)")
        
        # 6. Revenue Consistency
        print("\n🔍 6. REVENUE CONSISTENCY")
        print("-" * 30)
        
        if all(col in df.columns for col in ['paid_price', 'item_quantity']):
            df['calculated_revenue'] = df['paid_price'] * df['item_quantity']
            total_revenue = df['calculated_revenue'].sum()
            print(f"✓ Total revenue: ₱{total_revenue:,.2f}")
            
            # Check for unusual revenue patterns
            high_value_items = (df['calculated_revenue'] > 50000).sum()
            if high_value_items > 0:
                warnings.append(f"High value items (>₱50k): {high_value_items}")
                print(f"⚠️  High value items found: {high_value_items}")
        
        # 7. Platform-specific Validation
        print("\n🔍 7. PLATFORM-SPECIFIC VALIDATION")
        print("-" * 30)
        
        if 'platform_key' in df.columns:
            for platform_key, platform_name in [(1, 'Lazada'), (2, 'Shopee')]:
                platform_df = df[df['platform_key'] == platform_key]
                if len(platform_df) > 0:
                    print(f"✓ {platform_name}:")
                    print(f"  - Records: {len(platform_df):,}")
                    print(f"  - Revenue: ₱{(platform_df['paid_price'] * platform_df['item_quantity']).sum():,.2f}")
                    print(f"  - Avg order value: ₱{(platform_df['paid_price'] * platform_df['item_quantity']).mean():.2f}")
        
        # 8. Uniqueness Validation
        print("\n🔍 8. UNIQUENESS VALIDATION")
        print("-" * 30)
        
        if 'order_item_key' in df.columns:
            duplicate_keys = df['order_item_key'].duplicated().sum()
            if duplicate_keys > 0:
                issues.append(f"Duplicate order_item_key values: {duplicate_keys}")
            else:
                print("✓ Order item keys are unique")
        
        # 9. Temporal Consistency
        print("\n🔍 9. TEMPORAL CONSISTENCY")
        print("-" * 30)
        
        if 'time_key' in df.columns:
            date_range = f"{df['time_key'].min()} to {df['time_key'].max()}"
            print(f"✓ Date range: {date_range}")
            
            # Check for future dates
            today = int(datetime.now().strftime('%Y%m%d'))
            future_dates = (df['time_key'] > today).sum()
            if future_dates > 0:
                warnings.append(f"Future dates found: {future_dates}")
        
        # 10. Summary Report
        print("\n📊 INTEGRITY VALIDATION SUMMARY")
        print("=" * 60)
        
        print(f"Total records validated: {len(df):,}")
        print(f"Critical issues found: {len(issues)}")
        print(f"Warnings found: {len(warnings)}")
        
        if issues:
            print("\n🚨 CRITICAL ISSUES:")
            for i, issue in enumerate(issues, 1):
                print(f"  {i}. {issue}")
        
        if warnings:
            print("\n⚠️  WARNINGS:")
            for i, warning in enumerate(warnings, 1):
                print(f"  {i}. {warning}")
        
        if not issues:
            if not warnings:
                print("\n✅ PERFECT INTEGRITY - No issues or warnings found!")
            else:
                print("\n✅ GOOD INTEGRITY - No critical issues, minor warnings only")
        else:
            print("\n❌ INTEGRITY ISSUES FOUND - Review critical issues above")
        
        return len(issues) == 0
        
    except Exception as e:
        print(f"❌ Error during integrity validation: {e}")
        return False

if __name__ == "__main__":
    success = validate_fact_integrity()
    sys.exit(0 if success else 1)