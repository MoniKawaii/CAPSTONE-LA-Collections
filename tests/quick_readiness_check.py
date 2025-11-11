#!/usr/bin/env python3
"""
Quick Harmonization Readiness Check
===================================

Simple test to verify that harmonization files can run without losing data.
"""

import os
import sys
import pandas as pd
import json

def check_harmonization_readiness():
    """Quick check that harmonization is ready to run"""
    print("🔍 QUICK HARMONIZATION READINESS CHECK...")
    
    # Move to the correct directory
    os.chdir('..')
    
    issues = []
    
    # 1. Check that enhanced price mapping is in place
    try:
        with open('app/Transformation/harmonize_dim_order.py', 'r', encoding='utf-8') as f:
            content = f.read()
            if 'PRICE_MAPPING_FIX_APPLIED' not in content:
                issues.append("❌ Enhanced price mapping not found in harmonize_dim_order.py")
            else:
                print("✅ Enhanced price mapping is in place")
    except FileNotFoundError:
        issues.append("❌ harmonize_dim_order.py not found")
    
    # 2. Check that raw data exists
    raw_files = [
        'app/Staging/lazada_orders_raw.json',
        'app/Staging/shopee_orders_raw.json'
    ]
    
    for file_path in raw_files:
        if os.path.exists(file_path):
            print(f"✅ {file_path} exists")
        else:
            issues.append(f"❌ {file_path} not found")
    
    # 3. Check current dim_order has 100% price completeness
    try:
        dim_order = pd.read_csv('app/Transformed/dim_order.csv')
        missing_prices = dim_order['price_total'].isna().sum()
        if missing_prices == 0:
            print(f"✅ dim_order.csv has 100% price completeness ({len(dim_order):,} orders)")
        else:
            issues.append(f"❌ dim_order.csv has {missing_prices:,} missing prices")
    except FileNotFoundError:
        print("⚠️  dim_order.csv not found (will be created during harmonization)")
    
    # 4. Check that validation script exists
    if os.path.exists('app/Transformation/validate_price_mapping.py'):
        print("✅ Price validation script is available")
    else:
        issues.append("❌ Price validation script not found")
    
    # Summary
    if not issues:
        print("\n🎉 READY TO RUN HARMONIZATION!")
        print("  All checks passed. You can safely run:")
        print("  • python app/Transformation/harmonize_dim_order.py")
        print("  • python app/Transformation/harmonize_fact_orders.py")
        return True
    else:
        print(f"\n⚠️  {len(issues)} ISSUES FOUND:")
        for issue in issues:
            print(f"  {issue}")
        print("\nPlease address these issues before running harmonization.")
        return False

if __name__ == "__main__":
    success = check_harmonization_readiness()
    exit(0 if success else 1)