#!/usr/bin/env python3
"""
Comprehensive Harmonization Validation Test
==========================================

This script validates that all harmonization files work correctly and don't lose data.
Should be run after any changes to harmonization logic to ensure data integrity.
"""

import pandas as pd
import json
import os
import sys
from datetime import datetime

# Add parent directories to path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
grandparent_dir = os.path.dirname(parent_dir)
sys.path.append(parent_dir)
sys.path.append(grandparent_dir)

def test_raw_data_availability():
    """Test that all required raw data files are available"""
    print("🔍 TESTING RAW DATA AVAILABILITY...")
    
    required_files = [
        'app/Staging/lazada_orders_raw.json',
        'app/Staging/lazada_multiple_order_items_raw.json',
        'app/Staging/shopee_orders_raw.json',
        'app/Staging/shopee_paymentdetail_raw.json',
        'app/Staging/shopee_paymentdetail_2_raw.json'
    ]
    
    missing_files = []
    file_counts = {}
    
    for file_path in required_files:
        full_path = os.path.join(grandparent_dir, file_path)
        if os.path.exists(full_path):
            try:
                with open(full_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        count = len(data)
                    elif isinstance(data, dict) and 'data' in data:
                        count = len(data['data'])
                    else:
                        count = 1
                    file_counts[file_path] = count
                    print(f"✅ {file_path}: {count:,} records")
            except Exception as e:
                print(f"❌ {file_path}: Error reading - {e}")
                missing_files.append(file_path)
        else:
            print(f"❌ {file_path}: File not found")
            missing_files.append(file_path)
    
    if missing_files:
        print(f"⚠️  Warning: {len(missing_files)} required files are missing")
        return False, file_counts
    else:
        print("✅ All required raw data files are available")
        return True, file_counts

def test_config_mappings():
    """Test that all required configuration mappings are available"""
    print("\n🔍 TESTING CONFIGURATION MAPPINGS...")
    
    try:
        from config import (
            LAZADA_TO_UNIFIED_MAPPING, 
            SHOPEE_TO_UNIFIED_MAPPING, 
            ORDER_STATUS_MAPPING, 
            PAYMENT_METHOD_MAPPING,
            FACT_ORDERS_COLUMNS,
            DIM_ORDER_COLUMNS
        )
        
        print("✅ All configuration mappings imported successfully")
        
        # Test critical mappings exist
        critical_lazada_fields = ['order_id', 'price', 'statuses', 'created_at', 'updated_at']
        missing_lazada = [field for field in critical_lazada_fields if field not in LAZADA_TO_UNIFIED_MAPPING]
        
        critical_shopee_fields = ['order_sn', 'total_amount', 'order_status', 'create_time']
        missing_shopee = [field for field in critical_shopee_fields if field not in SHOPEE_TO_UNIFIED_MAPPING]
        
        if missing_lazada:
            print(f"❌ Missing critical Lazada mappings: {missing_lazada}")
            return False
            
        if missing_shopee:
            print(f"❌ Missing critical Shopee mappings: {missing_shopee}")
            return False
        
        print(f"✅ Lazada mappings: {len(LAZADA_TO_UNIFIED_MAPPING)} fields")
        print(f"✅ Shopee mappings: {len(SHOPEE_TO_UNIFIED_MAPPING)} fields")
        print(f"✅ Order status mappings: {len(ORDER_STATUS_MAPPING)} statuses")
        print(f"✅ Payment method mappings: {len(PAYMENT_METHOD_MAPPING)} methods")
        
        return True
        
    except ImportError as e:
        print(f"❌ Configuration import error: {e}")
        return False

def test_price_mapping_integrity():
    """Test that the enhanced price mapping logic is working"""
    print("\n🔍 TESTING PRICE MAPPING INTEGRITY...")
    
    # Check if the enhanced price mapping is in place
    harmonize_script = os.path.join(grandparent_dir, 'app', 'Transformation', 'harmonize_dim_order.py')
    
    if not os.path.exists(harmonize_script):
        print("❌ harmonize_dim_order.py not found")
        return False
    
    try:
        with open(harmonize_script, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Check for the enhanced price mapping marker
        if 'PRICE_MAPPING_FIX_APPLIED' in content:
            print("✅ Enhanced price mapping logic is in place")
            
            # Check for key components
            if 'price_sources = [' in content:
                print("✅ Multiple price source fallback implemented")
            else:
                print("⚠️  Multiple price source fallback not found")
            
            if 'isinstance(price_value, str)' in content:
                print("✅ String price handling implemented")
            else:
                print("⚠️  String price handling not found")
                
            return True
        else:
            print("❌ Enhanced price mapping logic not found")
            return False
            
    except Exception as e:
        print(f"❌ Error reading harmonization script: {e}")
        return False

def test_dimension_dependencies():
    """Test that dimension harmonization dependencies are correct"""
    print("\n🔍 TESTING DIMENSION DEPENDENCIES...")
    
    # Check if dimension files exist (should be run after harmonization)
    dimension_files = [
        'app/Transformed/dim_customer.csv',
        'app/Transformed/dim_product.csv', 
        'app/Transformed/dim_product_variant.csv',
        'app/Transformed/dim_order.csv'
    ]
    
    existing_dims = {}
    missing_dims = []
    
    for dim_file in dimension_files:
        full_path = os.path.join(grandparent_dir, dim_file)
        if os.path.exists(full_path):
            try:
                df = pd.read_csv(full_path)
                existing_dims[dim_file] = len(df)
                print(f"✅ {dim_file}: {len(df):,} records")
                
                # Check for price completeness in dim_order
                if 'dim_order.csv' in dim_file:
                    if 'price_total' in df.columns:
                        valid_prices = df['price_total'].notna().sum()
                        total_orders = len(df)
                        completion_rate = (valid_prices / total_orders * 100) if total_orders > 0 else 0
                        print(f"   💰 Price completeness: {completion_rate:.1f}% ({valid_prices:,}/{total_orders:,})")
                        
                        if completion_rate < 95.0:
                            print(f"   ⚠️  Price completeness below 95% threshold")
                            return False
                    else:
                        print(f"   ❌ price_total column missing")
                        return False
                        
            except Exception as e:
                print(f"❌ {dim_file}: Error reading - {e}")
                missing_dims.append(dim_file)
        else:
            print(f"⚠️  {dim_file}: Not found (run harmonization first)")
            missing_dims.append(dim_file)
    
    if len(existing_dims) >= 3:  # At least customer, product, and order dimensions
        print("✅ Core dimension files are available")
        return True
    else:
        print(f"❌ Missing critical dimension files: {missing_dims}")
        return False

def test_fact_orders_logic():
    """Test that fact_orders harmonization logic is correct"""
    print("\n🔍 TESTING FACT_ORDERS LOGIC...")
    
    fact_script = os.path.join(grandparent_dir, 'app', 'Transformation', 'harmonize_fact_orders.py')
    
    if not os.path.exists(fact_script):
        print("❌ harmonize_fact_orders.py not found")
        return False
    
    try:
        with open(fact_script, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Check for COMPLETED orders filter
        if "order_df[order_df['order_status'] == 'COMPLETED']" in content:
            print("✅ COMPLETED orders filter is implemented")
        else:
            print("❌ COMPLETED orders filter not found")
            return False
            
        # Check for proper price field mapping
        if "'paid_price'" in content:
            print("✅ paid_price field mapping found")
        else:
            print("❌ paid_price field mapping not found")
            return False
            
        # Check if fact_orders.csv exists and validate it
        fact_file = os.path.join(grandparent_dir, 'app', 'Transformed', 'fact_orders.csv')
        if os.path.exists(fact_file):
            df = pd.read_csv(fact_file)
            print(f"✅ fact_orders.csv exists: {len(df):,} records")
            
            # Check for required columns
            required_columns = ['order_item_key', 'orders_key', 'product_key', 'paid_price']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                print(f"❌ Missing required columns: {missing_columns}")
                return False
            else:
                print("✅ All required fact_orders columns present")
                
            # Check revenue totals
            total_revenue = df['paid_price'].sum()
            print(f"   💰 Total revenue in fact_orders: ₱{total_revenue:,.2f}")
            
        else:
            print("⚠️  fact_orders.csv not found (run harmonization first)")
        
        return True
        
    except Exception as e:
        print(f"❌ Error reading fact_orders script: {e}")
        return False

def test_validation_system():
    """Test that the validation system is in place"""
    print("\n🔍 TESTING VALIDATION SYSTEM...")
    
    validation_script = os.path.join(grandparent_dir, 'app', 'Transformation', 'validate_price_mapping.py')
    
    if os.path.exists(validation_script):
        print("✅ Price mapping validation script exists")
        
        try:
            # Test if the script can be imported and run
            sys.path.append(os.path.join(grandparent_dir, 'app', 'Transformation'))
            import validate_price_mapping
            
            # Check if main functions exist
            if hasattr(validate_price_mapping, 'validate_price_mapping'):
                print("✅ validate_price_mapping function available")
            else:
                print("❌ validate_price_mapping function not found")
                return False
                
            return True
            
        except ImportError as e:
            print(f"❌ Error importing validation script: {e}")
            return False
    else:
        print("❌ Price mapping validation script not found")
        return False

def run_comprehensive_test():
    """Run all validation tests"""
    print("=" * 80)
    print("🚀 COMPREHENSIVE HARMONIZATION VALIDATION TEST")
    print("=" * 80)
    
    test_results = {}
    
    # Run all tests
    tests = [
        ("Raw Data Availability", test_raw_data_availability),
        ("Configuration Mappings", test_config_mappings),
        ("Price Mapping Integrity", test_price_mapping_integrity),
        ("Dimension Dependencies", test_dimension_dependencies), 
        ("Fact Orders Logic", test_fact_orders_logic),
        ("Validation System", test_validation_system)
    ]
    
    for test_name, test_function in tests:
        try:
            if test_name == "Raw Data Availability":
                result, file_counts = test_function()
                test_results[test_name] = result
            else:
                result = test_function()
                test_results[test_name] = result
        except Exception as e:
            print(f"❌ {test_name} failed with error: {e}")
            test_results[test_name] = False
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 VALIDATION TEST SUMMARY")
    print("=" * 60)
    
    passed_tests = sum(1 for result in test_results.values() if result)
    total_tests = len(test_results)
    
    for test_name, result in test_results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"  {status}: {test_name}")
    
    print(f"\n📈 Overall Result: {passed_tests}/{total_tests} tests passed")
    
    if passed_tests == total_tests:
        print("🎉 ALL TESTS PASSED! Harmonization system is ready to run.")
        return True
    else:
        print("⚠️  Some tests failed. Please address issues before running harmonization.")
        return False

if __name__ == "__main__":
    success = run_comprehensive_test()
    
    if success:
        print("\n🎯 NEXT STEPS:")
        print("  1. Run harmonize_dim_order.py to ensure price mapping works")
        print("  2. Run harmonize_fact_orders.py to generate complete fact table")
        print("  3. Use validate_price_mapping.py to verify results")
        print("  4. Check data quality with the validation scripts in tests/")
    else:
        print("\n❌ Fix the failing tests before proceeding with harmonization")
    
    exit(0 if success else 1)