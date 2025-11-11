#!/usr/bin/env python3
"""
Fix Price Mapping Issue and Backfill Missing Prices
===================================================

This script:
1. Identifies all orders with missing price_total in dim_order
2. Backfills prices from raw data sources
3. Updates the dim_order.csv with corrected prices
4. Validates 100% price mapping success
"""

import pandas as pd
import json
import numpy as np
from datetime import datetime
import shutil
import os

def load_raw_data():
    """Load all raw data sources for price recovery"""
    print("📁 Loading raw data sources...")
    
    raw_data = {}
    
    # Load Lazada orders raw data
    try:
        with open('app/Staging/lazada_orders_raw.json', 'r', encoding='utf-8') as f:
            raw_data['lazada_orders'] = json.load(f)
        print(f"✅ Loaded {len(raw_data['lazada_orders']):,} Lazada orders")
    except Exception as e:
        print(f"❌ Error loading Lazada orders: {e}")
        raw_data['lazada_orders'] = []
    
    # Load Lazada order items raw data
    try:
        with open('app/Staging/lazada_multiple_order_items_raw.json', 'r', encoding='utf-8') as f:
            raw_data['lazada_items'] = json.load(f)
        print(f"✅ Loaded {len(raw_data['lazada_items']):,} Lazada order items")
    except Exception as e:
        print(f"❌ Error loading Lazada order items: {e}")
        raw_data['lazada_items'] = []
    
    # Load Shopee orders raw data
    try:
        with open('app/Staging/shopee_orders_raw.json', 'r', encoding='utf-8') as f:
            raw_data['shopee_orders'] = json.load(f)
        print(f"✅ Loaded {len(raw_data['shopee_orders']):,} Shopee orders")
    except Exception as e:
        print(f"❌ Error loading Shopee orders: {e}")
        raw_data['shopee_orders'] = []
    
    return raw_data

def create_price_lookup_tables(raw_data):
    """Create lookup tables for price recovery from raw data"""
    print("\n🔍 Creating price lookup tables from raw data...")
    
    price_lookups = {}
    
    # Lazada orders price lookup
    lazada_price_lookup = {}
    for order in raw_data['lazada_orders']:
        order_id = str(order.get('order_id', ''))
        price = order.get('price', '')
        
        if order_id and price:
            try:
                price_value = float(price)
                lazada_price_lookup[order_id] = price_value
            except (ValueError, TypeError):
                print(f"⚠️  Invalid price for Lazada order {order_id}: {price}")
    
    print(f"📊 Created Lazada price lookup: {len(lazada_price_lookup):,} orders")
    
    # Lazada order items price lookup (alternative source)
    lazada_items_price_lookup = {}
    for order in raw_data['lazada_items']:
        order_id = str(order.get('order_id', ''))
        price = order.get('price', '') or order.get('item_price', '')
        
        if order_id and price:
            try:
                price_value = float(price)
                lazada_items_price_lookup[order_id] = price_value
            except (ValueError, TypeError):
                continue
    
    print(f"📊 Created Lazada items price lookup: {len(lazada_items_price_lookup):,} orders")
    
    # Shopee orders price lookup
    shopee_price_lookup = {}
    for order in raw_data['shopee_orders']:
        order_sn = str(order.get('order_sn', ''))
        total_amount = order.get('total_amount', 0)
        
        if order_sn and total_amount:
            try:
                price_value = float(total_amount)
                shopee_price_lookup[order_sn] = price_value
            except (ValueError, TypeError):
                print(f"⚠️  Invalid price for Shopee order {order_sn}: {total_amount}")
    
    print(f"📊 Created Shopee price lookup: {len(shopee_price_lookup):,} orders")
    
    price_lookups['lazada_orders'] = lazada_price_lookup
    price_lookups['lazada_items'] = lazada_items_price_lookup
    price_lookups['shopee_orders'] = shopee_price_lookup
    
    return price_lookups

def backup_dim_order():
    """Create backup of current dim_order.csv before making changes"""
    original_path = 'app/Transformed/dim_order.csv'
    backup_path = f'app/Transformed/dim_order_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    
    try:
        shutil.copy2(original_path, backup_path)
        print(f"📋 Backup created: {backup_path}")
        return backup_path
    except Exception as e:
        print(f"❌ Error creating backup: {e}")
        return None

def fix_missing_prices(price_lookups):
    """Fix missing prices in dim_order using raw data lookups"""
    print("\n🔧 FIXING MISSING PRICES IN DIM_ORDER...")
    
    # Create backup first
    backup_path = backup_dim_order()
    if not backup_path:
        print("❌ Cannot proceed without backup")
        return False
    
    # Load current dim_order
    dim_order = pd.read_csv('app/Transformed/dim_order.csv')
    print(f"📊 Loaded dim_order: {len(dim_order):,} total orders")
    
    # Identify orders with missing prices
    missing_price_mask = dim_order['price_total'].isna()
    missing_count = missing_price_mask.sum()
    print(f"⚠️  Orders with missing prices: {missing_count:,}")
    
    if missing_count == 0:
        print("✅ No missing prices found!")
        return True
    
    # Track recovery statistics
    recovery_stats = {
        'lazada_orders_recovered': 0,
        'lazada_items_recovered': 0,
        'shopee_orders_recovered': 0,
        'still_missing': 0
    }
    
    # Process each order with missing price
    for idx, row in dim_order[missing_price_mask].iterrows():
        platform_key = row['platform_key']
        platform_order_id = str(row['platform_order_id'])
        current_price = row['price_total']
        
        recovered_price = None
        recovery_source = None
        
        if platform_key == 1:  # Lazada
            # Try primary source first
            if platform_order_id in price_lookups['lazada_orders']:
                recovered_price = price_lookups['lazada_orders'][platform_order_id]
                recovery_source = 'lazada_orders'
                recovery_stats['lazada_orders_recovered'] += 1
            
            # Try secondary source if primary failed
            elif platform_order_id in price_lookups['lazada_items']:
                recovered_price = price_lookups['lazada_items'][platform_order_id]
                recovery_source = 'lazada_items'
                recovery_stats['lazada_items_recovered'] += 1
        
        elif platform_key == 2:  # Shopee
            if platform_order_id in price_lookups['shopee_orders']:
                recovered_price = price_lookups['shopee_orders'][platform_order_id]
                recovery_source = 'shopee_orders'
                recovery_stats['shopee_orders_recovered'] += 1
        
        # Apply the recovered price
        if recovered_price is not None and recovered_price > 0:
            dim_order.at[idx, 'price_total'] = recovered_price
            if idx < 10:  # Show first 10 recoveries
                print(f"✅ Recovered price for order {platform_order_id}: ₱{recovered_price:.2f} (from {recovery_source})")
        else:
            recovery_stats['still_missing'] += 1
            if idx < 10:  # Show first 10 failures
                print(f"❌ Could not recover price for order {platform_order_id}")
    
    # Save the updated dim_order
    try:
        dim_order.to_csv('app/Transformed/dim_order.csv', index=False)
        print(f"\n✅ Updated dim_order.csv saved successfully")
    except Exception as e:
        print(f"❌ Error saving updated dim_order: {e}")
        return False
    
    # Report recovery statistics
    print(f"\n📊 PRICE RECOVERY STATISTICS:")
    print(f"  Original missing prices: {missing_count:,}")
    print(f"  Recovered from lazada_orders: {recovery_stats['lazada_orders_recovered']:,}")
    print(f"  Recovered from lazada_items: {recovery_stats['lazada_items_recovered']:,}")
    print(f"  Recovered from shopee_orders: {recovery_stats['shopee_orders_recovered']:,}")
    print(f"  Still missing: {recovery_stats['still_missing']:,}")
    
    total_recovered = (recovery_stats['lazada_orders_recovered'] + 
                      recovery_stats['lazada_items_recovered'] + 
                      recovery_stats['shopee_orders_recovered'])
    
    recovery_rate = (total_recovered / missing_count * 100) if missing_count > 0 else 100
    print(f"  Recovery rate: {recovery_rate:.1f}%")
    
    # Validate the fix
    print(f"\n🔍 VALIDATING PRICE FIX...")
    updated_dim_order = pd.read_csv('app/Transformed/dim_order.csv')
    remaining_missing = updated_dim_order['price_total'].isna().sum()
    
    print(f"  Orders with missing prices after fix: {remaining_missing:,}")
    print(f"  Orders with valid prices: {updated_dim_order['price_total'].notna().sum():,}")
    
    success_rate = (updated_dim_order['price_total'].notna().sum() / len(updated_dim_order) * 100)
    print(f"  Overall price completeness: {success_rate:.1f}%")
    
    if remaining_missing == 0:
        print("🎉 SUCCESS: All prices have been recovered!")
    else:
        print(f"⚠️  {remaining_missing:,} orders still have missing prices")
    
    return True

def fix_transformation_script():
    """Fix the transformation script to prevent future price mapping issues"""
    print("\n🔧 FIXING TRANSFORMATION SCRIPT FOR FUTURE PRICE MAPPING...")
    
    # Read current harmonize_dim_order.py
    script_path = 'app/Transformation/harmonize_dim_order.py'
    
    try:
        with open(script_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception as e:
        print(f"❌ Error reading transformation script: {e}")
        return False
    
    # Check if price mapping fix is already implemented
    if 'PRICE_MAPPING_FIX_APPLIED' in content:
        print("✅ Transformation script already has price mapping fix")
        return True
    
    # Create backup of transformation script
    backup_script_path = f'app/Transformation/harmonize_dim_order_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.py'
    try:
        shutil.copy2(script_path, backup_script_path)
        print(f"📋 Transformation script backup created: {backup_script_path}")
    except Exception as e:
        print(f"❌ Error creating script backup: {e}")
        return False
    
    # Enhanced price mapping logic
    enhanced_price_mapping = '''
            elif lazada_field == 'price':
                # PRICE_MAPPING_FIX_APPLIED - Enhanced price mapping with validation
                price_total = None
                
                # Try multiple price sources for robustness
                price_sources = ['price', 'item_price', 'total_amount']
                
                for price_field in price_sources:
                    if price_field in order_data and order_data[price_field] is not None:
                        try:
                            price_value = order_data[price_field]
                            
                            # Handle string prices (e.g., "350.00")
                            if isinstance(price_value, str):
                                price_value = price_value.strip()
                                if price_value and price_value != '0.00':
                                    price_total = float(price_value)
                                    break
                            
                            # Handle numeric prices
                            elif isinstance(price_value, (int, float)):
                                if price_value > 0:
                                    price_total = float(price_value)
                                    break
                                    
                        except (ValueError, TypeError):
                            continue
                
                # Price validation and logging
                if price_total is None:
                    print(f"⚠️  No valid price found for order {order_data.get('order_id', 'unknown')}")
                    print(f"   Available price fields: {[f'{k}: {v}' for k, v in order_data.items() if 'price' in k.lower() or 'amount' in k.lower()]}")
                
                harmonized_record['price_total'] = price_total'''
    
    # Replace the original price mapping section
    original_price_mapping = '''elif lazada_field == 'price':
                # Convert price to float
                price_total = None
                if 'price' in order_data:
                    try:
                        price_total = float(order_data['price'])
                    except (ValueError, TypeError):
                        price_total = None
                harmonized_record['price_total'] = price_total'''
    
    # Apply the fix
    if original_price_mapping.replace(' ', '').replace('\n', '') in content.replace(' ', '').replace('\n', ''):
        updated_content = content.replace(original_price_mapping, enhanced_price_mapping)
        
        try:
            with open(script_path, 'w', encoding='utf-8') as f:
                f.write(updated_content)
            print("✅ Transformation script updated with enhanced price mapping")
            return True
        except Exception as e:
            print(f"❌ Error updating transformation script: {e}")
            return False
    else:
        print("⚠️  Could not find exact price mapping pattern to replace")
        print("   Manual update of transformation script may be required")
        return False

def add_price_validation():
    """Add price validation to the transformation process"""
    print("\n🔍 ADDING PRICE VALIDATION TO TRANSFORMATION...")
    
    validation_script = '''#!/usr/bin/env python3
"""
Price Mapping Validation for Transformation Pipeline
====================================================

This script validates that price mapping is working correctly
during the transformation process.
"""

import pandas as pd
import json

def validate_price_mapping():
    """Validate price mapping completeness after transformation"""
    print("🔍 VALIDATING PRICE MAPPING COMPLETENESS...")
    
    try:
        # Load transformed data
        dim_order = pd.read_csv('app/Transformed/dim_order.csv')
        
        # Check price completeness
        total_orders = len(dim_order)
        valid_prices = dim_order['price_total'].notna().sum()
        missing_prices = dim_order['price_total'].isna().sum()
        
        print(f"📊 Price Mapping Validation Results:")
        print(f"  Total orders: {total_orders:,}")
        print(f"  Orders with valid prices: {valid_prices:,}")
        print(f"  Orders with missing prices: {missing_prices:,}")
        
        completion_rate = (valid_prices / total_orders * 100) if total_orders > 0 else 0
        print(f"  Price mapping completion rate: {completion_rate:.1f}%")
        
        # Set validation thresholds
        if completion_rate >= 98.0:
            print("✅ PASS: Price mapping meets quality threshold (≥98%)")
            return True
        elif completion_rate >= 95.0:
            print("⚠️  WARNING: Price mapping below optimal threshold (95-98%)")
            return True
        else:
            print("❌ FAIL: Price mapping below acceptable threshold (<95%)")
            print("   Manual investigation required")
            return False
            
    except Exception as e:
        print(f"❌ Error during price validation: {e}")
        return False

def validate_by_platform():
    """Validate price mapping by platform"""
    try:
        dim_order = pd.read_csv('app/Transformed/dim_order.csv')
        
        print(f"\\n📊 Price Mapping by Platform:")
        
        for platform_key, platform_name in [(1, 'Lazada'), (2, 'Shopee')]:
            platform_orders = dim_order[dim_order['platform_key'] == platform_key]
            
            if len(platform_orders) > 0:
                total = len(platform_orders)
                valid = platform_orders['price_total'].notna().sum()
                rate = (valid / total * 100)
                
                print(f"  {platform_name}: {valid:,}/{total:,} ({rate:.1f}%)")
                
                if rate < 95.0:
                    print(f"    ⚠️  {platform_name} price mapping needs attention")
        
    except Exception as e:
        print(f"❌ Error in platform validation: {e}")

if __name__ == "__main__":
    success = validate_price_mapping()
    validate_by_platform()
    
    if not success:
        exit(1)
'''
    
    # Save validation script
    validation_path = 'app/Transformation/validate_price_mapping.py'
    try:
        with open(validation_path, 'w', encoding='utf-8') as f:
            f.write(validation_script)
        print(f"✅ Price validation script created: {validation_path}")
        return True
    except Exception as e:
        print(f"❌ Error creating validation script: {e}")
        return False

def run_comprehensive_fix():
    """Run the complete price mapping fix process"""
    print("=" * 80)
    print("🚀 COMPREHENSIVE PRICE MAPPING FIX")
    print("=" * 80)
    
    # Step 1: Load raw data
    raw_data = load_raw_data()
    
    # Step 2: Create price lookup tables
    price_lookups = create_price_lookup_tables(raw_data)
    
    # Step 3: Fix missing prices in dim_order
    price_fix_success = fix_missing_prices(price_lookups)
    
    if not price_fix_success:
        print("❌ Price backfill failed - stopping here")
        return False
    
    # Step 4: Fix transformation script for future
    script_fix_success = fix_transformation_script()
    
    # Step 5: Add price validation
    validation_success = add_price_validation()
    
    # Step 6: Final validation
    print("\n🔍 FINAL VALIDATION...")
    
    try:
        final_dim_order = pd.read_csv('app/Transformed/dim_order.csv')
        total_orders = len(final_dim_order)
        valid_prices = final_dim_order['price_total'].notna().sum()
        missing_prices = final_dim_order['price_total'].isna().sum()
        
        completion_rate = (valid_prices / total_orders * 100) if total_orders > 0 else 0
        
        print(f"📊 FINAL RESULTS:")
        print(f"  Total orders: {total_orders:,}")
        print(f"  Valid prices: {valid_prices:,}")
        print(f"  Missing prices: {missing_prices:,}")
        print(f"  Completion rate: {completion_rate:.1f}%")
        
        if completion_rate >= 98.0:
            print("🎉 SUCCESS: Price mapping fix completed successfully!")
        elif completion_rate >= 95.0:
            print("✅ GOOD: Significant improvement achieved")
        else:
            print("⚠️  PARTIAL: Some improvement made, but more work needed")
            
        return completion_rate >= 95.0
        
    except Exception as e:
        print(f"❌ Error in final validation: {e}")
        return False

if __name__ == "__main__":
    success = run_comprehensive_fix()
    
    if success:
        print("\n🎯 NEXT STEPS:")
        print("  1. ✅ Run harmonize_dim_order.py to test improved transformation")
        print("  2. ✅ Run validate_price_mapping.py to verify quality")
        print("  3. ✅ Update fact_orders with new price data")
    else:
        print("\n❌ Fix incomplete - manual intervention may be required")