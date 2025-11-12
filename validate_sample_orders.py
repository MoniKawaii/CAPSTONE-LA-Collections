"""
Sample Orders Validation Script
Validates that the three problematic sample orders from sample_order.txt
are now correctly processed with unit-level granularity and proper pricing
"""

import pandas as pd
import os

def validate_sample_orders():
    """Validate the sample orders are correctly processed"""
    print("🔍 VALIDATING SAMPLE ORDERS FROM sample_order.txt")
    print("=" * 60)
    
    # Sample order SNs to validate
    sample_orders = ['210511BGWNB0FQ', '221019RJQ1CBAG', '230708E6R03S6G']
    
    try:
        # Load fact_orders to check unit-level granularity
        fact_orders_path = 'app/Transformed/fact_orders.csv'
        if not os.path.exists(fact_orders_path):
            print(f"❌ fact_orders.csv not found at {fact_orders_path}")
            return False
            
        fact_orders_df = pd.read_csv(fact_orders_path)
        print(f"✅ Loaded fact_orders: {len(fact_orders_df):,} records")
        
        # Load dim_order to check insurance premiums
        dim_order_path = 'app/Transformed/dim_order.csv'
        if not os.path.exists(dim_order_path):
            print(f"❌ dim_order.csv not found at {dim_order_path}")
            return False
            
        dim_order_df = pd.read_csv(dim_order_path)
        print(f"✅ Loaded dim_order: {len(dim_order_df):,} records")
        
        # Validate each sample order
        all_valid = True
        
        for order_sn in sample_orders:
            print(f"\n📋 Validating Order: {order_sn}")
            print("-" * 40)
            
            # Check in dim_order
            order_dim = dim_order_df[dim_order_df['platform_order_id'] == order_sn]
            if order_dim.empty:
                print(f"❌ Order {order_sn} not found in dim_order")
                all_valid = False
                continue
                
            order_record = order_dim.iloc[0]
            print(f"✅ Found in dim_order:")
            print(f"   orders_key: {order_record['orders_key']}")
            print(f"   price_total: ₱{order_record['price_total']}")
            print(f"   insurance_premium: ₱{order_record.get('insurance_premium_and_fees', 'N/A')}")
            print(f"   total_item_count: {order_record['total_item_count']}")
            
            # Check in fact_orders for unit-level granularity
            orders_key = order_record['orders_key']
            fact_records = fact_orders_df[fact_orders_df['orders_key'] == orders_key]
            
            if fact_records.empty:
                print(f"❌ No fact_orders records found for orders_key {orders_key}")
                all_valid = False
                continue
                
            print(f"✅ Found {len(fact_records)} fact_orders records:")
            
            # Validate unit-level granularity (item_quantity should be 1 for each record)
            unit_level_check = all(fact_records['item_quantity'] == 1)
            total_units = fact_records['item_quantity'].sum()
            total_revenue = fact_records['paid_price'].sum()
            
            print(f"   Unit-level granularity: {'✅' if unit_level_check else '❌'}")
            print(f"   Total units: {total_units}")
            print(f"   Total revenue: ₱{total_revenue:.2f}")
            print(f"   Records breakdown:")
            
            for idx, record in fact_records.iterrows():
                print(f"     - {record['order_item_key']}: qty={record['item_quantity']}, paid=₱{record['paid_price']:.2f}")
            
            if not unit_level_check:
                print(f"❌ Unit-level granularity validation failed for {order_sn}")
                all_valid = False
        
        # Summary validation
        print(f"\n📊 VALIDATION SUMMARY")
        print("=" * 40)
        
        # Check overall unit-level granularity
        total_records = len(fact_orders_df)
        unit_level_records = len(fact_orders_df[fact_orders_df['item_quantity'] == 1])
        unit_level_percentage = (unit_level_records / total_records) * 100
        
        print(f"Overall unit-level granularity:")
        print(f"   Total fact_orders records: {total_records:,}")
        print(f"   Records with qty=1: {unit_level_records:,}")
        print(f"   Unit-level percentage: {unit_level_percentage:.1f}%")
        
        # Check Shopee insurance premiums
        shopee_orders = dim_order_df[dim_order_df['platform_key'] == 2]
        shopee_with_insurance = shopee_orders[shopee_orders['insurance_premium_and_fees'].notna()]
        
        print(f"Insurance premium mapping:")
        print(f"   Shopee orders: {len(shopee_orders):,}")
        print(f"   With insurance premiums: {len(shopee_with_insurance):,}")
        print(f"   Coverage: {(len(shopee_with_insurance)/len(shopee_orders)*100):.1f}%")
        
        if all_valid and unit_level_percentage > 95:
            print(f"\n✅ ALL VALIDATIONS PASSED!")
            return True
        else:
            print(f"\n❌ Some validations failed!")
            return False
            
    except Exception as e:
        print(f"❌ Error during validation: {str(e)}")
        return False

if __name__ == "__main__":
    validate_sample_orders()