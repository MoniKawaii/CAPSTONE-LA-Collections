"""
Test Fact Orders Harmonization with Payment Details
==================================================

Simple test to verify the payment details integration is working correctly
"""

import sys
import os
import json

# Add the parent directory to sys.path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Test loading payment details
def test_payment_details_loading():
    """Test if payment details can be loaded successfully"""
    try:
        from harmonize_fact_orders import load_shopee_payment_details_raw
        
        print("🧪 Testing payment details loading...")
        payment_data = load_shopee_payment_details_raw()
        
        if payment_data:
            print(f"✅ Successfully loaded {len(payment_data)} payment records")
            
            # Show sample payment detail structure
            if len(payment_data) > 0:
                sample = payment_data[0]
                print("\n📋 Sample payment detail structure:")
                print(f"   - order_sn: {sample.get('order_sn', 'N/A')}")
                print(f"   - buyer_user_name: {sample.get('buyer_user_name', 'N/A')}")
                
                order_income = sample.get('order_income', {})
                items = order_income.get('items', [])
                print(f"   - order_income.items count: {len(items)}")
                
                if items:
                    item = items[0]
                    print(f"   - Sample item structure:")
                    print(f"     * item_id: {item.get('item_id', 'N/A')}")
                    print(f"     * model_id: {item.get('model_id', 'N/A')}")
                    print(f"     * selling_price: {item.get('selling_price', 'N/A')}")
                    print(f"     * discounted_price: {item.get('discounted_price', 'N/A')}")
                    print(f"     * discount_from_voucher_shopee: {item.get('discount_from_voucher_shopee', 'N/A')}")
                    print(f"     * discount_from_voucher_seller: {item.get('discount_from_voucher_seller', 'N/A')}")
                
                print(f"   - actual_shipping_fee: {order_income.get('actual_shipping_fee', 'N/A')}")
                
            return True
        else:
            print("❌ No payment data loaded")
            return False
            
    except Exception as e:
        print(f"❌ Error testing payment details: {e}")
        return False

def test_small_fact_orders():
    """Test fact orders harmonization with a small sample"""
    try:
        from harmonize_fact_orders import (
            load_shopee_orders_raw, 
            load_shopee_payment_details_raw,
            load_dim_order,
            load_dim_customer, 
            load_dim_product,
            load_dim_product_variant,
            extract_order_items_from_shopee
        )
        
        print("\n🧪 Testing small fact orders harmonization...")
        
        # Load data
        orders_data = load_shopee_orders_raw()[:5]  # Only first 5 orders
        payment_data = load_shopee_payment_details_raw()
        dim_order_df = load_dim_order()
        dim_customer_df = load_dim_customer()
        dim_product_df = load_dim_product()
        dim_variant_df = load_dim_product_variant()
        
        print(f"📊 Test data loaded:")
        print(f"   - Orders: {len(orders_data)}")
        print(f"   - Payment details: {len(payment_data)}")
        print(f"   - Dim orders: {len(dim_order_df)}")
        print(f"   - Dim customers: {len(dim_customer_df)}")
        print(f"   - Dim products: {len(dim_product_df)}")
        print(f"   - Dim variants: {len(dim_variant_df)}")
        
        if orders_data and payment_data:
            # Test extraction
            fact_df = extract_order_items_from_shopee(
                orders_data, payment_data, dim_order_df, dim_customer_df, 
                dim_product_df, dim_variant_df, 1
            )
            
            print(f"✅ Generated {len(fact_df)} fact order records")
            
            if not fact_df.empty:
                print("\n📋 Sample fact order record:")
                sample = fact_df.iloc[0]
                for col in fact_df.columns:
                    print(f"   - {col}: {sample[col]}")
                    
                print(f"\n💰 Pricing columns summary:")
                print(f"   - paid_price range: {fact_df['paid_price'].min():.2f} - {fact_df['paid_price'].max():.2f}")
                print(f"   - original_unit_price range: {fact_df['original_unit_price'].min():.2f} - {fact_df['original_unit_price'].max():.2f}")
                print(f"   - voucher_platform_amount range: {fact_df['voucher_platform_amount'].min():.2f} - {fact_df['voucher_platform_amount'].max():.2f}")
                print(f"   - voucher_seller_amount range: {fact_df['voucher_seller_amount'].min():.2f} - {fact_df['voucher_seller_amount'].max():.2f}")
            
            return True
        else:
            print("❌ Missing required data for test")
            return False
            
    except Exception as e:
        print(f"❌ Error testing fact orders: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🚀 Testing Payment Details Integration\n")
    
    # Test 1: Payment details loading
    test1_passed = test_payment_details_loading()
    
    # Test 2: Small fact orders harmonization
    test2_passed = test_small_fact_orders()
    
    print(f"\n🎯 Test Results:")
    print(f"   Payment Details Loading: {'✅ PASSED' if test1_passed else '❌ FAILED'}")
    print(f"   Fact Orders Harmonization: {'✅ PASSED' if test2_passed else '❌ FAILED'}")
    
    if test1_passed and test2_passed:
        print("\n🎉 All tests passed! Payment details integration is working correctly.")
    else:
        print("\n⚠️ Some tests failed. Please check the implementation.")