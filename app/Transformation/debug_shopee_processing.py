#!/usr/bin/env python3

from harmonize_fact_orders import extract_order_items_from_shopee, load_dimension_lookups
import json
import pandas as pd

def debug_shopee_processing():
    print("🔍 Debugging Shopee order processing for early orders")
    
    # Load dimension lookups
    dim_lookups, variant_df = load_dimension_lookups()
    
    # Load first 10 raw orders
    with open('../Staging/shopee_orders_raw.json', 'r', encoding='utf-8') as f:
        all_orders = json.load(f)
    
    # Take just the first 10 orders for debugging
    test_orders = all_orders[:10]
    
    print(f"Testing with first {len(test_orders)} orders:")
    for i, order in enumerate(test_orders):
        order_sn = order.get('order_sn')
        status = order.get('order_status')
        item_count = len(order.get('item_list', []))
        orders_key = dim_lookups['order'].get(str(order_sn))
        print(f"  {i+1}. {order_sn}: Status={status}, Items={item_count}, Orders_key={orders_key}")
    
    # Load payment details
    try:
        with open('../Staging/shopee_payment_details_raw.json', 'r', encoding='utf-8') as f:
            payment_details = json.load(f)
        print(f"\n✅ Loaded {len(payment_details)} payment details")
    except Exception as e:
        print(f"⚠️ Could not load payment details: {e}")
        payment_details = []
    
    # Try processing these orders
    print(f"\n🔄 Processing {len(test_orders)} test orders...")
    try:
        result_df = extract_order_items_from_shopee(test_orders, payment_details, dim_lookups, variant_df)
        print(f"✅ Processing successful! Generated {len(result_df)} fact records")
        
        if len(result_df) > 0:
            print(f"First few results:")
            print(result_df[['orders_key', 'customer_key', 'product_key']].head())
        else:
            print("❌ No records generated!")
            
    except Exception as e:
        print(f"❌ Processing failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_shopee_processing()