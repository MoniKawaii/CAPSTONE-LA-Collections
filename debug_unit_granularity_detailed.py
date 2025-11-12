"""
DEBUG FILE: Unit-Level Granularity Analysis
This file contains all the debugging logic extracted from harmonize_fact_orders.py
to allow the main harmonization to run at full speed.
"""

def debug_unit_granularity():
    """Debug unit-level granularity implementation for specific orders"""
    import json
    
    # Debug specific orders
    debug_orders = ['210511BGWNB0FQ', '221019RJQ1CBAG', '230708E6R03S6G']
    
    print("🔍 DEBUGGING UNIT-LEVEL GRANULARITY IMPLEMENTATION")
    print("=" * 60)
    
    # Load Shopee data
    with open('app/Staging/shopee_multiple_order_items_raw.json', 'r') as f:
        shopee_data = json.load(f)
    
    with open('app/Staging/shopee_paymentdetail_raw.json', 'r') as f:
        payment_data = json.load(f)
    
    # Create payment lookup
    payment_lookup = {}
    for payment in payment_data:
        order_sn = payment.get('order_sn', '')
        if order_sn:
            payment_lookup[str(order_sn)] = payment
    
    # Analyze each debug order
    for order_sn in debug_orders:
        print(f"\n🔍 ANALYZING ORDER: {order_sn}")
        print("-" * 40)
        
        # Find order items
        order_items = [item for item in shopee_data if str(item.get('order_sn', '')) == order_sn]
        
        print(f"📊 Found {len(order_items)} items for order {order_sn}")
        
        total_units_expected = 0
        for i, item in enumerate(order_items):
            item_id = item.get('item_id', 'N/A')
            model_id = item.get('model_id', 'N/A')
            quantity = item.get('model_quantity_purchased', 1)
            model_price = item.get('model_original_price', 0)
            
            total_units_expected += quantity
            
            print(f"  Item {i+1}: ID={item_id}, Model={model_id}, Qty={quantity}, Price={model_price}")
            print(f"    → Should create {quantity} individual records with qty=1 each")
        
        print(f"📈 TOTAL UNITS EXPECTED: {total_units_expected} individual records")
        
        # Check payment details
        payment_info = payment_lookup.get(order_sn)
        if payment_info:
            buyer_payment = payment_info.get('buyer_payment_info', {})
            discounted_price = buyer_payment.get('discounted_price', 0)
            platform_voucher = float(buyer_payment.get('shopee_discount', 0))
            print(f"💰 Payment Info: discounted_price={discounted_price}, platform_voucher={platform_voucher}")
        else:
            print("⚠️ No payment details found for this order")

def debug_specific_order_processing():
    """Run the actual harmonization logic for debug orders to see unit-level output"""
    from app.Transformation.harmonize_fact_orders import extract_order_items_from_shopee_multiple_items
    from app.Transformation.harmonize_fact_orders import load_shopee_orders_raw, load_shopee_payment_details
    
    print("\n🔍 TESTING UNIT-LEVEL GRANULARITY IN HARMONIZATION FUNCTION")
    print("=" * 70)
    
    # Load data
    shopee_data = load_shopee_orders_raw()
    payment_data = load_shopee_payment_details()
    
    # Filter for debug orders only
    debug_orders = ['210511BGWNB0FG', '221019RJQ1CBAG', '230708E6R03S6G']
    filtered_data = [item for item in shopee_data if str(item.get('order_sn', '')) in debug_orders]
    
    print(f"📊 Processing {len(filtered_data)} items from debug orders")
    
    # Process with actual function
    result_records = extract_order_items_from_shopee_multiple_items(
        filtered_data, payment_data, enable_debug=True
    )
    
    print(f"✅ Generated {len(result_records)} total records")
    
    # Analyze results by order
    for order_sn in debug_orders:
        order_records = [r for r in result_records if r.get('order_sn') == order_sn]
        print(f"\n📋 Order {order_sn}: {len(order_records)} records")
        
        for i, record in enumerate(order_records):
            qty = record.get('item_quantity', 0)
            paid_price = record.get('paid_price', 0)
            orig_price = record.get('original_unit_price', 0)
            print(f"  Record {i+1}: qty={qty}, paid_price={paid_price:.2f}, orig_price={orig_price:.2f}")
        
        # Verify all quantities are 1
        qty_1_count = sum(1 for r in order_records if r.get('item_quantity') == 1)
        if qty_1_count == len(order_records):
            print(f"  ✅ Unit-level granularity: ALL {len(order_records)} records have qty=1")
        else:
            print(f"  ❌ Unit-level granularity: Only {qty_1_count}/{len(order_records)} records have qty=1")

if __name__ == "__main__":
    debug_unit_granularity()
    debug_specific_order_processing()