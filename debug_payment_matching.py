#!/usr/bin/env python3

import json

def debug_payment_matching():
    """Debug the payment item matching logic for order 22110331NRG31Q"""
    
    print("🔍 Debugging payment item matching for order 22110331NRG31Q...")
    
    # Load raw data
    with open('app/Staging/shopee_multiple_order_items_raw.json', 'r') as f:
        order_items_data = json.load(f)
    
    with open('app/Staging/shopee_paymentdetail_raw.json', 'r') as f:
        payment_details_data = json.load(f)
    
    # Find order items for 22110331NRG31Q
    target_items = []
    for item in order_items_data:
        if str(item.get('order_sn', '')) == '22110331NRG31Q':
            target_items.append(item)
    
    print(f"✓ Found {len(target_items)} order items for 22110331NRG31Q")
    
    # Find payment details
    target_payment = None
    for payment in payment_details_data:
        if payment.get('order_sn') == '22110331NRG31Q':
            target_payment = payment
            break
    
    if not target_payment:
        print("❌ No payment details found")
        return
        
    print("✓ Found payment details")
    
    order_income = target_payment.get('order_income', {})
    payment_items = order_income.get('items', [])
    
    print(f"✓ Payment has {len(payment_items)} items in order_income.items")
    
    # Check matching logic for each order item
    print("\n📊 MATCHING ANALYSIS:")
    print("=" * 60)
    
    for i, item in enumerate(target_items):
        item_id = item.get('item_id', '')
        model_id = item.get('model_id', '')
        quantity = item.get('model_quantity_purchased', 1)
        unit_price = item.get('model_original_price', 0)
        
        print(f"Order Item {i+1}:")
        print(f"  item_id: {item_id}")
        print(f"  model_id: {model_id}")
        print(f"  quantity: {quantity}")
        print(f"  model_original_price: ₱{unit_price}")
        
        # Create matching key as done in harmonization
        current_item_key = f"{item_id}_{model_id}"
        print(f"  matching_key: {current_item_key}")
        
        # Look for matching payment item
        matching_payment_item = None
        for p in payment_items:
            payment_key = f"{p.get('item_id', '')}_{p.get('model_id', '')}"
            if payment_key == current_item_key:
                matching_payment_item = p
                break
        
        if matching_payment_item:
            print("  ✅ FOUND matching payment item:")
            print(f"     discount_from_voucher_seller: ₱{matching_payment_item.get('discount_from_voucher_seller', 0)}")
            print(f"     discount_from_voucher_shopee: ₱{matching_payment_item.get('discount_from_voucher_shopee', 0)}")
            print(f"     discounted_price: ₱{matching_payment_item.get('discounted_price', 0)}")
        else:
            print("  ❌ NO matching payment item found")
            print("     Will use FALLBACK logic with order-level vouchers")
        print()
    
    # Show order-level vouchers  
    print("📊 ORDER-LEVEL VOUCHERS:")
    print(f"  voucher_from_seller: ₱{order_income.get('voucher_from_seller', 0)}")
    print(f"  voucher_from_shopee: ₱{order_income.get('voucher_from_shopee', 0)}")
    
    # Show total units calculation
    total_units = sum(item.get('model_quantity_purchased', 1) for item in target_items)
    seller_voucher_per_unit = float(order_income.get('voucher_from_seller', 0)) / total_units if total_units > 0 else 0
    
    print(f"\n🧮 FALLBACK CALCULATION:")
    print(f"  Total units in order: {total_units}")
    print(f"  Order seller voucher: ₱{order_income.get('voucher_from_seller', 0)}")
    print(f"  Expected voucher per unit: ₱{seller_voucher_per_unit:.2f}")
    print(f"  Expected paid price: ₱{350 - seller_voucher_per_unit:.2f}")

if __name__ == "__main__":
    debug_payment_matching()