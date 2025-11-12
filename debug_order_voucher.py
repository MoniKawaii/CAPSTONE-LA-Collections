import json
import os

def debug_order_voucher():
    """Debug voucher calculation for order 22110331NRG31Q"""
    
    # Define the staging directory
    staging_dir = os.path.join(os.getcwd(), 'app', 'Staging')
    
    # Load payment details data
    payment_detail_file = os.path.join(staging_dir, 'shopee_paymentdetail_raw.json')
    with open(payment_detail_file, 'r', encoding='utf-8') as f:
        payment_details = json.load(f)
    
    # Load orders data
    orders_file = os.path.join(staging_dir, 'shopee_orders_raw.json')
    with open(orders_file, 'r', encoding='utf-8') as f:
        orders = json.load(f)
    
    target_order_sn = "22110331NRG31Q"
    
    print("=" * 80)
    print(f"VOUCHER DEBUG FOR ORDER: {target_order_sn}")
    print("=" * 80)
    
    # Find payment details
    target_payment = None
    for payment in payment_details:
        if payment['order_sn'] == target_order_sn:
            target_payment = payment
            break
    
    if not target_payment:
        print(f"❌ Payment details not found for {target_order_sn}")
        return
    
    # Find order details
    target_order = None
    for order in orders:
        if order['order_sn'] == target_order_sn:
            target_order = order
            break
    
    if not target_order:
        print(f"❌ Order details not found for {target_order_sn}")
        return
    
    print("📋 PAYMENT DETAILS VOUCHER INFO:")
    buyer_info = target_payment['buyer_payment_info']
    print(f"   • seller_voucher: {buyer_info.get('seller_voucher', 0)}")
    print(f"   • shopee_voucher: {buyer_info.get('shopee_voucher', 0)}")
    
    print("\n📋 ORDER ITEM DETAILS:")
    for i, item in enumerate(target_order.get('item_list', [])):
        print(f"   Item {i+1}:")
        print(f"     • model_id: {item.get('model_id')}")
        print(f"     • model_name: {item.get('model_name')}")
        print(f"     • model_quantity_purchased: {item.get('model_quantity_purchased')}")
        print(f"     • model_original_price: {item.get('model_original_price')}")
        print(f"     • model_discounted_price: {item.get('model_discounted_price')}")
    
    print("\n📋 PAYMENT ITEMS (Discount Details):")
    order_income = target_payment.get('order_income', {})
    if 'item_list' in order_income:
        for i, item in enumerate(order_income['item_list']):
            print(f"   Payment Item {i+1}:")
            print(f"     • model_id: {item.get('model_id')}")
            print(f"     • seller_discount: {item.get('seller_discount', 0)}")
            print(f"     • platform_discount: {item.get('platform_discount', 0)}")
            print(f"     • quantity: {item.get('quantity', 0)}")
            print(f"     • model_original_price: {item.get('model_original_price', 0)}")
            print(f"     • model_discounted_price: {item.get('model_discounted_price', 0)}")
    else:
        print("   ❌ No item_list found in order_income")
    
    # Calculate what should be the correct voucher amounts
    print(f"\n🧮 VOUCHER CALCULATION:")
    total_seller_voucher = abs(float(buyer_info.get('seller_voucher', 0)))
    total_platform_voucher = abs(float(buyer_info.get('shopee_voucher', 0)))
    total_items = len(target_order.get('item_list', []))
    total_units = sum(item.get('model_quantity_purchased', 0) for item in target_order.get('item_list', []))
    
    print(f"   • Total seller voucher: ₱{total_seller_voucher}")
    print(f"   • Total platform voucher: ₱{total_platform_voucher}")
    print(f"   • Total items: {total_items}")
    print(f"   • Total units: {total_units}")
    
    if total_units > 0:
        seller_voucher_per_unit = total_seller_voucher / total_units
        platform_voucher_per_unit = total_platform_voucher / total_units
        print(f"   • Seller voucher per unit: ₱{seller_voucher_per_unit:.2f}")
        print(f"   • Platform voucher per unit: ₱{platform_voucher_per_unit:.2f}")
        
        # Calculate expected paid price
        for item in target_order.get('item_list', []):
            original_price = float(item.get('model_original_price', 0))
            expected_paid_price = original_price - seller_voucher_per_unit - platform_voucher_per_unit
            print(f"   • Item {item.get('model_name', 'Unknown')}:")
            print(f"     - Original price: ₱{original_price}")
            print(f"     - Expected paid price: ₱{expected_paid_price:.2f}")

if __name__ == "__main__":
    debug_order_voucher()