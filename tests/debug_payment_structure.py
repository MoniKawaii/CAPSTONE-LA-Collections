import json
import pandas as pd

# Let's check what the actual payment data looks like for those discrepant records
payment_file = r"c:\Users\alyss\Desktop\CAPSTONE-LA-Collections\app\Staging\shopee_paymentdetail_raw.json"

# Load the first few shopee orders to see actual payment structure
shopee_orders_file = r"c:\Users\alyss\Desktop\CAPSTONE-LA-Collections\app\Staging\shopee_orders_raw.json" 

try:
    with open(shopee_orders_file, 'r', encoding='utf-8') as f:
        shopee_orders = json.load(f)
    
    with open(payment_file, 'r', encoding='utf-8') as f:
        payments = json.load(f)
    
    print(f"📊 Loaded {len(shopee_orders)} orders and {len(payments)} payment records")
    
    # Find the first few orders to see their structure
    for i, order in enumerate(shopee_orders[:3]):
        order_id = order.get('order_sn', 'N/A')
        print(f"\n🔍 ORDER {i+1} - {order_id}:")
        
        # Find corresponding payment
        payment_record = None
        for payment in payments:
            if payment.get('order_id') == order_id:
                payment_record = payment
                break
        
        if payment_record:
            print(f"  Payment found: {payment_record.get('order_id')}")
            payment_details = payment_record.get('response', {}).get('payment_detail_info_list', [])
            print(f"  Payment details count: {len(payment_details)}")
            
            # Show first payment item structure
            if payment_details:
                item = payment_details[0]
                print(f"  First item structure:")
                print(f"    item_id: {item.get('item_id')}")
                print(f"    model_id: {item.get('model_id')}")
                print(f"    selling_price: {item.get('selling_price')}")
                print(f"    discounted_price: {item.get('discounted_price')}")
                print(f"    discount_from_coin: {item.get('discount_from_coin')}")
                print(f"    discount_from_voucher_shopee: {item.get('discount_from_voucher_shopee')}")
                print(f"    discount_from_voucher_seller: {item.get('discount_from_voucher_seller')}")
                print(f"    seller_discount: {item.get('seller_discount')}")
                print(f"    shopee_discount: {item.get('shopee_discount')}")
                
                # Calculate what the script should be doing
                selling = float(item.get('selling_price', 0))
                discounted = float(item.get('discounted_price', 0))
                coin = float(item.get('discount_from_coin', 0))
                voucher_shopee = float(item.get('discount_from_voucher_shopee', 0))
                voucher_seller = float(item.get('discount_from_voucher_seller', 0))
                seller_discount = float(item.get('seller_discount', 0))
                shopee_discount = float(item.get('shopee_discount', 0))
                
                print(f"\n  📊 PRICING ANALYSIS:")
                print(f"    Selling price: ₱{selling}")
                print(f"    Discounted price: ₱{discounted}")
                print(f"    Difference: ₱{selling - discounted}")
                
                print(f"\n    Discount components:")
                print(f"    - Coin discount: ₱{coin}")
                print(f"    - Voucher Shopee: ₱{voucher_shopee}")
                print(f"    - Voucher Seller: ₱{voucher_seller}")
                print(f"    - Seller discount: ₱{seller_discount}")
                print(f"    - Shopee discount: ₱{shopee_discount}")
                total_discounts = coin + voucher_shopee + voucher_seller + seller_discount + shopee_discount
                print(f"    - Total discounts: ₱{total_discounts}")
                
                print(f"\n    Formula check:")
                print(f"    selling_price - total_discounts = ₱{selling} - ₱{total_discounts} = ₱{selling - total_discounts}")
                print(f"    discounted_price from API = ₱{discounted}")
                print(f"    Match: {abs(selling - total_discounts - discounted) < 0.01}")
        
        print("-" * 80)
        
except Exception as e:
    print(f"❌ Error: {e}")