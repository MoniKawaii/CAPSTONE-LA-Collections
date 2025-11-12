import json
import pandas as pd

# Check specific order items that have excessive vouchers
problem_orders = ["S00009410", "S00009411", "S00019284", "S00020642"]

# Read the transformed data to get the original order info
df = pd.read_csv('app/Transformed/fact_orders.csv')
problem_records = df[df['order_item_key'].isin(problem_orders)]

print("🔍 PROBLEMATIC TRANSFORMED RECORDS:")
for _, row in problem_records.iterrows():
    print(f"Order: {row['order_item_key']}")
    print(f"  Original price: {row['original_unit_price']}")
    print(f"  Paid price: {row['paid_price']}")
    print(f"  Platform vouchers: {row['voucher_platform_amount']}")
    print(f"  Seller vouchers: {row['voucher_seller_amount']}")
    print(f"  Orders key: {row['orders_key']}")
    print()

# Now check the raw Shopee order data with proper encoding
try:
    with open('app/Staging/shopee_orders_raw.json', 'r', encoding='utf-8') as f:
        shopee_orders = json.load(f)
    
    print("🔍 LOOKING FOR MATCHING RAW ORDERS:")
    # Extract order numbers from the order_item_keys to find source orders
    for item_key in problem_orders:
        # Get the orders_key from transformed data
        orders_key = df[df['order_item_key'] == item_key]['orders_key'].iloc[0]
        
        # Find corresponding raw order
        found_order = None
        for order in shopee_orders:
            if f"S{order.get('order_sn', '')}" == orders_key or str(order.get('order_sn', '')) in orders_key:
                found_order = order
                break
        
        if found_order:
            print(f"\n📋 RAW ORDER for {item_key} (orders_key: {orders_key}):")
            print(f"  Order SN: {found_order.get('order_sn', 'N/A')}")
            print(f"  Order status: {found_order.get('order_status', 'N/A')}")
            
            # Check items in this order
            item_list = found_order.get('item_list', [])
            print(f"  Items count: {len(item_list)}")
            
            for i, item in enumerate(item_list):
                print(f"  Item {i+1}:")
                print(f"    Model original price: {item.get('model_original_price', 'N/A')}")
                print(f"    Model discounted price: {item.get('model_discounted_price', 'N/A')}")
                print(f"    Voucher absorbed by Shopee: {item.get('voucher_absorbed_by_shopee', 'N/A')}")
                print(f"    Voucher absorbed by seller: {item.get('voucher_absorbed_by_seller', 'N/A')}")
                print(f"    Quantity: {item.get('model_quantity_purchased', 'N/A')}")
        else:
            print(f"\n❌ No raw order found for {item_key}")

except UnicodeDecodeError:
    print("❌ Encoding issue with shopee_orders_raw.json, trying payment details instead...")
    
    # Check payment details for these orders
    try:
        with open('app/Staging/shopee_paymentdetail_raw.json', 'r', encoding='utf-8') as f:
            payment_details = json.load(f)
        
        print("\n🔍 CHECKING PAYMENT DETAILS:")
        for item_key in problem_orders[:2]:  # Check first 2
            orders_key = df[df['order_item_key'] == item_key]['orders_key'].iloc[0]
            
            # Extract order_sn from orders_key (format like "8230.2")
            order_sn = orders_key.split('.')[0] if '.' in str(orders_key) else str(orders_key)
            
            for payment in payment_details:
                if str(payment.get('order_sn', '')) == order_sn:
                    print(f"\n📋 PAYMENT DETAIL for {item_key}:")
                    print(f"  Order SN: {payment.get('order_sn', 'N/A')}")
                    print(f"  Item count: {len(payment.get('item_list', []))}")
                    
                    for i, payment_item in enumerate(payment.get('item_list', [])):
                        print(f"  Payment item {i+1}:")
                        print(f"    Original price: {payment_item.get('original_price', 'N/A')}")
                        print(f"    Discounted price: {payment_item.get('discounted_price', 'N/A')}")
                        print(f"    Discount from voucher Shopee: {payment_item.get('discount_from_voucher_shopee', 'N/A')}")
                        print(f"    Discount from coin: {payment_item.get('discount_from_coin', 'N/A')}")
                        print(f"    Discount from voucher seller: {payment_item.get('discount_from_voucher_seller', 'N/A')}")
                    break
    except Exception as e:
        print(f"❌ Error reading payment details: {e}")

print("\n🎯 KEY FINDING:")
print("All problematic records have paid_price = 0.0")
print("This suggests model_discounted_price was 0 in the fallback logic")
print("When discounted_price = 0, the voucher amounts still get added but paid_price stays 0")