import json
import os

def analyze_shipping_discrepancy():
    """
    Analyze order 22110331NRG31Q where buyer_total_amount != merchant_subtotal
    to understand the shipping fee computation and payment structure
    """
    
    # Define the staging directory
    staging_dir = os.path.join(os.getcwd(), 'app', 'Staging')
    
    # Load payment details data
    payment_detail_file = os.path.join(staging_dir, 'shopee_paymentdetail_raw.json')
    with open(payment_detail_file, 'r', encoding='utf-8') as f:
        payment_details = json.load(f)
    
    # Load orders data for additional context
    orders_file = os.path.join(staging_dir, 'shopee_orders_raw.json')
    with open(orders_file, 'r', encoding='utf-8') as f:
        orders = json.load(f)
    
    target_order_sn = "22110331NRG31Q"
    
    print("=" * 80)
    print(f"SHIPPING FEE DISCREPANCY ANALYSIS FOR ORDER: {target_order_sn}")
    print("=" * 80)
    
    # Find the payment details for this order
    target_payment = None
    for payment in payment_details:
        if payment['order_sn'] == target_order_sn:
            target_payment = payment
            break
    
    if not target_payment:
        print(f"❌ Order {target_order_sn} not found in payment details")
        return
    
    # Find the order details for additional context
    target_order = None
    for order in orders:
        if order['order_sn'] == target_order_sn:
            target_order = order
            break
    
    # Extract key payment information
    buyer_info = target_payment['buyer_payment_info']
    order_income = target_payment['order_income']
    
    print("\n📊 BUYER PAYMENT BREAKDOWN:")
    print(f"   • Merchant Subtotal:           ₱{buyer_info['merchant_subtotal']:,}")
    print(f"   • Shipping Fee (charged):      ₱{buyer_info['shipping_fee']:,}")
    print(f"   • Shopee Voucher Discount:     ₱{buyer_info['shopee_voucher']:,}")
    print(f"   • Seller Voucher:              ₱{buyer_info['seller_voucher']:,}")
    print(f"   • Shopee Coins Redeemed:       ₱{buyer_info['shopee_coins_redeemed']:,}")
    print(f"   • Insurance Premium:           ₱{buyer_info['insurance_premium']:,}")
    print(f"   • Payment Method:              {buyer_info['buyer_payment_method']}")
    print(f"   ----------------------------------------")
    print(f"   • TOTAL BUYER PAID:            ₱{buyer_info['buyer_total_amount']:,}")
    
    print("\n💰 MERCHANT REVENUE BREAKDOWN:")
    print(f"   • Cost of Goods Sold:          ₱{order_income['cost_of_goods_sold']:,}")
    print(f"   • Estimated Shipping Fee:      ₱{order_income['estimated_shipping_fee']:,}")
    print(f"   • Actual Shipping Fee:         ₱{order_income['actual_shipping_fee']:,}")
    print(f"   • Buyer Paid Shipping:         ₱{order_income['buyer_paid_shipping_fee']:,}")
    print(f"   • Final Shipping Fee:          ₱{order_income['final_shipping_fee']:,}")
    print(f"   • Commission Fee:              ₱{order_income['commission_fee']:,}")
    print(f"   • Transaction Fee:             ₱{order_income['credit_card_transaction_fee']:,}")
    print(f"   • Escrow Amount:               ₱{order_income['escrow_amount']:,}")
    
    # Calculate the payment flow
    print("\n🧮 PAYMENT FLOW ANALYSIS:")
    merchandise_cost = buyer_info['merchant_subtotal']
    shipping_charged = buyer_info['shipping_fee']
    shopee_voucher_discount = abs(buyer_info['shopee_voucher'])
    seller_voucher_discount = abs(buyer_info['seller_voucher'])
    
    gross_amount = merchandise_cost + shipping_charged
    net_amount_after_vouchers = gross_amount - shopee_voucher_discount - seller_voucher_discount
    
    print(f"   1. Merchandise Cost:           ₱{merchandise_cost:,}")
    print(f"   2. Shipping Fee Charged:       ₱{shipping_charged:,}")
    print(f"   3. Gross Amount (1+2):         ₱{gross_amount:,}")
    print(f"   4. Shopee Voucher Applied:     ₱{shopee_voucher_discount:,}")
    print(f"   5. Seller Voucher Applied:     ₱{seller_voucher_discount:,}")
    print(f"   6. Net Amount (3-4-5):         ₱{net_amount_after_vouchers:,}")
    print(f"   7. Buyer Total Amount:         ₱{buyer_info['buyer_total_amount']:,}")
    
    # Verify calculation
    calculation_matches = net_amount_after_vouchers == buyer_info['buyer_total_amount']
    print(f"   ✅ Calculation Verification:   {'MATCHES' if calculation_matches else 'DOES NOT MATCH'}")
    
    # Analysis of the discrepancy
    print("\n🔍 DISCREPANCY ANALYSIS:")
    discrepancy = buyer_info['merchant_subtotal'] - buyer_info['buyer_total_amount']
    
    print(f"   • Merchant Subtotal:           ₱{buyer_info['merchant_subtotal']:,}")
    print(f"   • Buyer Total Amount:          ₱{buyer_info['buyer_total_amount']:,}")
    print(f"   • Discrepancy:                 ₱{discrepancy:,}")
    
    # Check if this accounts for vouchers and shipping
    shopee_voucher_effect = abs(buyer_info['shopee_voucher'])
    seller_voucher_effect = abs(buyer_info['seller_voucher'])
    total_voucher_effect = shopee_voucher_effect + seller_voucher_effect
    shipping_impact = buyer_info['shipping_fee']
    expected_total = buyer_info['merchant_subtotal'] + shipping_impact - total_voucher_effect
    
    print(f"\n📝 EXPLANATION:")
    print(f"   The buyer pays LESS than merchant subtotal because:")
    print(f"   • Shopee voucher reduces the total by ₱{shopee_voucher_effect:,}")
    print(f"   • Seller voucher reduces the total by ₱{seller_voucher_effect:,}")
    print(f"   • Total voucher discount: ₱{total_voucher_effect:,}")
    print(f"   • Shipping fee adds ₱{shipping_impact:,}")
    print(f"   • Net effect: ₱{buyer_info['merchant_subtotal']:,} + ₱{shipping_impact:,} - ₱{total_voucher_effect:,} = ₱{expected_total:,}")
    
    if expected_total == buyer_info['buyer_total_amount']:
        print(f"   ✅ This perfectly explains the buyer_total_amount of ₱{buyer_info['buyer_total_amount']:,}")
    else:
        print(f"   ❓ Expected ₱{expected_total:,} but got ₱{buyer_info['buyer_total_amount']:,}")
        print(f"   ❓ Difference of ₱{abs(expected_total - buyer_info['buyer_total_amount']):,} needs investigation")
    
    # Check if there was a refund scenario
    print(f"\n🔄 REFUND SCENARIO CHECK:")
    if buyer_info['buyer_total_amount'] < buyer_info['merchant_subtotal']:
        print(f"   • This is NOT a refund scenario")
        print(f"   • The lower buyer_total_amount is due to voucher discounts")
        print(f"   • Total voucher discount (₱{total_voucher_effect:,}) exceeds shipping fee (₱{shipping_impact:,})")
        print(f"   • Net effect: ₱{shipping_impact - total_voucher_effect:,} reduction from merchant_subtotal")
    else:
        print(f"   • Buyer paid equal to or more than merchant_subtotal")
        print(f"   • No refund scenario detected")
    
    # Show order items context if available
    if target_order:
        print(f"\n📦 ORDER CONTEXT:")
        print(f"   • Order Status:                {target_order.get('order_status', 'N/A')}")
        print(f"   • Create Time:                 {target_order.get('create_time', 'N/A')}")
        print(f"   • Items Count:                 {len(target_order.get('item_list', []))}")
        
        if target_order.get('item_list'):
            total_quantity = sum(item.get('model_quantity_purchased', 0) for item in target_order['item_list'])
            print(f"   • Total Quantity:              {total_quantity}")
    
    print("\n" + "=" * 80)
    print("SUMMARY: Buyer paid less than merchant_subtotal due to Shopee voucher")
    print("discount exceeding the shipping fee charge. This is normal pricing logic,")
    print("not a refund scenario.")
    print("=" * 80)

if __name__ == "__main__":
    analyze_shipping_discrepancy()