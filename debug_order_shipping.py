#!/usr/bin/env python3
"""
Debug script to analyze shipping fee computation for order 22110331NRG31Q
"""

import json
import os

def analyze_shipping_order():
    order_sn = "22110331NRG31Q"
    
    # Load payment details
    payment_file = "app/Staging/shopee_paymentdetail_raw.json"
    orders_file = "app/Staging/shopee_orders_raw.json"
    
    payment_data = None
    orders_data = None
    
    print(f"🔍 Analyzing shipping computation for order: {order_sn}")
    print("=" * 60)
    
    # Find payment details
    if os.path.exists(payment_file):
        try:
            with open(payment_file, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    try:
                        line = line.strip()
                        if not line:
                            continue
                        data = json.loads(line)
                        if data.get('order_sn') == order_sn:
                            payment_data = data
                            print(f"✅ Found payment details at line {line_num}")
                            break
                    except json.JSONDecodeError:
                        continue
                    except Exception as e:
                        if line_num < 10:  # Only show first few errors
                            print(f"Error at line {line_num}: {e}")
                        continue
        except Exception as e:
            print(f"Error opening payment file: {e}")
            return
    
    # Find orders data
    if os.path.exists(orders_file):
        try:
            with open(orders_file, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    try:
                        line = line.strip()
                        if not line:
                            continue
                        data = json.loads(line)
                        if data.get('order_sn') == order_sn:
                            orders_data = data
                            print(f"✅ Found order details at line {line_num}")
                            break
                    except json.JSONDecodeError:
                        continue
                    except Exception as e:
                        continue
        except Exception as e:
            print(f"Error opening orders file: {e}")
    
    if not payment_data:
        print("❌ Payment details not found")
        return
    
    if not orders_data:
        print("❌ Order details not found")
        return
    
    print("\n📊 PAYMENT BREAKDOWN ANALYSIS:")
    print("-" * 40)
    
    # Extract key payment info
    buyer_info = payment_data.get('buyer_payment_info', {})
    order_income = payment_data.get('order_income', {})
    
    # Key amounts
    buyer_total = buyer_info.get('buyer_total_amount', 0)
    merchant_subtotal = buyer_info.get('merchant_subtotal', 0)
    shipping_fee = buyer_info.get('shipping_fee', 0)
    buyer_paid_shipping = order_income.get('buyer_paid_shipping_fee', 0)
    
    print(f"Buyer Total Amount: ₱{buyer_total}")
    print(f"Merchant Subtotal: ₱{merchant_subtotal}")
    print(f"Shipping Fee (buyer_info): ₱{shipping_fee}")
    print(f"Buyer Paid Shipping Fee: ₱{buyer_paid_shipping}")
    
    # Check for discounts and vouchers
    seller_voucher = buyer_info.get('seller_voucher', 0)
    shopee_voucher = buyer_info.get('shopee_voucher', 0)
    shopee_coins = buyer_info.get('shopee_coins_redeemed', 0)
    
    print(f"\n💰 DISCOUNTS & VOUCHERS:")
    print(f"Seller Voucher: ₱{seller_voucher}")
    print(f"Shopee Voucher: ₱{shopee_voucher}")
    print(f"Shopee Coins Redeemed: ₱{shopee_coins}")
    
    # Calculate expected total
    expected_total = merchant_subtotal + shipping_fee - seller_voucher - shopee_voucher - shopee_coins
    difference = buyer_total - expected_total
    
    print(f"\n🧮 CALCULATION CHECK:")
    print(f"Expected Total: {merchant_subtotal} + {shipping_fee} - {seller_voucher} - {shopee_voucher} - {shopee_coins} = ₱{expected_total}")
    print(f"Actual Buyer Total: ₱{buyer_total}")
    print(f"Difference: ₱{difference}")
    
    if difference != 0:
        print(f"⚠️  DISCREPANCY DETECTED: ₱{difference}")
        
        # Check for other fees or adjustments
        print(f"\n🔍 CHECKING OTHER FEES/ADJUSTMENTS:")
        other_fees = [
            ('insurance_premium', buyer_info.get('insurance_premium', 0)),
            ('buyer_service_fee', buyer_info.get('buyer_service_fee', 0)),
            ('buyer_tax_amount', buyer_info.get('buyer_tax_amount', 0)),
            ('credit_card_promotion', buyer_info.get('credit_card_promotion', 0)),
            ('bulky_handling_fee', buyer_info.get('bulky_handling_fee', 0)),
            ('trade_in_discount', buyer_info.get('trade_in_discount', 0)),
        ]
        
        for fee_name, fee_value in other_fees:
            if fee_value != 0:
                print(f"  {fee_name}: ₱{fee_value}")
    else:
        print("✅ Calculation matches!")
    
    # Check order income details
    print(f"\n📋 ORDER INCOME DETAILS:")
    print(f"Order Original Price: ₱{order_income.get('order_original_price', 0)}")
    print(f"Order Discounted Price: ₱{order_income.get('order_discounted_price', 0)}")
    print(f"Order Seller Discount: ₱{order_income.get('order_seller_discount', 0)}")
    print(f"Seller Discount: ₱{order_income.get('seller_discount', 0)}")
    print(f"Shopee Discount: ₱{order_income.get('shopee_discount', 0)}")
    
    # From orders_raw
    print(f"\n📦 ORDER DETAILS (from orders_raw):")
    print(f"Total Amount: ₱{orders_data.get('total_amount', 0)}")
    print(f"Actual Shipping Fee: ₱{orders_data.get('actual_shipping_fee', 0)}")
    print(f"Estimated Shipping Fee: ₱{orders_data.get('estimated_shipping_fee', 0)}")
    
    # Check if there are specific shipping discounts
    shipping_discount = order_income.get('seller_shipping_discount', 0)
    shopee_shipping_rebate = order_income.get('shopee_shipping_rebate', 0)
    
    if shipping_discount != 0 or shopee_shipping_rebate != 0:
        print(f"\n🚚 SHIPPING DISCOUNTS:")
        print(f"Seller Shipping Discount: ₱{shipping_discount}")
        print(f"Shopee Shipping Rebate: ₱{shopee_shipping_rebate}")

if __name__ == "__main__":
    analyze_shipping_order()