#!/usr/bin/env python3

import json
import pandas as pd

def debug_missing_early_orders():
    print("🔍 Debugging missing early Shopee orders (1.2-24.2)")
    
    # Load raw Shopee orders
    try:
        with open('../Staging/shopee_orders_raw.json', 'r') as f:
            raw_orders = json.load(f)
        print(f"✅ Loaded {len(raw_orders)} raw Shopee orders")
    except Exception as e:
        print(f"❌ Error loading raw orders: {e}")
        return
    
    # Check for the specific missing orders
    missing_order_ids = [
        '2009196MH5Q37C', '201004EJ2XU2MN', '201005H6A997Y3', 
        '201006M8N0SE72', '201007NM38VY8N', '201007NM9D880Q'
    ]
    
    print(f"\n🔍 Searching for {len(missing_order_ids)} missing orders in raw data...")
    
    found_orders = []
    for order in raw_orders:
        order_sn = str(order.get('order_sn', ''))
        if order_sn in missing_order_ids:
            found_orders.append({
                'order_sn': order_sn,
                'order_status': order.get('order_status'),
                'total_amount': order.get('total_amount'),
                'item_count': len(order.get('item_list', [])),
                'has_items': bool(order.get('item_list'))
            })
    
    print(f"Found {len(found_orders)} of {len(missing_order_ids)} missing orders in raw data:")
    for order in found_orders:
        print(f"  ✅ {order['order_sn']}: Status={order['order_status']}, Amount={order['total_amount']}, Items={order['item_count']}")
    
    # Show first few raw orders for comparison
    print(f"\n📋 First 5 raw orders for reference:")
    for i, order in enumerate(raw_orders[:5]):
        order_sn = str(order.get('order_sn', ''))
        status = order.get('order_status')
        item_count = len(order.get('item_list', []))
        print(f"  {i+1}. {order_sn}: Status={status}, Items={item_count}")
    
    # Check if orders without item_list exist
    orders_without_items = [order for order in raw_orders if not order.get('item_list')]
    print(f"\n⚠️ Orders without item_list: {len(orders_without_items)}")
    
    if orders_without_items:
        print("Sample orders without items:")
        for order in orders_without_items[:3]:
            print(f"  - {order.get('order_sn')}: Status={order.get('order_status')}")

if __name__ == "__main__":
    debug_missing_early_orders()