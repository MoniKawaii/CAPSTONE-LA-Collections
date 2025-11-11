"""
Debug script to trace the harmonization mapping loop
"""
import json
import os
import sys

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import LAZADA_TO_UNIFIED_MAPPING

print("=== LAZADA_TO_UNIFIED_MAPPING contents ===")
for lazada_field, unified_field in LAZADA_TO_UNIFIED_MAPPING.items():
    if unified_field == 'price_total':
        print(f"✅ FOUND PRICE MAPPING: '{lazada_field}' → '{unified_field}'")
    
print(f"\nTotal mappings: {len(LAZADA_TO_UNIFIED_MAPPING)}")

# Load one Lazada order
staging_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'Staging')
orders_file = os.path.join(staging_dir, 'lazada_orders_raw.json')

with open(orders_file, 'r', encoding='utf-8') as f:
    orders_data = json.load(f)

order_data = orders_data[0]
order_id = order_data.get('order_id', 'unknown')

print(f"\n=== Processing Order: {order_id} ===")

# Simulate the exact harmonization loop
harmonized_record = {}

for lazada_field, unified_field in LAZADA_TO_UNIFIED_MAPPING.items():
    print(f"Checking mapping: '{lazada_field}' → '{unified_field}'")
    
    # Check the same condition as in the script
    if unified_field in ['orders_key', 'platform_order_id', 'order_status', 'order_date', 
                        'updated_at', 'price_total', 'total_item_count', 'payment_method', 
                        'shipping_city', 'platform_key']:
        
        print(f"  ✅ Field '{unified_field}' is in allowed list")
        
        if lazada_field == 'price':
            print(f"  🎯 PRICE FIELD DETECTED!")
            print(f"  Order data has price field: {'price' in order_data}")
            print(f"  Price value: {repr(order_data.get('price'))}")
            
            # Copy the exact logic from the script
            price_total = None
            price_sources = ['price', 'item_price', 'total_amount']
            
            for price_field in price_sources:
                print(f"    Trying price field: {price_field}")
                if price_field in order_data and order_data[price_field] is not None:
                    print(f"      Field exists with value: {repr(order_data[price_field])}")
                    try:
                        price_value = order_data[price_field]
                        
                        # Handle string prices (e.g., "350.00")
                        if isinstance(price_value, str):
                            price_value = price_value.strip()
                            if price_value and price_value != '0.00':
                                price_total = float(price_value)
                                print(f"      ✅ SUCCESS: price_total = {price_total}")
                                break
                        
                        # Handle numeric prices
                        elif isinstance(price_value, (int, float)):
                            if price_value > 0:
                                price_total = float(price_value)
                                print(f"      ✅ SUCCESS: price_total = {price_total}")
                                break
                                
                    except (ValueError, TypeError) as e:
                        print(f"      ❌ Error: {e}")
                        continue
                else:
                    print(f"      Field '{price_field}' not found or None")
            
            harmonized_record['price_total'] = price_total
            print(f"  Final price_total in record: {price_total}")
            break
    else:
        print(f"  ⚠️  Field '{unified_field}' NOT in allowed list")

print(f"\nFinal harmonized_record price_total: {harmonized_record.get('price_total')}")