"""
Debug script to check Lazada price extraction
"""
import json
import os

# Load a few Lazada orders
staging_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'Staging')
orders_file = os.path.join(staging_dir, 'lazada_orders_raw.json')

with open(orders_file, 'r', encoding='utf-8') as f:
    orders_data = json.load(f)

print(f"Loaded {len(orders_data)} Lazada orders")

# Check first 3 orders
for i in range(min(3, len(orders_data))):
    order = orders_data[i]
    order_id = order.get('order_id', 'unknown')
    
    print(f"\n=== Order {i+1}: {order_id} ===")
    
    # Check what price-related fields exist
    price_fields = {}
    for key, value in order.items():
        if 'price' in key.lower() or 'amount' in key.lower() or 'total' in key.lower():
            price_fields[key] = value
    
    print(f"All price-related fields: {price_fields}")
    
    # Test the price extraction logic
    price_total = None
    price_sources = ['price', 'item_price', 'total_amount']
    
    print(f"Testing price sources: {price_sources}")
    
    for price_field in price_sources:
        print(f"  Checking field '{price_field}'...")
        if price_field in order and order[price_field] is not None:
            print(f"    Field exists with value: {repr(order[price_field])}")
            try:
                price_value = order[price_field]
                
                # Handle string prices (e.g., "350.00")
                if isinstance(price_value, str):
                    price_value = price_value.strip()
                    print(f"    String value after strip: '{price_value}'")
                    if price_value and price_value != '0.00':
                        price_total = float(price_value)
                        print(f"    ✅ Successfully converted to float: {price_total}")
                        break
                    else:
                        print(f"    ❌ String is empty or '0.00'")
                
                # Handle numeric prices
                elif isinstance(price_value, (int, float)):
                    print(f"    Numeric value: {price_value}")
                    if price_value > 0:
                        price_total = float(price_value)
                        print(f"    ✅ Successfully converted to float: {price_total}")
                        break
                    else:
                        print(f"    ❌ Numeric value is 0 or negative")
                        
            except (ValueError, TypeError) as e:
                print(f"    ❌ Error converting: {e}")
                continue
        else:
            print(f"    Field does not exist or is None")
    
    print(f"  Final price_total: {price_total}")