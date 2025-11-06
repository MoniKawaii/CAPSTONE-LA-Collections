#!/usr/bin/env python3

import json

def check_missing_product():
    try:
        with open('../Staging/shopee_products_raw.json', 'r', encoding='utf-8') as f:
            products = json.load(f)
        print(f'Total raw Shopee products: {len(products)}')
        
        missing_id = 5451694941
        found_product = None
        for product in products:
            if product.get('item_id') == missing_id:
                found_product = product
                break
        
        if found_product:
            print(f'✅ Found product {missing_id} in raw data:')
            print(f'  - Name: {found_product.get("item_name")}')
            print(f'  - Status: {found_product.get("item_status")}')
            print(f'  - SKU: {found_product.get("item_sku")}')
        else:
            print(f'❌ Product {missing_id} not found in raw Shopee products')
            print('Sample product IDs:')
            sample_ids = [p.get('item_id') for p in products[:10]]
            print(sample_ids)
            
    except Exception as e:
        print(f'Error: {e}')

if __name__ == "__main__":
    check_missing_product()