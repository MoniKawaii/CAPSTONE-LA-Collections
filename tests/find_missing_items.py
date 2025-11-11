#!/usr/bin/env python3
"""
Find Missing Item IDs Across All Raw Files
==========================================

This script identifies item_ids that exist in raw data but are missing
from dim_product and dim_product_variant tables.
"""

import json
import pandas as pd
import os

def analyze_missing_items():
    """Find all missing item_ids from raw data"""
    print("📋 Analyzing item_ids across all raw files...")
    
    # Load existing dimension tables
    try:
        dim_product = pd.read_csv('app/Transformed/dim_product.csv')
        dim_variant = pd.read_csv('app/Transformed/dim_product_variant.csv')
        
        existing_products = set(dim_product['product_item_id'].astype(str))
        existing_variants = set(dim_variant['platform_sku_id'].astype(str))
        
        print(f"✅ Existing products: {len(existing_products)}")
        print(f"✅ Existing variants: {len(existing_variants)}")
        
    except Exception as e:
        print(f"❌ Error loading dimension tables: {e}")
        return
    
    # Collect all item_ids from raw files
    all_items = {
        'lazada_products': set(),
        'lazada_order_items': set(),
        'shopee_products': set(),
        'shopee_order_items': set()
    }
    
    # Lazada Products
    try:
        with open('app/Staging/lazada_products_raw.json', 'r', encoding='utf-8') as f:
            products = json.load(f)
        for product in products:
            if 'item_id' in product:
                all_items['lazada_products'].add(str(product['item_id']))
        print(f"📊 Lazada products: {len(all_items['lazada_products'])}")
    except Exception as e:
        print(f"⚠️ Error reading lazada_products_raw.json: {e}")
    
    # Lazada Order Items
    try:
        with open('app/Staging/lazada_multiple_order_items_raw.json', 'r', encoding='utf-8') as f:
            items = json.load(f)
        for item in items:
            if 'product_id' in item:
                all_items['lazada_order_items'].add(str(item['product_id']))
            if 'sku' in item:
                all_items['lazada_order_items'].add(str(item['sku']))
        print(f"📊 Lazada order items: {len(all_items['lazada_order_items'])}")
    except Exception as e:
        print(f"⚠️ Error reading lazada_multiple_order_items_raw.json: {e}")
    
    # Shopee Products  
    try:
        with open('app/Staging/shopee_products_raw.json', 'r', encoding='utf-8') as f:
            products = json.load(f)
        for product in products:
            if 'item_id' in product:
                all_items['shopee_products'].add(str(product['item_id']))
        print(f"📊 Shopee products: {len(all_items['shopee_products'])}")
    except Exception as e:
        print(f"⚠️ Error reading shopee_products_raw.json: {e}")
    
    # Shopee Orders (for item_ids in orders)
    try:
        with open('app/Staging/shopee_orders_raw.json', 'r', encoding='utf-8') as f:
            orders = json.load(f)
        for order in orders:
            if 'item_list' in order:
                for item in order['item_list']:
                    if 'item_id' in item:
                        all_items['shopee_order_items'].add(str(item['item_id']))
                    if 'model_id' in item:
                        all_items['shopee_order_items'].add(str(item['model_id']))
        print(f"📊 Shopee order items: {len(all_items['shopee_order_items'])}")
    except Exception as e:
        print(f"⚠️ Error reading shopee_orders_raw.json: {e}")
    
    # Find missing items
    print(f"\n🔍 MISSING ITEM ANALYSIS:")
    
    # Combine all found items
    all_lazada_items = all_items['lazada_products'] | all_items['lazada_order_items']
    all_shopee_items = all_items['shopee_products'] | all_items['shopee_order_items']
    
    # Find missing items
    missing_lazada = all_lazada_items - existing_products - existing_variants
    missing_shopee = all_shopee_items - existing_products - existing_variants
    
    print(f"🔍 Missing Lazada items: {len(missing_lazada)}")
    if missing_lazada:
        print(f"   Sample missing Lazada items: {list(missing_lazada)[:10]}")
    
    print(f"🔍 Missing Shopee items: {len(missing_shopee)}")
    if missing_shopee:
        print(f"   Sample missing Shopee items: {list(missing_shopee)[:10]}")
    
    return {
        'lazada_missing': missing_lazada,
        'shopee_missing': missing_shopee,
        'existing_products': existing_products,
        'existing_variants': existing_variants
    }

def create_missing_products_and_variants(missing_items):
    """Create 'Pulled Out Item' entries for missing items"""
    
    if not missing_items['lazada_missing'] and not missing_items['shopee_missing']:
        print("✅ No missing items found!")
        return
    
    print(f"\n🔧 CREATING PULLED OUT ITEMS...")
    
    # Load current dimension tables
    dim_product = pd.read_csv('app/Transformed/dim_product.csv')
    dim_variant = pd.read_csv('app/Transformed/dim_product_variant.csv')
    
    # Get next available keys
    max_product_key_lazada = dim_product[dim_product['platform_key'] == 1]['product_key'].max()
    max_product_key_shopee = dim_product[dim_product['platform_key'] == 2]['product_key'].max()
    
    max_variant_key_lazada = dim_variant[dim_variant['platform_key'] == 1]['product_variant_key'].max()
    max_variant_key_shopee = dim_variant[dim_variant['platform_key'] == 2]['product_variant_key'].max()
    
    new_products = []
    new_variants = []
    
    # Process missing Lazada items
    if missing_items['lazada_missing']:
        print(f"🔧 Processing {len(missing_items['lazada_missing'])} missing Lazada items...")
        
        for i, item_id in enumerate(missing_items['lazada_missing']):
            product_key = max_product_key_lazada + i + 1
            variant_key = max_variant_key_lazada + i + 1
            
            # Create product entry
            new_product = {
                'product_key': product_key,
                'product_item_id': item_id,
                'product_name': 'Pulled Out Item',
                'product_category': '',
                'product_status': 'Inactive/Removed', 
                'product_rating': None,
                'platform_key': 1
            }
            new_products.append(new_product)
            
            # Create variant entry
            new_variant = {
                'product_variant_key': variant_key,
                'product_key': product_key,
                'platform_sku_id': item_id,
                'canonical_sku': item_id,
                'scent': 'N/A',
                'volume': 'N/A',
                'current_price': None,
                'original_price': None,
                'created_at': None,
                'last_updated': None,
                'platform_key': 1
            }
            new_variants.append(new_variant)
    
    # Process missing Shopee items  
    if missing_items['shopee_missing']:
        print(f"🔧 Processing {len(missing_items['shopee_missing'])} missing Shopee items...")
        
        for i, item_id in enumerate(missing_items['shopee_missing']):
            product_key = max_product_key_shopee + i + 1
            variant_key = max_variant_key_shopee + i + 1
            
            # Create product entry
            new_product = {
                'product_key': product_key,
                'product_item_id': item_id,
                'product_name': 'Pulled Out Item',
                'product_category': '',
                'product_status': 'Inactive/Removed',
                'product_rating': None,
                'platform_key': 2
            }
            new_products.append(new_product)
            
            # Create variant entry
            new_variant = {
                'product_variant_key': variant_key,
                'product_key': product_key,
                'platform_sku_id': item_id,
                'canonical_sku': item_id,
                'scent': 'N/A',
                'volume': 'N/A', 
                'current_price': None,
                'original_price': None,
                'created_at': None,
                'last_updated': None,
                'platform_key': 2
            }
            new_variants.append(new_variant)
    
    # Add new entries to dimension tables
    if new_products:
        print(f"📊 Adding {len(new_products)} new products...")
        new_products_df = pd.DataFrame(new_products)
        updated_products = pd.concat([dim_product, new_products_df], ignore_index=True)
        updated_products.to_csv('app/Transformed/dim_product.csv', index=False)
        
    if new_variants:
        print(f"📊 Adding {len(new_variants)} new variants...")
        new_variants_df = pd.DataFrame(new_variants)
        updated_variants = pd.concat([dim_variant, new_variants_df], ignore_index=True)
        updated_variants.to_csv('app/Transformed/dim_product_variant.csv', index=False)
    
    print(f"\n✅ SUMMARY:")
    print(f"   Added Lazada pulled out items: {len([p for p in new_products if p['platform_key'] == 1])}")
    print(f"   Added Shopee pulled out items: {len([p for p in new_products if p['platform_key'] == 2])}")
    print(f"   Total new products: {len(new_products)}")
    print(f"   Total new variants: {len(new_variants)}")

if __name__ == "__main__":
    missing_items = analyze_missing_items()
    if missing_items:
        create_missing_products_and_variants(missing_items)