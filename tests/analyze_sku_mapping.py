"""
SKU Mapping Analysis for Lazada Data
===================================

This script analyzes the relationship between order sku_id and product SkuId
to understand the configuration needed for proper mapping.
"""

import json
import os

def analyze_sku_mapping():
    """Analyze SKU mapping between order and product data"""
    
    base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    staging_path = os.path.join(base_path, 'app', 'Staging')
    
    print("🔍 Loading Lazada data files...")
    
    # Load product data
    with open(os.path.join(staging_path, 'lazada_products_raw.json'), 'r', encoding='utf-8') as f:
        products_data = json.load(f)
    
    # Load order data
    with open(os.path.join(staging_path, 'lazada_multiple_order_items_raw.json'), 'r', encoding='utf-8') as f:
        order_data = json.load(f)
    
    print(f"✓ Loaded {len(products_data)} products")
    print(f"✓ Loaded {len(order_data)} order records")
    
    # Extract all SkuIds from products
    print("\n📊 Analyzing product SKU structure...")
    product_sku_map = {}
    all_product_skus = set()
    
    for product in products_data:
        item_id = product.get('item_id')
        skus = product.get('skus', [])
        
        for sku in skus:
            sku_id = sku.get('SkuId')
            seller_sku = sku.get('SellerSku')
            shop_sku = sku.get('ShopSku')
            
            if sku_id:
                product_sku_map[str(sku_id)] = {
                    'item_id': item_id,
                    'seller_sku': seller_sku,
                    'shop_sku': shop_sku,
                    'sku_id': sku_id
                }
                all_product_skus.add(str(sku_id))
    
    print(f"  - Total product SKUs: {len(all_product_skus)}")
    
    # Extract all sku_ids from orders
    print("\n📊 Analyzing order SKU structure...")
    order_skus = set()
    order_sku_examples = {}
    
    for order_record in order_data:
        order_id = order_record.get('order_id')
        order_items = order_record.get('order_items', [])
        
        for item in order_items:
            sku_id = str(item.get('sku_id', ''))
            if sku_id and sku_id != '':
                order_skus.add(sku_id)
                if sku_id not in order_sku_examples:
                    order_sku_examples[sku_id] = {
                        'order_id': order_id,
                        'name': item.get('name', ''),
                        'paid_price': item.get('paid_price', 0)
                    }
    
    print(f"  - Total order SKUs: {len(order_skus)}")
    
    # Find overlap
    print("\n🔍 Analyzing SKU overlap...")
    matching_skus = all_product_skus & order_skus
    missing_from_products = order_skus - all_product_skus
    missing_from_orders = all_product_skus - order_skus
    
    print(f"  - Matching SKUs: {len(matching_skus)}")
    print(f"  - SKUs in orders but not in products: {len(missing_from_products)}")
    print(f"  - SKUs in products but not in orders: {len(missing_from_orders)}")
    
    overlap_percentage = len(matching_skus) / len(order_skus) * 100 if order_skus else 0
    print(f"  - Order SKU coverage: {overlap_percentage:.1f}%")
    
    # Show examples of matching SKUs
    print(f"\n✅ Examples of MATCHING SKUs:")
    for sku_id in list(matching_skus)[:5]:
        product_info = product_sku_map[sku_id]
        order_info = order_sku_examples.get(sku_id, {})
        
        print(f"  SKU {sku_id}:")
        print(f"    Product: item_id={product_info['item_id']}, seller_sku={product_info['seller_sku']}")
        print(f"    Order: name={order_info.get('name', 'N/A')[:50]}...")
    
    # Show examples of missing SKUs
    print(f"\n❌ Examples of MISSING SKUs (in orders, not in products):")
    for sku_id in list(missing_from_products)[:5]:
        order_info = order_sku_examples.get(sku_id, {})
        print(f"  SKU {sku_id}:")
        print(f"    Order: name={order_info.get('name', 'N/A')[:50]}...")
        print(f"    Price: ${order_info.get('paid_price', 0)}")
    
    # Check current dimension table mapping
    print(f"\n🔍 Analyzing current dimension table mapping...")
    
    try:
        import pandas as pd
        dim_variant_path = os.path.join(os.path.dirname(staging_path), 'Transformed', 'dim_product_variant.csv')
        dim_variant_df = pd.read_csv(dim_variant_path)
        
        lazada_variants = dim_variant_df[dim_variant_df['platform_key'] == 1]
        dim_skus = set(lazada_variants['platform_sku_id'].astype(str))
        
        print(f"  - SKUs in dimension table: {len(dim_skus)}")
        
        # Check overlap with order SKUs
        dim_order_overlap = dim_skus & order_skus
        print(f"  - Dimension SKUs that match orders: {len(dim_order_overlap)}")
        
        # Show sample dimension SKUs
        print(f"  - Sample dimension SKUs: {list(dim_skus)[:5]}")
        
    except Exception as e:
        print(f"  - Error loading dimension table: {e}")
    
    # Generate configuration recommendations
    print(f"\n🎯 CONFIGURATION RECOMMENDATIONS:")
    print("=" * 50)
    
    print(f"1. 📋 Current Mapping Status:")
    print(f"   - Product data contains SkuId field (product level)")
    print(f"   - Order data contains sku_id field (transaction level)")
    print(f"   - Current overlap: {overlap_percentage:.1f}%")
    
    if overlap_percentage < 90:
        print(f"\n2. ⚠️ SKU Mapping Issue Identified:")
        print(f"   - {len(missing_from_products)} order SKUs not found in product data")
        print(f"   - This suggests missing products or variant extraction issues")
        
        print(f"\n3. 🔧 Recommended Config Updates:")
        print(f"   - Verify product extraction captures ALL products with active orders")
        print(f"   - Ensure product SKU extraction uses correct SkuId field")
        print(f"   - Consider extracting products by order SKU list to ensure completeness")
        
        print(f"\n4. 📝 Mapping Configuration:")
        print(f"   - Source field for products: skus[*].SkuId")
        print(f"   - Source field for orders: order_items[*].sku_id")
        print(f"   - These should map 1:1 when product extraction is complete")
    
    else:
        print(f"\n2. ✅ Good SKU Mapping:")
        print(f"   - High overlap indicates proper extraction")
        print(f"   - Current configuration should be working")
    
    return {
        'total_order_skus': len(order_skus),
        'total_product_skus': len(all_product_skus),
        'matching_skus': len(matching_skus),
        'missing_from_products': len(missing_from_products),
        'overlap_percentage': overlap_percentage,
        'sample_missing': list(missing_from_products)[:10],
        'sample_matching': list(matching_skus)[:10]
    }


if __name__ == "__main__":
    print("🚀 Starting SKU Mapping Analysis")
    print("=" * 50)
    
    try:
        results = analyze_sku_mapping()
        
        print(f"\n📊 SUMMARY STATISTICS:")
        print(f"  - Order SKUs: {results['total_order_skus']}")
        print(f"  - Product SKUs: {results['total_product_skus']}")
        print(f"  - Matching: {results['matching_skus']}")
        print(f"  - Coverage: {results['overlap_percentage']:.1f}%")
        
        print(f"\n✅ Analysis complete!")
        
    except Exception as e:
        print(f"❌ Error during analysis: {e}")
        import traceback
        traceback.print_exc()