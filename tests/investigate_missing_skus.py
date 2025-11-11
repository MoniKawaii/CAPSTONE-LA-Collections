"""
Detailed Investigation of Missing Lazada SKUs
=============================================

This script investigates why specific SKUs (17089061731, 17167753965) 
are present in order data but missing from product data.
"""

import json
import os
import pandas as pd

def investigate_missing_skus():
    """Investigate specific missing SKUs"""
    
    base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    staging_path = os.path.join(base_path, 'app', 'Staging')
    
    print("🔍 Investigating Missing SKU Issue...")
    
    # Load data
    with open(os.path.join(staging_path, 'lazada_products_raw.json'), 'r', encoding='utf-8') as f:
        products_data = json.load(f)
    
    with open(os.path.join(staging_path, 'lazada_multiple_order_items_raw.json'), 'r', encoding='utf-8') as f:
        order_data = json.load(f)
    
    # Target missing SKUs
    missing_skus = ['17089061731', '17167753965']
    
    print(f"\n🎯 Investigating SKUs: {missing_skus}")
    
    # Check if these SKUs exist anywhere in the product data
    for target_sku in missing_skus:
        print(f"\n📋 Analyzing SKU: {target_sku}")
        
        found_in_products = False
        
        # Search through all products
        for i, product in enumerate(products_data):
            item_id = product.get('item_id')
            skus = product.get('skus', [])
            
            # Check all SKU fields
            for sku in skus:
                sku_id = sku.get('SkuId')
                seller_sku = sku.get('SellerSku')
                shop_sku = sku.get('ShopSku')
                
                if (str(sku_id) == target_sku or 
                    str(seller_sku) == target_sku or 
                    target_sku in str(shop_sku)):
                    
                    print(f"  ✅ Found in product {i+1}:")
                    print(f"    - item_id: {item_id}")
                    print(f"    - SkuId: {sku_id}")
                    print(f"    - SellerSku: {seller_sku}")
                    print(f"    - ShopSku: {shop_sku}")
                    found_in_products = True
                    break
            
            if found_in_products:
                break
        
        if not found_in_products:
            print(f"  ❌ NOT FOUND in product data")
            
            # Check what orders contain this SKU
            orders_with_sku = []
            for order_record in order_data:
                order_id = order_record.get('order_id')
                order_items = order_record.get('order_items', [])
                
                for item in order_items:
                    if str(item.get('sku_id')) == target_sku:
                        orders_with_sku.append({
                            'order_id': order_id,
                            'name': item.get('name', ''),
                            'paid_price': item.get('paid_price', 0),
                            'item_price': item.get('item_price', 0)
                        })
            
            print(f"  📊 Found in {len(orders_with_sku)} order items:")
            for i, order_item in enumerate(orders_with_sku[:3]):  # Show first 3
                print(f"    Order {order_item['order_id']}: {order_item['name'][:50]}...")
    
    # Analyze the date ranges to see if it's a timing issue
    print(f"\n📅 Analyzing Data Time Ranges...")
    
    # Product data timestamps
    product_timestamps = []
    for product in products_data[:100]:  # Sample first 100
        created_time = product.get('created_time')
        updated_time = product.get('updated_time')
        if created_time:
            product_timestamps.append(int(created_time))
        if updated_time:
            product_timestamps.append(int(updated_time))
    
    if product_timestamps:
        from datetime import datetime
        min_product_time = min(product_timestamps) / 1000  # Convert to seconds
        max_product_time = max(product_timestamps) / 1000
        
        print(f"  Product data range:")
        print(f"    - Earliest: {datetime.fromtimestamp(min_product_time).strftime('%Y-%m-%d')}")
        print(f"    - Latest: {datetime.fromtimestamp(max_product_time).strftime('%Y-%m-%d')}")
    
    # Order data timestamps
    order_dates = []
    for order_record in order_data[:100]:  # Sample first 100
        created_at = order_record.get('created_at')
        if created_at:
            try:
                order_dates.append(pd.to_datetime(created_at).date())
            except:
                pass
    
    if order_dates:
        print(f"  Order data range:")
        print(f"    - Earliest: {min(order_dates)}")
        print(f"    - Latest: {max(order_dates)}")
    
    # Check current dimension tables
    print(f"\n🗂️ Checking Current Dimension Tables...")
    
    try:
        transformed_path = os.path.join(base_path, 'app', 'Transformed')
        
        # Check product table
        dim_product_df = pd.read_csv(os.path.join(transformed_path, 'dim_product.csv'))
        lazada_products = dim_product_df[dim_product_df['platform_key'] == 1]
        print(f"  - Products in dim_product (Lazada): {len(lazada_products)}")
        
        # Check variant table
        dim_variant_df = pd.read_csv(os.path.join(transformed_path, 'dim_product_variant.csv'))
        lazada_variants = dim_variant_df[dim_variant_df['platform_key'] == 1]
        print(f"  - Variants in dim_variant (Lazada): {len(lazada_variants)}")
        
        # Check if missing SKUs are in DEFAULT variants
        for sku in missing_skus:
            sku_variants = lazada_variants[lazada_variants['platform_sku_id'].astype(str) == sku]
            default_variants = lazada_variants[lazada_variants['platform_sku_id'].str.contains(f'DEFAULT_', na=False)]
            print(f"    SKU {sku}: {len(sku_variants)} exact matches, {len(default_variants)} DEFAULT variants")
            
    except Exception as e:
        print(f"  ❌ Error loading dimension tables: {e}")
    
    # Check original raw data structure
    print(f"\n🔧 Raw Data Structure Analysis...")
    
    # Count total products vs total SKUs
    total_products = len(products_data)
    total_skus_in_products = sum(len(product.get('skus', [])) for product in products_data)
    
    print(f"  - Total products in raw data: {total_products}")
    print(f"  - Total SKUs in products: {total_skus_in_products}")
    
    # Sample some products to show structure
    print(f"  - Sample product structure:")
    if products_data:
        sample = products_data[0]
        print(f"    item_id: {sample.get('item_id')}")
        print(f"    name: {sample.get('name', 'N/A')[:50]}...")
        print(f"    skus count: {len(sample.get('skus', []))}")
        if sample.get('skus'):
            sku = sample['skus'][0]
            print(f"    first sku SkuId: {sku.get('SkuId')}")
    
    return {
        'total_products': total_products,
        'total_skus': total_skus_in_products,
        'missing_skus': missing_skus
    }


def check_potential_solutions():
    """Check potential solutions for the missing SKU issue"""
    
    print(f"\n🎯 POTENTIAL SOLUTIONS:")
    print("=" * 50)
    
    print(f"1. 📊 Data Extraction Issue:")
    print(f"   - Missing SKUs suggest incomplete product data extraction")
    print(f"   - Orders reference products that weren't captured in product API calls")
    print(f"   - Solution: Re-run product extraction with broader criteria")
    
    print(f"\n2. 🔄 Product Sync Timing:")
    print(f"   - Products may have been created/updated after extraction")
    print(f"   - Order data captures transactions for newer products")
    print(f"   - Solution: Extract products by order-referenced SKUs")
    
    print(f"\n3. 🎚️ API Pagination/Filtering:")
    print(f"   - Product API may have pagination limits")
    print(f"   - Some products might be filtered out (status, availability)")
    print(f"   - Solution: Check API parameters and pagination completeness")
    
    print(f"\n4. 🔧 Configuration Update Needed:")
    print(f"   - Current config maps 'SkuId' correctly")
    print(f"   - Issue is data availability, not configuration")
    print(f"   - Recommended: Add fallback logic for missing products")
    
    print(f"\n📝 CONFIG RECOMMENDATIONS:")
    print(f"   - CURRENT: Mapping is correct (SkuId → platform_sku_id)")
    print(f"   - NEEDED: Enhanced product extraction to capture ALL referenced SKUs")
    print(f"   - WORKAROUND: Add DEFAULT variants for missing SKUs in fact table processing")


if __name__ == "__main__":
    print("🚀 Starting Missing SKU Investigation")
    print("=" * 50)
    
    try:
        results = investigate_missing_skus()
        check_potential_solutions()
        
        print(f"\n✅ Investigation complete!")
        
    except Exception as e:
        print(f"❌ Error during investigation: {e}")
        import traceback
        traceback.print_exc()