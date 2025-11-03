"""
Fetch missing Shopee variant data for products with has_model=True
This script identifies products that have variants (has_model=True) but no variant data
and calls the Shopee API to fetch their model lists.
"""

import json
import sys
import os

# Add app directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from app.Extraction.shopee_api_calls import ShopeeDataExtractor

def main():
    print("=" * 70)
    print("FETCH MISSING SHOPEE VARIANT DATA")
    print("=" * 70)
    
    # Load products
    with open('app/Staging/shopee_products_raw.json', 'r') as f:
        products = json.load(f)
    
    # Load existing variants
    with open('app/Staging/shopee_product_variant_raw.json', 'r') as f:
        variants = json.load(f)
    
    # Get item_ids that have variant data
    variant_item_ids = set(v.get('item_id') for v in variants)
    
    # Find products with has_model=True but no variant data
    missing = []
    for p in products:
        if p.get('has_model', False) and p.get('item_id') not in variant_item_ids:
            missing.append({
                'item_id': p.get('item_id'),
                'name': p.get('item_name', 'N/A')
            })
    
    print(f"\n📊 Status Report:")
    print(f"   Products with has_model=True: {sum(1 for p in products if p.get('has_model', False))}")
    print(f"   Products with variant data: {len(variant_item_ids)}")
    print(f"   Missing variant data: {len(missing)}")
    
    if not missing:
        print("\n✅ All products with has_model=True already have variant data!")
        print("   No API calls needed.")
        return
    
    print(f"\n📋 Products missing variant data:")
    for i, m in enumerate(missing, 1):
        print(f"   {i}. {m['item_id']}: {m['name'][:60]}")
    
    # Ask for confirmation
    print(f"\n⚠️  This will make API calls to fetch variant data for {len(missing)} products.")
    response = input(f"Continue? (yes/no): ").strip().lower()
    
    if response not in ['yes', 'y']:
        print("❌ Cancelled by user")
        return
    
    # Initialize extractor
    print(f"\n🔄 Initializing Shopee API extractor...")
    extractor = ShopeeDataExtractor()
    
    # Call extract_product_variants with start_fresh=True to re-fetch all variants
    print(f"\n🔄 Fetching variant data from Shopee API...")
    print(f"   This will fetch variants for ALL {sum(1 for p in products if p.get('has_model', False))} products with has_model=True")
    
    new_variants = extractor.extract_product_variants(start_fresh=True)
    
    print(f"\n✅ Variant extraction complete!")
    print(f"   Total variants fetched: {len(new_variants)}")
    print(f"   API calls used: {extractor.api_calls_made}")
    
    # Check what we got
    new_variant_item_ids = set(v.get('item_id') for v in new_variants)
    still_missing = [m for m in missing if m['item_id'] not in new_variant_item_ids]
    
    if still_missing:
        print(f"\n⚠️  {len(still_missing)} products still missing variant data:")
        for m in still_missing:
            print(f"   - {m['item_id']}: {m['name'][:60]}")
        print(f"\n   These products may not have variant data available in the API.")
    else:
        print(f"\n🎉 SUCCESS! All missing variant data has been fetched!")
        print(f"\n📝 Next step: Run the harmonization script to update dim_product_variant")
        print(f"   python3 app/Transformation/harmonize_dim_product.py")

if __name__ == "__main__":
    main()
