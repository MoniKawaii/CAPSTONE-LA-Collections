#!/usr/bin/env python3
"""
Test script for enhanced Lazada product extraction
Targets missing SKUs: 17089061731, 17167753965
"""

from app.Extraction.lazada_api_calls import LazadaDataExtractor
import os
import json

def main():
    print("🔍 Testing Enhanced Lazada Product Extraction")
    print("=" * 60)
    
    # Initialize extractor
    extractor = LazadaDataExtractor()
    
    # Check current product count
    current_file = 'app/Staging/lazada_products_raw.json'
    if os.path.exists(current_file):
        current_data = extractor._load_from_json('lazada_products_raw.json')
        current_count = len(current_data) if current_data else 0
        print(f"📊 Current products: {current_count}")
        
        # Check if missing SKUs are already present
        missing_skus = ['17089061731', '17167753965']
        found_before = []
        
        if current_data:
            for product in current_data:
                skus = product.get('skus', [])
                for sku in skus:
                    sku_id = str(sku.get('SkuId', ''))
                    if sku_id in missing_skus:
                        found_before.append(sku_id)
        
        print(f"📋 Missing SKUs currently found: {found_before}")
    else:
        print("📊 No existing product file found")
        current_count = 0
    
    # Ask for confirmation
    print(f"\n🚀 About to run enhanced extraction with multiple strategies:")
    print("   - Standard extraction (filter=all)")
    print("   - Live products extraction") 
    print("   - Sold out products extraction")
    print("   - SKU-based extraction (from order data)")
    print("   - Detailed product info (/product/item/get)")
    print(f"   - Target missing SKUs: 17089061731, 17167753965")
    print(f"\n📋 This will also extract detailed product information to:")
    print("   - lazada_products_raw.json (basic product list)")
    print("   - lazada_productitem_raw.json (detailed product info)")
    
    confirm = input("\nProceed with enhanced extraction? (y/N): ").strip().lower()
    if confirm not in ['y', 'yes']:
        print("❌ Extraction cancelled")
        return
    
    print("\n🚀 Starting enhanced product extraction...")
    
    try:
        # Run enhanced extraction
        products = extractor.extract_all_products(start_fresh=True, enhanced_extraction=True)
        
        print(f"\n✅ Enhanced extraction complete!")
        print(f"📊 Results summary:")
        print(f"   Total products: {len(products)}")
        print(f"   Previous count: {current_count}")
        print(f"   Net change: +{len(products) - current_count}")
        print(f"   API calls used: {extractor.api_calls_made}")
        
        # Check detailed product file
        detailed_file = 'app/Staging/lazada_productitem_raw.json'
        if os.path.exists(detailed_file):
            detailed_data = extractor._load_from_json('lazada_productitem_raw.json')
            detailed_count = len(detailed_data) if detailed_data else 0
            print(f"   Detailed products: {detailed_count}")
        else:
            print(f"   Detailed products: Not yet created")
        
        # Check for missing SKUs
        missing_skus = ['17089061731', '17167753965']
        found_after = []
        sku_to_product = {}
        
        for product in products:
            skus = product.get('skus', [])
            for sku in skus:
                sku_id = str(sku.get('SkuId', ''))
                if sku_id in missing_skus:
                    found_after.append(sku_id)
                    sku_to_product[sku_id] = {
                        'item_id': product.get('item_id'),
                        'primary_category': product.get('primary_category'),
                        'status': product.get('status'),
                        'product_name': sku.get('SellerSku', 'Unknown')
                    }
        
        print(f"\n🎯 Missing SKU Recovery Results:")
        print(f"   Missing SKUs found: {found_after}")
        
        if found_after:
            print("   📋 Product details for recovered SKUs:")
            for sku_id in found_after:
                details = sku_to_product[sku_id]
                print(f"     • SKU {sku_id}: {details['product_name']}")
                print(f"       Item ID: {details['item_id']}, Status: {details['status']}")
        
        newly_found = set(found_after) - set(found_before if 'found_before' in locals() else [])
        if newly_found:
            print(f"   🆕 Newly recovered SKUs: {list(newly_found)}")
            print("   ✅ Enhanced extraction successfully recovered missing products!")
        else:
            print("   ⚠️ Missing SKUs still not found - may need manual investigation")
            
    except Exception as e:
        print(f"❌ Error during extraction: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()