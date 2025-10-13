"""
Test script to validate Shopee and Lazada unified configuration
Run this to ensure everything is set up correctly
"""

import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import (
    LAZADA_TOKENS,
    SHOPEE_TOKENS,
    LAZADA_TO_UNIFIED_MAPPING,
    SHOPEE_TO_UNIFIED_MAPPING,
    get_platform_mapping,
    get_platform_key,
    get_staging_filename,
    get_all_platforms,
    get_platform_tokens,
    get_platform_api_url,
    validate_config
)

def test_tokens():
    """Test if tokens are loaded correctly"""
    print("=" * 60)
    print("Testing Token Loading")
    print("=" * 60)
    
    print("\n📦 Lazada Tokens:")
    print(f"  App Key: {'✅ Set' if LAZADA_TOKENS.get('app_key') else '❌ Missing'}")
    print(f"  App Secret: {'✅ Set' if LAZADA_TOKENS.get('app_secret') else '❌ Missing'}")
    print(f"  Access Token: {'✅ Set' if LAZADA_TOKENS.get('access_token') else '❌ Missing'}")
    
    print("\n📦 Shopee Tokens:")
    print(f"  Partner ID: {'✅ Set' if SHOPEE_TOKENS.get('partner_id') else '❌ Missing'}")
    print(f"  Partner Key: {'✅ Set' if SHOPEE_TOKENS.get('partner_key') else '❌ Missing'}")
    print(f"  Shop ID: {'✅ Set' if SHOPEE_TOKENS.get('shop_id') else '❌ Missing'}")
    print(f"  Access Token: {'✅ Set' if SHOPEE_TOKENS.get('access_token') else '❌ Missing'}")

def test_mappings():
    """Test if unified mappings are correctly defined"""
    print("\n" + "=" * 60)
    print("Testing Unified Mappings")
    print("=" * 60)
    
    # Test key fields exist in both mappings
    key_fields = [
        'item_id',  # Product ID
        'platform_key',  # Platform identifier
    ]
    
    print("\n📋 Lazada Mapping:")
    print(f"  Total fields: {len(LAZADA_TO_UNIFIED_MAPPING)}")
    for field in key_fields:
        if field in LAZADA_TO_UNIFIED_MAPPING:
            print(f"  ✅ {field} → {LAZADA_TO_UNIFIED_MAPPING[field]}")
        else:
            print(f"  ❌ {field} missing")
    
    print("\n📋 Shopee Mapping:")
    print(f"  Total fields: {len(SHOPEE_TO_UNIFIED_MAPPING)}")
    for field in key_fields:
        if field in SHOPEE_TO_UNIFIED_MAPPING:
            print(f"  ✅ {field} → {SHOPEE_TO_UNIFIED_MAPPING[field]}")
        else:
            print(f"  ❌ {field} missing")

def test_helper_functions():
    """Test platform helper functions"""
    print("\n" + "=" * 60)
    print("Testing Helper Functions")
    print("=" * 60)
    
    platforms = get_all_platforms()
    print(f"\n🌐 Supported Platforms: {platforms}")
    
    for platform in platforms:
        print(f"\n📊 {platform.upper()}:")
        
        try:
            # Test platform key
            key = get_platform_key(platform)
            print(f"  Platform Key: {key}")
            
            # Test mapping retrieval
            mapping = get_platform_mapping(platform)
            print(f"  Mapping Fields: {len(mapping)}")
            
            # Test filename generation
            products_file = get_staging_filename(platform, 'products')
            orders_file = get_staging_filename(platform, 'orders')
            print(f"  Products File: {products_file}")
            print(f"  Orders File: {orders_file}")
            
            # Test API URL
            api_url = get_platform_api_url(platform)
            print(f"  API URL: {api_url}")
            
            # Test tokens
            tokens = get_platform_tokens(platform)
            has_access = bool(tokens.get('access_token'))
            print(f"  Access Token: {'✅ Set' if has_access else '❌ Missing'}")
            
        except Exception as e:
            print(f"  ❌ Error: {e}")

def test_mapping_consistency():
    """Test if both mappings produce consistent unified field names"""
    print("\n" + "=" * 60)
    print("Testing Mapping Consistency")
    print("=" * 60)
    
    # Check if key unified fields are present in both
    unified_fields = {
        'product_item_id',
        'product_name',
        'product_category',
        'product_status',
        'product_price',
        'product_rating',
        'platform_key'
    }
    
    lazada_unified = set(LAZADA_TO_UNIFIED_MAPPING.values())
    shopee_unified = set(SHOPEE_TO_UNIFIED_MAPPING.values())
    
    common_fields = lazada_unified & shopee_unified
    
    print(f"\n✅ Common unified fields: {len(common_fields)}")
    print(f"📊 Lazada unique fields: {len(lazada_unified - shopee_unified)}")
    print(f"📊 Shopee unique fields: {len(shopee_unified - lazada_unified)}")
    
    print("\n🔍 Key Product Fields:")
    for field in unified_fields:
        in_lazada = field in lazada_unified
        in_shopee = field in shopee_unified
        status = "✅" if (in_lazada and in_shopee) else "⚠️"
        print(f"  {status} {field}: Lazada={in_lazada}, Shopee={in_shopee}")

def test_validation():
    """Test configuration validation"""
    print("\n" + "=" * 60)
    print("Testing Configuration Validation")
    print("=" * 60)
    
    validation = validate_config()
    
    print(f"\n✅ Lazada Tokens Valid: {validation['tokens_valid']}")
    print(f"✅ Shopee Tokens Valid: {validation['shopee_tokens_valid']}")
    print(f"📊 DataFrames Created: {validation['dataframes_created']}")
    print(f"🌐 API URLs Configured: {validation['api_urls_configured']}")
    print(f"📋 Schema Compliance: {validation['schema_compliance']}")
    
    print(f"\n📈 Table Count:")
    for key, value in validation['table_count'].items():
        print(f"  {key}: {value}")
    
    print(f"\n🔧 Environment Configuration:")
    for key, value in validation['environment_config'].items():
        status = "✅" if value else "❌"
        print(f"  {status} {key}: {value}")

def test_staging_directory():
    """Test if staging directory exists"""
    print("\n" + "=" * 60)
    print("Testing Staging Directory")
    print("=" * 60)
    
    staging_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'app', 'Staging')
    
    if os.path.exists(staging_dir):
        print(f"\n✅ Staging directory exists: {staging_dir}")
        
        # List existing files
        files = os.listdir(staging_dir)
        if files:
            print(f"\n📁 Existing files ({len(files)}):")
            for file in sorted(files):
                file_path = os.path.join(staging_dir, file)
                size = os.path.getsize(file_path)
                print(f"  - {file} ({size:,} bytes)")
        else:
            print("\n📁 Staging directory is empty (ready for extraction)")
    else:
        print(f"\n❌ Staging directory not found: {staging_dir}")
        print("   It will be created when you run extraction")

def main():
    """Run all tests"""
    print("🚀 LA Collections - Multi-Platform Configuration Test")
    print("Testing Lazada and Shopee unified integration\n")
    
    try:
        test_tokens()
        test_mappings()
        test_helper_functions()
        test_mapping_consistency()
        test_validation()
        test_staging_directory()
        
        print("\n" + "=" * 60)
        print("✅ All Tests Completed!")
        print("=" * 60)
        print("\n💡 Next Steps:")
        print("1. Run Lazada extraction: python app/Extraction/lazada_api_calls.py")
        print("2. Run Shopee extraction: python app/Extraction/shopee_api_calls.py")
        print("3. Harmonize data using transformation scripts")
        print("4. Load to data warehouse")
        
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
