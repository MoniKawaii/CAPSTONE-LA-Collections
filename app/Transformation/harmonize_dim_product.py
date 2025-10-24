"""
Product Dimension Harmonization Script - Multi-Platform (Lazada & Shopee)
Maps product data from both Lazada and Shopee raw JSON files to the standardized dimensional model

Data Sources:
Lazada:
- lazada_products_raw.json 
- lazada_productitem_raw.json

Shopee:
- shopee_products_raw.json
- shopee_productitem_raw.json

Target Schema: Dim_Product table structure with platform_key (1=Lazada, 2=Shopee)
"""

import pandas as pd
import json
import os
import sys

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import get_empty_dataframe, LAZADA_TO_UNIFIED_MAPPING, SHOPEE_TO_UNIFIED_MAPPING, apply_data_types


def load_products_raw(platform='lazada'):
    """
    Load product data from raw JSON files for specified platform
    
    Args:
        platform (str): 'lazada' or 'shopee'
        
    Returns:
        tuple: (products_data, productitem_data) as lists of dictionaries
    """
    staging_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'Staging')
    
    # Define file names based on platform
    products_file = os.path.join(staging_dir, f'{platform}_products_raw.json')
    productitem_file = os.path.join(staging_dir, f'{platform}_productitem_raw.json')
    
    products_data = []
    productitem_data = []
    
    if os.path.exists(products_file):
        with open(products_file, 'r', encoding='utf-8') as f:
            products_data = json.load(f)
        print(f"✅ Loaded {len(products_data)} products from {platform}_products_raw.json")
    else:
        print(f"⚠️ File not found: {products_file}")
    
    if os.path.exists(productitem_file):
        with open(productitem_file, 'r', encoding='utf-8') as f:
            productitem_data = json.load(f)
        print(f"✅ Loaded {len(productitem_data)} product items from {platform}_productitem_raw.json")
    else:
        print(f"⚠️ File not found: {productitem_file}")
    
    return products_data, productitem_data


def load_lazada_products():
    """
    Load Lazada product data from raw JSON files (legacy function)
    
    Returns:
        tuple: (products_data, productitem_data) as lists of dictionaries
    """
    return load_products_raw('lazada')

import pandas as pd
import json
import os
import sys

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import get_empty_dataframe, LAZADA_TO_UNIFIED_MAPPING, apply_data_types

def load_lazada_products():
    """
    Load Lazada product data from raw JSON files
    
    Returns:
        tuple: (products_data, productitem_data) as lists of dictionaries
    """
    staging_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'Staging')
    
    # Load products raw data
    products_file = os.path.join(staging_dir, 'lazada_products_raw.json')
    productitem_file = os.path.join(staging_dir, 'lazada_productitem_raw.json')
    
    products_data = []
    productitem_data = []
    
    if os.path.exists(products_file):
        with open(products_file, 'r', encoding='utf-8') as f:
            products_data = json.load(f)
        print(f"✅ Loaded {len(products_data)} products from lazada_products_raw.json")
    else:
        print(f"⚠️ File not found: {products_file}")
    
    if os.path.exists(productitem_file):
        with open(productitem_file, 'r', encoding='utf-8') as f:
            productitem_data = json.load(f)
        print(f"✅ Loaded {len(productitem_data)} product items from lazada_productitem_raw.json")
    else:
        print(f"⚠️ File not found: {productitem_file}")
    
    return products_data, productitem_data

def get_category_name(primary_category, platform='lazada'):
    """
    Map primary category ID to category name
    Supports both Lazada and Shopee category structures
    
    Args:
        primary_category (int/str): Category ID from platform
        platform (str): 'lazada' or 'shopee'
        
    Returns:
        str: Category name
    """
    if platform == 'lazada':
        # Lazada category mapping
        category_mapping = {
            22490: "Home Fragrance",
            7539: "Home Improvement", 
            8632: "Health & Beauty",
            18986: "Electronics",
            24428: "Fashion",
            24442: "Automotive",
            10100546: "Baby & Toys"
        }
        return category_mapping.get(primary_category, f"Category_{primary_category}")
    
    elif platform == 'shopee':
        # Shopee uses category_id field - map common categories
        # Note: Shopee category structure may be different
        if isinstance(primary_category, str):
            return primary_category  # Already a category name
        
        shopee_category_mapping = {
            # Add Shopee-specific category mappings here as needed
            # Example mappings (adjust based on your actual data)
        }
        return shopee_category_mapping.get(primary_category, f"Category_{primary_category}")
    
    return f"Category_{primary_category}"

def extract_price_from_skus(skus, platform='lazada'):
    """
    Extract price from SKU data (if available)
    Supports both Lazada and Shopee data structures
    
    Args:
        skus (list): List of SKU objects
        platform (str): 'lazada' or 'shopee'
        
    Returns:
        float: Price value or None
    """
    if not skus:
        return None
    
    for sku in skus:
        if platform == 'lazada':
            # Lazada uses 'price' field
            if 'price' in sku:
                try:
                    return float(sku['price'])
                except (ValueError, TypeError):
                    continue
        elif platform == 'shopee':
            # Shopee uses 'original_price' or 'current_price' (in cents)
            price_field = sku.get('original_price') or sku.get('model_original_price')
            if price_field:
                try:
                    # Shopee prices are in cents, convert to dollars
                    return float(price_field) / 100
                except (ValueError, TypeError):
                    continue
    
    return None

def extract_product_variants(product_data, product_key, platform='lazada'):
    """
    Extract variant data from product SKUs array
    Supports both Lazada and Shopee data structures
    
    Args:
        product_data (dict): Raw product data from platform API
        product_key (int): Product key from the main product record
        platform (str): 'lazada' or 'shopee'
        
    Returns:
        list: List of variant records
    """
    variants = []
    
    if platform == 'lazada':
        skus = product_data.get('skus', [])
        
        for sku_data in skus:
            variant_record = {
                'variant_key': None,  # Will be generated as surrogate key
                'product_key': product_key,
                'platform_sku_id': str(sku_data.get('SkuId', '')),
                'variant_sku': str(sku_data.get('SellerSku', '')),
                'variant_attribute_1': sku_data.get('Variation1', ''),
                'variant_attribute_2': sku_data.get('Variation2', ''),
                'variant_attribute_3': sku_data.get('Variation3', ''),
                'variant_price': sku_data.get('price', None),
                'variant_stock': sku_data.get('quantity', None),
                'platform_key': 1  # Lazada = 1
            }
            variants.append(variant_record)
    
    elif platform == 'shopee':
        # Shopee uses 'model' array for variants
        models = product_data.get('model', [])
        
        for model_data in models:
            # Extract tier variations (e.g., Size, Color)
            tier_index = model_data.get('tier_index', [])
            
            variant_record = {
                'variant_key': None,  # Will be generated as surrogate key
                'product_key': product_key,
                'platform_sku_id': str(model_data.get('model_id', '')),
                'variant_sku': str(model_data.get('model_sku', '')),
                'variant_attribute_1': tier_index[0] if len(tier_index) > 0 else '',
                'variant_attribute_2': tier_index[1] if len(tier_index) > 1 else '',
                'variant_attribute_3': '',  # Shopee typically has max 2 tiers
                'variant_price': model_data.get('original_price', 0) / 100 if model_data.get('original_price') else None,  # Convert from cents
                'variant_stock': model_data.get('stock_info_v2', {}).get('normal_stock', None) if 'stock_info_v2' in model_data else model_data.get('normal_stock', None),
                'platform_key': 2  # Shopee = 2
            }
            variants.append(variant_record)
    
    return variants

def get_base_sku_from_variants(skus, platform='lazada'):
    """
    Extract base SKU from the first available variant
    Supports both Lazada and Shopee data structures
    
    Args:
        skus (list): List of SKU/model objects
        platform (str): 'lazada' or 'shopee'
        
    Returns:
        str: Base SKU or empty string
    """
    if not skus:
        return ''
    
    first_variant = skus[0]
    
    if platform == 'lazada':
        # Use the first variant's SellerSku as base, or derive from SkuId
        seller_sku = first_variant.get('SellerSku', '')
        if seller_sku:
            # Extract base part (remove variant suffixes like -RED, -L, etc.)
            base_sku = seller_sku.split('-')[0] if '-' in seller_sku else seller_sku
            return base_sku
        
        # Fallback to SkuId if no SellerSku
        return str(first_variant.get('SkuId', ''))
    
    elif platform == 'shopee':
        # Shopee uses model_sku
        model_sku = first_variant.get('model_sku', '')
        if model_sku:
            # Extract base part if it has separators
            base_sku = model_sku.split('-')[0] if '-' in model_sku else model_sku
            return base_sku
        
        # Fallback to model_id
        return str(first_variant.get('model_id', ''))
    
    return ''

def harmonize_product_record(product_data, source_file, platform='lazada'):
    """
    Harmonize a single product record from platform format to dimensional model
    Supports both Lazada and Shopee data structures
    
    Args:
        product_data (dict): Raw product data from platform API
        source_file (str): Source file identifier ('products' or 'productitem')
        platform (str): 'lazada' or 'shopee'
        
    Returns:
        dict: Harmonized product record
    """
    # Platform-specific field extraction
    if platform == 'lazada':
        # Get product attributes
        attributes = product_data.get('attributes', {})
        
        # Extract price - try multiple sources
        price = None
        if 'skus' in product_data and product_data['skus']:
            price = extract_price_from_skus(product_data['skus'], platform='lazada')
        
        # Get base SKU from variants
        base_sku = get_base_sku_from_variants(product_data.get('skus', []), platform='lazada')
        
        # Map using LAZADA_TO_UNIFIED_MAPPING
        harmonized_record = {
            'product_key': None,  # Will be generated as surrogate key
            'product_item_id': str(product_data.get('item_id', '')),
            'product_name': attributes.get('name', ''),
            'product_sku_base': base_sku,
            'product_category': get_category_name(product_data.get('primary_category'), platform='lazada'),
            'product_status': product_data.get('status', ''),
            'product_price': price,
            'product_rating': None,  # Leave null as requested
            'platform_key': 1  # Lazada = 1
        }
    
    elif platform == 'shopee':
        # Shopee product structure
        # Extract price from models or use item price
        models = product_data.get('model', [])
        price = None
        
        # Try to get price from first model
        if models:
            price = extract_price_from_skus(models, platform='shopee')
        
        # Fallback to item-level price
        if not price:
            original_price = product_data.get('price_info', {}).get('original_price')
            if original_price:
                price = float(original_price) / 100  # Convert from cents
        
        # Get base SKU from models
        base_sku = get_base_sku_from_variants(models, platform='shopee') if models else str(product_data.get('item_id', ''))
        
        # Get category - Shopee uses category_id
        category = product_data.get('category_id')
        
        # Map using SHOPEE_TO_UNIFIED_MAPPING
        harmonized_record = {
            'product_key': None,  # Will be generated as surrogate key
            'product_item_id': str(product_data.get('item_id', '')),
            'product_name': product_data.get('item_name', ''),
            'product_sku_base': base_sku,
            'product_category': get_category_name(category, platform='shopee'),
            'product_status': product_data.get('item_status', ''),  # NORMAL, DELETED, BANNED, etc.
            'product_price': price,
            'product_rating': None,  # Leave null as requested
            'platform_key': 2  # Shopee = 2
        }
    
    else:
        raise ValueError(f"Unsupported platform: {platform}")
    
    return harmonized_record

def harmonize_dim_product():
    """
    Main function to harmonize product data from both Lazada and Shopee into dimensional model
    
    Returns:
        tuple: (product_df, variant_df) - Harmonized product and variant dimension tables
    """
    print("🔄 Starting Product & Variant Dimension Harmonization (Multi-Platform)...")
    
    # Get empty DataFrames with proper structure
    dim_product_df = get_empty_dataframe('dim_product')
    dim_product_variant_df = get_empty_dataframe('dim_product_variant')
    
    print(f"📋 Product schema: {list(dim_product_df.columns)}")
    print(f"📋 Variant schema: {list(dim_product_variant_df.columns)}")
    
    # Combine all products and variants from both platforms
    all_products = []
    all_variants = []
    all_raw_products = []
    
    # Process Lazada products
    print("\n📦 Processing Lazada products...")
    lazada_products_data, lazada_productitem_data = load_products_raw('lazada')
    
    if lazada_products_data or lazada_productitem_data:
        # Process products_raw.json
        for product in lazada_products_data:
            harmonized = harmonize_product_record(product, 'products', platform='lazada')
            all_products.append(harmonized)
            all_raw_products.append(('lazada', product))
        
        # Process productitem_raw.json (avoid duplicates by item_id)
        existing_item_ids = {p['product_item_id'] for p in all_products}
        
        for product in lazada_productitem_data:
            item_id = str(product.get('item_id', ''))
            if item_id not in existing_item_ids:
                harmonized = harmonize_product_record(product, 'productitem', platform='lazada')
                all_products.append(harmonized)
                all_raw_products.append(('lazada', product))
                existing_item_ids.add(item_id)
        
        print(f"   ✓ Processed {len([p for p in all_products if p['platform_key'] == 1])} Lazada products")
    else:
        print("   ⚠️ No Lazada product data found")
    
    # Process Shopee products
    print("\n🛍️ Processing Shopee products...")
    shopee_products_data, shopee_productitem_data = load_products_raw('shopee')
    
    if shopee_products_data or shopee_productitem_data:
        # Get existing item_ids to avoid duplicates
        existing_item_ids = {p['product_item_id'] for p in all_products}
        
        # Process shopee products_raw.json
        for product in shopee_products_data:
            item_id = str(product.get('item_id', ''))
            if item_id not in existing_item_ids:
                harmonized = harmonize_product_record(product, 'products', platform='shopee')
                all_products.append(harmonized)
                all_raw_products.append(('shopee', product))
                existing_item_ids.add(item_id)
        
        # Process shopee productitem_raw.json
        for product in shopee_productitem_data:
            item_id = str(product.get('item_id', ''))
            if item_id not in existing_item_ids:
                harmonized = harmonize_product_record(product, 'productitem', platform='shopee')
                all_products.append(harmonized)
                all_raw_products.append(('shopee', product))
                existing_item_ids.add(item_id)
        
        print(f"   ✓ Processed {len([p for p in all_products if p['platform_key'] == 2])} Shopee products")
    else:
        print("   ⚠️ No Shopee product data found")
    
    # Convert products to DataFrame and generate keys
    if all_products:
        dim_product_df = pd.DataFrame(all_products)
        
        # Generate surrogate keys (product_key)
        dim_product_df['product_key'] = range(1, len(dim_product_df) + 1)
        
        # Apply proper data types according to schema
        dim_product_df = apply_data_types(dim_product_df, 'dim_product')
        
        # Now extract variants using the generated product_keys and raw product data
        variant_key_counter = 1
        for idx, (platform, raw_product) in enumerate(all_raw_products):
            if idx < len(dim_product_df):  # Ensure we don't exceed product count
                product_key = dim_product_df.iloc[idx]['product_key']
                variants = extract_product_variants(raw_product, product_key, platform=platform)
                
                # Add variant keys
                for variant in variants:
                    variant['variant_key'] = variant_key_counter
                    variant_key_counter += 1
                
                all_variants.extend(variants)
        
        # Convert variants to DataFrame
        if all_variants:
            dim_product_variant_df = pd.DataFrame(all_variants)
            dim_product_variant_df = apply_data_types(dim_product_variant_df, 'dim_product_variant')
        
        print(f"\n✅ Harmonized {len(dim_product_df)} products and {len(dim_product_variant_df)} variants")
        print(f"\n📊 Product Summary by Platform:")
        lazada_count = len(dim_product_df[dim_product_df['platform_key'] == 1])
        shopee_count = len(dim_product_df[dim_product_df['platform_key'] == 2])
        print(f"   • Lazada products: {lazada_count}")
        print(f"   • Shopee products: {shopee_count}")
        print(f"   • Total products: {len(dim_product_df)}")
        
        print(f"\n📊 Product Status:")
        status_counts = dim_product_df['product_status'].value_counts().to_dict()
        for status, count in status_counts.items():
            print(f"   • {status}: {count}")
        
        print(f"\n📊 Additional Metrics:")
        print(f"   • Products with prices: {len(dim_product_df[dim_product_df['product_price'].notna()])}")
        print(f"   • Unique categories: {dim_product_df['product_category'].nunique()}")
        
        print(f"\n📊 Variant Summary by Platform:")
        if len(dim_product_variant_df) > 0:
            lazada_variants = len(dim_product_variant_df[dim_product_variant_df['platform_key'] == 1])
            shopee_variants = len(dim_product_variant_df[dim_product_variant_df['platform_key'] == 2])
            print(f"   • Lazada variants: {lazada_variants}")
            print(f"   • Shopee variants: {shopee_variants}")
            print(f"   • Total variants: {len(dim_product_variant_df)}")
            print(f"   • Variants with attribute 1: {len(dim_product_variant_df[dim_product_variant_df['variant_attribute_1'] != ''])}")
            print(f"   • Variants with attribute 2: {len(dim_product_variant_df[dim_product_variant_df['variant_attribute_2'] != ''])}")
        
        # Show sample of data
        print("\n📋 Sample of harmonized product data (both platforms):")
        sample_cols = ['product_key', 'product_item_id', 'product_name', 'product_sku_base', 'product_status', 'product_price', 'platform_key']
        available_cols = [col for col in sample_cols if col in dim_product_df.columns]
        print(dim_product_df[available_cols].head(5).to_string(index=False))
        
        if len(dim_product_variant_df) > 0:
            print("\n📋 Sample of harmonized variant data (both platforms):")
            variant_cols = ['variant_key', 'product_key', 'platform_sku_id', 'variant_sku', 'variant_attribute_1', 'variant_attribute_2', 'platform_key']
            available_variant_cols = [col for col in variant_cols if col in dim_product_variant_df.columns]
            print(dim_product_variant_df[available_variant_cols].head(5).to_string(index=False))
        
    else:
        print("⚠️ No product data found to harmonize from any platform")
    
    return dim_product_df, dim_product_variant_df

def save_harmonized_data(product_df, variant_df, output_dir=None):
    """
    Save harmonized product and variant data to CSV files
    
    Args:
        product_df (pd.DataFrame): Harmonized product DataFrame
        variant_df (pd.DataFrame): Harmonized variant DataFrame
        output_dir (str): Output directory path (optional)
        
    Returns:
        tuple: (product_file_path, variant_file_path)
    """
    if output_dir is None:
        output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'Transformed')
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Save product data
    product_path = os.path.join(output_dir, 'dim_product.csv')
    product_df.to_csv(product_path, index=False)
    print(f"💾 Saved harmonized product data to: {product_path}")
    
    # Save variant data
    variant_path = os.path.join(output_dir, 'dim_product_variant.csv')
    variant_df.to_csv(variant_path, index=False)
    print(f"💾 Saved harmonized variant data to: {variant_path}")
    
    return product_path, variant_path

if __name__ == "__main__":
    print("=" * 70)
    print("🚀 Multi-Platform Product Harmonization (Lazada & Shopee)")
    print("=" * 70)
    
    # Run the harmonization process
    product_df, variant_df = harmonize_dim_product()
    
    if not product_df.empty:
        product_file, variant_file = save_harmonized_data(product_df, variant_df)
        print(f"\n🎉 Product & Variant dimension harmonization completed successfully!")
        print(f"📁 Product file: {product_file}")
        print(f"📁 Variant file: {variant_file}")
        print(f"\n✅ This dimension includes data from BOTH Lazada and Shopee platforms")
    else:
        print("❌ No data was harmonized. Please check your source files.")
        print("   Expected files:")
        print("   • lazada_products_raw.json")
        print("   • lazada_productitem_raw.json")
        print("   • shopee_products_raw.json")
        print("   • shopee_productitem_raw.json")
        
    # Display mapping used
    print(f"\n🔗 Field mappings used:")
    print(f"\n   Lazada Mappings:")
    for lazada_field, unified_field in list(LAZADA_TO_UNIFIED_MAPPING.items())[:5]:
        print(f"      {lazada_field} → {unified_field}")
    print(f"   ... and more")
    
    print(f"\n   Shopee Mappings:")
    for shopee_field, unified_field in list(SHOPEE_TO_UNIFIED_MAPPING.items())[:5]:
        print(f"      {shopee_field} → {unified_field}")
    print(f"   ... and more")