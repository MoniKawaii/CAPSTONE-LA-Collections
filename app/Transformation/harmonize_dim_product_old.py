"""
Product Dimension Harmonization Script
Maps Lazada and Shopee product data from raw JSON files to the standardized dimensional model

Data Sources:
Lazada:
- lazada_products_raw.json 
- lazada_productitem_raw.json
- lazada_productreview_raw.json

Shopee:
- shopee_products_raw.json
- shopee_productitem_raw.json  
- shopee_product_variant_raw.json
- shopee_productcategory_raw.json
- shopee_productreview_raw.json

Target Schema: Dim_Product and Dim_Product_Variant table structures
"""

import os
import sys
import json
import pandas as pd
from datetime import datetime

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import get_empty_dataframe, LAZADA_TO_UNIFIED_MAPPING, SHOPEE_TO_UNIFIED_MAPPING, apply_data_types, get_staging_filename

def load_lazada_data():
    """
    Load all Lazada product data from raw JSON files
    
    Returns:
        tuple: (products_data, productitem_data) as lists of dictionaries
    """
    staging_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'Staging')
    
    # Load products raw data
    products_file = os.path.join(staging_dir, get_staging_filename('lazada', 'products'))
    productitem_file = os.path.join(staging_dir, get_staging_filename('lazada', 'productitem'))
    
    products_data = []
    productitem_data = []
    
    if os.path.exists(products_file):
        with open(products_file, 'r', encoding='utf-8') as f:
            products_data = json.load(f)
            print(f"✅ Loaded {len(products_data)} products from {os.path.basename(products_file)}")
    else:
        print(f"⚠️ File not found: {products_file}")
    
    if os.path.exists(productitem_file):
        with open(productitem_file, 'r', encoding='utf-8') as f:
            productitem_data = json.load(f)
            print(f"✅ Loaded {len(productitem_data)} product items from {os.path.basename(productitem_file)}")
    else:
        print(f"⚠️ File not found: {productitem_file}")
    
    return products_data, productitem_data

def load_shopee_data():
    """
    Load all Shopee product data from raw JSON files
    
    Returns:
        tuple: (products_data, productitem_data, variant_data, category_data, review_data)
    """
    staging_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'Staging')
    
    # Define all Shopee files
    files_to_load = {
        'products': get_staging_filename('shopee', 'products'),
        'productitem': get_staging_filename('shopee', 'productitem'),
        'product_variant': get_staging_filename('shopee', 'product_variant'),
        'productcategory': get_staging_filename('shopee', 'productcategory'),
        'productreview': get_staging_filename('shopee', 'productreview')
    }
    
    loaded_data = {}
    
    for data_type, filename in files_to_load.items():
        file_path = os.path.join(staging_dir, filename)
        
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    loaded_data[data_type] = data
                    print(f"✅ Loaded {len(data)} records from {filename}")
            except Exception as e:
                print(f"⚠️ Error loading {filename}: {e}")
                loaded_data[data_type] = []
        else:
            print(f"⚠️ File not found: {filename}")
            loaded_data[data_type] = []
    
    return (
        loaded_data.get('products', []),
        loaded_data.get('productitem', []),
        loaded_data.get('product_variant', []),
        loaded_data.get('productcategory', []),
        loaded_data.get('productreview', [])
    )

def get_lazada_category_name(primary_category):
    """
    Map Lazada primary category ID to category name
    
    Args:
        primary_category (int): Category ID from Lazada
        
    Returns:
        str: Category name
    """
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

def get_shopee_category_name(category_id, category_data):
    """
    Map Shopee category ID to category name from shopee_productcategory_raw.json
    
    Args:
        category_id (int): Category ID from Shopee
        category_data (list): List of category records from shopee_productcategory_raw.json
        
    Returns:
        str: Category name
    """
    if not category_data:
        return f"Category_{category_id}"
    
    # Create category mapping from the loaded data
    category_mapping = {}
    for category in category_data:
        if isinstance(category, dict) and 'category_id' in category and 'category_name' in category:
            category_mapping[category['category_id']] = category['category_name']
    
    return category_mapping.get(category_id, f"Category_{category_id}")

def extract_lazada_price_from_skus(skus):
    """
    Extract price from Lazada SKU data
    
    Args:
        skus (list): List of SKU objects
        
    Returns:
        float: Price value or None
    """
    if not skus:
        return None
    
    for sku in skus:
        if isinstance(sku, dict) and 'price' in sku:
            try:
                return float(sku['price'])
            except (ValueError, TypeError):
                continue
    return None

def get_lazada_base_sku_from_variants(skus):
    """
    Extract base SKU from Lazada variants
    
    Args:
        skus (list): List of SKU objects
        
    Returns:
        str: Base SKU or empty string
    """
    if not skus:
        return ""
    
    # Use the first variant's SellerSku as base
    first_sku = skus[0]
    if isinstance(first_sku, dict):
        seller_sku = first_sku.get('SellerSku', '')
        if seller_sku:
            # Remove variant suffixes like -1752677181110-0
            return seller_sku.split('-')[0] if '-' in seller_sku else seller_sku
    
    return str(first_sku.get('SkuId', '')) if isinstance(first_sku, dict) else ""

def get_shopee_base_sku_from_variants(variant_data, item_id):
    """
    Extract base SKU from Shopee variants for a specific item
    
    Args:
        variant_data (list): List of variant records
        item_id: Item ID to find variants for
        
    Returns:
        str: Base SKU or empty string
    """
    if not variant_data:
        return ""
    
    for variant_record in variant_data:
        if isinstance(variant_record, dict) and variant_record.get('item_id') == item_id:
            model_list = variant_record.get('model_list', [])
            if model_list and len(model_list) > 0:
                first_model = model_list[0]
                if isinstance(first_model, dict):
                    model_sku = first_model.get('model_sku', '')
                    if model_sku:
                        # Remove variant suffixes and return base SKU
                        return model_sku.split('-')[0] if '-' in model_sku else model_sku
    
    return ""

def extract_lazada_product_variants(product_data, product_key, variant_key_counter):
    """
    Extract variant data from Lazada product SKUs array
    
    Args:
        product_data (dict): Raw product data from Lazada API
        product_key (float): Product key from the main product record
        variant_key_counter (dict): Global variant counter with 'current' key
        
    Returns:
        list: List of variant records
    """
    variants = []
    skus = product_data.get('skus', [])
    platform_key = 1  # Lazada
    
    for idx, sku_data in enumerate(skus):
        if isinstance(sku_data, dict):
            # Generate simple variant key: sequential number + platform decimal (.1 for Lazada)
            variant_key = variant_key_counter['current'] + 0.1
            variant_key_counter['current'] += 1.0
            
            variant = {
                'product_variant_key': variant_key,
                'product_key': product_key,
                'platform_sku_id': str(sku_data.get('SkuId', '')),
                'variant_sku': sku_data.get('SellerSku', ''),
                'variant_attribute_1': sku_data.get('Variation1', ''),
                'variant_attribute_2': sku_data.get('Variation2', ''),
                'variant_attribute_3': sku_data.get('Variation3', ''),
                'platform_key': platform_key
            }
            variants.append(variant)
    
    return variants

def extract_shopee_product_variants(variant_data, product_key, item_id, variant_key_counter):
    """
    Extract variant data from Shopee model list for a specific product
    
    Args:
        variant_data (list): All variant data from shopee_product_variant_raw.json
        product_key (float): Product key from the main product record
        item_id: Item ID to find variants for
        variant_key_counter (dict): Global variant counter with 'current' key
        
    Returns:
        list: List of variant records
    """
    variants = []
    platform_key = 2  # Shopee
    
    for variant_record in variant_data:
        if isinstance(variant_record, dict) and variant_record.get('item_id') == item_id:
            model_list = variant_record.get('model_list', [])
            
            for idx, model in enumerate(model_list):
                if isinstance(model, dict):
                    # Generate simple variant key: sequential number + platform decimal (.2 for Shopee)
                    variant_key = variant_key_counter['current'] + 0.2
                    variant_key_counter['current'] += 1.0
                    
                    # Extract tier_index attributes
                    tier_index = model.get('tier_index', [])
                    attr1 = str(tier_index[0]) if len(tier_index) > 0 else ''
                    attr2 = str(tier_index[1]) if len(tier_index) > 1 else ''
                    attr3 = str(tier_index[2]) if len(tier_index) > 2 else ''
                    
                    variant = {
                        'product_variant_key': variant_key,
                        'product_key': product_key,
                        'platform_sku_id': str(model.get('model_id', '')),
                        'variant_sku': model.get('model_sku', ''),
                        'variant_attribute_1': attr1,
                        'variant_attribute_2': attr2,
                        'variant_attribute_3': attr3,
                        'platform_key': platform_key
                    }
                    variants.append(variant)
            break  # Found the item, no need to continue
    
    return variants

def harmonize_lazada_product_record(product_data, product_key):
    """
    Harmonize a single Lazada product record to dimensional model
    
    Args:
        product_data (dict): Raw product data from Lazada API
        product_key (float): Generated product key
        
    Returns:
        dict: Harmonized product record
    """
    attributes = product_data.get('attributes', {})
    
    # Extract price from SKUs
    price = extract_lazada_price_from_skus(product_data.get('skus', []))
    
    # Get base SKU from variants
    base_sku = get_lazada_base_sku_from_variants(product_data.get('skus', []))
    
    return {
        'product_key': product_key,
        'product_item_id': str(product_data.get('item_id', '')),
        'product_name': attributes.get('name', ''),
        'product_sku_base': base_sku,
        'product_category': get_lazada_category_name(product_data.get('primary_category')),
        'product_status': product_data.get('status', 'Unknown'),
        'product_price': price,
        'product_rating': None,  # Lazada doesn't provide rating in products endpoint
        'platform_key': 1  # Lazada
    }

def harmonize_shopee_product_record(product_data, product_key, productitem_data, variant_data, category_data):
    """
    Harmonize a single Shopee product record to dimensional model
    
    Args:
        product_data (dict): Raw product data from Shopee API
        product_key (float): Generated product key
        productitem_data (list): Product item data for additional details
        variant_data (list): Variant data for SKU extraction
        category_data (list): Category data for name mapping
        
    Returns:
        dict: Harmonized product record
    """
    item_id = product_data.get('item_id')
    
    # Find matching product item data for additional details
    product_item = None
    for item in productitem_data:
        if isinstance(item, dict) and item.get('item_id') == item_id:
            product_item = item
            break
    
    # Get category name from category mapping
    category_name = get_shopee_category_name(product_data.get('category_id'), category_data)
    
    # Get base SKU from variants or product item
    base_sku = get_shopee_base_sku_from_variants(variant_data, item_id)
    if not base_sku and product_item:
        base_sku = product_item.get('item_sku', '')
    
    # Extract price - prioritize from price_info array
    price = None
    price_info = product_data.get('price_info', [])
    if price_info and len(price_info) > 0 and isinstance(price_info[0], dict):
        price = price_info[0].get('current_price')
    
    # Try to convert price to float
    if price is not None:
        try:
            price = float(price)
        except (ValueError, TypeError):
            price = None
    
    return {
        'product_key': product_key,
        'product_item_id': str(item_id or ''),
        'product_name': product_data.get('item_name', ''),
        'product_sku_base': base_sku,
        'product_category': category_name,
        'product_status': product_data.get('item_status', 'Unknown'),
        'product_price': price,
        'product_rating': product_data.get('rating_star'),
        'platform_key': 2  # Shopee
    }

def harmonize_dim_product():
    """
    Main harmonization function for product and variant dimensions
    """
    print("🔄 Starting Product & Variant Dimension Harmonization (Lazada + Shopee)...")
    
    # Initialize empty dataframes
    product_df = get_empty_dataframe('dim_product')
    variant_df = get_empty_dataframe('dim_product_variant')
    
    print(f"📋 Product schema: {product_df.columns.tolist()}")
    print(f"📋 Variant schema: {variant_df.columns.tolist()}")
    
    # Load data
    print("\n📥 Loading Lazada data...")
    lazada_products, lazada_productitem = load_lazada_data()
    
    print("\n📥 Loading Shopee data...")
    shopee_products, shopee_productitem, shopee_variants, shopee_categories, shopee_reviews = load_shopee_data()
    
    # Process Lazada products
    print("\n🔄 Processing Lazada products...")
    product_key_counter = 1.0
    variant_key_counter = {'current': 1.0}  # Global variant counter
    
    for product_data in lazada_products:
        if isinstance(product_data, dict):
            # Generate Lazada product key (1.1, 2.1, 3.1, etc.)
            lazada_product_key = product_key_counter + 0.1
            
            # Harmonize product record
            harmonized_product = harmonize_lazada_product_record(product_data, lazada_product_key)
            product_df = pd.concat([product_df, pd.DataFrame([harmonized_product])], ignore_index=True)
            
            # Extract variants with global counter
            variants = extract_lazada_product_variants(product_data, lazada_product_key, variant_key_counter)
            if variants:
                variant_df = pd.concat([variant_df, pd.DataFrame(variants)], ignore_index=True)
            
            product_key_counter += 1.0
    
    lazada_count = len(lazada_products)
    print(f"✅ Processed {lazada_count} Lazada products")
    
    # Process Shopee products
    print("\n🔄 Processing Shopee products...")
    
    for product_data in shopee_products:
        if isinstance(product_data, dict):
            # Generate Shopee product key (1.2, 2.2, 3.2, etc.)
            shopee_product_key = product_key_counter + 0.2
            
            # Harmonize product record
            harmonized_product = harmonize_shopee_product_record(
                product_data, shopee_product_key, shopee_productitem, shopee_variants, shopee_categories
            )
            product_df = pd.concat([product_df, pd.DataFrame([harmonized_product])], ignore_index=True)
            
            # Extract variants with global counter
            variants = extract_shopee_product_variants(shopee_variants, shopee_product_key, product_data.get('item_id'), variant_key_counter)
            if variants:
                variant_df = pd.concat([variant_df, pd.DataFrame(variants)], ignore_index=True)
            
            product_key_counter += 1.0
    
    shopee_count = len(shopee_products)
    print(f"✅ Processed {shopee_count} Shopee products")
    
    # Apply data types
    product_df = apply_data_types(product_df, 'dim_product')
    variant_df = apply_data_types(variant_df, 'dim_product_variant')
    
    print(f"\n✅ Harmonized {len(product_df)} products and {len(variant_df)} variants")
    
    # Summary statistics
    print(f"\n📊 Product Summary:")
    print(f"   - Total products: {len(product_df)}")
    print(f"   - Lazada products: {lazada_count}")
    print(f"   - Shopee products: {shopee_count}")
    print(f"   - Products with prices: {product_df['product_price'].notna().sum()}")
    print(f"   - Unique categories: {product_df['product_category'].nunique()}")
    
    print(f"\n📊 Variant Summary:")
    print(f"   - Total variants: {len(variant_df)}")
    print(f"   - Lazada variants: {len(variant_df[variant_df['platform_key'] == 1])}")
    print(f"   - Shopee variants: {len(variant_df[variant_df['platform_key'] == 2])}")
    
    # Show samples
    print(f"\n📋 Sample of Lazada products:")
    lazada_products_sample = product_df[product_df['platform_key'] == 1].head(3)
    if not lazada_products_sample.empty:
        print(lazada_products_sample[['product_key', 'product_item_id', 'product_name', 'product_sku_base', 'product_status', 'product_price', 'platform_key']].to_string())
    
    print(f"\n📋 Sample of Shopee products:")
    shopee_products_sample = product_df[product_df['platform_key'] == 2].head(3)
    if not shopee_products_sample.empty:
        print(shopee_products_sample[['product_key', 'product_item_id', 'product_name', 'product_sku_base', 'product_status', 'product_price', 'platform_key']].to_string())
    
    print(f"\n📋 Sample of Lazada variants:")
    lazada_variants_sample = variant_df[variant_df['platform_key'] == 1].head(3)
    if not lazada_variants_sample.empty:
        print(lazada_variants_sample[['product_variant_key', 'product_key', 'platform_sku_id', 'variant_sku', 'variant_attribute_1', 'variant_attribute_2', 'platform_key']].to_string())
    
    print(f"\n📋 Sample of Shopee variants:")
    shopee_variants_sample = variant_df[variant_df['platform_key'] == 2].head(3)
    if not shopee_variants_sample.empty:
        print(shopee_variants_sample[['product_variant_key', 'product_key', 'platform_sku_id', 'variant_sku', 'variant_attribute_1', 'variant_attribute_2', 'platform_key']].to_string())
    
    return product_df, variant_df

def save_harmonized_data(product_df, variant_df, output_dir=None):
    """
    Save harmonized data to CSV files
    
    Args:
        product_df (pd.DataFrame): Harmonized product data
        variant_df (pd.DataFrame): Harmonized variant data
        output_dir (str): Output directory path (optional)
    """
    if output_dir is None:
        output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'Transformed')
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Save files
    product_file = os.path.join(output_dir, 'dim_product.csv')
    variant_file = os.path.join(output_dir, 'dim_product_variant.csv')
    
    product_df.to_csv(product_file, index=False)
    variant_df.to_csv(variant_file, index=False)
    
    print(f"\n💾 Saved harmonized product data to: {product_file}")
    print(f"💾 Saved harmonized variant data to: {variant_file}")
    
    return product_file, variant_file

if __name__ == "__main__":
    try:
        # Run harmonization
        product_df, variant_df = harmonize_dim_product()
        
        # Save results
        product_file, variant_file = save_harmonized_data(product_df, variant_df)
        
        print(f"\n🎉 Product & Variant dimension harmonization completed successfully!")
        print(f"📁 Product file: {product_file}")
        print(f"📁 Variant file: {variant_file}")
        
        # Show field mappings used
        print(f"\n🔗 Field mappings used:")
        print(f"\nLazada Product mappings:")
        for source, target in LAZADA_TO_UNIFIED_MAPPING.items():
            if 'product' in target.lower() and 'variant' not in target.lower():
                print(f"   {source} → {target}")
        
        print(f"\nShopee Product mappings:")
        for source, target in SHOPEE_TO_UNIFIED_MAPPING.items():
            if 'product' in target.lower() and 'variant' not in target.lower():
                print(f"   {source} → {target}")
        
        print(f"\nLazada Variant mappings:")
        for source, target in LAZADA_TO_UNIFIED_MAPPING.items():
            if 'variant' in target.lower() or 'Variation' in source or 'Sku' in source:
                print(f"   {source} → {target}")
        
        print(f"\nShopee Variant mappings:")
        for source, target in SHOPEE_TO_UNIFIED_MAPPING.items():
            if 'variant' in target.lower() or 'model' in source.lower() or 'tier_index' in source:
                print(f"   {source} → {target}")
                
    except Exception as e:
        print(f"❌ Error during harmonization: {e}")
        import traceback
        traceback.print_exc()