"""
Product Dimension Harmonization Script
Maps Lazada and Shopee product data from raw JSON files to the standardized dimensional model

Data Sources:
- lazada_products_raw.json 
- lazada_productitem_raw.json
- shopee_products_raw.json

Target Schema: Dim_Product table structure
"""

import pandas as pd
import json
import os
import sys

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import get_empty_dataframe, LAZADA_TO_UNIFIED_MAPPING, SHOPEE_TO_UNIFIED_MAPPING, apply_data_types

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

def load_shopee_products():
    """
    Load Shopee product data from raw JSON files
    
    Returns:
        list: products_data as list of dictionaries
    """
    staging_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'Staging')
    
    # Load products raw data
    products_file = os.path.join(staging_dir, 'shopee_products_raw.json')
    
    products_data = []
    
    if os.path.exists(products_file):
        with open(products_file, 'r', encoding='utf-8') as f:
            products_data = json.load(f)
        print(f"✅ Loaded {len(products_data)} products from shopee_products_raw.json")
    else:
        print(f"⚠️ File not found: {products_file}")
    
    return products_data

def load_shopee_orders():
    """
    Load Shopee order data to extract variant information
    
    Returns:
        list: orders_data as list of dictionaries
    """
    staging_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'Staging')
    
    # Load orders raw data
    orders_file = os.path.join(staging_dir, 'shopee_orders_raw.json')
    
    orders_data = []
    
    if os.path.exists(orders_file):
        with open(orders_file, 'r', encoding='utf-8') as f:
            orders_data = json.load(f)
        print(f"✅ Loaded {len(orders_data)} orders from shopee_orders_raw.json for variant extraction")
    else:
        print(f"⚠️ File not found: {orders_file}")
    
    return orders_data

def get_category_name(primary_category):
    """
    Map primary category ID to category name
    This is a placeholder - you may want to maintain a proper category mapping
    
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

def extract_price_from_skus(skus):
    """
    Extract price from SKU data (if available)
    
    Args:
        skus (list): List of SKU objects
        
    Returns:
        float: Price value or None
    """
    if not skus:
        return None
    
    for sku in skus:
        if 'price' in sku:
            try:
                return float(sku['price'])
            except (ValueError, TypeError):
                continue
    return None

def extract_product_variants(product_data, product_key):
    """
    Extract variant data from product SKUs array
    
    Args:
        product_data (dict): Raw product data from Lazada API
        product_key (int): Product key from the main product record
        
    Returns:
        list: List of variant records
    """
    variants = []
    skus = product_data.get('skus', [])
    
    for sku_data in skus:
        variant_record = {
            'variant_key': None,  # Will be generated as surrogate key
            'product_key': product_key,
            'platform_sku_id': str(sku_data.get('SkuId', '')),
            'variant_sku': str(sku_data.get('SellerSku', '')),
            'variant_attribute_1': sku_data.get('Variation1', ''),  # Use Variation1 from mapping
            'variant_attribute_2': sku_data.get('Variation2', ''),  # Use Variation2 from mapping
            'variant_attribute_3': sku_data.get('Variation3', ''),  # Use Variation3 from mapping
            'variant_price': sku_data.get('price', None),
            'variant_stock': sku_data.get('quantity', None),
            'platform_key': 1  # Lazada = 1
        }
        variants.append(variant_record)
    
    return variants

def get_base_sku_from_variants(skus):
    """
    Extract base SKU from the first available variant
    
    Args:
        skus (list): List of SKU objects
        
    Returns:
        str: Base SKU or empty string
    """
    if not skus:
        return ''
    
    # Use the first variant's SellerSku as base, or derive from SkuId
    first_sku = skus[0]
    seller_sku = first_sku.get('SellerSku', '')
    if seller_sku:
        # Extract base part (remove variant suffixes like -RED, -L, etc.)
        base_sku = seller_sku.split('-')[0] if '-' in seller_sku else seller_sku
        return base_sku
    
    # Fallback to SkuId if no SellerSku
    return str(first_sku.get('SkuId', ''))

def harmonize_product_record(product_data, source_file):
    """
    Harmonize a single product record from Lazada format to dimensional model
    
    Args:
        product_data (dict): Raw product data from Lazada API
        source_file (str): Source file identifier ('products' or 'productitem')
        
    Returns:
        dict: Harmonized product record
    """
    # Get product attributes
    attributes = product_data.get('attributes', {})
    
    # Extract price - try multiple sources
    price = None
    if 'skus' in product_data and product_data['skus']:
        price = extract_price_from_skus(product_data['skus'])
    
    # Get base SKU from variants
    base_sku = get_base_sku_from_variants(product_data.get('skus', []))
    
    # Map using LAZADA_TO_UNIFIED_MAPPING
    harmonized_record = {
        'product_key': None,  # Will be generated as surrogate key
        'product_item_id': str(product_data.get('item_id', '')),
        'product_name': attributes.get('name', ''),
        'product_sku_base': base_sku,  # Updated field name
        'product_category': get_category_name(product_data.get('primary_category')),
        'product_status': product_data.get('status', ''),
        'product_price': price,
        'product_rating': None,  # Leave null as requested
        'platform_key': 1  # Lazada = 1
    }
    
    return harmonized_record

def extract_shopee_product_variants(product_data, product_key):
    """
    Extract variant data from Shopee product model_list array
    
    Args:
        product_data (dict): Raw product data from Shopee API
        product_key (int): Product key from the main product record
        
    Returns:
        list: List of variant records
    """
    variants = []
    model_list = product_data.get('model_list', [])
    tier_variation = product_data.get('tier_variation', [])
    
    for model_data in model_list:
        # Get tier indexes
        tier_index = model_data.get('tier_index', [])
        
        # Map tier indexes to variation names
        variant_attr_1 = ''
        variant_attr_2 = ''
        variant_attr_3 = ''
        
        if len(tier_index) > 0 and len(tier_variation) > 0:
            tier_0 = tier_variation[0]
            options_0 = tier_0.get('option_list', [])
            if tier_index[0] < len(options_0):
                variant_attr_1 = options_0[tier_index[0]].get('option', '')
        
        if len(tier_index) > 1 and len(tier_variation) > 1:
            tier_1 = tier_variation[1]
            options_1 = tier_1.get('option_list', [])
            if tier_index[1] < len(options_1):
                variant_attr_2 = options_1[tier_index[1]].get('option', '')
        
        if len(tier_index) > 2 and len(tier_variation) > 2:
            tier_2 = tier_variation[2]
            options_2 = tier_2.get('option_list', [])
            if tier_index[2] < len(options_2):
                variant_attr_3 = options_2[tier_index[2]].get('option', '')
        
        variant_record = {
            'variant_key': None,  # Will be generated as surrogate key
            'product_key': product_key,
            'platform_sku_id': str(model_data.get('model_id', '')),
            'variant_sku': str(model_data.get('model_sku', '')),
            'variant_attribute_1': variant_attr_1,
            'variant_attribute_2': variant_attr_2,
            'variant_attribute_3': variant_attr_3,
            'variant_price': model_data.get('price', None),
            'variant_stock': model_data.get('stock_info', {}).get('current_stock', None) if isinstance(model_data.get('stock_info'), dict) else None,
            'platform_key': 2  # Shopee = 2
        }
        variants.append(variant_record)
    
    return variants

def extract_shopee_variants_from_orders(orders_data, product_key_map):
    """
    Extract Shopee variants from order data
    Since Shopee API doesn't provide model_list in basic product info,
    we extract unique variants from actual orders
    
    Args:
        orders_data (list): List of order dictionaries
        product_key_map (dict): Mapping of item_id to product_key
        
    Returns:
        list: List of variant records
    """
    # Dictionary to store unique variants (key: model_id, value: variant data)
    variants_dict = {}
    
    for order in orders_data:
        item_list = order.get('item_list', [])
        
        for item in item_list:
            item_id = item.get('item_id')
            model_id = item.get('model_id')
            model_sku = item.get('model_sku', '')
            model_name = item.get('model_name', '')
            
            # Skip if no model_id or if product not in our product list
            if not model_id or item_id not in product_key_map:
                continue
            
            # Use model_id as unique identifier
            model_key = f"{item_id}_{model_id}"
            
            if model_key not in variants_dict:
                product_key = product_key_map[item_id]
                
                # Parse model_name to extract variant attributes
                # Format is usually like "Variant1,Variant2" or single variant
                variant_attrs = model_name.split(',') if model_name else []
                variant_attr_1 = variant_attrs[0].strip() if len(variant_attrs) > 0 else ''
                variant_attr_2 = variant_attrs[1].strip() if len(variant_attrs) > 1 else ''
                variant_attr_3 = variant_attrs[2].strip() if len(variant_attrs) > 2 else ''
                
                variant_record = {
                    'variant_key': None,  # Will be generated as surrogate key
                    'product_key': product_key,
                    'platform_sku_id': str(model_id),
                    'variant_sku': str(model_sku),
                    'variant_attribute_1': variant_attr_1,
                    'variant_attribute_2': variant_attr_2,
                    'variant_attribute_3': variant_attr_3,
                    'variant_price': item.get('model_original_price', None),
                    'variant_stock': None,  # Not available in order data
                    'platform_key': 2  # Shopee = 2
                }
                
                variants_dict[model_key] = variant_record
    
    return list(variants_dict.values())

def get_shopee_base_sku_from_variants(model_list):
    """
    Extract base SKU from the first available Shopee variant
    
    Args:
        model_list (list): List of model objects from Shopee
        
    Returns:
        str: Base SKU or empty string
    """
    if not model_list:
        return ''
    
    # Use the first variant's model_sku as base
    first_model = model_list[0]
    model_sku = first_model.get('model_sku', '')
    if model_sku:
        # Extract base part (remove variant suffixes)
        base_sku = model_sku.split('-')[0] if '-' in model_sku else model_sku
        return base_sku
    
    # Fallback to model_id if no model_sku
    return str(first_model.get('model_id', ''))

def harmonize_shopee_product_record(product_data):
    """
    Harmonize a single product record from Shopee format to dimensional model
    
    Args:
        product_data (dict): Raw product data from Shopee API
        
    Returns:
        dict: Harmonized product record
    """
    # Extract price from price_info
    price = None
    price_info = product_data.get('price_info', {})
    if isinstance(price_info, dict):
        current_price = price_info.get('current_price')
        if current_price is not None:
            try:
                price = float(current_price)
            except (ValueError, TypeError):
                price = None
    
    # Get base SKU from model variants
    base_sku = get_shopee_base_sku_from_variants(product_data.get('model_list', []))
    
    # Get category (Shopee uses category_id, may need lookup table)
    category_id = product_data.get('category_id', '')
    category_name = f"Category_{category_id}" if category_id else ''
    
    # Map using SHOPEE_TO_UNIFIED_MAPPING
    harmonized_record = {
        'product_key': None,  # Will be generated as surrogate key
        'product_item_id': str(product_data.get('item_id', '')),
        'product_name': product_data.get('item_name', ''),
        'product_sku_base': base_sku,
        'product_category': category_name,
        'product_status': product_data.get('item_status', ''),
        'product_price': price,
        'product_rating': product_data.get('rating_star', None),
        'platform_key': 2  # Shopee = 2
    }
    
    return harmonized_record

def harmonize_dim_product():
    """
    Main function to harmonize Lazada and Shopee product data into dimensional model
    
    Returns:
        tuple: (product_df, variant_df) - Harmonized product and variant dimension tables
    """
    print("🔄 Starting Product & Variant Dimension Harmonization (Lazada + Shopee)...")
    
    # Get empty DataFrames with proper structure
    dim_product_df = get_empty_dataframe('dim_product')
    dim_product_variant_df = get_empty_dataframe('dim_product_variant')
    
    print(f"📋 Product schema: {list(dim_product_df.columns)}")
    print(f"📋 Variant schema: {list(dim_product_variant_df.columns)}")
    
    # Load raw data from both platforms
    print("\n📥 Loading Lazada data...")
    products_data, productitem_data = load_lazada_products()
    
    print("\n📥 Loading Shopee data...")
    shopee_products_data = load_shopee_products()
    
    # Combine all product data
    all_products = []
    all_variants = []
    product_key_counter = 1
    variant_key_counter = 1
    
    # Process Lazada products
    print(f"\n🔄 Processing Lazada products...")
    
    # Process products_raw.json
    for product in products_data:
        harmonized = harmonize_product_record(product, 'products')
        harmonized['product_key'] = product_key_counter
        all_products.append(harmonized)
        
        # Extract variants for this product
        variants = extract_product_variants(product, product_key_counter)
        for variant in variants:
            variant['variant_key'] = variant_key_counter
            variant_key_counter += 1
        all_variants.extend(variants)
        
        product_key_counter += 1
    
    # Process productitem_raw.json (avoid duplicates by item_id)
    existing_item_ids = {p['product_item_id'] for p in all_products}
    
    for product in productitem_data:
        item_id = str(product.get('item_id', ''))
        if item_id not in existing_item_ids:
            harmonized = harmonize_product_record(product, 'productitem')
            harmonized['product_key'] = product_key_counter
            all_products.append(harmonized)
            
            # Extract variants for this product
            variants = extract_product_variants(product, product_key_counter)
            for variant in variants:
                variant['variant_key'] = variant_key_counter
                variant_key_counter += 1
            all_variants.extend(variants)
            
            product_key_counter += 1
    
    lazada_product_count = len(all_products)
    print(f"✅ Processed {lazada_product_count} Lazada products")
    
    # Process Shopee products
    print(f"\n🔄 Processing Shopee products...")
    
    # Create mapping of item_id to product_key for Shopee variant extraction
    shopee_product_key_map = {}
    
    for product in shopee_products_data:
        harmonized = harmonize_shopee_product_record(product)
        harmonized['product_key'] = product_key_counter
        all_products.append(harmonized)
        
        # Store mapping for variant extraction from orders
        item_id = product.get('item_id')
        if item_id:
            shopee_product_key_map[item_id] = product_key_counter
        
        # Extract variants for this product (from model_list if available)
        variants = extract_shopee_product_variants(product, product_key_counter)
        for variant in variants:
            variant['variant_key'] = variant_key_counter
            variant_key_counter += 1
        all_variants.extend(variants)
        
        product_key_counter += 1
    
    shopee_product_count = len(all_products) - lazada_product_count
    print(f"✅ Processed {shopee_product_count} Shopee products")
    
    # Extract Shopee variants from order data
    print(f"\n🔄 Extracting Shopee variants from order data...")
    shopee_orders_data = load_shopee_orders()
    shopee_variants_from_orders = extract_shopee_variants_from_orders(shopee_orders_data, shopee_product_key_map)
    
    # Add unique variants from orders
    existing_shopee_variant_ids = {v['platform_sku_id'] for v in all_variants if v['platform_key'] == 2}
    new_variants_count = 0
    
    for variant in shopee_variants_from_orders:
        if variant['platform_sku_id'] not in existing_shopee_variant_ids:
            variant['variant_key'] = variant_key_counter
            variant_key_counter += 1
            all_variants.append(variant)
            existing_shopee_variant_ids.add(variant['platform_sku_id'])
            new_variants_count += 1
    
    print(f"✅ Extracted {new_variants_count} additional Shopee variants from {len(shopee_orders_data)} orders")
    
    # Convert products to DataFrame
    if all_products:
        dim_product_df = pd.DataFrame(all_products)
        
        # Apply proper data types according to schema
        dim_product_df = apply_data_types(dim_product_df, 'dim_product')
        
        # Convert variants to DataFrame
        if all_variants:
            dim_product_variant_df = pd.DataFrame(all_variants)
            dim_product_variant_df = apply_data_types(dim_product_variant_df, 'dim_product_variant')
        
        print(f"\n✅ Harmonized {len(dim_product_df)} products and {len(dim_product_variant_df)} variants")
        
        print(f"\n📊 Product Summary:")
        print(f"   - Total products: {len(dim_product_df)}")
        print(f"   - Lazada products: {len(dim_product_df[dim_product_df['platform_key'] == 1])}")
        print(f"   - Shopee products: {len(dim_product_df[dim_product_df['platform_key'] == 2])}")
        print(f"   - Products with prices: {len(dim_product_df[dim_product_df['product_price'].notna()])}")
        print(f"   - Unique categories: {dim_product_df['product_category'].nunique()}")
        
        print(f"\n📊 Variant Summary:")
        print(f"   - Total variants: {len(dim_product_variant_df)}")
        print(f"   - Lazada variants: {len(dim_product_variant_df[dim_product_variant_df['platform_key'] == 1])}")
        print(f"   - Shopee variants: {len(dim_product_variant_df[dim_product_variant_df['platform_key'] == 2])}")
        
        # Show sample of data from each platform
        print("\n📋 Sample of Lazada products:")
        lazada_df = dim_product_df[dim_product_df['platform_key'] == 1]
        if not lazada_df.empty:
            sample_cols = ['product_key', 'product_item_id', 'product_name', 'product_sku_base', 'product_status', 'product_price', 'platform_key']
            available_cols = [col for col in sample_cols if col in lazada_df.columns]
            print(lazada_df[available_cols].head(3).to_string(index=False))
        
        print("\n📋 Sample of Shopee products:")
        shopee_df = dim_product_df[dim_product_df['platform_key'] == 2]
        if not shopee_df.empty:
            sample_cols = ['product_key', 'product_item_id', 'product_name', 'product_sku_base', 'product_status', 'product_price', 'platform_key']
            available_cols = [col for col in sample_cols if col in shopee_df.columns]
            print(shopee_df[available_cols].head(3).to_string(index=False))
        
        if len(dim_product_variant_df) > 0:
            print("\n📋 Sample of Lazada variants:")
            lazada_variants = dim_product_variant_df[dim_product_variant_df['platform_key'] == 1]
            if not lazada_variants.empty:
                variant_cols = ['variant_key', 'product_key', 'platform_sku_id', 'variant_sku', 'variant_attribute_1', 'variant_attribute_2', 'platform_key']
                available_variant_cols = [col for col in variant_cols if col in lazada_variants.columns]
                print(lazada_variants[available_variant_cols].head(3).to_string(index=False))
            
            print("\n📋 Sample of Shopee variants:")
            shopee_variants = dim_product_variant_df[dim_product_variant_df['platform_key'] == 2]
            if not shopee_variants.empty:
                variant_cols = ['variant_key', 'product_key', 'platform_sku_id', 'variant_sku', 'variant_attribute_1', 'variant_attribute_2', 'platform_key']
                available_variant_cols = [col for col in variant_cols if col in shopee_variants.columns]
                print(shopee_variants[available_variant_cols].head(3).to_string(index=False))
        
    else:
        print("⚠️ No product data found to harmonize")
    
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
    # Run the harmonization process
    product_df, variant_df = harmonize_dim_product()
    
    if not product_df.empty:
        product_file, variant_file = save_harmonized_data(product_df, variant_df)
        print(f"\n🎉 Product & Variant dimension harmonization completed successfully!")
        print(f"📁 Product file: {product_file}")
        print(f"📁 Variant file: {variant_file}")
    else:
        print("❌ No data was harmonized. Please check your source files.")
        
    # Display mappings used
    print(f"\n🔗 Field mappings used:")
    
    print(f"\nLazada Product mappings:")
    lazada_product_mappings = {
        'item_id': 'product_item_id',
        'attributes.name': 'product_name',
        'primary_category': 'product_category',
        'status': 'product_status',
        'skus[].price': 'product_price',
        'skus[].SellerSku': 'product_sku_base'
    }
    for lazada_field, unified_field in lazada_product_mappings.items():
        print(f"   {lazada_field} → {unified_field}")
    
    print(f"\nShopee Product mappings:")
    shopee_product_mappings = {
        'item_id': 'product_item_id',
        'item_name': 'product_name',
        'category_id': 'product_category',
        'item_status': 'product_status',
        'price_info.current_price': 'product_price',
        'rating_star': 'product_rating',
        'model_list[].model_sku': 'product_sku_base'
    }
    for shopee_field, unified_field in shopee_product_mappings.items():
        print(f"   {shopee_field} → {unified_field}")
    
    print(f"\nLazada Variant mappings:")
    lazada_variant_mappings = {
        'skus[].SkuId': 'platform_sku_id',
        'skus[].SellerSku': 'variant_sku',
        'skus[].Variation1': 'variant_attribute_1',
        'skus[].Variation2': 'variant_attribute_2',
        'skus[].Variation3': 'variant_attribute_3'
    }
    for lazada_field, unified_field in lazada_variant_mappings.items():
        print(f"   {lazada_field} → {unified_field}")
    
    print(f"\nShopee Variant mappings:")
    shopee_variant_mappings = {
        'model_list[].model_id': 'platform_sku_id',
        'model_list[].model_sku': 'variant_sku',
        'tier_index[0]': 'variant_attribute_1',
        'tier_index[1]': 'variant_attribute_2',
        'tier_index[2]': 'variant_attribute_3'
    }
    for shopee_field, unified_field in shopee_variant_mappings.items():
        print(f"   {shopee_field} → {unified_field}")
