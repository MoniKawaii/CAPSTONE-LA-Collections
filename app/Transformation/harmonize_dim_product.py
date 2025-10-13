"""
Product Dimension Harmonization Script
Maps Lazada product data from raw JSON files to the standardized dimensional model

Data Sources:
- lazada_products_raw.json 
- lazada_productitem_raw.json

Target Schema: Dim_Product table structure
"""

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

def harmonize_dim_product():
    """
    Main function to harmonize Lazada product data into dimensional model
    
    Returns:
        tuple: (product_df, variant_df) - Harmonized product and variant dimension tables
    """
    print("🔄 Starting Product & Variant Dimension Harmonization...")
    
    # Get empty DataFrames with proper structure
    dim_product_df = get_empty_dataframe('dim_product')
    dim_product_variant_df = get_empty_dataframe('dim_product_variant')
    
    print(f"📋 Product schema: {list(dim_product_df.columns)}")
    print(f"📋 Variant schema: {list(dim_product_variant_df.columns)}")
    
    # Load raw data
    products_data, productitem_data = load_lazada_products()
    
    # Combine all product data
    all_products = []
    all_variants = []
    
    # Process products_raw.json
    for product in products_data:
        harmonized = harmonize_product_record(product, 'products')
        all_products.append(harmonized)
    
    # Process productitem_raw.json (avoid duplicates by item_id)
    existing_item_ids = {p['product_item_id'] for p in all_products}
    
    for product in productitem_data:
        item_id = str(product.get('item_id', ''))
        if item_id not in existing_item_ids:
            harmonized = harmonize_product_record(product, 'productitem')
            all_products.append(harmonized)
        else:
            print(f"⚠️ Skipping duplicate item_id: {item_id}")
    
    # Convert products to DataFrame and generate keys
    if all_products:
        dim_product_df = pd.DataFrame(all_products)
        
        # Generate surrogate keys (product_key)
        dim_product_df['product_key'] = range(1, len(dim_product_df) + 1)
        
        # Apply proper data types according to schema
        dim_product_df = apply_data_types(dim_product_df, 'dim_product')
        
        # Now extract variants using the generated product_keys
        all_raw_products = products_data + [p for p in productitem_data 
                                          if str(p.get('item_id', '')) not in existing_item_ids]
        
        variant_key_counter = 1
        for idx, product in enumerate(all_raw_products):
            if idx < len(dim_product_df):  # Ensure we don't exceed product count
                product_key = dim_product_df.iloc[idx]['product_key']
                variants = extract_product_variants(product, product_key)
                
                # Add variant keys
                for variant in variants:
                    variant['variant_key'] = variant_key_counter
                    variant_key_counter += 1
                
                all_variants.extend(variants)
        
        # Convert variants to DataFrame
        if all_variants:
            dim_product_variant_df = pd.DataFrame(all_variants)
            dim_product_variant_df = apply_data_types(dim_product_variant_df, 'dim_product_variant')
        
        print(f"✅ Harmonized {len(dim_product_df)} products and {len(dim_product_variant_df)} variants")
        print(f"📊 Product Summary:")
        print(f"   - Active products: {len(dim_product_df[dim_product_df['product_status'] == 'Active'])}")
        print(f"   - Inactive products: {len(dim_product_df[dim_product_df['product_status'] == 'InActive'])}")
        print(f"   - Products with prices: {len(dim_product_df[dim_product_df['product_price'].notna()])}")
        print(f"   - Unique categories: {dim_product_df['product_category'].nunique()}")
        
        print(f"📊 Variant Summary:")
        print(f"   - Total variants: {len(dim_product_variant_df)}")
        if len(dim_product_variant_df) > 0:
            print(f"   - Variants with colors: {len(dim_product_variant_df[dim_product_variant_df['variant_attribute_1'] != ''])}")
            print(f"   - Variants with sizes: {len(dim_product_variant_df[dim_product_variant_df['variant_attribute_2'] != ''])}")
            print(f"   - Variants with materials: {len(dim_product_variant_df[dim_product_variant_df['variant_attribute_3'] != ''])}")
        
        # Show sample of data
        print("\n📋 Sample of harmonized product data:")
        sample_cols = ['product_key', 'product_item_id', 'product_name', 'product_sku_base', 'product_status', 'product_price', 'platform_key']
        available_cols = [col for col in sample_cols if col in dim_product_df.columns]
        print(dim_product_df[available_cols].head(3).to_string(index=False))
        
        if len(dim_product_variant_df) > 0:
            print("\n📋 Sample of harmonized variant data:")
            variant_cols = ['variant_key', 'product_key', 'platform_sku_id', 'variant_sku', 'variant_attribute_1', 'variant_attribute_2']
            available_variant_cols = [col for col in variant_cols if col in dim_product_variant_df.columns]
            print(dim_product_variant_df[available_variant_cols].head(3).to_string(index=False))
        
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
        
    # Display mapping used
    print(f"\n🔗 Field mappings used:")
    for lazada_field, unified_field in LAZADA_TO_UNIFIED_MAPPING.items():
        print(f"   {lazada_field} → {unified_field}")