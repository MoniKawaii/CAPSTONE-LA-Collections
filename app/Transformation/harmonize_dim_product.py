"""
Product Dimension Harmonization Script
Maps Lazada product data from raw JSON files to the standardized dimensional model

Data Sources:
- lazada_products_raw.json 
- lazada_productitem_raw.json

Target Schema: Dim_Product and Dim_Product_Variant tables
"""

import pandas as pd
import json
import os
import sys

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# Assuming config.py provides: 
# get_empty_dataframe(table_name)
# LAZADA_TO_UNIFIED_MAPPING
# apply_data_types(df, table_name)
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
    Map primary category ID to category name (Placeholder implementation)
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
    Extract price from the first available SKU.
    """
    if not skus:
        return None
    
    for sku in skus:
        if 'price' in sku:
            try:
                # Assuming the primary price for Dim_Product is the minimum/first available price
                return float(sku['price'])
            except (ValueError, TypeError):
                continue
    return None

def extract_base_sku(skus):
    """
    Extract base SKU from the first available variant, stripping suffixes if present.
    """
    if not skus:
        return ''
    
    first_sku = skus[0]
    seller_sku = first_sku.get('SellerSku', '')
    if seller_sku:
        # Extract base part (remove variant suffixes like -RED, -L, etc.)
        base_sku = seller_sku.split('-')[0] if '-' in seller_sku else seller_sku
        return base_sku
    
    # Fallback to SkuId if no SellerSku
    return str(first_sku.get('SkuId', ''))


def extract_product_variants(product_data, product_key):
    """
    Extract variant data from product SKUs array, handling single-variant products
    and multiple, dynamic 'saleProp' attributes.
    
    Crucially, ensures that even products without 'saleProp' or 'skus' array 
    still result in one variant record (The Universal Rule).
    
    Args:
        product_data (dict): Raw product data from Lazada API
        product_key (int): Product key from the main product record
        
    Returns:
        list: List of variant records
    """
    variants = []
    skus = product_data.get('skus', [])
    item_id = str(product_data.get('item_id', ''))
    
    # If no SKUs are listed (e.g., single-item product or bad data), 
    # create a single synthetic SKU to guarantee linkage.
    if not skus:
        base_price = extract_price_from_skus(product_data.get('skus', []))
        skus = [{
            'SkuId': item_id, 
            'SellerSku': extract_base_sku([]), # Base SKU will be derived from item_id in this case
            'price': base_price,
            'quantity': 0, 
            'saleProp': {} # Empty saleProp forces empty attributes below
        }]

    for sku_data in skus:
        sale_props = sku_data.get('saleProp', {})
        
        # 1. Prioritize dynamic extraction from saleProp dictionary values
        prop_values = list(sale_props.values())
        
        # 2. Fallback to generic Variation1, Variation2 fields if saleProp is empty
        if not prop_values:
            prop_values = [
                sku_data.get('Variation1'),
                sku_data.get('Variation2'),
                sku_data.get('Variation3')
            ]
            prop_values = [v for v in prop_values if v is not None and v != ''] # Filter out blanks

        # 3. Assign attributes, using an empty string ('') as the placeholder
        attr1 = str(prop_values[0]) if len(prop_values) > 0 else ''
        attr2 = str(prop_values[1]) if len(prop_values) > 1 else ''
        attr3 = str(prop_values[2]) if len(prop_values) > 2 else ''
        
        variant_record = {
            'variant_key': None, 
            'product_key': product_key,
            'platform_sku_id': str(sku_data.get('SkuId', item_id)),
            'variant_sku': str(sku_data.get('SellerSku', extract_base_sku(skus))),
            'variant_attribute_1': attr1,
            'variant_attribute_2': attr2,
            'variant_attribute_3': attr3,
            'variant_price': sku_data.get('price', None),
            'platform_key': 1
        }
        variants.append(variant_record)
    
    return variants

def harmonize_product_record(product_data, source_file):
    """
    Harmonize a single product record from Lazada format to dimensional model (Dim_Product)
    """
    attributes = product_data.get('attributes', {})
    price = extract_price_from_skus(product_data.get('skus', []))
    base_sku = extract_base_sku(product_data.get('skus', []))
    
    harmonized_record = {
        'product_key': None, 
        'product_item_id': str(product_data.get('item_id', '')),
        'product_name': attributes.get('name', ''),
        'product_sku_base': base_sku, 
        'product_category': get_category_name(product_data.get('primary_category')),
        'product_status': product_data.get('status', ''),
        'product_price': price,
        'product_rating': None, 
        'platform_key': 1 
    }
    
    return harmonized_record

def validate_product_variant_linkage(product_df, variant_df):
    """
    Validation check to ensure every product in Dim_Product has at least one 
    corresponding record in Dim_Product_Variant via product_key.
    
    Returns:
        bool: True if validation passes, False otherwise.
    """
    if product_df.empty:
        print("✅ Validation successful: No products found, validation trivially passes.")
        return True

    # 1. Get all product_keys from Dim_Product
    all_product_keys = set(product_df['product_key'].dropna().unique())
    
    # 2. Get unique product_keys present in Dim_Product_Variant
    linked_product_keys = set(variant_df['product_key'].dropna().unique())

    # 3. Find the set difference (products that exist but have no variant link)
    unlinked_keys = all_product_keys - linked_product_keys
    
    if not unlinked_keys:
        print("✅ Validation successful: All products in Dim_Product have at least one corresponding record in Dim_Product_Variant.")
        return True
    else:
        print(f"❌ Validation FAILED: Found {len(unlinked_keys)} products in Dim_Product with no corresponding variants.")
        unlinked_item_ids = product_df[product_df['product_key'].isin(unlinked_keys)]['product_item_id'].tolist()
        print(f"   Unlinked Product Keys (Sample): {list(unlinked_keys)[:5]}...")
        print(f"   Unlinked Item IDs (Sample): {unlinked_item_ids[:5]}...")
        return False


def harmonize_dim_product():
    """
    Main function to harmonize Lazada product data into dimensional model.
    """
    print("🔄 Starting Product & Variant Dimension Harmonization...")
    
    # Initialize empty DataFrames with proper structure
    dim_product_df = get_empty_dataframe('dim_product')
    dim_product_variant_df = get_empty_dataframe('dim_product_variant')
    
    print(f"📋 Product schema: {list(dim_product_df.columns)}")
    print(f"📋 Variant schema: {list(dim_product_variant_df.columns)}")
    
    products_data, productitem_data = load_lazada_products()
    
    all_products = []
    all_raw_data = [] # To maintain order and map keys back
    existing_item_ids = set()

    # --- 1. Harmonize Dim_Product first ---
    for product in products_data + productitem_data:
        item_id = str(product.get('item_id', ''))
        if item_id and item_id not in existing_item_ids:
            all_products.append(harmonize_product_record(product, 'products'))
            all_raw_data.append(product)
            existing_item_ids.add(item_id)
        elif item_id:
             # print(f"⚠️ Skipping duplicate item_id: {item_id}")
             pass

    if all_products:
        dim_product_df = pd.DataFrame(all_products)
        dim_product_df['product_key'] = range(1, len(dim_product_df) + 1)
        dim_product_df = apply_data_types(dim_product_df, 'dim_product')
        
        # Create a lookup map for safe variant extraction (item_id -> product_key)
        key_lookup = dim_product_df.set_index('product_item_id')['product_key'].to_dict()
        
        # --- 2. Harmonize Dim_Product_Variant second ---
        all_variants = []
        variant_key_counter = 1
        
        for product in all_raw_data:
            item_id = str(product.get('item_id', ''))
            
            if item_id in key_lookup:
                product_key = key_lookup[item_id]
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
        
        # Show sample of data
        print("\n📋 Sample of harmonized product data (Dim_Product):")
        sample_cols = ['product_key', 'product_item_id', 'product_name', 'product_sku_base', 'product_status', 'product_price']
        available_cols = [col for col in sample_cols if col in dim_product_df.columns]
        print(dim_product_df[available_cols].head(3).to_string(index=False))
        
        if not dim_product_variant_df.empty:
            print("\n📋 Sample of harmonized variant data (Dim_Product_Variant):")
            variant_cols = ['variant_key', 'product_key', 'platform_sku_id', 'variant_attribute_1', 'variant_attribute_2', 'variant_attribute_3']
            available_variant_cols = [col for col in variant_cols if col in dim_product_variant_df.columns]
            print(dim_product_variant_df[available_variant_cols].head(3).to_string(index=False))
        
    else:
        print("⚠️ No product data found to harmonize")
    
    return dim_product_df, dim_product_variant_df

def save_harmonized_data(product_df, variant_df, output_dir=None):
    """
    Save harmonized product and variant data to CSV files
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
        # --- CRITICAL VALIDATION STEP ---
        if validate_product_variant_linkage(product_df, variant_df):
            product_file, variant_file = save_harmonized_data(product_df, variant_df)
            print(f"\n🎉 Product & Variant dimension harmonization completed successfully!")
            print(f"📁 Product file: {product_file}")
            print(f"📁 Variant file: {variant_file}")
        else:
            print("\n❌ Harmonization failed validation. Data not saved to maintain integrity.")
    else:
        print("❌ No data was harmonized. Please check your source files.")
