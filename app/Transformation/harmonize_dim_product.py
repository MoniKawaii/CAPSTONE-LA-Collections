"""
Product Dimension Harmonization Script
Maps Lazada and Shopee product data from raw JSON files to the standardized dimensional model

Key Requirements:
- EXCLUDE product_sku_base entirely from processing and output
- Standardize product_status mapping for Shopee items
- Add canonical_sku to dim_product_variant (unified join key)
- Implement Lazada special pricing logic (current date: 2025-11-01)
- Parse variant attributes from model_name/saleProp
- Ensure all IDs are integers

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
from datetime import datetime, timedelta
import re

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import get_empty_dataframe, LAZADA_TO_UNIFIED_MAPPING, SHOPEE_TO_UNIFIED_MAPPING, apply_data_types, get_staging_filename

# Current date for price calculations
CURRENT_DATE = datetime(2025, 11, 1)

def standardize_product_status(platform, status):
    """
    Standardize product status across platforms
    
    Args:
        platform (str): 'lazada' or 'shopee'
        status (str): Original status from platform
        
    Returns:
        str: Standardized status
    """
    if platform == 'shopee':
        shopee_status_mapping = {
            'NORMAL': 'Active',
            'UNLIST': 'Inactive/Removed',
            'BANNED': 'Inactive/Removed', 
            'SELLER_DELETE': 'Inactive/Removed',
            'SHOPEE_DELETE': 'Inactive/Removed',
            'REVIEWING': 'Pending/Reviewing'
        }
        return shopee_status_mapping.get(status, status)
    elif platform == 'lazada':
        # Lazada typically uses 'active', 'inactive', etc.
        lazada_status_mapping = {
            'active': 'Active',
            'inactive': 'Inactive/Removed',
            'pending': 'Pending/Reviewing'
        }
        return lazada_status_mapping.get(status.lower(), status)
    
    return status

def parse_variant_attributes(platform, variant_data):
    """
    Parse variant attributes from platform-specific data into scent and volume
    
    Smart parsing logic:
    - Detects volume by looking for "ml", "ML", "mL", "l", "L" patterns
    - Assigns volume-related attributes to volume column
    - Assigns all other attributes to scent column
    - Returns 'N/A' if no attributes found
    
    Args:
        platform (str): 'lazada' or 'shopee'
        variant_data (dict): Variant data from platform
        
    Returns:
        tuple: (scent, volume)
    """
    scent = 'N/A'
    volume = 'N/A'
    
    # Collect all raw attributes first
    raw_attributes = []
    
    if platform == 'shopee':
        # Parse from model_name if available
        model_name = variant_data.get('model_name', '')
        if model_name and model_name.strip():
            # Split by common delimiters
            parts = re.split(r'[,;|\-_]', model_name.strip())
            parts = [p.strip() for p in parts if p.strip()]
            raw_attributes.extend(parts)
        
        # Also check tier_index if available (fallback)
        if not raw_attributes:
            tier_index = variant_data.get('tier_index', [])
            if tier_index:
                raw_attributes = [str(idx) for idx in tier_index]
                
    elif platform == 'lazada':
        # Parse from saleProp or Variation fields
        sale_prop = variant_data.get('saleProp', '')
        if sale_prop:
            # Parse JSON-like saleProp structure
            try:
                if isinstance(sale_prop, str) and sale_prop.strip():
                    prop_data = json.loads(sale_prop)
                elif isinstance(sale_prop, (list, dict)):
                    prop_data = sale_prop
                else:
                    prop_data = None
                    
                if isinstance(prop_data, list) and len(prop_data) > 0:
                    for prop in prop_data:
                        if isinstance(prop, dict):
                            value = prop.get('propValue', prop.get('value', ''))
                            if value:
                                raw_attributes.append(str(value))
            except:
                pass
        
        # Fallback to Variation fields
        if not raw_attributes:
            for i in range(1, 4):
                var_value = variant_data.get(f'Variation{i}', 'N/A')
                if var_value and var_value != 'N/A':
                    raw_attributes.append(var_value)
    
    # Smart assignment: separate volume from scent
    volume_pattern = re.compile(r'\d+\s*(ml|ML|mL|Ml|l|L)\b', re.IGNORECASE)
    scent_parts = []
    volume_parts = []
    
    for attr in raw_attributes:
        attr_str = str(attr).strip()
        if volume_pattern.search(attr_str):
            volume_parts.append(attr_str)
        else:
            scent_parts.append(attr_str)
    
    # Assign to scent and volume columns
    if scent_parts:
        scent = ', '.join(scent_parts)
    if volume_parts:
        volume = ', '.join(volume_parts)
    
    return scent, volume

def calculate_lazada_current_price(sku_data, current_date=CURRENT_DATE):
    """
    Calculate current selling price for Lazada based on special pricing windows
    
    Args:
        sku_data (dict): SKU data from Lazada
        current_date (datetime): Current date for calculation
        
    Returns:
        float: Current selling price
    """
    try:
        # Get base price
        base_price = float(sku_data.get('price', 0))
        special_price = sku_data.get('special_price')
        
        if special_price is not None:
            special_price = float(special_price)
            
            # Check if special pricing is active
            special_from = sku_data.get('special_from_time')
            special_to = sku_data.get('special_to_time')
            
            if special_from and special_to:
                try:
                    # Parse timestamps (assuming Unix timestamps)
                    if isinstance(special_from, (int, float)):
                        from_date = datetime.fromtimestamp(special_from)
                    else:
                        from_date = datetime.strptime(str(special_from), '%Y-%m-%d %H:%M:%S')
                    
                    if isinstance(special_to, (int, float)):
                        to_date = datetime.fromtimestamp(special_to)
                    else:
                        to_date = datetime.strptime(str(special_to), '%Y-%m-%d %H:%M:%S')
                    
                    # Check if current date is within special pricing window
                    if from_date <= current_date <= to_date:
                        return special_price
                except:
                    pass
        
        return base_price
        
    except (ValueError, TypeError):
        return 0.0

def load_lazada_data():
    """
    Load all Lazada product data from raw JSON files
    
    Returns:
        tuple: (products_data, productitem_data, review_data) as lists of dictionaries
    """
    staging_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'Staging')
    
    # Load products raw data
    products_file = os.path.join(staging_dir, get_staging_filename('lazada', 'products'))
    productitem_file = os.path.join(staging_dir, get_staging_filename('lazada', 'productitem'))
    productreview_file = os.path.join(staging_dir, get_staging_filename('lazada', 'productreview'))
    
    products_data = []
    productitem_data = []
    review_data = []
    
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
    
    if os.path.exists(productreview_file):
        with open(productreview_file, 'r', encoding='utf-8') as f:
            review_data = json.load(f)
            print(f"✅ Loaded {len(review_data)} product reviews from {os.path.basename(productreview_file)}")
    else:
        print(f"⚠️ File not found: {productreview_file}")
    
    return products_data, productitem_data, review_data

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

def calculate_lazada_average_ratings(review_data):
    """
    Calculate average ratings by item_id from Lazada product review data
    
    Args:
        review_data (list): List of review records from lazada_productreview_raw.json
        
    Returns:
        dict: Dictionary mapping item_id to average rating
    """
    if not review_data:
        return {}
    
    # Aggregate ratings by item_id
    item_ratings = {}
    
    for review in review_data:
        if isinstance(review, dict):
            item_id = review.get('item_id')
            rating = review.get('rating')
            
            if item_id is not None and rating is not None:
                try:
                    rating = float(rating)
                    if item_id not in item_ratings:
                        item_ratings[item_id] = []
                    item_ratings[item_id].append(rating)
                except (ValueError, TypeError):
                    continue
    
    # Calculate averages
    average_ratings = {}
    for item_id, ratings in item_ratings.items():
        if ratings:
            average_ratings[item_id] = round(sum(ratings) / len(ratings), 2)
    
    print(f"📊 Calculated average ratings for {len(average_ratings)} Lazada products")
    if average_ratings:
        sample_items = list(average_ratings.items())[:3]
        print(f"📋 Sample ratings: {sample_items}")
    
    return average_ratings

def calculate_shopee_average_ratings(review_data):
    """
    Calculate average ratings by item_id from Shopee product review data
    
    Args:
        review_data (list): List of review records from shopee_productreview_raw.json
        
    Returns:
        dict: Dictionary mapping item_id to average rating
    """
    if not review_data:
        return {}
    
    # Aggregate ratings by item_id
    item_ratings = {}
    
    for review in review_data:
        if isinstance(review, dict):
            item_id = review.get('item_id')
            rating = review.get('rating')
            
            if item_id is not None and rating is not None:
                try:
                    rating = float(rating)
                    if item_id not in item_ratings:
                        item_ratings[item_id] = []
                    item_ratings[item_id].append(rating)
                except (ValueError, TypeError):
                    continue
    
    # Calculate averages
    average_ratings = {}
    for item_id, ratings in item_ratings.items():
        if ratings:
            average_ratings[item_id] = round(sum(ratings) / len(ratings), 2)
    
    print(f"📊 Calculated average ratings for {len(average_ratings)} Shopee products")
    if average_ratings:
        sample_items = list(average_ratings.items())[:3]
        print(f"📋 Sample ratings: {sample_items}")
    
    return average_ratings

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

def extract_lazada_product_variants(product_data, product_key, variant_key_counter):
    """
    Extract variant data from Lazada product SKUs array
    If no SKUs exist, creates a base variant record
    
    Args:
        product_data (dict): Raw product data from Lazada API
        product_key (int): Product key from the main product record
        variant_key_counter (dict): Global variant counter with 'current' key
        
    Returns:
        list: List of variant records (always at least 1)
    """
    variants = []
    skus = product_data.get('skus', [])
    platform_key = 1  # Lazada
    current_timestamp = datetime.now()
    
    if skus and len(skus) > 0:
        # Extract variants from SKU list
        for idx, sku_data in enumerate(skus):
            if isinstance(sku_data, dict):
                # Generate Lazada variant key with .1 decimal (e.g., 1.1, 2.1, 3.1)
                variant_key = float(f"{variant_key_counter['current']}.1")
                variant_key_counter['current'] += 1
                
                # Parse variant attributes into scent and volume
                scent, volume = parse_variant_attributes('lazada', sku_data)
                
                # Get canonical_sku (unified join key)
                # Fallback: SellerSku → SkuId → "not_found"
                seller_sku = sku_data.get('SellerSku')
                sku_id = sku_data.get('SkuId')
                
                if seller_sku and str(seller_sku).strip():
                    canonical_sku = str(seller_sku).strip()
                elif sku_id:
                    canonical_sku = str(sku_id)
                else:
                    canonical_sku = 'not_found'
                
                # Extract price information
                current_price = calculate_lazada_current_price(sku_data, CURRENT_DATE)
                original_price = float(sku_data.get('price', 0)) if sku_data.get('price') is not None else None
                
                variant = {
                    'product_variant_key': variant_key,
                    'product_key': product_key,
                    'platform_sku_id': str(sku_data.get('SkuId', '')),
                    'canonical_sku': canonical_sku,
                    'scent': scent,
                    'volume': volume,
                    'current_price': current_price if current_price > 0 else None,
                    'original_price': original_price,
                    'created_at': current_timestamp,
                    'last_updated': current_timestamp,
                    'platform_key': platform_key
                }
                variants.append(variant)
    else:
        # No SKUs found, create a base variant record
        variant_key = float(f"{variant_key_counter['current']}.1")
        variant_key_counter['current'] += 1
        
        item_id = product_data.get('item_id', '')
        
        # Fallback for canonical_sku: item_id or "not_found"
        canonical_sku = str(item_id) if item_id else 'not_found'
        
        base_variant = {
            'product_variant_key': variant_key,
            'product_key': product_key,
            'platform_sku_id': str(item_id) if item_id else 'not_found',
            'canonical_sku': canonical_sku,
            'scent': 'Base Product',
            'volume': 'N/A',
            'current_price': None,
            'original_price': None,
            'created_at': current_timestamp,
            'last_updated': current_timestamp,
            'platform_key': platform_key
        }
        variants.append(base_variant)
    
    return variants

def extract_shopee_product_variants(variant_data, product_key, item_id, variant_key_counter, product_name=None, item_sku=None, product_price_info=None):
    """
    Extract variant data from Shopee model list for a specific product
    If no variants exist, creates a base variant record
    
    Args:
        variant_data (list): All variant data from shopee_product_variant_raw.json
        product_key (int): Product key from the main product record
        item_id: Item ID to find variants for
        variant_key_counter (dict): Global variant counter with 'current' key
        product_name (str): Product name for base variant (optional)
        item_sku (str): Item SKU for base variant (optional)
        product_price_info (list): Product-level price info for base variants (optional)
        
    Returns:
        list: List of variant records (always at least 1)
    """
    variants = []
    platform_key = 2  # Shopee
    found_variants = False
    current_timestamp = datetime.now()
    
    for variant_record in variant_data:
        if isinstance(variant_record, dict) and variant_record.get('item_id') == item_id:
            # API returns 'model' not 'model_list'
            model_list = variant_record.get('model', variant_record.get('model_list', []))
            
            for idx, model in enumerate(model_list):
                if isinstance(model, dict):
                    # Generate Shopee variant key with .2 decimal (e.g., 1.2, 2.2, 3.2)
                    variant_key = float(f"{variant_key_counter['current']}.2")
                    variant_key_counter['current'] += 1
                    
                    # Parse variant attributes into scent and volume
                    scent, volume = parse_variant_attributes('shopee', model)
                    
                    # Get canonical_sku (unified join key)
                    # Fallback: model_sku → model_id → "not_found"
                    model_sku = model.get('model_sku')
                    model_id = model.get('model_id')
                    
                    if model_sku and str(model_sku).strip():
                        canonical_sku = str(model_sku).strip()
                    elif model_id:
                        canonical_sku = str(model_id)
                    else:
                        canonical_sku = 'not_found'
                    
                    # Extract price information from price_info (it's a list)
                    price_info_list = model.get('price_info', [])
                    price_info = price_info_list[0] if isinstance(price_info_list, list) and len(price_info_list) > 0 else {}
                    current_price = float(price_info.get('current_price', 0)) if price_info.get('current_price') is not None else None
                    original_price = float(price_info.get('original_price', 0)) if price_info.get('original_price') is not None else None
                    
                    # Set to None if price is 0
                    if current_price == 0:
                        current_price = None
                    if original_price == 0:
                        original_price = None
                    
                    variant = {
                        'product_variant_key': variant_key,
                        'product_key': product_key,
                        'platform_sku_id': str(model.get('model_id', '')),
                        'canonical_sku': canonical_sku,
                        'scent': scent,
                        'volume': volume,
                        'current_price': current_price,
                        'original_price': original_price,
                        'created_at': current_timestamp,
                        'last_updated': current_timestamp,
                        'platform_key': platform_key
                    }
                    variants.append(variant)
                    found_variants = True
            break  # Found the item, no need to continue
    
    # If no variants found, create a base variant record
    if not found_variants:
        variant_key = float(f"{variant_key_counter['current']}.2")
        variant_key_counter['current'] += 1
        
        # Fallback for canonical_sku: item_sku → item_id → "not_found"
        if item_sku and str(item_sku).strip():
            canonical_sku = str(item_sku).strip()
        elif item_id:
            canonical_sku = str(item_id)
        else:
            canonical_sku = 'not_found'
        
        # Extract price from product-level price_info for base variants
        base_current_price = None
        base_original_price = None
        if product_price_info and len(product_price_info) > 0:
            if isinstance(product_price_info[0], dict):
                base_current_price = product_price_info[0].get('current_price')
                base_original_price = product_price_info[0].get('original_price')
                
                # Convert to float and handle 0 values
                if base_current_price is not None:
                    base_current_price = float(base_current_price) if base_current_price != 0 else None
                if base_original_price is not None:
                    base_original_price = float(base_original_price) if base_original_price != 0 else None
        
        base_variant = {
            'product_variant_key': variant_key,
            'product_key': product_key,
            'platform_sku_id': str(item_id) if item_id else 'not_found',
            'canonical_sku': canonical_sku,
            'scent': 'Base Product',
            'volume': 'N/A',
            'current_price': base_current_price,
            'original_price': base_original_price,
            'created_at': current_timestamp,
            'last_updated': current_timestamp,
            'platform_key': platform_key
        }
        variants.append(base_variant)
    
    return variants

def harmonize_lazada_product_record(product_data, product_key, average_ratings=None, overall_average_rating=None):
    """
    Harmonize a single Lazada product record to dimensional model
    
    Args:
        product_data (dict): Raw product data from Lazada API
        product_key (int): Generated product key
        average_ratings (dict): Dictionary mapping item_id to average rating
        overall_average_rating (float): Overall average rating for imputation
        
    Returns:
        dict: Harmonized product record
    """
    attributes = product_data.get('attributes', {})
    item_id = str(product_data.get('item_id', ''))
    
    # Standardize status
    status = standardize_product_status('lazada', product_data.get('status', 'Unknown'))
    
    # Get rating from average_ratings or use overall average for imputation
    product_rating = None
    if average_ratings and item_id in average_ratings:
        product_rating = average_ratings[item_id]
    elif overall_average_rating is not None:
        product_rating = overall_average_rating
    
    return {
        'product_key': product_key,
        'product_item_id': item_id,
        'product_name': attributes.get('name', ''),
        'product_category': get_lazada_category_name(product_data.get('primary_category')),
        'product_status': status,
        'product_rating': product_rating,
        'platform_key': 1  # Lazada
    }

def harmonize_shopee_product_record(product_data, product_key, productitem_data, variant_data, category_data, average_ratings):
    """
    Harmonize a single Shopee product record to dimensional model
    
    Args:
        product_data (dict): Raw product data from Shopee API
        product_key (int): Generated product key
        productitem_data (list): Product item data for additional details
        variant_data (list): Variant data for price extraction
        category_data (list): Category data for name mapping
        average_ratings (dict): Dictionary mapping item_id to average rating
        
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
    
    # Get average rating from calculated ratings
    product_rating = average_ratings.get(item_id)
    
    # Standardize status
    status = standardize_product_status('shopee', product_data.get('item_status', 'Unknown'))
    
    return {
        'product_key': product_key,
        'product_item_id': str(item_id or ''),
        'product_name': product_data.get('item_name', ''),
        'product_category': category_name,
        'product_status': status,
        'product_rating': product_rating,  # Use calculated average rating from reviews
        'platform_key': 2  # Shopee
    }

def harmonize_dim_product():
    """
    Main harmonization function for product and variant dimensions
    """
    print("🔄 Starting Product & Variant Dimension Harmonization (Lazada + Shopee)...")
    print("🔧 Key Features:")
    print("   - Excluded product_sku_base from processing")
    print("   - Standardized product_status mapping")
    print("   - Added canonical_sku as unified join key")
    print("   - Implemented Lazada special pricing logic")
    print("   - Parsed variant attributes from model_name/saleProp")
    print("   - Using float decimal IDs (x.1 for Lazada, x.2 for Shopee)")
    
    # Initialize empty dataframes
    product_df = get_empty_dataframe('dim_product')
    variant_df = get_empty_dataframe('dim_product_variant')
    
    print(f"📋 Product schema: {product_df.columns.tolist()}")
    print(f"📋 Variant schema: {variant_df.columns.tolist()}")
    
    # Load data
    print("\n📥 Loading Lazada data...")
    lazada_products, lazada_productitem, lazada_reviews = load_lazada_data()
    
    print("\n📥 Loading Shopee data...")
    shopee_products, shopee_productitem, shopee_variants, shopee_categories, shopee_reviews = load_shopee_data()
    
    # Calculate average ratings
    print("\n📊 Calculating average ratings...")
    lazada_average_ratings = calculate_lazada_average_ratings(lazada_reviews)
    shopee_average_ratings = calculate_shopee_average_ratings(shopee_reviews)
    
    # Calculate overall average rating for imputation (combine both platforms)
    all_ratings = []
    if lazada_average_ratings:
        all_ratings.extend(lazada_average_ratings.values())
    if shopee_average_ratings:
        all_ratings.extend(shopee_average_ratings.values())
    
    overall_average_rating = None
    if all_ratings:
        overall_average_rating = round(sum(all_ratings) / len(all_ratings), 2)
        print(f"📈 Overall average rating across platforms: {overall_average_rating}")
    else:
        print("⚠️ No ratings available for imputation")
    
    # Process Lazada products
    print("\n🔄 Processing Lazada products...")
    product_key_counter = 1  # Base counter for products
    variant_key_counter = {'current': 1}  # Base counter for variants
    
    for product_data in lazada_products:
        if isinstance(product_data, dict):
            # Generate Lazada product key with .1 decimal (e.g., 1.1, 2.1, 3.1)
            lazada_product_key = float(f"{product_key_counter}.1")
            
            # Harmonize product record with ratings
            harmonized_product = harmonize_lazada_product_record(
                product_data, 
                lazada_product_key, 
                lazada_average_ratings, 
                overall_average_rating
            )
            product_df = pd.concat([product_df, pd.DataFrame([harmonized_product])], ignore_index=True)
            
            # Extract variants with global counter (guaranteed to have at least 1)
            variants = extract_lazada_product_variants(product_data, lazada_product_key, variant_key_counter)
            # Always add variants (now guaranteed to have at least 1)
            variant_df = pd.concat([variant_df, pd.DataFrame(variants)], ignore_index=True)
            
            product_key_counter += 1
    
    lazada_count = len(lazada_products)
    print(f"✅ Processed {lazada_count} Lazada products")
    
    # Process Shopee products
    print("\n🔄 Processing Shopee products...")
    
    shopee_product_key_counter = 1  # Separate counter for Shopee products
    
    for product_data in shopee_products:
        if isinstance(product_data, dict):
            # Generate Shopee product key with .2 decimal (e.g., 1.2, 2.2, 3.2)
            shopee_product_key = float(f"{shopee_product_key_counter}.2")
            
            # Harmonize product record with calculated ratings
            harmonized_product = harmonize_shopee_product_record(
                product_data, shopee_product_key, shopee_productitem, shopee_variants, shopee_categories, shopee_average_ratings
            )
            product_df = pd.concat([product_df, pd.DataFrame([harmonized_product])], ignore_index=True)
            
            # Extract variants with global counter, pass product info for base variants
            variants = extract_shopee_product_variants(
                shopee_variants, 
                shopee_product_key, 
                product_data.get('item_id'), 
                variant_key_counter,
                product_name=product_data.get('item_name'),
                item_sku=product_data.get('item_sku'),
                product_price_info=product_data.get('price_info')
            )
            # Always add variants (now guaranteed to have at least 1)
            variant_df = pd.concat([variant_df, pd.DataFrame(variants)], ignore_index=True)
            
            shopee_product_key_counter += 1
    
    shopee_count = len(shopee_products)
    print(f"✅ Processed {shopee_count} Shopee products")
    
    # Create default variants for all products (to use when SKU/model_id lookup fails)
    print("\n🔄 Creating default variants for all products...")
    default_variants_created = 0
    current_timestamp = datetime.now()
    
    for _, product_row in product_df.iterrows():
        product_key = product_row['product_key']
        platform_key = product_row['platform_key']
        product_item_id = product_row['product_item_id']
        
        # Generate platform-appropriate variant key
        if platform_key == 1:  # Lazada
            default_variant_key = float(f"{variant_key_counter['current']}.1")
        else:  # Shopee
            default_variant_key = float(f"{variant_key_counter['current']}.2")
        
        variant_key_counter['current'] += 1
        
        # Create default variant record (looks like a regular variant)
        default_variant = {
            'product_variant_key': default_variant_key,
            'product_key': product_key,
            'platform_sku_id': f"DEFAULT_{product_item_id}",
            'canonical_sku': f"DEFAULT_{product_item_id}",
            'scent': 'Not Specified',
            'volume': 'Not Specified',
            'current_price': None,
            'original_price': None,
            'created_at': current_timestamp,
            'last_updated': current_timestamp,
            'platform_key': platform_key
        }
        
        variant_df = pd.concat([variant_df, pd.DataFrame([default_variant])], ignore_index=True)
        default_variants_created += 1
    
    print(f"✅ Created {default_variants_created} default variants (one per product)")
    print(f"   - These will be used as fallback when SKU/model_id lookup fails in fact_orders")
    
    # Apply data types
    product_df = apply_data_types(product_df, 'dim_product')
    variant_df = apply_data_types(variant_df, 'dim_product_variant')
    
    print(f"\n✅ Harmonized {len(product_df)} products and {len(variant_df)} variants")
    
    # Summary statistics
    print(f"\n📊 Product Summary:")
    print(f"   - Total products: {len(product_df)}")
    print(f"   - Lazada products: {lazada_count}")
    print(f"   - Shopee products: {shopee_count}")
    print(f"   - Products with ratings: {product_df['product_rating'].notna().sum()}")
    print(f"   - Average rating (Shopee only): {product_df[product_df['platform_key'] == 2]['product_rating'].mean():.2f}" if product_df[product_df['platform_key'] == 2]['product_rating'].notna().sum() > 0 else "   - Average rating (Shopee only): N/A")
    print(f"   - Unique categories: {product_df['product_category'].nunique()}")
    print(f"   - Status distribution: {product_df['product_status'].value_counts().to_dict()}")
    
    print(f"\n📊 Variant Summary:")
    print(f"   - Total variants: {len(variant_df)}")
    print(f"   - Lazada variants: {len(variant_df[variant_df['platform_key'] == 1])}")
    print(f"   - Shopee variants: {len(variant_df[variant_df['platform_key'] == 2])}")
    print(f"   - Default variants (fallback): {len(variant_df[variant_df['platform_sku_id'].str.startswith('DEFAULT_', na=False)])}")
    print(f"   - Specific variants: {len(variant_df[~variant_df['platform_sku_id'].str.startswith('DEFAULT_', na=False)])}")
    print(f"   - Variants with canonical_sku: {variant_df['canonical_sku'].notna().sum()}")
    print(f"   - Variants with prices: {variant_df['current_price'].notna().sum()}")
    print(f"   - Average current price: ₱{variant_df['current_price'].mean():.2f}" if variant_df['current_price'].notna().sum() > 0 else "   - Average current price: N/A")
    
    # Show samples
    print(f"\n📋 Sample of Lazada products:")
    lazada_products_sample = product_df[product_df['platform_key'] == 1].head(3)
    if not lazada_products_sample.empty:
        print(lazada_products_sample[['product_key', 'product_item_id', 'product_name', 'product_status', 'platform_key']].to_string())
    
    print(f"\n📋 Sample of Shopee products:")
    shopee_products_sample = product_df[product_df['platform_key'] == 2].head(3)
    if not shopee_products_sample.empty:
        print(shopee_products_sample[['product_key', 'product_item_id', 'product_name', 'product_status', 'product_rating', 'platform_key']].to_string())
    
    print(f"\n📋 Sample of Lazada variants:")
    lazada_variants_sample = variant_df[variant_df['platform_key'] == 1].head(3)
    if not lazada_variants_sample.empty:
        print(lazada_variants_sample[['product_variant_key', 'product_key', 'canonical_sku', 'scent', 'volume', 'current_price', 'platform_key']].to_string())
    
    print(f"\n📋 Sample of Shopee variants:")
    shopee_variants_sample = variant_df[variant_df['platform_key'] == 2].head(3)
    if not shopee_variants_sample.empty:
        print(shopee_variants_sample[['product_variant_key', 'product_key', 'canonical_sku', 'scent', 'volume', 'current_price', 'platform_key']].to_string())
    
    # Apply data types before saving
    product_df = apply_data_types(product_df, 'dim_product')
    variant_df = apply_data_types(variant_df, 'dim_product_variant')
    
    # Save the data
    save_harmonized_data(product_df, variant_df)
    
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
        # Run harmonization (includes saving)
        product_df, variant_df = harmonize_dim_product()
        
        print(f"\n🎉 Product & Variant dimension harmonization completed successfully!")
        
        # Show key improvements
        print(f"🔧 Key Improvements Implemented:")
        print(f"✅ Removed product_sku_base from schema and processing")
        print(f"✅ Standardized product_status (Shopee: NORMAL→Active, UNLIST→Inactive/Removed, etc.)")
        print(f"✅ Added canonical_sku as unified join key (model_sku for Shopee, SellerSku for Lazada)")
        print(f"✅ Implemented Lazada special pricing logic with time windows")
        print(f"✅ Parsed variant attributes from model_name/saleProp with 'N/A' defaults")
        print(f"✅ Used float decimal IDs throughout (product_key: x.1/x.2, product_variant_key: x.1/x.2)")
        print(f"✅ Calculated Lazada and Shopee product ratings from review data")
        print(f"✅ Enhanced Shopee price extraction (fallback to variant data for multi-model products)")
        print(f"✅ Created default variants for all products (fallback for missing SKU/model_id lookups)")
                
    except Exception as e:
        print(f"❌ Error during harmonization: {e}")
        import traceback
        traceback.print_exc()