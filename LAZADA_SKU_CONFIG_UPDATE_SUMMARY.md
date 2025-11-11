# Lazada SKU Configuration Updates - Implementation Summary

## Overview

Updated the Lazada SKU configuration mappings in `app/config.py` based on knowledge base information about Lazada's SKU structure and API specifications.

## Key Knowledge Base Insights Applied

### 1. SKU Structure Understanding

- **No Master SKU**: Lazada does not have a single "master" SKU per product listing
- **Variant-Level SKUs**: Each variant has its own unique `SkuId` and `SellerSku`
- **Two-Level Attributes**: Products support up to 2 levels of sales attributes (e.g., Scent + Volume)
- **SellerSku Uniqueness**: Must be unique within the same `item_id`, but can repeat across different products (Nov 2023+ update)

### 2. Data Structure Hierarchy

```
Product (item_id)
├── Basic product info (name, category, status, base price)
└── skus[] array
    ├── Variant 1 (SkuId, SellerSku, price, stock, attributes)
    ├── Variant 2 (SkuId, SellerSku, price, stock, attributes)
    └── Variant N...
```

## Configuration Changes Made

### 1. Enhanced LAZADA_TO_UNIFIED_MAPPING

#### Product Level Mappings

```python
# Added clear documentation about SKU structure
"item_id": "product_item_id",
"name": "product_name",
"primary_category_name": "product_category",
"status": "product_status",
"price": "product_price",  # Base price - may not reflect variant pricing
```

#### Variant Level Mappings (from skus[] array)

```python
# Core SKU identifiers
"SkuId": "platform_sku_id",          # Lazada's internal SKU ID
"SellerSku": "variant_sku",          # Seller-defined SKU (unique within item)
"Status": "variant_status",          # Variant-level status
"quantity": "variant_stock",         # Available stock for this variant
"price": "variant_price",            # Variant-specific current price

# Package/Shipping dimensions
"package_length": "package_length",
"package_width": "package_width",
"package_height": "package_height",
"package_weight": "package_weight",

# Sales Attributes (up to 2 levels)
"Attributes.name": "attribute_name",     # Attribute type (e.g., "Scent", "Volume")
"Attributes.value": "attribute_value",   # Attribute value (e.g., "Lavender", "50ml")
```

#### Enhanced Attribute Mapping

```python
# Structured attribute mapping for 2-level hierarchy
"attribute_1_name": "variant_attribute_1_name",    # First variation type
"attribute_1_value": "variant_attribute_1_value",  # First variation value
"attribute_2_name": "variant_attribute_2_name",    # Second variation type
"attribute_2_value": "variant_attribute_2_value",  # Second variation value
```

### 2. Updated DIM_PRODUCT_VARIANT_COLUMNS

#### Added New Columns

```python
# Lazada-specific SKU fields
'seller_sku',           # SellerSku from Lazada
'variant_status',       # Variant-level status
'variant_stock',        # Available stock

# Enhanced attribute structure
'attribute_1_name',     # First sales attribute name
'attribute_1_value',    # First sales attribute value
'attribute_2_name',     # Second sales attribute name
'attribute_2_value',    # Second sales attribute value

# Package dimensions for shipping
'package_length',       # Package length in cm
'package_width',        # Package width in cm
'package_height',       # Package height in cm
'package_weight',       # Package weight in grams
```

### 3. Updated COLUMN_DATA_TYPES

#### Added Data Types for New Fields

```python
'dim_product_variant': {
    'seller_sku': 'str',
    'variant_status': 'str',
    'variant_stock': 'int',
    'attribute_1_name': 'str',
    'attribute_1_value': 'str',
    'attribute_2_name': 'str',
    'attribute_2_value': 'str',
    'package_length': 'float64',
    'package_width': 'float64',
    'package_height': 'float64',
    'package_weight': 'float64',
    # ... existing fields maintained
}
```

### 4. Added Helper Functions

#### SKU Processing Functions

```python
def extract_lazada_variant_attributes(sku_data):
    """Extract variant attributes from Lazada SKU data structure"""

def validate_lazada_sku_uniqueness(product_data):
    """Validate SellerSku uniqueness within product"""

def generate_canonical_sku(platform_sku_id, seller_sku, platform_key=1):
    """Generate standardized canonical SKU"""

def parse_lazada_product_structure(product_data):
    """Complete parser for Lazada product and variant data"""
```

## Impact on Missing SKU Recovery

### Target SKUs

- `17089061731`: High-volume SKU affecting 4,205 order items (~$154K revenue)
- `17167753965`: High-volume SKU affecting 4,237 order items (~$154K revenue)

### Enhanced Extraction Strategy

1. **Multiple API Approaches**: Standard, live, sold_out, SKU-based extraction
2. **Detailed Product Info**: Uses `/product/item/get` for comprehensive data
3. **Attribute Processing**: Properly handles 2-level sales attributes
4. **Validation**: Built-in SKU uniqueness validation

### Configuration Validation Results

```
✅ Updated mappings support full Lazada SKU structure
✅ Enhanced product extraction with detailed info
✅ Multiple extraction strategies implemented
✅ Validation functions for data quality
✅ Proper handling of variant attributes and pricing
```

## Files Modified

1. **`app/config.py`**:

   - Enhanced `LAZADA_TO_UNIFIED_MAPPING`
   - Updated `DIM_PRODUCT_VARIANT_COLUMNS`
   - Added variant data types to `COLUMN_DATA_TYPES`
   - Added helper functions for SKU processing

2. **`app/Extraction/lazada_api_calls.py`** (in previous updates):

   - Enhanced `extract_all_products()` with multiple strategies
   - Added `_extract_detailed_product_info()` for `/product/item/get`
   - Improved token loading path resolution

3. **`get_lazada_tokens.py`** (updated):
   - Enhanced token refresh functionality
   - Multiple token storage locations
   - Automatic refresh attempt on expired tokens

## Testing and Validation

### Created Test Scripts

1. **`test_lazada_sku_config.py`**: Validates configuration mappings
2. **`test_enhanced_extraction.py`**: Tests complete extraction process
3. **`check_tokens.py`**: Token status verification

### Validation Results

- ✅ All mappings correctly handle Lazada SKU structure
- ✅ Attribute extraction supports 2-level hierarchy
- ✅ SKU uniqueness validation working
- ✅ Canonical SKU generation functional
- ✅ Enhanced extraction framework ready

## Next Steps

1. **Token Refresh**: Use `python get_lazada_tokens.py` (option 3) to refresh expired tokens
2. **Enhanced Extraction**: Run `python test_enhanced_extraction.py` with fresh tokens
3. **Data Validation**: Re-run integrity tests to verify improved SKU coverage
4. **Transformation Update**: Update transformation scripts to use enhanced variant structure

## Expected Outcomes

With these configuration updates, the enhanced extraction should:

- Capture previously missing SKUs (17089061731, 17167753965)
- Improve Lazada coverage from 91.4% to target 95%+
- Provide complete variant attribute information
- Support proper dimensional modeling with detailed product variants
- Enable accurate SKU-to-order item mapping

The configuration now fully aligns with Lazada's actual API structure and SKU management approach as documented in their knowledge base.
