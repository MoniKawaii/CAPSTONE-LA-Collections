# Shopee API Integration Summary

## ✅ What Has Been Implemented

### 1. **Extended config.py for Shopee Support**

#### Added Shopee Token Loading
- `load_shopee_tokens()` - Loads tokens from `tokens/shopee_tokens.json`
- `SHOPEE_TOKENS` - Global token dictionary similar to Lazada
- Includes: `access_token`, `refresh_token`, `partner_id`, `partner_key`, `shop_id`

#### Added Shopee API URLs
- `SHOPEE_API_URL` - Production URL
- `SHOPEE_TEST_API_URL` - Sandbox URL
- `SHOPEE_BASE_URL` - Auto-selected based on environment variable
- `SHOPEE_API_ENV` - Environment selector (sandbox/production)

#### Created Shopee Unified Mapping
- `SHOPEE_TO_UNIFIED_MAPPING` - Complete field mapping that mirrors Lazada structure
- Maps Shopee API fields to dimensional model columns
- Ensures both platforms produce compatible data for harmonization

### 2. **Shopee Unified Field Mapping (Mirrors Lazada)**

```python
SHOPEE_TO_UNIFIED_MAPPING = {
    # Dim_Product
    "item_id": "product_item_id",
    "item_name": "product_name",
    "category_id": "product_category",
    "item_status": "product_status",
    "price_info.current_price": "product_price",
    "rating_star": "product_rating",
    "platform_key": "platform_key",  # Always 2 for Shopee
    
    # Dim_Product_Variant
    "model_id": "platform_sku_id",
    "model_sku": "variant_sku",
    "tier_index.0": "variant_attribute_1",
    "tier_index.1": "variant_attribute_2",
    "tier_index.2": "variant_attribute_3",
    
    # Dim_Order
    "order_sn": "platform_order_id",
    "order_status": "order_status",
    "create_time": "order_date",
    "update_time": "updated_at",
    "total_amount": "price_total",
    "item_list": "total_item_count",
    "payment_method": "payment_method",
    "recipient_address.city": "shipping_city",
    
    # Dim_Customer
    "buyer_username": "buyer_username",
    "recipient_address.city": "customer_city",
    "recipient_address.phone": "customer_phone",
    # Generated: 'SP' + first_char + last_char of buyer_username + phone digits
    
    # Fact_Orders (from item_list)
    "model_quantity_purchased": "item_quantity",
    "model_original_price": "original_unit_price",
    "model_discounted_price": "paid_price",
    "voucher_absorbed_by_seller": "voucher_seller_amount",
    "voucher_absorbed_by_shopee": "voucher_platform_amount",
    "actual_shipping_fee": "shipping_fee_paid_by_buyer",
}
```

### 3. **Complete shopee_api_calls.py Implementation**

All methods mirror lazada_api_calls.py exactly:

#### Core Methods
- `__init__()` - Initialize with Shopee credentials and HMAC-SHA256 signature
- `_generate_signature()` - Generate Shopee API signatures
- `_make_api_call()` - API call with rate limiting and error handling
- `_save_to_json()` - Save to `app/Staging/` directory
- `_load_from_json()` - Load existing data

#### Data Extraction Methods
1. **`extract_all_products()`**
   - Gets all products with pagination
   - Uses `/api/v2/product/get_item_list` and `/api/v2/product/get_item_base_info`
   - Saves to `shopee_products_raw.json`
   - Batch size: 50 items

2. **`extract_all_orders()`**
   - Extracts orders in 15-day chunks (Shopee's API limit vs Lazada's 90-day)
   - Default period: 2020-04-01 to 2025-04-30
   - Saves to `shopee_orders_raw.json`
   - Includes full order details with customer info

3. **`extract_all_order_items()`**
   - Processes items from order data (included in Shopee order response)
   - Saves to `shopee_multiple_order_items_raw.json`
   - Same output structure as Lazada

4. **`extract_traffic_metrics()`**
   - Monthly aggregated traffic data
   - Uses shop performance as proxy
   - Saves to `shopee_reportoverview_raw.json`
   - Compatible format with Lazada traffic data

5. **`extract_product_details()`**
   - Detailed product information in batches of 50
   - Saves to `shopee_productitem_raw.json`
   - Same structure as Lazada product details

6. **`extract_review_history_list()` & `extract_review_details()`**
   - Two-step review extraction process
   - Step 1: Get review entries per product
   - Step 2: Process detailed review info
   - Saves to `shopee_reviewhistorylist_raw.json` and `shopee_productreview_raw.json`

7. **`run_complete_extraction()`**
   - Runs all extraction steps in order
   - Tracks API usage
   - Progress reporting

#### Convenience Functions
- `run_full_extraction()` - Complete historical extraction
- `extract_recent_data()` - Last 30 days only
- `extract_product_reviews_only()` - Reviews only
- `extract_review_history_only()` - Review IDs only
- `extract_review_details_only()` - Review details only

### 4. **Platform Helper Functions in config.py**

```python
# Get platform-specific configuration
get_platform_mapping('shopee')      # Returns SHOPEE_TO_UNIFIED_MAPPING
get_platform_key('shopee')          # Returns 2
get_staging_filename('shopee', 'products')  # Returns 'shopee_products_raw.json'
get_all_platforms()                 # Returns ['lazada', 'shopee']
get_platform_tokens('shopee')       # Returns SHOPEE_TOKENS
get_platform_api_url('shopee')      # Returns SHOPEE_BASE_URL
```

### 5. **Updated Validation**

The `validate_config()` function now checks:
- ✅ Lazada tokens valid
- ✅ Shopee tokens valid
- ✅ Shopee API configured
- ✅ Shopee tokens loaded
- ✅ Both platform mappings available

## 📊 Output File Structure

All files saved to `app/Staging/` directory:

### Shopee Files (Mirror Lazada)
```
shopee_products_raw.json              # All products
shopee_orders_raw.json                # All orders with details
shopee_multiple_order_items_raw.json  # Order line items
shopee_reportoverview_raw.json        # Traffic metrics
shopee_productitem_raw.json           # Product details
shopee_reviewhistorylist_raw.json     # Review entries
shopee_productreview_raw.json         # Detailed reviews
```

### Lazada Files (Existing)
```
lazada_products_raw.json
lazada_orders_raw.json
lazada_multiple_order_items_raw.json
lazada_reportoverview_raw.json
lazada_productitem_raw.json
lazada_reviewhistorylist_raw.json
lazada_productreview_raw.json
```

## 🔄 Harmonization Ready

### Unified Column Names
Both platforms now produce data with identical column names:
- `product_item_id`, `product_name`, `product_category`, `product_price`, `product_rating`
- `platform_order_id`, `order_status`, `order_date`, `price_total`, `shipping_city`
- `customer_city`, `buyer_segment`, `customer_since`, `last_order_date`
- `item_quantity`, `paid_price`, `original_unit_price`, `voucher_seller_amount`, `voucher_platform_amount`
- `impressions`, `clicks`, `ctr`, `spend`, `revenue`, `roi`

### Platform Identification
- Lazada: `platform_key = 1`, `platform_name = 'Lazada'`
- Shopee: `platform_key = 2`, `platform_name = 'Shopee'`

### Customer ID Generation
- Lazada: `'LZ' + first_char + last_char of name + phone digits`
- Shopee: `'SP' + first_char + last_char of username + phone digits`

## 🚀 Usage Examples

### Extract All Shopee Data
```python
from app.Extraction.shopee_api_calls import ShopeeDataExtractor

extractor = ShopeeDataExtractor()
results = extractor.run_complete_extraction(start_fresh=True)
```

### Extract Recent Data Only
```python
from app.Extraction.shopee_api_calls import extract_recent_data

results = extract_recent_data()  # Last 30 days
```

### Platform-Agnostic Harmonization
```python
from app.config import get_platform_mapping, get_platform_key, get_staging_filename

# Works for both platforms
for platform in ['lazada', 'shopee']:
    mapping = get_platform_mapping(platform)
    key = get_platform_key(platform)
    filename = get_staging_filename(platform, 'products')
    
    # Load and harmonize data using same logic
    # ...
```

## 🎯 Key Differences from Lazada

### API Architecture
- **Authentication**: HMAC-SHA256 signature (vs Lazada SDK)
- **Time Chunks**: 15-day maximum (vs Lazada's 90-day)
- **Batch Sizes**: 50 products, 100 orders (same as Lazada)
- **Order Details**: Included in response (vs separate call in Lazada)

### Field Names
- `order_sn` (Shopee) vs `order_id` (Lazada)
- `item_id` (both platforms use this)
- `model_id` (Shopee) vs `SkuId` (Lazada)
- `buyer_username` (Shopee) vs `first_name` (Lazada)

### API Endpoints
- Shopee: `/api/v2/product/get_item_list`, `/api/v2/order/get_order_list`
- Lazada: `/products/get`, `/orders/get`

## ✅ Harmonization Strategy

Both platforms now produce compatible data for:
1. **Dim_Product** - Same columns, platform_key differentiates
2. **Dim_Order** - Same columns, platform_key differentiates
3. **Dim_Customer** - Unified customer identification
4. **Fact_Orders** - Identical measures and dimensions
5. **Fact_Traffic** - Common metrics (impressions, clicks, CTR, etc.)

## 🔧 Environment Variables Needed

Add to your `.env` file:
```bash
# Shopee Configuration
SHOPEE_PARTNER_ID=your_partner_id
SHOPEE_PARTNER_KEY=your_partner_key
SHOPEE_SHOP_ID=your_shop_id
SHOPEE_API_ENV=sandbox  # or "production"
```

Tokens are loaded from `tokens/shopee_tokens.json`:
```json
{
  "access_token": "your_access_token",
  "refresh_token": "your_refresh_token",
  "expire_in": 14400,
  "timestamp": 1760277804
}
```

## 📝 Next Steps for Harmonization

1. **Use the unified mappings** in transformation scripts
2. **Apply platform_key** to differentiate sources
3. **Merge data** using identical column names
4. **Generate customer_key** consistently across platforms
5. **Link to Dim_Time** using same time_key format
6. **Combine traffic metrics** for cross-platform analysis

## 🎉 Result

You now have:
- ✅ Complete Shopee API extraction matching Lazada structure
- ✅ Unified field mappings for both platforms
- ✅ Identical output formats for harmonization
- ✅ Platform helper functions for easy integration
- ✅ Ready for dimensional model loading
- ✅ Compatible with existing transformation scripts
