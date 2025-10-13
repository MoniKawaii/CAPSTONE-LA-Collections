# Shopee API Calls Reference

## 📋 All API Endpoints Used in shopee_api_calls.py

This document lists all Shopee Open Platform API v2.0 endpoints used in the implementation, along with their parameters, usage context, and documentation references.

---

## 🛍️ Product APIs

### 1. `/api/v2/product/get_item_list`
**Method:** GET  
**Used in:** `extract_all_products()` - Step 1  
**Purpose:** Get list of item IDs for all products in shop  
**Line:** 181

**Parameters:**
```python
item_status = "NORMAL"     # Product status filter
offset = 0                 # Pagination offset
page_size = 100            # Max 100 per page
```

**Query String:**
```
/api/v2/product/get_item_list?item_status=NORMAL&offset=0&page_size=100
```

**Expected Response:**
```json
{
  "response": {
    "item": [
      {"item_id": 123, "item_status": "NORMAL"},
      {"item_id": 456, "item_status": "NORMAL"}
    ],
    "has_next_page": true,
    "next_offset": 100,
    "total_count": 500
  }
}
```

**Documentation:** Shopee Open Platform > Product > Get Item List

---

### 2. `/api/v2/product/get_item_base_info`
**Method:** GET  
**Used in:** 
- `extract_all_products()` - Step 2 (Line 230)
- `extract_product_details()` (Line 625)

**Purpose:** Get detailed product information for specific items  
**Batch Size:** Max 50 items per request

**Parameters:**
```python
item_id_list = "123,456,789"  # Comma-separated item IDs (max 50)
```

**Query String:**
```
/api/v2/product/get_item_base_info?item_id_list=123,456,789
```

**Expected Response:**
```json
{
  "response": {
    "item_list": [
      {
        "item_id": 123,
        "item_name": "Product Name",
        "item_sku": "SKU123",
        "item_status": "NORMAL",
        "price_info": {
          "current_price": 99.99,
          "original_price": 149.99
        },
        "stock_info": {
          "stock_summary_info": {
            "total_available_stock": 100
          }
        },
        "model_list": [...],
        "category_id": 12345,
        "brand": {...},
        "rating_star": 4.5
      }
    ]
  }
}
```

**Documentation:** Shopee Open Platform > Product > Get Item Base Info

---

### 3. `/api/v2/product/get_comment`
**Method:** GET  
**Used in:** `extract_review_history_list()` (Line 688)  
**Purpose:** Get product reviews/comments for specific item

**Parameters:**
```python
item_id = 123              # Product item ID
page_size = 50             # Max reviews per page
```

**Query String:**
```
/api/v2/product/get_comment?item_id=123&page_size=50
```

**Expected Response:**
```json
{
  "response": {
    "item_comment_list": [
      {
        "comment_id": 789,
        "rating_star": 5,
        "comment": "Great product!",
        "ctime": 1625097600,
        "author_username": "buyer123",
        "product_items": [...]
      }
    ],
    "more": false
  }
}
```

**Documentation:** Shopee Open Platform > Product > Get Comment

---

## 📦 Order APIs

### 4. `/api/v2/order/get_order_list`
**Method:** GET  
**Used in:** `_extract_orders_chunk()` (Line 328)  
**Purpose:** Get list of orders within time range  
**Time Limit:** 15 days maximum per request

**Parameters:**
```python
time_range_field = "create_time"  # Time field to filter by
time_from = 1617235200            # Unix timestamp (start)
time_to = 1618531200              # Unix timestamp (end, max 15 days)
page_size = 100                   # Max 100 orders per page
order_status = "ALL"              # Status filter
cursor = ""                       # Pagination cursor
```

**Query String:**
```
/api/v2/order/get_order_list?time_range_field=create_time&time_from=1617235200&time_to=1618531200&page_size=100&order_status=ALL
```

**Expected Response:**
```json
{
  "response": {
    "order_list": [
      {
        "order_sn": "2104010001234567",
        "order_status": "COMPLETED"
      }
    ],
    "more": true,
    "next_cursor": "cursor_string"
  }
}
```

**Documentation:** Shopee Open Platform > Order > Get Order List

---

### 5. `/api/v2/order/get_order_detail`
**Method:** GET  
**Used in:** `_get_order_details()` (Line 373)  
**Purpose:** Get detailed order information  
**Batch Size:** Max 50 order_sn per request

**Parameters:**
```python
order_sn_list = "210401001,210401002"  # Comma-separated order SNs (max 50)
response_optional_fields = "buyer_user_id,buyer_username,estimated_shipping_fee,..."
```

**Query String:**
```
/api/v2/order/get_order_detail?order_sn_list=210401001,210401002&response_optional_fields=buyer_user_id,buyer_username,estimated_shipping_fee,recipient_address,actual_shipping_fee,goods_to_declare,note,note_update_time,item_list,pay_time,dropshipper,credit_card_number,dropshipper_phone,split_up,buyer_cancel_reason,cancel_by,cancel_reason,actual_shipping_fee_confirmed,buyer_cpf_id,fulfillment_flag,pickup_done_time,package_list,shipping_carrier,payment_method,total_amount,buyer_username,invoice_data
```

**Expected Response:**
```json
{
  "response": {
    "order_list": [
      {
        "order_sn": "210401001",
        "order_status": "COMPLETED",
        "create_time": 1617235200,
        "update_time": 1617321600,
        "buyer_username": "buyer123",
        "recipient_address": {
          "name": "John Doe",
          "phone": "1234567890",
          "city": "Manila",
          "state": "Metro Manila",
          "full_address": "123 Main St"
        },
        "item_list": [
          {
            "item_id": 123,
            "item_name": "Product Name",
            "model_id": 456,
            "model_name": "Variant",
            "model_quantity_purchased": 2,
            "model_original_price": 99.99,
            "model_discounted_price": 79.99
          }
        ],
        "payment_method": "COD",
        "total_amount": 159.98,
        "actual_shipping_fee": 50.00,
        "voucher_absorbed_by_seller": 10.00,
        "voucher_absorbed_by_shopee": 10.00
      }
    ]
  }
}
```

**Documentation:** Shopee Open Platform > Order > Get Order Detail

---

## 🏪 Public/Shop APIs

### 6. `/api/v2/public/get_shop_info`
**Method:** GET  
**Used in:** 
- `_extract_monthly_traffic()` (Line 505)
- `_extract_single_period_traffic()` (Line 573)

**Purpose:** Get shop information (used as proxy for traffic metrics)  
**Note:** This is a public endpoint - actual advertising APIs may differ

**Parameters:**
```python
# No additional parameters beyond authentication
```

**Query String:**
```
/api/v2/public/get_shop_info
```

**Expected Response:**
```json
{
  "response": {
    "shop_id": 12345,
    "shop_name": "My Shop",
    "rating": 4.8,
    "response_rate": 95,
    "response_time": 120,
    "country": "PH",
    "is_official_shop": false,
    "item_count": 500,
    "follower_count": 1000
  }
}
```

**Documentation:** Shopee Open Platform > Shop > Get Shop Info

---

## 🔐 Authentication & Request Structure

### Common Parameters (All Requests)
```python
partner_id = 1192028              # Your Partner ID
timestamp = 1625097600            # Current Unix timestamp
access_token = "your_token"       # Shop access token
shop_id = 67890                   # Shop ID
sign = "generated_signature"      # HMAC-SHA256 signature
```

### Signature Generation
```python
def _generate_signature(path, timestamp, access_token, shop_id, body=None):
    # Base string format
    base_string = f"{partner_id}{path}{timestamp}"
    
    # Add token and shop_id for authenticated calls
    if access_token and shop_id:
        base_string += f"{access_token}{shop_id}"
    
    # Add body for POST requests
    if body:
        base_string += json.dumps(body, separators=(',', ':'), sort_keys=True)
    
    # Generate HMAC-SHA256
    signature = hmac.new(
        partner_key.encode('utf-8'),
        base_string.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()
    
    return signature
```

### Full URL Format
```
https://partner.test-stable.shopeemobile.com/api/v2/{endpoint}?partner_id={id}&timestamp={ts}&access_token={token}&shop_id={shop}&sign={signature}&{params}
```

---

## 📊 API Usage Summary

| API Endpoint | Method | Used In | Batch Size | Rate Limit Notes |
|-------------|--------|---------|------------|------------------|
| `/api/v2/product/get_item_list` | GET | Products extraction | 100/page | Paginated |
| `/api/v2/product/get_item_base_info` | GET | Product details | 50/batch | Multiple calls |
| `/api/v2/product/get_comment` | GET | Reviews | 50/page | Per product |
| `/api/v2/order/get_order_list` | GET | Orders list | 100/page | 15-day chunks |
| `/api/v2/order/get_order_detail` | GET | Order details | 50/batch | Multiple calls |
| `/api/v2/public/get_shop_info` | GET | Traffic proxy | N/A | Single call |

---

## 🚨 Important API Limitations

### Time Range Constraints
- **Order extraction:** Maximum 15 days per `get_order_list` call
- This is why orders are chunked into 15-day periods (vs Lazada's 90-day)

### Batch Size Limits
- **Products:** Max 50 item_ids per `get_item_base_info` call
- **Orders:** Max 50 order_sn per `get_order_detail` call
- **Reviews:** Max 50 reviews per page

### Pagination
- **Products list:** Uses `offset` and `page_size`, returns `has_next_page`
- **Orders list:** Uses `cursor` based pagination, returns `next_cursor`
- **Reviews:** Page-based pagination per product

### Rate Limiting
- Implementation includes 1.5 second delay between calls
- Auto-retry on rate limit errors (60 second wait)
- Daily limit tracking (10,000 calls/day configured)

---

## 📚 Official Documentation References

To verify and cross-reference these API calls, check:

1. **Shopee Open Platform Documentation**
   - URL: https://open.shopee.com/documents
   - Navigate to: API v2.0 > [Category] > [Endpoint Name]

2. **Product APIs**
   - Product > Get Item List
   - Product > Get Item Base Info
   - Product > Get Comment

3. **Order APIs**
   - Order > Get Order List
   - Order > Get Order Detail

4. **Shop APIs**
   - Shop > Get Shop Info (Public)

5. **Authentication**
   - Authentication & Authorization > Sign API Requests

---

## 🔍 Verification Checklist

Use this checklist to verify each API call against official docs:

- [ ] Endpoint path is correct (`/api/v2/...`)
- [ ] HTTP method is correct (GET/POST)
- [ ] Required parameters are included
- [ ] Optional parameters are properly formatted
- [ ] Batch size limits are respected
- [ ] Time range constraints are followed
- [ ] Response structure matches documentation
- [ ] Error handling covers documented error codes
- [ ] Authentication signature is properly generated
- [ ] Rate limiting is implemented

---

## ⚠️ Known Differences from Documentation

### Traffic/Advertising Metrics
- **Current Implementation:** Uses `/api/v2/public/get_shop_info` as proxy
- **Reason:** Actual advertising API endpoints require Shopee Ads access
- **Impact:** Limited traffic metrics (impressions, clicks set to 0)
- **Recommendation:** Update to proper advertising API when available

### Response Optional Fields
The `get_order_detail` call requests many optional fields. Not all may be available depending on:
- Shop permissions
- Order status
- Country/region settings
- API access level

---

## 📝 Notes for Implementation

1. **Environment:** Currently configured for sandbox (`partner.test-stable.shopeemobile.com`)
2. **Production:** Change to `partner.shopeemobile.com` via `SHOPEE_API_ENV` environment variable
3. **Tokens:** Must obtain valid access tokens via OAuth flow (see `get_shopee_tokens.py`)
4. **Shop ID:** Required for most authenticated calls

---

## 🛠️ Testing Recommendations

1. **Start with shop info:** Test `/api/v2/public/get_shop_info` first (no shop access needed)
2. **Test product list:** Verify product API access with small page_size
3. **Test single order:** Get one order detail before bulk extraction
4. **Check response fields:** Not all optional fields may be returned
5. **Monitor rate limits:** Track actual API call counts

---

**Last Updated:** October 14, 2025  
**API Version:** Shopee Open Platform API v2.0  
**Implementation File:** `app/Extraction/shopee_api_calls.py`
