// ===================================
// DIMENSION TABLES (The Context)
// ===================================
Project LA_Collections {

database_type: 'PostgreSQL'
}

Table Dim_Platform {
platform_key int [pk, not null, note: 'Surrogate key for the marketplace (1=Lazada, 2=Shopee)']
platform_name varchar [not null, unique]
platform_region varchar
}

Table Dim_Time {
time_key int [pk, not null, note: 'Surrogate key, format YYYYMMDD']
date date [not null, unique]
year int
quarter varchar
month int
month_name varchar
week int
day int [note: '28, 29, 30, 31 depending o n the month']
day_of_week int
day_of_the_year int
is_weekend boolean [note: 'TRUE for saturday and sunday']
is_payday boolean [note: 'TRUE for 15th day and 30th or 31st day, or 13th month pay']
is_mega_sale_day boolean [note: 'TRUE for 11.11, 12.12, etc. black friday, christmas']
}

Table Dim_Customer {
customer_key int [pk, not null, note: 'Internal surrogate ID']
platform_customer_id varchar [not null, unique, note: 'buyer_user_id for shopee // none for lazada so just generate']
buyer_segment varchar [note: 'Calculated: New Buyer or Returning Buyer']
total_orders int [note: 'Calculated: Count how many times platform_customer_id appears in orders']
customer_since date
last_order_date date [note: 'For Recency (RFM) analysis']
platform_key int [not null]
}

Table Dim_Product {

product_key int [pk, not null, note: 'internal surrogate ID']
product_item_id varchar [not null, note: 'lazada item_id, shopee item_id (This is the natural key)']
product_name varchar
product_category varchar [note: 'Specific product category']
product_status varchar [note: 'for lazada status: Active,InActive,Pending QC,Suspended,Deleted // for shopee item_status: NORMAL, BANNED, UNLIST, SELLER_DELETE, SHOPEE_DELETE, REVIEWING.']
product_rating decimal [note: 'product_rating for lazada // rating_star for shopee'] 
platform_key int [not null]

note: '''
This table contains 71 products (parent-level products).
Pricing and SKU information is stored at the variant level in Dim_Product_Variant.
Each product can have multiple variants with different prices and SKU identifiers.
'''
}

Table Dim_Product_Variant {
product_variant_key float [pk, not null, note: 'Surrogate key for product variants (e.g., 1.1, 100.1, 290.2). Format: X.1 for Lazada, X.2 for Shopee']
product_key float [not null, note: 'FK to Dim_Product - links variant to parent product']
platform_sku_id varchar [not null, note: 'SKU identifier from platform (Lazada: SkuId, Shopee: model_id)']
canonical_sku varchar [not null, note: 'Unified SKU for joining across platforms (SellerSku for Lazada, model_sku for Shopee)']
scent varchar [note: 'Variant scent attribute (e.g., Lavender, Ocean Breeze, Rose). NULL if not applicable']
volume varchar [note: 'Variant volume/size (e.g., 100ml, 50ml). NULL if not applicable']
current_price decimal [note: 'Current selling price for this variant. NULL for DEFAULT variants']
original_price decimal [note: 'Original price before discounts. NULL for DEFAULT variants']
created_at timestamp [note: 'Timestamp when variant record was created']
last_updated timestamp [note: 'Timestamp of last update']
platform_key int [not null, note: 'FK to Dim_Platform (1=Lazada, 2=Shopee)']

note: '''
This table contains 361 variants across 71 products:
- 104 Lazada variants (keys: 1.1 to 104.1)
- 186 Shopee specific variants (keys: 105.2 to 290.2) 
- 71 DEFAULT variants (created for products with no specific variants, platform_sku_id starts with "DEFAULT_")

Each product can have multiple variants (e.g., different scents, sizes).
DEFAULT variants are automatically created as fallback when SKU/model_id lookups fail.
'''
}

Table Dim_Order {

orders_key int [pk, not null, note: 'Surrogate Primary Key']
// Natural Keys
platform_order_id varchar [not null, note: 'Lazada: order_id; Shopee: order_sn']

// Attributes
order_status varchar [not null, note: 'Lazada: status; Shopee: order_status']
order_date timestamp [not null, note: 'Lazada: created_at; Shopee: create_time']
updated_at timestamp [note: 'Lazada: updated_at; Shopee: update_time']

// High-Level Measures (Non-Additive)
price_total decimal [note: 'note: LAZADA Total Paid = price + shipping_fee - voucher // shopee total_amount']
total_item_count int [note: 'Lazada: Total Number of Items = Count of objects in the order_items array; Shopee: item_count (total line items)']

// Operational Details
payment_method varchar 
shipping_city varchar

// Foreign Key
platform_key int [not null, note: 'FK to Dim_Platform (1=Lazada, 2=Shopee)']
}

// ===================================
// FACT TABLES (The Events and Metrics)
// ===================================

Table Fact_Orders {
order_item_key varchar [pk, not null, note: 'Primary Key. Your unique Line Item ID (Lazada/Shopee order_item_id or aggregated ID).']

// Foreign Keys (FKs)
orders_key int [not null, note: 'FK to Dim_Order (Order Header).']
product_key int [not null, note: 'FK to Dim_Product (SKU/Model ID). *TYPE FIXED: MUST BE INT*']
time_key int [not null, note: 'FK to Dim_Time. Date of order creation.']
customer_key int [not null, note: 'FK to Dim_Customer.']
platform_key int [not null, note: 'FK to Dim_Platform.']
 // Measures (Measures are always NOT NULL if data is successfully loaded/aggregated)
item_quantity int [not null, note: 'UNIFIED QUANTITY. Shopee: model_quantity. Lazada: COUNT of unit-level records grouped by Order ID and SKU ID.']
paid_price decimal [not null, note: 'Total Revenue for this LINE ITEM. Shopee: model_discounted_price * model_quantity_purchased. Lazada: SUM of paid_price from all aggregated unit records.']

// Discounts & Pricing (NULLable if the API doesn't provide them, e.g., Shopee vouchers)
original_unit_price decimal [note: 'The non-discounted price per unit (Lazada: item_price; Shopee: original_price).']
voucher_platform_amount decimal [note: 'Voucher amount subsidized by the Platform. Shopee: NULL.']
voucher_seller_amount decimal [note: 'Voucher amount subsidized by the Seller. Shopee: NULL.']

// Revenue-Side Shipping Fee
shipping_fee_paid_by_buyer decimal [note: 'Shipping fee amount paid by the buyer for this item. Lazada: shipping_amount. Shopee: actual_shipping_fee portioned to this item.']
}

Table Fact_Traffic {
traffic_event_key bigint [pk, not null]
time_key int [not null]
platform_key int [not null]

// Measures (Aggregated Daily) - Keeping only additive measures
clicks int [note: 'Additive measure, total clicks (same for both platforms)']
impressions int [not null, note: 'Additive measure, total impressions (Lazada: impressions, Shopee: impression)']
}

///
Table Fact_Sales_Aggregate {
  // -- Dimensions (Composite Grain: daily per platform per customer per product)
  time_key int [not null, note: 'FK to Dim_Time. The aggregation day.']
  platform_key int [not null, note: 'FK to Dim_Platform.']
  customer_key float [not null, note: 'FK to Dim_Customer.']
  product_key float [not null, note: 'FK to Dim_Product.']
  
  // -- SALES METRICS (Additives)
  total_orders int [not null, note: 'Count of distinct orders for this grain.']
  total_items_sold int [not null, note: 'Sum of Fact_Orders.item_quantity.']
  gross_revenue float [not null, note: 'Sum of Fact_Orders.paid_price (Item revenue only).']
  total_discounts float [not null, note: 'Sum of all vouchers/discounts.']
  net_sales float [not null, note: 'Calculated: gross_revenue - total_discounts']
  
  note: '''
  This aggregate fact table summarizes sales at the daily + platform + customer + product grain.
  No primary key defined - grain is the combination of time_key, platform_key, customer_key, product_key.
  Contains 39,868 aggregated records.
  Shipping revenue is tracked separately in Fact_Orders, not aggregated here.
  '''
}
}

// ===================================
// RELATIONSHIPS (The Top-Level Refs)
// ===================================

// Fact Orders Relationships
Ref: Fact_Orders.orders_key > Dim_Order.orders_key
Ref: Fact_Orders.product_key > Dim_Product.product_key // -- This join is now consistent (INT to INT)
Ref: Fact_Orders.product_variant_key > Dim_Product_Variant.product_variant_key // -- Links orders to specific product variants
Ref: Fact_Orders.time_key > Dim_Time.time_key
Ref: Fact_Orders.customer_key > Dim_Customer.customer_key
Ref: Fact_Orders.platform_key > Dim_Platform.platform_key

// Fact Sales Aggregate Relationships
Ref: Fact_Sales_Aggregate.time_key > Dim_Time.time_key
Ref: Fact_Sales_Aggregate.platform_key > Dim_Platform.platform_key
Ref: Fact_Sales_Aggregate.customer_key > Dim_Customer.customer_key
Ref: Fact_Sales_Aggregate.product_key > Dim_Product.product_key

// Product Variant Relationships
Ref: Dim_Product_Variant.product_key > Dim_Product.product_key // -- Links variants to parent products

// Dimension to Platform Relationships
Ref: Dim_Order.platform_key > Dim_Platform.platform_key
Ref: Dim_Product.platform_key > Dim_Platform.platform_key
Ref: Dim_Customer.platform_key > Dim_Platform.platform_key

// Fact Traffic Relationships (Note: fact_traffic.csv not generated yet)
Ref: Fact_Traffic.time_key > Dim_Time.time_key
Ref: Fact_Traffic.platform_key > Dim_Platform.platform_key


Ref: "Dim_Time"."day_of_the_year" < "Dim_Time"."is_weekend"