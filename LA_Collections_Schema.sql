CREATE TABLE "Dim_Platform" (
  "platform_key" int PRIMARY KEY NOT NULL,
  "platform_name" varchar UNIQUE NOT NULL,
  "platform_region" varchar
);

CREATE TABLE "Dim_Time" (
  "time_key" int PRIMARY KEY NOT NULL,
  "date" date UNIQUE NOT NULL,
  "year" int,
  "quarter" varchar,
  "month" int,
  "month_name" varchar,
  "week" int,
  "day" int,
  "day_of_week" int,
  "day_of_the_year" int,
  "is_weekend" boolean,
  "is_payday" boolean,
  "is_mega_sale_day" boolean
);

CREATE TABLE "Dim_Customer" (
  "customer_key" int PRIMARY KEY NOT NULL,
  "platform_customer_id" varchar UNIQUE NOT NULL,
  "customer_city" varchar,
  "buyer_segment" varchar,
  "total_orders" int,
  "customer_since" date,
  "last_order_date" date,
  "platform_key" int NOT NULL
);

CREATE TABLE "Dim_Product" (
  "product_key" int PRIMARY KEY NOT NULL,
  "product_item_id" varchar NOT NULL,
  "product_name" varchar,
  "product_sku_base" varchar,
  "product_category" varchar,
  "product_status" varchar,
  "product_price" decimal,
  "product_rating" decimal,
  "platform_key" int NOT NULL
);

CREATE TABLE "Dim_Product_Variant" (
"product_variant_key" int PRIMARY KEY NOT NULL,
"product_key" int NOT NULL,
"platform_sku_id" varchar, -- Platform internal SKU ID (used for API calls)
"variant_sku" varchar NOT NULL, -- The specific SKU used for inventory (SellerSku/ShopSku)
"variant_attribute_1" varchar NULL, -- e.g., 'Color: Red'
"variant_attribute_2" varchar NULL, -- e.g., 'Size: L'
"variant_attribute_3" varchar NULL -- e.g., 'Material: Cotton'
"platform_key" int NOT NULL,
);

CREATE TABLE "Dim_Order" (
  "order_key" int PRIMARY KEY NOT NULL,
  "platform_order_id" varchar NOT NULL,
  "order_status" varchar NOT NULL,
  "order_date" date NOT NULL,
  "updated_at" date NOT NULL,
  "price_total" decimal,
  "total_item_count" int,
  "payment_method" varchar,
  "shipping_city" varchar,
  "platform_key" int NOT NULL
);

CREATE TABLE "Fact_Orders" (
  "order_item_key" int PRIMARY KEY NOT NULL,
  "orders_key" int NOT NULL,
  "product_key" int NOT NULL,
  "product_variant_key" int NOT NULL,
  "time_key" int NOT NULL,
  "customer_key" int NOT NULL,
  "platform_key" int NOT NULL,
  "item_quantity" int NOT NULL,
  "paid_price" decimal NOT NULL,
  "original_unit_price" decimal,
  "voucher_platform_amount" decimal,
  "voucher_seller_amount" decimal,
  "shipping_fee_paid_by_buyer" decimal
);

CREATE TABLE "Fact_Traffic" (
  "traffic_event_key" bigint PRIMARY KEY NOT NULL,
  "time_key" int NOT NULL,
  "platform_key" int NOT NULL,
  "clicks" int,
  "impressions" int NOT NULL
);

CREATE TABLE "Fact_Sales_Aggregate" (
  "sales_summary_key" serial PRIMARY KEY,
  "time_key" int NOT NULL,
  "platform_key" int NOT NULL,
  "buyer_segment" varchar NOT NULL,
  "total_orders" int DEFAULT 0,
  "successful_orders" int DEFAULT 0,
  "cancelled_orders" int DEFAULT 0,
  "returned_orders" int DEFAULT 0,
  "total_items_sold" int DEFAULT 0,
  "gross_revenue" decimal DEFAULT 0,
  "shipping_revenue" decimal DEFAULT 0,
  "total_discounts" decimal DEFAULT 0,
  "unique_customers" int DEFAULT 0,
  "created_at" DATE,
  "updated_at" DATE
);

COMMENT ON COLUMN "Dim_Platform"."platform_key" IS 'Surrogate key for the marketplace (1=Lazada, 2=Shopee)';

COMMENT ON COLUMN "Dim_Time"."time_key" IS 'Surrogate key, format YYYYMMDD';

COMMENT ON COLUMN "Dim_Time"."day" IS '28, 29, 30, 31 depending o n the month';

COMMENT ON COLUMN "Dim_Time"."is_weekend" IS 'TRUE for saturday and sunday';

COMMENT ON COLUMN "Dim_Time"."is_payday" IS 'TRUE for 15th day and 30th or 31st day, or 13th month pay';

COMMENT ON COLUMN "Dim_Time"."is_mega_sale_day" IS 'TRUE for 11.11, 12.12, etc. black friday, christmas';

COMMENT ON COLUMN "Dim_Customer"."customer_key" IS 'Internal surrogate ID';

COMMENT ON COLUMN "Dim_Customer"."platform_customer_id" IS 'buyer_user_id for shopee // none for lazada so just generate';

COMMENT ON COLUMN "Dim_Customer"."customer_city" IS 'Customer Location derived from shipping address';

COMMENT ON COLUMN "Dim_Customer"."buyer_segment" IS 'Calculated: New Buyer or Returning Buyer';

COMMENT ON COLUMN "Dim_Customer"."total_orders" IS 'Calculated: Count how many times platform_customer_id appears in orders';

COMMENT ON COLUMN "Dim_Customer"."last_order_date" IS 'For Recency (RFM) analysis';

COMMENT ON COLUMN "Dim_Product"."product_key" IS 'internal surrogate ID';

COMMENT ON COLUMN "Dim_Product"."product_item_id" IS 'lazada item_id, shopee item_id (This is the natural key)';

COMMENT ON COLUMN "Dim_Product"."product_category" IS 'Specific product category';

COMMENT ON COLUMN "Dim_Product"."product_status" IS 'for lazada status: Active,InActive,Pending QC,Suspended,Deleted // for shopee item_status: NORMAL, BANNED, UNLIST, SELLER_DELETE, SHOPEE_DELETE, REVIEWING.';

COMMENT ON COLUMN "Dim_Product"."product_price" IS 'price for lazada // original_price for shopee';

COMMENT ON COLUMN "Dim_Product"."product_rating" IS 'product_rating for lazada // rating_star for shopee';

COMMENT ON COLUMN "Dim_Order"."orders_key" IS 'Surrogate Primary Key';

COMMENT ON COLUMN "Dim_Order"."platform_order_id" IS 'Lazada: order_id; Shopee: order_sn';

COMMENT ON COLUMN "Dim_Order"."order_status" IS 'Lazada: status; Shopee: order_status';

COMMENT ON COLUMN "Dim_Order"."order_date" IS 'Lazada: created_at; Shopee: create_time';

COMMENT ON COLUMN "Dim_Order"."updated_at" IS 'Lazada: updated_at; Shopee: update_time';

COMMENT ON COLUMN "Dim_Order"."price_total" IS 'note: LAZADA Total Paid = price + shipping_fee - voucher // shopee total_amount';

COMMENT ON COLUMN "Dim_Order"."total_item_count" IS 'Lazada: Total Number of Items = Count of objects in the order_items array; Shopee: item_count (total line items)';

COMMENT ON COLUMN "Fact_Orders"."order_item_key" IS 'Primary Key. Your unique Line Item ID (Lazada/Shopee order_item_id or aggregated ID).';

COMMENT ON COLUMN "Fact_Orders"."orders_key" IS 'FK to Dim_Order (Order Header).';

COMMENT ON COLUMN "Fact_Orders"."product_key" IS 'FK to Dim_Product (SKU/Model ID). **TYPE FIXED: MUST BE INT**';

COMMENT ON COLUMN "Fact_Orders"."time_key" IS 'FK to Dim_Time. Date of order creation.';

COMMENT ON COLUMN "Fact_Orders"."customer_key" IS 'FK to Dim_Customer.';

COMMENT ON COLUMN "Fact_Orders"."platform_key" IS 'FK to Dim_Platform.';

COMMENT ON COLUMN "Fact_Orders"."item_quantity" IS 'UNIFIED QUANTITY. Shopee: model_quantity. Lazada: COUNT of unit-level records grouped by Order_ID and SKU_ID.';

COMMENT ON COLUMN "Fact_Orders"."paid_price" IS 'Total Revenue for this LINE ITEM. Shopee: model_discounted_price * model_quantity_purchased. Lazada: SUM of paid_price from all aggregated unit records.';

COMMENT ON COLUMN "Fact_Orders"."original_unit_price" IS 'The non-discounted price per unit (Lazada: item_price; Shopee: original_price).';

COMMENT ON COLUMN "Fact_Orders"."voucher_platform_amount" IS 'Voucher amount subsidized by the Platform. Shopee: NULL.';

COMMENT ON COLUMN "Fact_Orders"."voucher_seller_amount" IS 'Voucher amount subsidized by the Seller. Shopee: NULL.';

COMMENT ON COLUMN "Fact_Orders"."shipping_fee_paid_by_buyer" IS 'Shipping fee amount paid by the buyer for this item. Lazada: shipping_amount. Shopee: actual_shipping_fee portioned to this item.';

COMMENT ON COLUMN "Fact_Traffic"."clicks" IS 'Additive measure, total clicks (same for both platforms)';

COMMENT ON COLUMN "Fact_Traffic"."impressions" IS 'Additive measure, total impressions (Lazada: impressions, Shopee: impression)';

COMMENT ON COLUMN "Fact_Sales_Aggregate"."sales_summary_key" IS 'Surrogate Key';

COMMENT ON COLUMN "Fact_Sales_Aggregate"."time_key" IS 'FK to Dim_Time. The aggregation day.';

COMMENT ON COLUMN "Fact_Sales_Aggregate"."platform_key" IS 'FK to Dim_Platform.';

COMMENT ON COLUMN "Fact_Sales_Aggregate"."buyer_segment" IS 'Slice by customer type (New/Returning) - Pulled from Dim_Customer.';

COMMENT ON COLUMN "Fact_Sales_Aggregate"."total_orders" IS 'Count of distinct Order IDs.';

COMMENT ON COLUMN "Fact_Sales_Aggregate"."total_items_sold" IS 'Sum of Fact_Orders.item_quantity.';

COMMENT ON COLUMN "Fact_Sales_Aggregate"."gross_revenue" IS 'Sum of Fact_Orders.paid_price (Item revenue only).';

COMMENT ON COLUMN "Fact_Sales_Aggregate"."shipping_revenue" IS 'Sum of Fact_Orders.shipping_fee_paid_by_buyer.';

COMMENT ON COLUMN "Fact_Sales_Aggregate"."total_discounts" IS 'Sum of all vouchers/discounts.';

COMMENT ON COLUMN "Fact_Sales_Aggregate"."unique_customers" IS 'Count of distinct customer_key in this segment.';

ALTER TABLE "Fact_Sales_Aggregate" ADD FOREIGN KEY ("time_key") REFERENCES "Dim_Time" ("time_key");

ALTER TABLE "Fact_Sales_Aggregate" ADD FOREIGN KEY ("platform_key") REFERENCES "Dim_Platform" ("platform_key");

ALTER TABLE "Fact_Orders" ADD FOREIGN KEY ("orders_key") REFERENCES "Dim_Order" ("orders_key");

ALTER TABLE "Fact_Orders" ADD FOREIGN KEY ("product_key") REFERENCES "Dim_Product" ("product_key");

ALTER TABLE "Fact_Orders" ADD FOREIGN KEY ("time_key") REFERENCES "Dim_Time" ("time_key");

ALTER TABLE "Fact_Orders" ADD FOREIGN KEY ("customer_key") REFERENCES "Dim_Customer" ("customer_key");

ALTER TABLE "Fact_Orders" ADD FOREIGN KEY ("platform_key") REFERENCES "Dim_Platform" ("platform_key");

ALTER TABLE "Fact_Traffic" ADD FOREIGN KEY ("time_key") REFERENCES "Dim_Time" ("time_key");

ALTER TABLE "Fact_Traffic" ADD FOREIGN KEY ("platform_key") REFERENCES "Dim_Platform" ("platform_key");

ALTER TABLE "Dim_Time" ADD FOREIGN KEY ("is_weekend") REFERENCES "Dim_Time" ("day_of_the_year");
