CREATE TABLE "Dim_Platform" (
  "platform_key" int PRIMARY KEY NOT NULL,
  "platform_name" varchar UNIQUE NOT NULL,
  "platform_region" varchar NOT NULL
);

CREATE TABLE "Dim_Time" (
  "time_key" int PRIMARY KEY NOT NULL,
  "date" date UNIQUE NOT NULL,
  "day_of_week" int,
  "month" int,
  "year" int,
  "is_mega_sale_day" boolean
);

CREATE TABLE "Dim_Customer" (
  "customer_key" int PRIMARY KEY NOT NULL,
  "platform_buyer_id" varchar UNIQUE NOT NULL,
  "city" varchar,
  "region" varchar,
  "buyer_segment" varchar,
  "LTV_tier" varchar,
  "last_order_date" date
);

CREATE TABLE "Dim_Product" (
  "product_key" int PRIMARY KEY NOT NULL,
  "lazada_item_id" varchar,
  "shopee_item_id" varchar,
  "product_name" varchar,
  "category_l2" varchar,
  "product_rating" decimal,
  "review_count" int,
  "stock_on_hand" int,
  "promo_type" varchar
);

CREATE TABLE "Fact_Orders" (
  "order_item_key" bigint PRIMARY KEY NOT NULL,
  "time_key" int NOT NULL,
  "product_key" int NOT NULL,
  "customer_key" int NOT NULL,
  "platform_key" int NOT NULL,
  "paid_price" decimal NOT NULL,
  "item_quantity" int NOT NULL,
  "cancellation_reason" varchar,
  "return_reason" varchar,
  "seller_commission_fee" decimal,
  "platform_subsidy_amount" decimal
);

CREATE TABLE "Fact_Traffic" (
  "traffic_event_key" bigint PRIMARY KEY NOT NULL,
  "time_key" int NOT NULL,
  "product_key" int NOT NULL,
  "customer_key" int NOT NULL,
  "platform_key" int NOT NULL,
  "page_views" int NOT NULL,
  "visits" int NOT NULL,
  "add_to_cart_count" int,
  "wishlist_add_count" int
);

CREATE TABLE "Fact_Activity" (
  "activity_event_key" bigint PRIMARY KEY NOT NULL,
  "time_key" int NOT NULL,
  "customer_key" int NOT NULL,
  "platform_key" int NOT NULL,
  "activity_type" varchar NOT NULL,
  "chat_response_time_seconds" int,
  "follower_count_change" int
);

COMMENT ON COLUMN "Dim_Platform"."platform_key" IS 'Surrogate key for the marketplace (1=Lazada, 2=Shopee)';

COMMENT ON COLUMN "Dim_Platform"."platform_region" IS 'e.g., PH, MY, SG';

COMMENT ON COLUMN "Dim_Time"."time_key" IS 'Surrogate key, format YYYYMMDD';

COMMENT ON COLUMN "Dim_Time"."is_mega_sale_day" IS 'TRUE for 11.11, 12.12, etc.';

COMMENT ON COLUMN "Dim_Customer"."customer_key" IS 'Internal anonymous surrogate ID';

COMMENT ON COLUMN "Dim_Customer"."platform_buyer_id" IS 'Masked, unique buyer ID from the marketplace API (PII-compliant)';

COMMENT ON COLUMN "Dim_Customer"."city" IS 'Customer Location derived from shipping address';

COMMENT ON COLUMN "Dim_Customer"."region" IS 'e.g., Metro Manila, Provincial';

COMMENT ON COLUMN "Dim_Customer"."buyer_segment" IS 'Calculated: New Buyer or Returning Buyer';

COMMENT ON COLUMN "Dim_Customer"."LTV_tier" IS 'Calculated: Gold, Silver, Bronze';

COMMENT ON COLUMN "Dim_Customer"."last_order_date" IS 'For Recency (RFM) analysis';

COMMENT ON COLUMN "Dim_Product"."product_key" IS 'Your internal universal SKU ID';

COMMENT ON COLUMN "Dim_Product"."lazada_item_id" IS 'Lazada specific product ID';

COMMENT ON COLUMN "Dim_Product"."shopee_item_id" IS 'Shopee specific product ID';

COMMENT ON COLUMN "Dim_Product"."category_l2" IS 'Specific product category';

COMMENT ON COLUMN "Dim_Product"."stock_on_hand" IS 'Snapshot of inventory level';

COMMENT ON COLUMN "Dim_Product"."promo_type" IS 'e.g., Flash Sale, Platform Voucher';

COMMENT ON COLUMN "Fact_Orders"."order_item_key" IS 'Unique ID for each line item in an order';

COMMENT ON COLUMN "Fact_Orders"."paid_price" IS 'Total revenue for this item (for AOV and Sales Revenue)';

COMMENT ON COLUMN "Fact_Orders"."item_quantity" IS 'Units Sold';

COMMENT ON COLUMN "Fact_Orders"."seller_commission_fee" IS 'Fee paid by seller to platform';

COMMENT ON COLUMN "Fact_Orders"."platform_subsidy_amount" IS 'Voucher/discount amount subsidized by the platform';

COMMENT ON COLUMN "Fact_Activity"."activity_type" IS 'e.g., CHAT_SENT, SHOP_FOLLOWED, COUPON_CLAIMED';

COMMENT ON COLUMN "Fact_Activity"."follower_count_change" IS 'e.g., +1 when shop is followed';

ALTER TABLE "Fact_Orders" ADD FOREIGN KEY ("time_key") REFERENCES "Dim_Time" ("time_key");

ALTER TABLE "Fact_Orders" ADD FOREIGN KEY ("product_key") REFERENCES "Dim_Product" ("product_key");

ALTER TABLE "Fact_Orders" ADD FOREIGN KEY ("customer_key") REFERENCES "Dim_Customer" ("customer_key");

ALTER TABLE "Fact_Orders" ADD FOREIGN KEY ("platform_key") REFERENCES "Dim_Platform" ("platform_key");

ALTER TABLE "Fact_Traffic" ADD FOREIGN KEY ("time_key") REFERENCES "Dim_Time" ("time_key");

ALTER TABLE "Fact_Traffic" ADD FOREIGN KEY ("product_key") REFERENCES "Dim_Product" ("product_key");

ALTER TABLE "Fact_Traffic" ADD FOREIGN KEY ("customer_key") REFERENCES "Dim_Customer" ("customer_key");

ALTER TABLE "Fact_Traffic" ADD FOREIGN KEY ("platform_key") REFERENCES "Dim_Platform" ("platform_key");

ALTER TABLE "Fact_Activity" ADD FOREIGN KEY ("time_key") REFERENCES "Dim_Time" ("time_key");

ALTER TABLE "Fact_Activity" ADD FOREIGN KEY ("customer_key") REFERENCES "Dim_Customer" ("customer_key");

ALTER TABLE "Fact_Activity" ADD FOREIGN KEY ("platform_key") REFERENCES "Dim_Platform" ("platform_key");
