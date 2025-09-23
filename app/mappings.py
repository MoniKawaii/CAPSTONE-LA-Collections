MAPPINGS = {
    "Shopee": {
        "Date": "date",
        "Page Views": "page_views",
        "Visitors": "unique_visitors",
        "Sales (PHP)": "total_sales_value",
        "Orders": "total_orders",
        "Sales per Order": "average_order_value",
        "Conversion Rate (Confirmed Order)": "conversion_rate",
        "Cancelled Orders": "cancelled_orders_count",
        "Cancelled Sales": "cancelled_sales_value",
        "Returned/Refunded Orders": "returned_orders_count",
        "Returned/Refunded Sales": "refunded_sales_value",
        "Buyers": "unique_buyers",
        "Potential Buyers": "potential_buyers",
        "New Buyers": "new_buyers",
        "Existing Buyers": "existing_buyers",
        "Repeat Purchase Rate": "repeat_purchase_rate",
        # Lazada-only metrics → Shopee doesn’t provide them
        # We'll fill with None when uploading
    },
    "Lazada": {
        "Date": "date",
        "Pageviews": "page_views",
        "Visitors": "unique_visitors",
        "Revenue": "total_sales_value",
        "Orders": "total_orders",
        "Average Order Value": "average_order_value",
        "Conversion Rate": "conversion_rate",
        "Cancelled Amount": "cancelled_sales_value",
        "Return/Refund Amount": "refunded_sales_value",
        "Buyers": "unique_buyers",
        "Units Sold": "units_sold",
        "Revenue per Buyer": "revenue_per_buyer",
        "Visitor Value": "visitor_value",
        "Add to Cart Users": "add_to_cart_users",
        "Add to Cart Units": "add_to_cart_units",
        "Wishlists": "wishlist_count",
        "Wishlist Users": "wishlist_users",
        "Average Basket Size": "average_basket_size",
        # Shopee-only metrics → Lazada doesn’t provide them
    }
}
# note: The keys are the original column names from the CSVs 
## MAYBE NOT YET FINAL