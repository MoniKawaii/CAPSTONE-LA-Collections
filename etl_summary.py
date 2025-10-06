#!/usr/bin/env python3
"""
ETL Data Summary

Shows the extracted fields structure for each star schema table
"""

import json
from collections import Counter

def analyze_etl_data():
    """Analyze the extracted ETL data"""
    
    with open('lazada_etl_data.json', 'r') as f:
        data = json.load(f)
    
    print("🔍 LAZADA ETL DATA ANALYSIS")
    print("=" * 50)
    
    # Analyze each dimension and fact table
    for table_name, table_data in data.items():
        print(f"\n📋 {table_name.upper()}")
        print("-" * 30)
        
        if table_data:
            # Show field structure
            sample_record = table_data[0]
            print(f"📊 Records Count: {len(table_data)}")
            print("🔧 Fields:")
            
            for field, value in sample_record.items():
                field_type = type(value).__name__
                print(f"  • {field}: {field_type} = {value}")
            
            # Show some statistics for fact_orders
            if table_name == 'fact_orders':
                print("\n📈 FACT_ORDERS STATISTICS:")
                
                # Order status distribution
                statuses = [order['order_status'] for order in table_data]
                status_counts = Counter(statuses)
                print(f"  📦 Order Status Distribution:")
                for status, count in status_counts.items():
                    print(f"    • {status}: {count} orders")
                
                # Payment methods
                payment_methods = [order['payment_method'] for order in table_data]
                payment_counts = Counter(payment_methods)
                print(f"  💳 Payment Methods:")
                for method, count in payment_counts.items():
                    print(f"    • {method}: {count} orders")
                
                # Price statistics
                prices = [float(order['paid_price']) for order in table_data]
                print(f"  💰 Price Statistics:")
                print(f"    • Total Revenue: ₱{sum(prices):,.2f}")
                print(f"    • Average Order Value: ₱{sum(prices)/len(prices):,.2f}")
                print(f"    • Min Order: ₱{min(prices):,.2f}")
                print(f"    • Max Order: ₱{max(prices):,.2f}")
                
                # Items count
                items = [order['item_quantity'] for order in table_data]
                print(f"  📦 Items Statistics:")
                print(f"    • Total Items Sold: {sum(items)}")
                print(f"    • Average Items per Order: {sum(items)/len(items):.1f}")
        else:
            print("  ❌ No data")
    
    print(f"\n✅ ETL EXTRACTION COMPLETE")
    print(f"📁 Data saved in: lazada_etl_data.json")

def show_mapping_to_database():
    """Show how extracted fields map to database schema"""
    
    print(f"\n🗃️  DATABASE SCHEMA MAPPING")
    print("=" * 50)
    
    mappings = {
        "Dim_Time": {
            "description": "Time dimension for date-based analysis",
            "source_field": "created_at from orders",
            "extracted_fields": {
                "date": "Order creation date",
                "day_of_week": "1-7 (Monday=1)",
                "month": "1-12",
                "year": "YYYY format",
                "is_mega_sale_day": "Boolean (currently false, can be enhanced)"
            }
        },
        "Dim_Customer": {
            "description": "Customer dimension",
            "source_field": "customer info + shipping address",
            "extracted_fields": {
                "platform_buyer_id": "Generated from name + city",
                "city": "From shipping address",
                "region": "Platform region (Philippines)",
                "buyer_segment": "Default 'Regular' (can be enhanced)",
                "LTV_tier": "Default 'Standard' (can be enhanced)",
                "last_order_date": "Most recent order date"
            }
        },
        "Dim_Platform": {
            "description": "Platform dimension",
            "source_field": "Static + API endpoint",
            "extracted_fields": {
                "platform_name": "Lazada",
                "platform_region": "Philippines"
            }
        },
        "Fact_Orders": {
            "description": "Main fact table with order transactions",
            "source_field": "Order details from API",
            "extracted_fields": {
                "order_id": "Lazada order ID",
                "order_date": "Order creation date",
                "customer_id": "Links to Dim_Customer",
                "platform": "Links to Dim_Platform", 
                "paid_price": "Order total price",
                "item_quantity": "Number of items",
                "shipping_fee": "Shipping cost",
                "voucher_amount": "Total voucher discounts",
                "order_status": "Current order status",
                "payment_method": "COD, etc.",
                "cancellation_reason": "If canceled",
                "seller_commission_fee": "Currently 0 (not in API)",
                "platform_subsidy_amount": "Platform voucher amount"
            }
        }
    }
    
    for table, info in mappings.items():
        print(f"\n📋 {table}")
        print(f"   📝 {info['description']}")
        print(f"   🔗 Source: {info['source_field']}")
        print(f"   🔧 Fields:")
        for field, desc in info['extracted_fields'].items():
            print(f"      • {field}: {desc}")

if __name__ == "__main__":
    analyze_etl_data()
    show_mapping_to_database()