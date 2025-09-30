"""
Mock Lazada Data Generator for Testing

This module generates realistic mock data for testing the star schema
while we resolve the Lazada API signature issues.
"""

import pandas as pd
import random
from datetime import datetime, timedelta
import sys
sys.path.append('./app')

from supabase_client import supabase

class MockLazadaDataGenerator:
    """Generate mock Lazada data for testing purposes"""
    
    def __init__(self):
        self.platform_key = 1  # Lazada
        
        # Sample data for realistic generation
        self.sample_products = [
            "iPhone 15 Pro Max 256GB Natural Titanium",
            "Samsung Galaxy S24 Ultra 512GB Phantom Black", 
            "MacBook Air M2 13-inch 8GB 256GB Midnight",
            "Dell XPS 13 Laptop Intel i7 16GB 512GB",
            "Sony WH-1000XM5 Noise Canceling Headphones",
            "Apple AirPods Pro 2nd Generation",
            "Nike Air Force 1 White Sneakers Size 9",
            "Adidas Ultraboost 22 Running Shoes Black",
            "Uniqlo Heattech Long Sleeve T-Shirt",
            "H&M Denim Jacket Classic Blue",
            "Instant Pot Duo 7-in-1 Electric Pressure Cooker",
            "Dyson V15 Detect Absolute Cordless Vacuum",
            "KitchenAid Stand Mixer 5-Quart Artisan Series",
            "Nespresso Vertuo Next Coffee Machine",
            "Ring Video Doorbell 4 Wireless Smart Doorbell"
        ]
        
        self.categories = [
            "Electronics", "Fashion", "Home & Living", "Sports & Outdoors", 
            "Health & Beauty", "Books & Media", "Toys & Games", "Automotive"
        ]
        
        self.philippines_cities = [
            "Manila", "Quezon City", "Caloocan", "Las Piñas", "Makati", 
            "Pasay", "Pasig", "Parañaque", "Valenzuela", "Muntinlupa",
            "Cebu City", "Davao City", "Cagayan de Oro", "Iloilo City", "Bacolod"
        ]
    
    def generate_mock_orders(self, num_orders=50, days_back=30):
        """Generate mock order data"""
        print(f"Generating {num_orders} mock orders for the last {days_back} days...")
        
        mock_orders = []
        
        for i in range(num_orders):
            # Random date within the last days_back days
            order_date = datetime.now() - timedelta(
                days=random.randint(0, days_back),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )
            
            # Random customer info
            customer_id = f"customer_{random.randint(1000, 9999)}"
            city = random.choice(self.philippines_cities)
            
            # Random number of items in order (1-5)
            num_items = random.randint(1, 5)
            order_items = []
            
            for j in range(num_items):
                product = random.choice(self.sample_products)
                category = random.choice(self.categories)
                quantity = random.randint(1, 3)
                
                # Price based on product type
                if "iPhone" in product or "MacBook" in product:
                    base_price = random.uniform(40000, 80000)  # PHP
                elif "Samsung" in product or "Dell" in product:
                    base_price = random.uniform(25000, 60000)
                elif "Nike" in product or "Adidas" in product:
                    base_price = random.uniform(3000, 8000)
                elif "Dyson" in product or "KitchenAid" in product:
                    base_price = random.uniform(15000, 35000)
                else:
                    base_price = random.uniform(500, 5000)
                
                item_price = round(base_price * quantity, 2)
                
                order_item = {
                    'sku_id': f"LZ_{abs(hash(product)) % 100000}",
                    'name': product,
                    'category': category,
                    'quantity': quantity,
                    'item_price': item_price,
                    'rating': round(random.uniform(3.5, 5.0), 1),
                    'review_count': random.randint(10, 500),
                    'stock': random.randint(0, 100)
                }
                order_items.append(order_item)
            
            # Calculate total order value
            total_value = sum(item['item_price'] for item in order_items)
            
            mock_order = {
                'order_id': f"LZ{random.randint(100000000, 999999999)}",
                'created_at': order_date.isoformat(),
                'status': random.choice(['delivered', 'shipped', 'pending', 'cancelled']),
                'customer_first_name': f"Customer",
                'customer_last_name': f"{random.randint(1000, 9999)}",
                'customer_id': customer_id,
                'address_shipping': {
                    'city': city,
                    'region': 'Metro Manila' if city in ['Manila', 'Quezon City', 'Makati'] else 'Provincial'
                },
                'order_items': order_items,
                'total_amount': total_value,
                'shipping_fee': random.uniform(50, 200),
                'voucher_amount': random.uniform(0, total_value * 0.1) if random.random() > 0.7 else 0
            }
            
            mock_orders.append(mock_order)
        
        print(f"Generated {len(mock_orders)} mock orders")
        return mock_orders
    
    def generate_mock_products(self, num_products=20):
        """Generate mock product catalog"""
        print(f"Generating {num_products} mock products...")
        
        mock_products = []
        
        for i, product_name in enumerate(self.sample_products[:num_products]):
            category = random.choice(self.categories)
            
            mock_product = {
                'item_id': f"LZ_{abs(hash(product_name)) % 100000}",
                'name': product_name,
                'category': category,
                'rating': round(random.uniform(3.0, 5.0), 1),
                'review_count': random.randint(5, 1000),
                'stock': random.randint(0, 500),
                'price': random.uniform(100, 50000),
                'promotion_type': random.choice([None, 'Flash Sale', 'Daily Deal', 'Mega Sale']) if random.random() > 0.6 else None
            }
            
            mock_products.append(mock_product)
        
        print(f"Generated {len(mock_products)} mock products")
        return mock_products
    
    def generate_mock_traffic_data(self, num_days=30):
        """Generate mock traffic and engagement metrics"""
        print(f"Generating mock traffic data for {num_days} days...")
        
        traffic_data = []
        
        for i in range(num_days):
            date = datetime.now() - timedelta(days=i)
            
            # Simulate daily metrics
            page_views = random.randint(100, 1000)
            unique_visitors = int(page_views * random.uniform(0.3, 0.7))  # 30-70% unique
            
            traffic_record = {
                'date': date.date(),
                'page_views': page_views,
                'unique_visitors': unique_visitors,
                'add_to_cart_count': int(unique_visitors * random.uniform(0.1, 0.3)),
                'wishlist_count': int(unique_visitors * random.uniform(0.05, 0.15)),
                'conversion_rate': round(random.uniform(0.02, 0.08), 4),  # 2-8%
                'bounce_rate': round(random.uniform(0.3, 0.7), 4)  # 30-70%
            }
            
            traffic_data.append(traffic_record)
        
        print(f"Generated traffic data for {len(traffic_data)} days")
        return traffic_data

def load_mock_data_to_star_schema():
    """Generate mock data and load it into the star schema"""
    try:
        print("=== Mock Lazada Data Generation & ETL ===")
        
        # Import the transformer from the API client
        from lazada_api_client import LazadaDataTransformer
        
        # Initialize components
        generator = MockLazadaDataGenerator()
        transformer = LazadaDataTransformer()
        
        # Generate mock data
        mock_orders = generator.generate_mock_orders(num_orders=25, days_back=30)
        mock_products = generator.generate_mock_products(num_products=15)
        mock_traffic = generator.generate_mock_traffic_data(num_days=30)
        
        # Transform orders to star schema
        print("\nTransforming mock orders to star schema...")
        fact_orders = transformer.transform_orders_to_facts(mock_orders)
        
        # Load into database
        print("\nLoading data into star schema...")
        success = transformer.load_facts_to_database(fact_orders)
        
        if success:
            # Query database to verify
            orders_result = supabase.table('Fact_Orders').select('*', count='exact').execute()
            customers_result = supabase.table('Dim_Customer').select('*', count='exact').execute()
            products_result = supabase.table('Dim_Product').select('*', count='exact').execute()
            time_result = supabase.table('Dim_Time').select('*', count='exact').execute()
            
            return {
                'success': True,
                'mock_orders_generated': len(mock_orders),
                'fact_records_created': len(fact_orders),
                'database_stats': {
                    'total_orders': orders_result.count,
                    'total_customers': customers_result.count,
                    'total_products': products_result.count,
                    'total_time_dimensions': time_result.count
                },
                'message': f'Successfully loaded {len(fact_orders)} fact records from {len(mock_orders)} mock orders'
            }
        else:
            return {
                'success': False,
                'message': 'Failed to load mock data into database'
            }
            
    except Exception as e:
        return {
            'success': False,
            'message': f'Error in mock data generation: {str(e)}'
        }

if __name__ == "__main__":
    # Test mock data generation
    result = load_mock_data_to_star_schema()
    print(f"\nResult: {result}")