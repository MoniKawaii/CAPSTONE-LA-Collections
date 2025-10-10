"""
Test Lazada Dimensional ETL with Sample Data
This script tests the dimensional transformation without requiring live API access
"""

import pandas as pd
import json
import sys
import os
from datetime import datetime, timedelta

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from app.lazada.lazada_dimensional_transformer import LazadaDimensionalTransformer

def create_sample_data():
    """Create sample Lazada data for testing"""
    
    # Sample orders data
    sample_orders = [
        {
            'order_id': 12345,
            'created_at': '2024-01-15T10:30:00+08:00',
            'updated_at': '2024-01-15T12:00:00+08:00',
            'status': 'delivered',
            'payment_method': 'credit_card',
            'address_billing': {
                'first_name': 'Juan',
                'last_name': 'Dela Cruz',
                'customer_email': 'juan@email.com',
                'phone': '09123456789',
                'address1': '123 Main St',
                'city': 'Manila',
                'country': 'Philippines',
                'post_code': '1000'
            },
            'address_shipping': {
                'first_name': 'Juan',
                'last_name': 'Dela Cruz',
                'address1': '123 Main St',
                'city': 'Manila',
                'country': 'Philippines',
                'post_code': '1000'
            }
        },
        {
            'order_id': 12346,
            'created_at': '2024-01-16T14:15:00+08:00',
            'updated_at': '2024-01-16T15:30:00+08:00',
            'status': 'shipped',
            'payment_method': 'gcash',
            'address_billing': {
                'first_name': 'Maria',
                'last_name': 'Santos',
                'customer_email': 'maria@email.com',
                'phone': '09187654321',
                'address1': '456 Side St',
                'city': 'Quezon City',
                'country': 'Philippines',
                'post_code': '1100'
            },
            'address_shipping': {
                'first_name': 'Maria',
                'last_name': 'Santos',
                'address1': '456 Side St',
                'city': 'Quezon City',
                'country': 'Philippines',
                'post_code': '1100'
            }
        }
    ]
    
    # Sample order items data
    sample_order_items = [
        {
            'order_id': 12345,
            'order_item_id': 67890,
            'sku': 'LACOL-SHIRT-001',
            'shop_sku': 'SHIRT001',
            'product_main_sku': 'SHIRT-MAIN-001',
            'name': 'LA Collections Cotton T-Shirt',
            'variation': 'Size: L, Color: Blue',
            'item_price': 1500.0,
            'paid_price': 1350.0,
            'purchase_order_number': 2,
            'tracking_code_pre': 'LBC123456789',
            'voucher_code': 'NEWCUST15'
        },
        {
            'order_id': 12345,
            'order_item_id': 67891,
            'sku': 'LACOL-PANTS-002',
            'shop_sku': 'PANTS002',
            'product_main_sku': 'PANTS-MAIN-002',
            'name': 'LA Collections Denim Jeans',
            'variation': 'Size: 32, Color: Dark Blue',
            'item_price': 2500.0,
            'paid_price': 2250.0,
            'purchase_order_number': 1,
            'tracking_code_pre': 'LBC123456790'
        },
        {
            'order_id': 12346,
            'order_item_id': 67892,
            'sku': 'LACOL-DRESS-003',
            'shop_sku': 'DRESS003',
            'product_main_sku': 'DRESS-MAIN-003',
            'name': 'LA Collections Summer Dress',
            'variation': 'Size: M, Color: Red',
            'item_price': 3000.0,
            'paid_price': 2700.0,
            'purchase_order_number': 1,
            'tracking_code_pre': 'LBC123456791',
            'voucher_code': 'SUMMER10'
        }
    ]
    
    # Sample products data
    sample_products = [
        {
            'seller_sku': 'LACOL-SHIRT-001',
            'product_id': 'PROD123001',
            'primary_category': 'Fashion > Men > Clothing > T-Shirts',
            'brand': 'LA Collections',
            'status': 'active',
            'package_weight': 0.3,
            'package_length': 25,
            'package_width': 20,
            'package_height': 3
        },
        {
            'seller_sku': 'LACOL-PANTS-002',
            'product_id': 'PROD123002',
            'primary_category': 'Fashion > Men > Clothing > Jeans',
            'brand': 'LA Collections',
            'status': 'active',
            'package_weight': 0.6,
            'package_length': 35,
            'package_width': 25,
            'package_height': 5
        },
        {
            'seller_sku': 'LACOL-DRESS-003',
            'product_id': 'PROD123003',
            'primary_category': 'Fashion > Women > Clothing > Dresses',
            'brand': 'LA Collections',
            'status': 'active',
            'package_weight': 0.4,
            'package_length': 30,
            'package_width': 25,
            'package_height': 4
        }
    ]
    
    # Sample vouchers data
    sample_vouchers = [
        {
            'voucher_code': 'NEWCUST15',
            'voucher_name': 'New Customer 15% Off',
            'discount_type': 'percentage',
            'discount_value': 15.0,
            'start_time': '2024-01-01T00:00:00+08:00',
            'end_time': '2024-12-31T23:59:59+08:00',
            'status': 'active',
            'description': '15% discount for new customers'
        },
        {
            'voucher_code': 'SUMMER10',
            'voucher_name': 'Summer Sale 10% Off',
            'discount_type': 'percentage',
            'discount_value': 10.0,
            'start_time': '2024-01-01T00:00:00+08:00',
            'end_time': '2024-08-31T23:59:59+08:00',
            'status': 'active',
            'description': '10% discount for summer collection'
        }
    ]
    
    return {
        'orders': pd.DataFrame(sample_orders),
        'order_items': pd.DataFrame(sample_order_items),
        'products': pd.DataFrame(sample_products),
        'vouchers': pd.DataFrame(sample_vouchers)
    }

def main():
    """Test the dimensional transformation with sample data"""
    print("🧪 Testing Lazada Dimensional ETL with Sample Data")
    print("="*60)
    
    try:
        # Create sample data
        print("📊 Creating sample data...")
        raw_data = create_sample_data()
        
        print(f"  • Orders: {len(raw_data['orders'])} records")
        print(f"  • Order Items: {len(raw_data['order_items'])} records")
        print(f"  • Products: {len(raw_data['products'])} records")
        print(f"  • Vouchers: {len(raw_data['vouchers'])} records")
        
        # Initialize transformer
        print("\n🔄 Initializing transformer...")
        transformer = LazadaDimensionalTransformer()
        
        # Transform to dimensional format
        print("\n⚙️ Running dimensional transformation...")
        dimensional_data = transformer.transform_all_dimensions(raw_data)
        
        # Save to CSV
        print("\n💾 Saving to CSV files...")
        output_dir = "data/test_dimensional_output"
        transformer.save_to_csv(dimensional_data, output_dir)
        
        # Display sample records
        print("\n📋 SAMPLE DIMENSIONAL DATA:")
        print("="*60)
        
        for table_name, df in dimensional_data.items():
            print(f"\n🗃️ {table_name.upper()}:")
            if not df.empty:
                print(f"   Columns: {list(df.columns)}")
                print(f"   Sample record:")
                sample = df.iloc[0].to_dict()
                for key, value in sample.items():
                    print(f"     • {key}: {value}")
            else:
                print("   (No data)")
        
        print("\n" + "="*60)
        print("✅ DIMENSIONAL ETL TEST COMPLETED SUCCESSFULLY!")
        print(f"📁 Output files saved to: {output_dir}")
        print("="*60)
        
    except Exception as e:
        print(f"\n❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()