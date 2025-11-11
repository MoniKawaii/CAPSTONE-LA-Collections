"""
Test Fact Orders Data Integrity and Computation Accuracy
========================================================

This test suite validates the fact_orders.csv against raw data sources to ensure:
1. No missing order items from raw data
2. Accurate revenue and pricing computations
3. Correct foreign key relationships
4. Proper data transformations

Test Categories:
- Data Completeness: Check if all order items are captured
- Revenue Accuracy: Validate pricing computations
- Foreign Key Integrity: Ensure all relationships are valid
- Platform-Specific Logic: Test Lazada and Shopee transformations
- Edge Cases: Handle missing/null values properly
"""

import pandas as pd
import numpy as np
import json
import os
import sys
from datetime import datetime
import unittest

# Add parent directories to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'app'))

from app.config import FACT_ORDERS_COLUMNS


class TestFactOrdersIntegrity(unittest.TestCase):
    """Test suite for fact orders data integrity and accuracy"""
    
    @classmethod
    def setUpClass(cls):
        """Load all necessary data files for testing"""
        cls.base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        cls.staging_path = os.path.join(cls.base_path, 'app', 'Staging')
        cls.transformed_path = os.path.join(cls.base_path, 'app', 'Transformed')
        
        # Load fact orders (transformed data)
        fact_orders_path = os.path.join(cls.transformed_path, 'fact_orders.csv')
        cls.fact_orders_df = pd.read_csv(fact_orders_path)
        print(f"✓ Loaded {len(cls.fact_orders_df)} fact order records")
        
        # Load dimension tables
        cls.dim_order_df = pd.read_csv(os.path.join(cls.transformed_path, 'dim_order.csv'))
        cls.dim_customer_df = pd.read_csv(os.path.join(cls.transformed_path, 'dim_customer.csv'))
        cls.dim_product_df = pd.read_csv(os.path.join(cls.transformed_path, 'dim_product.csv'))
        cls.dim_variant_df = pd.read_csv(os.path.join(cls.transformed_path, 'dim_product_variant.csv'))
        
        # Load raw data
        cls._load_raw_data()
        
        print(f"✓ Test setup complete")
    
    @classmethod
    def _load_raw_data(cls):
        """Load raw JSON data files"""
        
        # Load Lazada raw data
        with open(os.path.join(cls.staging_path, 'lazada_multiple_order_items_raw.json'), 'r', encoding='utf-8') as f:
            cls.lazada_order_items_raw = json.load(f)
        
        with open(os.path.join(cls.staging_path, 'lazada_orders_raw.json'), 'r', encoding='utf-8') as f:
            cls.lazada_orders_raw = json.load(f)
        
        # Load Shopee raw data
        with open(os.path.join(cls.staging_path, 'shopee_orders_raw.json'), 'r', encoding='utf-8') as f:
            cls.shopee_orders_raw = json.load(f)
        
        # Load Shopee payment details
        cls.shopee_payment_details = {}
        payment_files = ['shopee_paymentdetail_raw.json', 'shopee_paymentdetail_2_raw.json']
        
        for filename in payment_files:
            try:
                with open(os.path.join(cls.staging_path, filename), 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for record in data:
                        order_sn = record.get('order_sn')
                        if order_sn:
                            cls.shopee_payment_details[order_sn] = record
            except FileNotFoundError:
                print(f"⚠️ Payment detail file not found: {filename}")
        
        print(f"✓ Loaded raw data:")
        print(f"  - Lazada order items: {len(cls.lazada_order_items_raw)} orders")
        print(f"  - Lazada orders: {len(cls.lazada_orders_raw)} orders")
        print(f"  - Shopee orders: {len(cls.shopee_orders_raw)} orders")
        print(f"  - Shopee payment details: {len(cls.shopee_payment_details)} orders")

    def test_fact_orders_schema(self):
        """Test that fact orders has correct schema"""
        print("\n🔍 Testing fact orders schema...")
        
        # Check all required columns are present
        expected_columns = set(FACT_ORDERS_COLUMNS)
        actual_columns = set(self.fact_orders_df.columns)
        
        missing_columns = expected_columns - actual_columns
        extra_columns = actual_columns - expected_columns
        
        self.assertEqual(len(missing_columns), 0, f"Missing columns: {missing_columns}")
        self.assertEqual(len(extra_columns), 0, f"Extra columns: {extra_columns}")
        
        # Check data types
        self.assertTrue(self.fact_orders_df['order_item_key'].dtype == 'object')
        self.assertTrue(pd.api.types.is_numeric_dtype(self.fact_orders_df['paid_price']))
        self.assertTrue(pd.api.types.is_numeric_dtype(self.fact_orders_df['original_unit_price']))
        
        print("✅ Schema validation passed")

    def test_lazada_completeness(self):
        """Test that all Lazada order items are captured in fact table"""
        print("\n🔍 Testing Lazada order items completeness...")
        
        # Count raw Lazada order items from COMPLETED orders
        completed_order_ids = set(
            self.dim_order_df[
                (self.dim_order_df['platform_key'] == 1) & 
                (self.dim_order_df['order_status'] == 'COMPLETED')
            ]['platform_order_id'].astype(str)
        )
        
        raw_item_count = 0
        raw_order_items = []
        
        for order_record in self.lazada_order_items_raw:
            order_id = str(order_record.get('order_id', ''))
            if order_id in completed_order_ids:
                order_items = order_record.get('order_items', [])
                raw_item_count += len(order_items)
                for item in order_items:
                    raw_order_items.append({
                        'order_id': order_id,
                        'sku_id': item.get('sku_id'),
                        'item_price': item.get('item_price'),
                        'paid_price': item.get('paid_price')
                    })
        
        # Count transformed Lazada items
        lazada_fact_count = len(self.fact_orders_df[self.fact_orders_df['platform_key'] == 1])
        
        print(f"📊 Lazada Items:")
        print(f"  - Raw items (completed orders): {raw_item_count}")
        print(f"  - Transformed items: {lazada_fact_count}")
        print(f"  - Coverage: {(lazada_fact_count/raw_item_count*100):.1f}%")
        
        # Allow for some missing items due to missing products in dim_product
        coverage_threshold = 0.95  # 95% minimum coverage
        self.assertGreaterEqual(
            lazada_fact_count / raw_item_count, 
            coverage_threshold,
            f"Lazada coverage below {coverage_threshold*100}%"
        )
        
        print("✅ Lazada completeness test passed")

    def test_shopee_completeness(self):
        """Test that all Shopee order items are captured in fact table"""
        print("\n🔍 Testing Shopee order items completeness...")
        
        # Count raw Shopee order items from COMPLETED orders
        completed_order_sns = set(
            self.dim_order_df[
                (self.dim_order_df['platform_key'] == 2) & 
                (self.dim_order_df['order_status'] == 'COMPLETED')
            ]['platform_order_id'].astype(str)
        )
        
        raw_item_count = 0
        raw_order_items = []
        
        for order in self.shopee_orders_raw:
            order_sn = str(order.get('order_sn', ''))
            if order_sn in completed_order_sns:
                item_list = order.get('item_list', [])
                for item in item_list:
                    quantity = item.get('model_quantity_purchased', 1)
                    raw_item_count += quantity  # Count individual items
                    raw_order_items.append({
                        'order_sn': order_sn,
                        'item_id': item.get('item_id'),
                        'model_id': item.get('model_id'),
                        'model_original_price': item.get('model_original_price'),
                        'model_discounted_price': item.get('model_discounted_price'),
                        'quantity': quantity
                    })
        
        # Count transformed Shopee items
        shopee_fact_count = self.fact_orders_df[self.fact_orders_df['platform_key'] == 2]['item_quantity'].sum()
        
        print(f"📊 Shopee Items:")
        print(f"  - Raw items (completed orders): {raw_item_count}")
        print(f"  - Transformed items: {int(shopee_fact_count)}")
        print(f"  - Coverage: {(shopee_fact_count/raw_item_count*100):.1f}%")
        
        # Allow for some missing items due to missing products in dim_product or customers
        coverage_threshold = 0.90  # 90% minimum coverage (Shopee has more edge cases)
        self.assertGreaterEqual(
            shopee_fact_count / raw_item_count, 
            coverage_threshold,
            f"Shopee coverage below {coverage_threshold*100}%"
        )
        
        print("✅ Shopee completeness test passed")

    def test_revenue_accuracy_lazada(self):
        """Test Lazada revenue computation accuracy"""
        print("\n🔍 Testing Lazada revenue accuracy...")
        
        # Get completed Lazada orders from fact table
        lazada_facts = self.fact_orders_df[self.fact_orders_df['platform_key'] == 1]
        fact_total_revenue = lazada_facts['paid_price'].sum()
        
        # Calculate raw revenue from completed orders
        completed_order_ids = set(
            self.dim_order_df[
                (self.dim_order_df['platform_key'] == 1) & 
                (self.dim_order_df['order_status'] == 'COMPLETED')
            ]['platform_order_id'].astype(str)
        )
        
        raw_total_revenue = 0
        for order_record in self.lazada_order_items_raw:
            order_id = str(order_record.get('order_id', ''))
            if order_id in completed_order_ids:
                order_items = order_record.get('order_items', [])
                for item in order_items:
                    raw_total_revenue += float(item.get('paid_price', 0))
        
        print(f"💰 Lazada Revenue:")
        print(f"  - Raw revenue (completed): ${raw_total_revenue:,.2f}")
        print(f"  - Fact revenue: ${fact_total_revenue:,.2f}")
        print(f"  - Difference: ${abs(fact_total_revenue - raw_total_revenue):,.2f}")
        print(f"  - Accuracy: {(min(fact_total_revenue, raw_total_revenue)/max(fact_total_revenue, raw_total_revenue)*100):.2f}%")
        
        # Allow 5% variance for rounding and missing items
        variance_threshold = 0.05
        revenue_ratio = abs(fact_total_revenue - raw_total_revenue) / max(fact_total_revenue, raw_total_revenue)
        
        self.assertLessEqual(
            revenue_ratio, 
            variance_threshold,
            f"Lazada revenue variance {revenue_ratio*100:.2f}% exceeds {variance_threshold*100}%"
        )
        
        print("✅ Lazada revenue accuracy test passed")

    def test_revenue_accuracy_shopee(self):
        """Test Shopee revenue computation accuracy using payment details"""
        print("\n🔍 Testing Shopee revenue accuracy...")
        
        # Get completed Shopee orders from fact table
        shopee_facts = self.fact_orders_df[self.fact_orders_df['platform_key'] == 2]
        fact_total_revenue = shopee_facts['paid_price'].sum()
        
        # Calculate raw revenue from completed orders using payment details
        completed_order_sns = set(
            self.dim_order_df[
                (self.dim_order_df['platform_key'] == 2) & 
                (self.dim_order_df['order_status'] == 'COMPLETED')
            ]['platform_order_id'].astype(str)
        )
        
        raw_total_revenue = 0
        for order in self.shopee_orders_raw:
            order_sn = str(order.get('order_sn', ''))
            if order_sn in completed_order_sns:
                # Use payment details if available for accurate pricing
                payment_detail = self.shopee_payment_details.get(order_sn, {})
                order_income = payment_detail.get('order_income', {})
                payment_items = order_income.get('items', [])
                
                if payment_items:
                    # Use payment detail pricing
                    for payment_item in payment_items:
                        discounted_price = float(payment_item.get('discounted_price', 0))
                        coin_discount = float(payment_item.get('discount_from_coin', 0))
                        voucher_shopee = float(payment_item.get('discount_from_voucher_shopee', 0))
                        voucher_seller = float(payment_item.get('discount_from_voucher_seller', 0))
                        
                        final_paid = discounted_price - coin_discount - voucher_shopee - voucher_seller
                        raw_total_revenue += final_paid
                else:
                    # Fallback to basic order data
                    item_list = order.get('item_list', [])
                    for item in item_list:
                        quantity = item.get('model_quantity_purchased', 1)
                        unit_price = float(item.get('model_discounted_price', 0))
                        raw_total_revenue += (unit_price * quantity)
        
        print(f"💰 Shopee Revenue:")
        print(f"  - Raw revenue (completed): ${raw_total_revenue:,.2f}")
        print(f"  - Fact revenue: ${fact_total_revenue:,.2f}")
        print(f"  - Difference: ${abs(fact_total_revenue - raw_total_revenue):,.2f}")
        print(f"  - Accuracy: {(min(fact_total_revenue, raw_total_revenue)/max(fact_total_revenue, raw_total_revenue)*100):.2f}%")
        
        # Allow 10% variance for Shopee due to complex payment logic and missing items
        variance_threshold = 0.10
        revenue_ratio = abs(fact_total_revenue - raw_total_revenue) / max(fact_total_revenue, raw_total_revenue)
        
        self.assertLessEqual(
            revenue_ratio, 
            variance_threshold,
            f"Shopee revenue variance {revenue_ratio*100:.2f}% exceeds {variance_threshold*100}%"
        )
        
        print("✅ Shopee revenue accuracy test passed")

    def test_foreign_key_integrity(self):
        """Test that all foreign keys are valid and no nulls exist"""
        print("\n🔍 Testing foreign key integrity...")
        
        # Check for null values in foreign key columns
        fk_columns = ['orders_key', 'customer_key', 'product_key', 'time_key']
        
        for column in fk_columns:
            null_count = self.fact_orders_df[column].isnull().sum()
            self.assertEqual(null_count, 0, f"Found {null_count} null values in {column}")
        
        # Check that foreign keys exist in dimension tables
        
        # Test orders_key
        fact_order_keys = set(self.fact_orders_df['orders_key'].unique())
        dim_order_keys = set(self.dim_order_df['orders_key'].unique())
        invalid_order_keys = fact_order_keys - dim_order_keys
        self.assertEqual(len(invalid_order_keys), 0, f"Invalid orders_key values: {invalid_order_keys}")
        
        # Test customer_key  
        fact_customer_keys = set(self.fact_orders_df['customer_key'].unique())
        dim_customer_keys = set(self.dim_customer_df['customer_key'].unique())
        invalid_customer_keys = fact_customer_keys - dim_customer_keys
        self.assertEqual(len(invalid_customer_keys), 0, f"Invalid customer_key values: {invalid_customer_keys}")
        
        # Test product_key
        fact_product_keys = set(self.fact_orders_df['product_key'].unique())
        dim_product_keys = set(self.dim_product_df['product_key'].unique())
        invalid_product_keys = fact_product_keys - dim_product_keys
        self.assertEqual(len(invalid_product_keys), 0, f"Invalid product_key values: {invalid_product_keys}")
        
        # Test product_variant_key (allow 0.0 for missing variants)
        fact_variant_keys = set(self.fact_orders_df['product_variant_key'].unique()) - {0.0}
        dim_variant_keys = set(self.dim_variant_df['product_variant_key'].unique())
        invalid_variant_keys = fact_variant_keys - dim_variant_keys
        self.assertEqual(len(invalid_variant_keys), 0, f"Invalid product_variant_key values: {invalid_variant_keys}")
        
        print("✅ Foreign key integrity test passed")

    def test_platform_key_distribution(self):
        """Test platform key distribution and order item key format"""
        print("\n🔍 Testing platform key distribution...")
        
        platform_counts = self.fact_orders_df['platform_key'].value_counts().sort_index()
        
        print(f"📊 Platform Distribution:")
        for platform_key, count in platform_counts.items():
            platform_name = "Lazada" if platform_key == 1 else "Shopee" if platform_key == 2 else "Unknown"
            print(f"  - {platform_name} (key={platform_key}): {count:,} records")
        
        # Test that we have both platforms
        self.assertIn(1, platform_counts.index, "No Lazada records found")
        self.assertIn(2, platform_counts.index, "No Shopee records found")
        
        # Test order_item_key format
        lazada_facts = self.fact_orders_df[self.fact_orders_df['platform_key'] == 1]
        shopee_facts = self.fact_orders_df[self.fact_orders_df['platform_key'] == 2]
        
        # All Lazada keys should start with 'L'
        lazada_key_format = lazada_facts['order_item_key'].str.startswith('L').all()
        self.assertTrue(lazada_key_format, "Not all Lazada order_item_keys start with 'L'")
        
        # All Shopee keys should start with 'S'
        shopee_key_format = shopee_facts['order_item_key'].str.startswith('S').all()
        self.assertTrue(shopee_key_format, "Not all Shopee order_item_keys start with 'S'")
        
        print("✅ Platform key distribution test passed")

    def test_data_quality_checks(self):
        """Test general data quality issues"""
        print("\n🔍 Testing data quality...")
        
        # Check for negative prices
        negative_paid = (self.fact_orders_df['paid_price'] < 0).sum()
        negative_original = (self.fact_orders_df['original_unit_price'] < 0).sum()
        
        self.assertEqual(negative_paid, 0, f"Found {negative_paid} negative paid_price values")
        self.assertEqual(negative_original, 0, f"Found {negative_original} negative original_unit_price values")
        
        # Check for zero quantities
        zero_quantity = (self.fact_orders_df['item_quantity'] <= 0).sum()
        self.assertEqual(zero_quantity, 0, f"Found {zero_quantity} zero or negative item_quantity values")
        
        # Check for reasonable date range (time_key)
        min_date = self.fact_orders_df['time_key'].min()
        max_date = self.fact_orders_df['time_key'].max()
        
        # Should be reasonable dates (between 2020 and 2026)
        self.assertGreaterEqual(min_date, 20200101, f"Minimum date {min_date} too early")
        self.assertLessEqual(max_date, 20260101, f"Maximum date {max_date} too late")
        
        # Check for duplicate order_item_keys
        duplicate_keys = self.fact_orders_df['order_item_key'].duplicated().sum()
        self.assertEqual(duplicate_keys, 0, f"Found {duplicate_keys} duplicate order_item_keys")
        
        print(f"✅ Data quality checks passed")
        print(f"  - No negative prices found")
        print(f"  - No zero quantities found") 
        print(f"  - Date range: {min_date} to {max_date}")
        print(f"  - No duplicate keys found")

    def test_revenue_reconciliation(self):
        """Test revenue reconciliation between fact table and dimension table"""
        print("\n🔍 Testing revenue reconciliation...")
        
        # Group fact orders by orders_key and sum revenue
        fact_revenue_by_order = self.fact_orders_df.groupby('orders_key')['paid_price'].sum()
        
        # Get completed orders from dimension table
        completed_orders = self.dim_order_df[self.dim_order_df['order_status'] == 'COMPLETED']
        
        # Compare revenues for orders that exist in both
        common_orders = set(fact_revenue_by_order.index) & set(completed_orders['orders_key'])
        
        total_variance = 0
        order_count = 0
        large_variances = []
        
        for orders_key in list(common_orders)[:100]:  # Test first 100 orders
            fact_revenue = fact_revenue_by_order.get(orders_key, 0)
            dim_row = completed_orders[completed_orders['orders_key'] == orders_key]
            
            if not dim_row.empty:
                dim_revenue = dim_row.iloc[0]['price_total']
                
                if dim_revenue > 0:  # Avoid division by zero
                    variance = abs(fact_revenue - dim_revenue) / dim_revenue
                    total_variance += variance
                    order_count += 1
                    
                    if variance > 0.1:  # More than 10% variance
                        large_variances.append({
                            'orders_key': orders_key,
                            'fact_revenue': fact_revenue,
                            'dim_revenue': dim_revenue,
                            'variance': variance * 100
                        })
        
        avg_variance = total_variance / order_count if order_count > 0 else 0
        
        print(f"📊 Revenue Reconciliation:")
        print(f"  - Orders tested: {order_count}")
        print(f"  - Average variance: {avg_variance*100:.2f}%")
        print(f"  - Large variances (>10%): {len(large_variances)}")
        
        if large_variances:
            print(f"  - Sample large variance:")
            sample = large_variances[0]
            print(f"    Order {sample['orders_key']}: Fact=${sample['fact_revenue']:.2f}, Dim=${sample['dim_revenue']:.2f}, Variance={sample['variance']:.1f}%")
        
        # Allow 15% average variance due to different calculation methods
        self.assertLessEqual(avg_variance, 0.15, f"Average revenue variance {avg_variance*100:.2f}% too high")
        
        print("✅ Revenue reconciliation test passed")

    def test_missing_orders_analysis(self):
        """Analyze which orders are missing and why"""
        print("\n🔍 Analyzing missing orders...")
        
        # Get all orders from raw data
        all_lazada_orders = set(str(order.get('order_id', '')) for order in self.lazada_order_items_raw)
        all_shopee_orders = set(str(order.get('order_sn', '')) for order in self.shopee_orders_raw)
        
        # Get orders in fact table
        fact_lazada_orders = set(
            self.dim_order_df[
                self.dim_order_df['orders_key'].isin(
                    self.fact_orders_df[self.fact_orders_df['platform_key'] == 1]['orders_key']
                )
            ]['platform_order_id'].astype(str)
        )
        
        fact_shopee_orders = set(
            self.dim_order_df[
                self.dim_order_df['orders_key'].isin(
                    self.fact_orders_df[self.fact_orders_df['platform_key'] == 2]['orders_key']
                )
            ]['platform_order_id'].astype(str)
        )
        
        # Find missing orders
        missing_lazada = all_lazada_orders - fact_lazada_orders
        missing_shopee = all_shopee_orders - fact_shopee_orders
        
        print(f"📊 Missing Orders Analysis:")
        print(f"  - Total Lazada orders in raw: {len(all_lazada_orders)}")
        print(f"  - Lazada orders in fact table: {len(fact_lazada_orders)}")
        print(f"  - Missing Lazada orders: {len(missing_lazada)}")
        print(f"  - Lazada coverage: {len(fact_lazada_orders)/len(all_lazada_orders)*100:.1f}%")
        
        print(f"  - Total Shopee orders in raw: {len(all_shopee_orders)}")
        print(f"  - Shopee orders in fact table: {len(fact_shopee_orders)}")
        print(f"  - Missing Shopee orders: {len(missing_shopee)}")
        print(f"  - Shopee coverage: {len(fact_shopee_orders)/len(all_shopee_orders)*100:.1f}%")
        
        # Analyze reasons for missing orders
        if missing_lazada:
            print(f"\n🔍 Analyzing missing Lazada orders (sample of 5):")
            for order_id in list(missing_lazada)[:5]:
                dim_order = self.dim_order_df[
                    (self.dim_order_df['platform_order_id'].astype(str) == order_id) &
                    (self.dim_order_df['platform_key'] == 1)
                ]
                
                if dim_order.empty:
                    print(f"  - {order_id}: Not in dim_order (likely filtered out)")
                else:
                    status = dim_order.iloc[0]['order_status']
                    print(f"  - {order_id}: Status = {status} (non-completed)")
        
        if missing_shopee:
            print(f"\n🔍 Analyzing missing Shopee orders (sample of 5):")
            for order_sn in list(missing_shopee)[:5]:
                dim_order = self.dim_order_df[
                    (self.dim_order_df['platform_order_id'].astype(str) == order_sn) &
                    (self.dim_order_df['platform_key'] == 2)
                ]
                
                if dim_order.empty:
                    print(f"  - {order_sn}: Not in dim_order (likely filtered out)")
                else:
                    status = dim_order.iloc[0]['order_status']
                    print(f"  - {order_sn}: Status = {status} (non-completed)")
        
        print("✅ Missing orders analysis complete")


def run_integrity_tests():
    """Run all integrity tests and generate report"""
    
    print("🚀 Starting Fact Orders Integrity Tests")
    print("=" * 60)
    
    # Create test suite
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestFactOrdersIntegrity)
    
    # Run tests with detailed output
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    print("\n" + "=" * 60)
    print("📊 TEST SUMMARY")
    print("=" * 60)
    
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    
    if result.failures:
        print("\n❌ FAILURES:")
        for test, traceback in result.failures:
            print(f"  - {test}: {traceback.split('AssertionError:')[-1].strip()}")
    
    if result.errors:
        print("\n❌ ERRORS:")
        for test, traceback in result.errors:
            print(f"  - {test}: {traceback.split('Exception:')[-1].strip()}")
    
    if result.wasSuccessful():
        print("\n✅ ALL TESTS PASSED - Data integrity verified!")
    else:
        print(f"\n❌ {len(result.failures + result.errors)} tests failed - Review data quality issues")
    
    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_integrity_tests()
    sys.exit(0 if success else 1)