#!/usr/bin/env python3
"""
Analyze high-value items in fact_orders to validate legitimacy
"""

import pandas as pd
import numpy as np
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

def analyze_high_value_items():
    """Analyze high-value items for legitimacy"""
    print("🔍 ANALYZING HIGH-VALUE ITEMS")
    print("=" * 40)
    
    try:
        # Load fact orders
        df = pd.read_csv('../app/Transformed/fact_orders.csv')
        
        # Calculate line revenue
        df['line_revenue'] = df['paid_price'] * df['item_quantity']
        
        # Find high-value items (>₱50k)
        high_value = df[df['line_revenue'] > 50000].copy()
        print(f"Found {len(high_value)} high-value items (>₱50k)")
        
        if len(high_value) > 0:
            print("\nHigh-value items analysis:")
            print("-" * 40)
            
            # Sort by revenue descending
            high_value = high_value.sort_values('line_revenue', ascending=False)
            
            for idx, row in high_value.iterrows():
                platform = "Lazada" if row['platform_key'] == 1 else "Shopee"
                print(f"\n{platform} - Order: {row['orders_key']}")
                print(f"  Product: {row['product_key']}")
                print(f"  Quantity: {row['item_quantity']}")
                print(f"  Unit Price: ₱{row['paid_price']:,.2f}")
                print(f"  Line Revenue: ₱{row['line_revenue']:,.2f}")
                
                # Check if it's bulk quantity causing high value
                if row['item_quantity'] > 10:
                    print(f"  🔍 High quantity order (bulk purchase)")
                if row['paid_price'] > 5000:
                    print(f"  🔍 High unit price item")
        
        # Overall statistics
        print(f"\n📊 HIGH-VALUE STATISTICS:")
        print(f"Total high-value revenue: ₱{high_value['line_revenue'].sum():,.2f}")
        print(f"Average high-value amount: ₱{high_value['line_revenue'].mean():,.2f}")
        print(f"Highest single item: ₱{high_value['line_revenue'].max():,.2f}")
        
        # Check distribution
        print(f"\nBy platform:")
        platform_stats = high_value.groupby('platform_key').agg({
            'line_revenue': ['count', 'sum', 'mean']
        }).round(2)
        
        for platform_key in high_value['platform_key'].unique():
            platform_name = "Lazada" if platform_key == 1 else "Shopee"
            platform_data = high_value[high_value['platform_key'] == platform_key]
            print(f"  {platform_name}: {len(platform_data)} items, ₱{platform_data['line_revenue'].sum():,.2f} total")
        
        # Validate against business logic
        print(f"\n🧐 BUSINESS LOGIC CHECK:")
        
        # Check for potential data quality issues
        issues = []
        
        for idx, row in high_value.iterrows():
            # Check for unrealistic quantities
            if row['item_quantity'] > 100:
                issues.append(f"Order {row['orders_key']}: Extremely high quantity ({row['item_quantity']})")
            
            # Check for unrealistic unit prices
            if row['paid_price'] > 10000:
                issues.append(f"Order {row['orders_key']}: Very high unit price (₱{row['paid_price']:,.2f})")
            
            # Check for discount anomalies
            if row['paid_price'] > row['original_unit_price'] * 1.1:  # Paid more than 110% of original
                issues.append(f"Order {row['orders_key']}: Paid price exceeds original by >10%")
        
        if issues:
            print("⚠️  Potential issues found:")
            for issue in issues:
                print(f"  - {issue}")
        else:
            print("✅ All high-value items appear legitimate")
        
        return len(issues) == 0
        
    except Exception as e:
        print(f"❌ Error analyzing high-value items: {e}")
        return False

if __name__ == "__main__":
    analyze_high_value_items()