"""
Enhanced Sales Aggregate Analysis
Show how one detailed table can support all slicing needs through SQL aggregation
"""

import pandas as pd
import os

def demonstrate_single_table_slicing():
    """Show how to achieve data cube functionality with one detailed table"""
    
    print("🎲 SINGLE TABLE DATA CUBE APPROACH")
    print("=" * 60)
    
    # Load fact orders to simulate the enhanced sales aggregate
    base_path = r'app\Transformed'
    fact_orders = pd.read_csv(os.path.join(base_path, 'fact_orders.csv'))
    
    # Create the most detailed sales aggregate (Time × Platform × Customer × Product)
    detailed_agg = fact_orders.groupby([
        'time_key', 'platform_key', 'customer_key', 'product_key'
    ]).agg({
        'orders_key': 'nunique',
        'item_quantity': 'sum',
        'paid_price': 'sum',
        'voucher_platform_amount': 'sum',
        'voucher_seller_amount': 'sum'
    }).reset_index()
    
    # Add derived columns
    detailed_agg['total_discounts'] = detailed_agg['voucher_platform_amount'] + detailed_agg['voucher_seller_amount']
    detailed_agg['gross_revenue'] = detailed_agg['paid_price'] + detailed_agg['total_discounts']
    
    print(f"📊 Enhanced Sales Aggregate Table:")
    print(f"   Records: {len(detailed_agg):,}")
    print(f"   Grain: Time × Platform × Customer × Product")
    print(f"   Total Revenue: ${detailed_agg['paid_price'].sum():,.2f}")
    
    slicing_examples = [
        {
            'question': '🎯 Customer Analysis: "Customer lifetime value by platform"',
            'group_by': ['platform_key', 'customer_key'],
            'description': 'Aggregate out time and product dimensions'
        },
        {
            'question': '📦 Product Analysis: "Product performance over time"',
            'group_by': ['time_key', 'product_key'],
            'description': 'Aggregate out platform and customer dimensions'
        },
        {
            'question': '📈 Executive Dashboard: "Daily sales by platform"',
            'group_by': ['time_key', 'platform_key'],
            'description': 'Aggregate out customer and product dimensions'
        },
        {
            'question': '🔍 Customer-Product Affinity: "What products do customers buy together?"',
            'group_by': ['customer_key', 'product_key'],
            'description': 'Aggregate out time and platform dimensions'
        },
        {
            'question': '📅 Seasonal Trends: "Monthly sales patterns"',
            'group_by': ['time_key'],
            'description': 'Aggregate out all other dimensions - total business view',
            'time_transform': 'Convert time_key to month for grouping'
        }
    ]
    
    print(f"\n🔍 SLICING EXAMPLES (SQL-equivalent operations):")
    print("=" * 60)
    
    for i, example in enumerate(slicing_examples, 1):
        print(f"\n{i}. {example['question']}")
        print(f"   📊 GROUP BY: {', '.join(example['group_by'])}")
        print(f"   💭 Logic: {example['description']}")
        
        # Perform the actual aggregation
        if 'time_transform' in example:
            # Special handling for time-based aggregation
            temp_df = detailed_agg.copy()
            temp_df['month_key'] = temp_df['time_key'].astype(str).str[:6]  # YYYYMM
            result = temp_df.groupby(['month_key']).agg({
                'orders_key': 'sum',
                'item_quantity': 'sum',
                'paid_price': 'sum',
                'gross_revenue': 'sum'
            }).reset_index()
            print(f"   📈 Result: {len(result)} months, ${result['paid_price'].sum():,.2f} total")
        else:
            result = detailed_agg.groupby(example['group_by']).agg({
                'orders_key': 'sum',
                'item_quantity': 'sum', 
                'paid_price': 'sum',
                'gross_revenue': 'sum'
            }).reset_index()
            
            records = len(result)
            revenue = result['paid_price'].sum()
            compression = len(detailed_agg) / records
            
            print(f"   📈 Result: {records:,} records ({compression:.1f}x compression)")
            print(f"   💰 Revenue: ${revenue:,.2f}")
            
            # Show sample results
            if records <= 10:
                print(f"   📋 Sample: {result.head(3)[example['group_by'] + ['paid_price']].to_dict('records')}")

def show_sql_equivalents():
    """Show the SQL queries that would achieve the same slicing"""
    
    print(f"\n{'=' * 60}")
    print(f"💻 SQL QUERY EQUIVALENTS")
    print(f"{'=' * 60}")
    
    sql_examples = [
        {
            'purpose': 'Customer Lifetime Value',
            'sql': """
SELECT customer_key, platform_key,
       SUM(paid_price) as lifetime_value,
       SUM(item_quantity) as total_items,
       SUM(orders_key) as total_orders
FROM fact_sales_aggregate 
GROUP BY customer_key, platform_key
ORDER BY lifetime_value DESC;"""
        },
        {
            'purpose': 'Product Performance by Month',
            'sql': """
SELECT SUBSTR(time_key, 1, 6) as month_key, product_key,
       SUM(paid_price) as monthly_revenue,
       SUM(item_quantity) as items_sold
FROM fact_sales_aggregate 
GROUP BY SUBSTR(time_key, 1, 6), product_key
ORDER BY month_key, monthly_revenue DESC;"""
        },
        {
            'purpose': 'Daily Platform Summary',
            'sql': """
SELECT time_key, platform_key,
       SUM(paid_price) as daily_revenue,
       SUM(orders_key) as total_orders,
       COUNT(DISTINCT customer_key) as unique_customers
FROM fact_sales_aggregate 
GROUP BY time_key, platform_key
ORDER BY time_key;"""
        },
        {
            'purpose': 'Cross-Selling Analysis',
            'sql': """
SELECT customer_key, 
       COUNT(DISTINCT product_key) as products_bought,
       SUM(paid_price) as total_spent
FROM fact_sales_aggregate
GROUP BY customer_key
HAVING COUNT(DISTINCT product_key) > 1
ORDER BY products_bought DESC;"""
        }
    ]
    
    for i, example in enumerate(sql_examples, 1):
        print(f"\n{i}. {example['purpose']}:")
        print(f"{example['sql']}")

def recommend_approach():
    """Recommend the best approach for single-table data cube"""
    
    print(f"\n{'=' * 60}")
    print(f"💡 RECOMMENDATION: ENHANCED SINGLE TABLE")
    print(f"{'=' * 60}")
    
    print(f"""
🎯 KEEP ONE TABLE with detailed grain (Time × Platform × Customer × Product)

✅ ADVANTAGES:
   • Single source of truth
   • No data synchronization issues
   • All slicing achievable through SQL GROUP BY
   • Flexible - can answer any business question
   • Simple to maintain

⚠️  CONSIDERATIONS:
   • Order counts will be inflated for product-level analysis
   • Larger table size (but still manageable at 78K records)
   • Need to educate users on proper aggregation

🚀 SOLUTION FOR ORDER COUNT ISSUE:
   Add a flag column to distinguish analysis types:
   
   • use_for_customer_analysis: When grouping by customer (orders accurate)
   • use_for_product_analysis: When grouping by product (expect inflation)
   • use_for_executive_summary: When grouping by time/platform (orders accurate)

📊 ENHANCED SCHEMA SUGGESTION:
   time_key, platform_key, customer_key, product_key,
   total_orders, total_items_sold, gross_revenue, total_discounts, net_sales,
   --> ADD: analysis_context (JSON field with guidance)
   
🎲 QUERY PATTERNS:
   • Customer analysis: GROUP BY customer_key, platform_key  
   • Product analysis: GROUP BY product_key, time_key (expect order inflation)
   • Executive summary: GROUP BY time_key, platform_key
   • Advanced: Any combination of dimensions
""")

if __name__ == "__main__":
    demonstrate_single_table_slicing()
    show_sql_equivalents() 
    recommend_approach()