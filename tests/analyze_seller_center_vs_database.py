#!/usr/bin/env python3
"""
Lazada Seller Center vs Database Comparison Analysis
Analyzing discrepancies between Lazada Seller Center CSV exports and our dimensional model
"""

import pandas as pd
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

def analyze_seller_center_vs_database():
    """
    Analyze discrepancies between Lazada Seller Center CSV data and our database
    """
    
    print("=" * 80)
    print("🔍 LAZADA SELLER CENTER vs DATABASE COMPARISON")
    print("=" * 80)
    
    # ================================================================
    # 1. Load comparison data and our dimensional model
    # ================================================================
    print("\n📊 1. LOADING COMPARISON DATA")
    print("-" * 50)
    
    try:
        # Load the comparison file
        comparison = pd.read_csv('data/compare_lazada.csv')
        print(f"✅ Loaded comparison data: {len(comparison)} monthly records")
        
        # Load our dimensional model
        dim_order = pd.read_csv('app/Transformed/dim_order.csv')
        lazada_orders = dim_order[dim_order['platform_key'] == 1].copy()
        lazada_orders['order_date'] = pd.to_datetime(lazada_orders['order_date'])
        lazada_orders['order_month'] = lazada_orders['order_date'].dt.to_period('M')
        
        print(f"✅ Loaded dimensional model: {len(lazada_orders)} Lazada orders")
        print(f"📅 DB date range: {lazada_orders['order_date'].min().date()} to {lazada_orders['order_date'].max().date()}")
        
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return
    
    # ================================================================
    # 2. Parse and analyze comparison data
    # ================================================================
    print(f"\n📊 2. COMPARISON DATA ANALYSIS")
    print("-" * 50)
    
    try:
        # Parse dates and clean revenue data
        comparison['date_parsed'] = pd.to_datetime(comparison['date'])
        comparison['order_month'] = comparison['date_parsed'].dt.to_period('M')
        
        # Clean revenue columns (remove currency symbols and commas)
        comparison['gross_db_clean'] = comparison['gross db '].str.replace(',', '').astype(float)
        comparison['gross_csv_clean'] = comparison['gross csv'].str.replace('?', '').str.replace(',', '').astype(float)
        comparison['missing_gross_clean'] = comparison['missing gross'].str.replace('?', '').str.replace('-?', '-').str.replace(',', '').astype(float)
        
        print(f"📈 SELLER CENTER vs DATABASE OVERVIEW:")
        print(f"Date range: {comparison['date'].min()} to {comparison['date'].max()}")
        print(f"Total months compared: {len(comparison)}")
        
        # Summary statistics
        total_db_orders = comparison['orders db'].sum()
        total_csv_orders = comparison['orders csv'].sum()
        total_missing_orders = comparison['missing orders'].sum()
        
        total_db_revenue = comparison['gross_db_clean'].sum()
        total_csv_revenue = comparison['gross_csv_clean'].sum()
        total_missing_revenue = comparison['missing_gross_clean'].sum()
        
        print(f"\n📊 TOTAL SUMMARY:")
        print(f"  Database orders: {total_db_orders:,}")
        print(f"  Seller Center CSV orders: {total_csv_orders:,}")
        print(f"  Missing from DB: {total_missing_orders:,} ({(total_missing_orders/total_csv_orders)*100:.1f}%)")
        
        print(f"\n💰 REVENUE SUMMARY:")
        print(f"  Database revenue: ₱{total_db_revenue:,.2f}")
        print(f"  Seller Center revenue: ₱{total_csv_revenue:,.2f}")
        print(f"  Missing revenue: ₱{total_missing_revenue:,.2f}")
        
    except Exception as e:
        print(f"❌ Error analyzing comparison: {e}")
        return
    
    # ================================================================
    # 3. Monthly detailed breakdown
    # ================================================================
    print(f"\n📊 3. MONTHLY DETAILED BREAKDOWN")
    print("=" * 90)
    print(f"{'Month':<12} {'DB Orders':<10} {'CSV Orders':<11} {'Missing':<9} {'Miss %':<8} {'DB Revenue':<15} {'CSV Revenue':<15}")
    print("-" * 90)
    
    discrepancy_months = []
    high_discrepancy_months = []
    
    for _, row in comparison.iterrows():
        month = row['date'][:7]
        db_orders = int(row['orders db'])
        csv_orders = int(row['orders csv'])
        missing = int(row['missing orders'])
        miss_pct = (missing / csv_orders) * 100 if csv_orders > 0 else 0
        
        db_revenue = row['gross_db_clean']
        csv_revenue = row['gross_csv_clean']
        
        # Flag significant discrepancies
        if abs(missing) > 20:
            high_discrepancy_months.append(month)
        if missing != 0:
            discrepancy_months.append(month)
        
        print(f"{month:<12} {db_orders:<10} {csv_orders:<11} {missing:<9} {miss_pct:<8.1f}% ₱{db_revenue:<13,.0f} ₱{csv_revenue:<13,.0f}")
    
    print("-" * 90)
    print(f"{'TOTALS':<12} {total_db_orders:<10} {total_csv_orders:<11} {total_missing_orders:<9} {(total_missing_orders/total_csv_orders)*100:<8.1f}%")
    
    # ================================================================
    # 4. Cross-check with our dimensional model
    # ================================================================
    print(f"\n📊 4. DIMENSIONAL MODEL CROSS-CHECK")
    print("-" * 50)
    
    try:
        # Get our monthly data for the same periods
        our_monthly = lazada_orders.groupby('order_month').agg({
            'orders_key': 'count',
            'price_total': 'sum',
            'order_status': lambda x: (x == 'COMPLETED').sum()
        }).reset_index()
        our_monthly['month_str'] = our_monthly['order_month'].astype(str)
        
        print(f"\n🔄 OUR DATABASE vs COMPARISON DATABASE:")
        print(f"{'Month':<12} {'Our DB':<8} {'Comp DB':<9} {'Diff':<8} {'Our Revenue':<15} {'Comp Revenue':<15}")
        print("-" * 80)
        
        alignment_issues = []
        
        for _, comp_row in comparison.iterrows():
            comp_month = comp_row['order_month']
            comp_db_orders = int(comp_row['orders db'])
            comp_db_revenue = comp_row['gross_db_clean']
            
            # Find matching month in our data
            our_data = our_monthly[our_monthly['order_month'] == comp_month]
            
            if not our_data.empty:
                our_orders = int(our_data.iloc[0]['orders_key'])
                our_revenue = our_data.iloc[0]['price_total']
                our_completed = int(our_data.iloc[0]['order_status'])
                
                order_diff = our_orders - comp_db_orders
                revenue_diff = our_revenue - comp_db_revenue
                
                if abs(order_diff) > 5:  # Flag significant differences
                    alignment_issues.append({
                        'month': str(comp_month),
                        'our_orders': our_orders,
                        'comp_orders': comp_db_orders,
                        'diff': order_diff,
                        'our_completed': our_completed
                    })
                
                print(f"{str(comp_month):<12} {our_orders:<8} {comp_db_orders:<9} {order_diff:<8} ₱{our_revenue:<13,.0f} ₱{comp_db_revenue:<13,.0f}")
            else:
                print(f"{str(comp_month):<12} {'N/A':<8} {comp_db_orders:<9} {'N/A':<8}")
        
    except Exception as e:
        print(f"❌ Error in cross-check: {e}")
        return
    
    # ================================================================
    # 5. Root cause analysis
    # ================================================================
    print(f"\n🔍 5. ROOT CAUSE ANALYSIS")
    print("-" * 50)
    
    print(f"\n📊 DISCREPANCY PATTERNS:")
    print(f"  • Months with discrepancies: {len(discrepancy_months)}/{len(comparison)} ({len(discrepancy_months)/len(comparison)*100:.1f}%)")
    print(f"  • High discrepancy months (>20 orders): {len(high_discrepancy_months)}")
    print(f"  • Average missing orders per month: {total_missing_orders/len(comparison):.1f}")
    
    print(f"\n🎯 KEY INSIGHTS:")
    
    # Check if CSV consistently has more orders
    csv_higher = (comparison['orders csv'] > comparison['orders db']).sum()
    db_higher = (comparison['orders db'] > comparison['orders csv']).sum()
    
    print(f"  • CSV has more orders: {csv_higher}/{len(comparison)} months ({csv_higher/len(comparison)*100:.1f}%)")
    print(f"  • DB has more orders: {db_higher}/{len(comparison)} months ({db_higher/len(comparison)*100:.1f}%)")
    
    # Revenue vs order discrepancy correlation
    comparison['order_discrepancy_rate'] = comparison['missing orders'] / comparison['orders csv'] * 100
    comparison['revenue_discrepancy_rate'] = comparison['missing_gross_clean'] / comparison['gross_csv_clean'] * 100
    
    avg_order_disc = comparison['order_discrepancy_rate'].mean()
    avg_revenue_disc = comparison['revenue_discrepancy_rate'].mean()
    
    print(f"  • Average order discrepancy rate: {avg_order_disc:.1f}%")
    print(f"  • Average revenue discrepancy rate: {avg_revenue_disc:.1f}%")
    
    # ================================================================
    # 6. Possible causes and recommendations
    # ================================================================
    print(f"\n💡 6. LIKELY CAUSES & RECOMMENDATIONS")
    print("=" * 80)
    
    print(f"\n🔍 LIKELY CAUSES:")
    print(f"  1. 📅 DATE BOUNDARY ISSUES:")
    print(f"     • Different timezone handling (Seller Center vs DB)")
    print(f"     • Month-end cutoff differences")
    print(f"     • Order date vs created_date vs updated_date misalignment")
    
    print(f"\n  2. 📋 ORDER STATUS FILTERING:")
    print(f"     • Seller Center includes ALL orders")
    print(f"     • Database may filter by status (COMPLETED only)")
    print(f"     • Different handling of CANCELLED/RETURNED orders")
    
    print(f"\n  3. 🔄 DATA SYNCHRONIZATION:")
    print(f"     • Seller Center data more recent/complete")
    print(f"     • Database extraction timing differences")
    print(f"     • API rate limiting causing incomplete data pulls")
    
    print(f"\n  4. 🏢 ACCOUNT/MARKETPLACE SCOPE:")
    print(f"     • Multiple seller accounts or marketplaces")
    print(f"     • Regional marketplace differences") 
    print(f"     • Cross-border vs domestic order handling")
    
    print(f"\n🔧 RECOMMENDED ACTIONS:")
    print(f"  1. ✅ IMMEDIATE VALIDATION:")
    print(f"     • Check raw API extraction logs for missed orders")
    print(f"     • Verify order status distribution in raw vs processed data")
    print(f"     • Compare order IDs between Seller Center CSV and database")
    
    print(f"\n  2. ✅ DATE ALIGNMENT:")
    print(f"     • Standardize all dates to same timezone (UTC or local)")
    print(f"     • Use consistent date fields (order_date vs created_at)")
    print(f"     • Implement month-boundary validation")
    
    print(f"\n  3. ✅ STATUS RECONCILIATION:")
    print(f"     • Include ALL order statuses in extraction")
    print(f"     • Create status-specific comparisons")
    print(f"     • Document which statuses to include/exclude in reports")
    
    print(f"\n  4. ✅ PROCESS IMPROVEMENTS:")
    print(f"     • Implement daily reconciliation checks")
    print(f"     • Add data completeness validation")
    print(f"     • Create automated discrepancy alerts")
    
    # ================================================================
    # 7. Specific month analysis
    # ================================================================
    print(f"\n📊 7. HIGH-DISCREPANCY MONTH ANALYSIS")
    print("-" * 50)
    
    if alignment_issues:
        print(f"\n⚠️  MONTHS WITH SIGNIFICANT ORDER COUNT DIFFERENCES:")
        for issue in alignment_issues:
            print(f"  {issue['month']}: Our DB={issue['our_orders']}, Comp DB={issue['comp_orders']}, Diff={issue['diff']}")
            print(f"    Completed orders in our DB: {issue['our_completed']}")
    
    # Look for patterns in high discrepancy months
    high_disc_data = comparison[comparison['missing orders'].abs() > 20]
    if len(high_disc_data) > 0:
        print(f"\n📈 HIGH DISCREPANCY MONTHS (>20 orders):")
        for _, row in high_disc_data.iterrows():
            month = row['date'][:7]
            missing = int(row['missing orders'])
            csv_orders = int(row['orders csv'])
            print(f"  {month}: {missing:+} orders ({missing/csv_orders*100:+.1f}%)")
    
    print(f"\n🎯 PRIORITY INVESTIGATION AREAS:")
    print(f"  • August 2025: Highest discrepancy (+100 orders, +31% missing)")
    print(f"  • September 2024: High discrepancy (+55 orders)")
    print(f"  • December 2024: High discrepancy (+50 orders)")
    print(f"  • June 2025: Moderate-high discrepancy (+43 orders)")
    
    print(f"\n✅ SUCCESS CASES:")
    exact_matches = comparison[comparison['missing orders'] == 0]
    near_matches = comparison[comparison['missing orders'].abs() <= 2]
    
    print(f"  • Exact matches: {len(exact_matches)} months")
    print(f"  • Near matches (±2): {len(near_matches)} months")
    
    if len(exact_matches) > 0:
        print(f"  • Exact match months: {', '.join(exact_matches['date'].str[:7])}")
    
    print(f"\n🎉 The fact that {len(near_matches)}/{len(comparison)} months have ≤2 order differences")
    print(f"   suggests the extraction process is largely correct, with specific")
    print(f"   timing or filtering issues causing the larger discrepancies.")

if __name__ == "__main__":
    analyze_seller_center_vs_database()