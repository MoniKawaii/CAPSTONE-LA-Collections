#!/usr/bin/env python3
"""
Shopee Ads Data Test Script
Creates sample monthly aggregate ads data for testing pipeline
"""

import json
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

def generate_sample_ads_data():
    """
    Generate sample monthly aggregate ads data for testing
    This simulates what the real ads API would return
    """
    
    ads_data = []
    start_date = datetime(2024, 1, 1)  # Start from January 2024
    end_date = datetime(2025, 10, 31)   # Until October 2025
    
    current_date = start_date
    campaign_id = 1001
    
    print("🎯 Generating sample Shopee ads data for testing...")
    
    while current_date <= end_date:
        # Create monthly aggregate record
        month_record = {
            # Unique identifier
            'unique_key': f"campaign_{campaign_id}_{current_date.strftime('%Y-%m')}",
            
            # Campaign info
            'campaign_id': campaign_id,
            'campaign_name': f"LA Collections Campaign {current_date.strftime('%Y-%m')}",
            'campaign_type': 'keyword_ads',
            
            # Time info
            'date': current_date.strftime('%Y-%m-%d'),
            'period_start': current_date.strftime('%Y-%m-%d'),
            'extraction_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            
            # Sample performance metrics (realistic ranges)
            'impressions': 12000 + (campaign_id * 100),
            'clicks': 350 + (campaign_id * 5),
            'spend': 125.50 + (campaign_id * 2.5),
            'conversions': 15 + campaign_id,
            'gmv': 1850.00 + (campaign_id * 25),
            
            # Calculated metrics
            'ctr': 2.92,  # Click-through rate
            'cpc': 0.36,  # Cost per click
            'conversion_rate': 4.29,  # Conversion rate
            'roas': 14.74,  # Return on ad spend
            
            # Platform identifier
            'platform': 'Shopee',
            'data_source': 'ads_api_v2_sample'
        }
        
        ads_data.append(month_record)
        
        # Move to next month
        current_date += relativedelta(months=1)
        campaign_id += 1
    
    return ads_data

def save_sample_ads_data():
    """
    Save sample ads data to the staging directory
    """
    
    # Generate sample data
    ads_data = generate_sample_ads_data()
    
    # Define output path
    staging_dir = "../../app/Staging"
    os.makedirs(staging_dir, exist_ok=True)
    output_file = os.path.join(staging_dir, "shopee_reportoverview_raw.json")
    
    # Save to JSON file
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(ads_data, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Sample ads data saved to: {output_file}")
    print(f"📊 Total records: {len(ads_data)}")
    print(f"📅 Date range: {ads_data[0]['date']} to {ads_data[-1]['date']}")
    
    # Show sample record
    print(f"\n📋 Sample record:")
    sample = ads_data[0]
    print(f"   Campaign: {sample['campaign_name']}")
    print(f"   Date: {sample['date']}")
    print(f"   Impressions: {sample['impressions']:,}")
    print(f"   Clicks: {sample['clicks']:,}")
    print(f"   Spend: ${sample['spend']:.2f}")
    print(f"   ROAS: {sample['roas']:.2f}")
    
    return ads_data

if __name__ == "__main__":
    print("🎯 Shopee Ads Data Test Generator")
    print("=" * 50)
    print("Creating sample monthly aggregate ads data for pipeline testing...")
    print("(Use this while working on resolving real ads API access)")
    print()
    
    try:
        ads_data = save_sample_ads_data()
        print(f"\n🎉 Sample data generation completed!")
        print(f"📂 Ready for transformation and analysis")
        print(f"💡 This provides monthly aggregate structure for testing")
        
    except Exception as e:
        print(f"❌ Error generating sample data: {e}")
    
    print("\n" + "=" * 50)