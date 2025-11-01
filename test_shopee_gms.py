#!/usr/bin/env python3
"""
Quick Test: Shopee GMS Campaign Performance - Single Month
Test the new GMS API endpoint with enhanced rate limiting
"""

import sys
import os
from datetime import datetime, timedelta

# Add the app directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from app.Extraction.shopee_api_calls import ShopeeDataExtractor

def test_single_month_gms():
    """
    Test GMS campaign performance for just one month
    """
    print("🧪 Testing Shopee GMS Campaign Performance API")
    print("=" * 60)
    
    try:
        # Initialize extractor
        extractor = ShopeeDataExtractor()
        print(f"✅ Extractor initialized for Shop ID: {extractor.shop_id}")
        
        # Test with recent month (October 2025)
        start_date = datetime(2025, 4, 1)
        end_date = datetime(2025, 4, 30)
        
        print(f"\n🎯 Testing single month: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        # Extract ads data for just this month
        ads_data = extractor.extract_traffic_metrics(
            start_date=start_date,
            end_date=end_date,
            start_fresh=True,  # Fresh extraction for test
            monthly_aggregate=True,
            incremental=False
        )
        
        # Report results
        if ads_data:
            print(f"\n✅ Test successful!")
            print(f"   📊 Records extracted: {len(ads_data)}")
            print(f"   📁 Data type: Monthly aggregated GMS performance")
            
            # Show sample
            if ads_data:
                sample = ads_data[0]
                print(f"\n📋 Sample GMS record:")
                print(f"   Campaign: {sample.get('campaign_name', 'N/A')}")
                print(f"   Date: {sample.get('date', 'N/A')}")
                print(f"   Impressions: {sample.get('impressions', 0):,}")
                print(f"   Clicks: {sample.get('clicks', 0):,}")
                print(f"   Spend: ${sample.get('spend', 0):.2f}")
                print(f"   Sales: ${sample.get('sales', 0):.2f}")
                print(f"   Units Sold: {sample.get('units_sold', 0):,}")
                print(f"   ROAS: {sample.get('roas', 0):.2f}")
        else:
            print(f"\n⚠️ No data extracted")
            print(f"   This could indicate:")
            print(f"   - No GMS campaigns in October 2025")
            print(f"   - API still rate-limited")
            print(f"   - Account doesn't have GMS access")
        
        # Show API usage
        print(f"\n📈 API Usage:")
        print(f"   Calls made: {extractor.api_calls_made}")
        print(f"   Daily limit: {extractor.max_daily_calls}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        print(f"   Traceback: {traceback.format_exc()}")
        return False

if __name__ == "__main__":
    print("🧪 Shopee GMS API Single Month Test")
    print(f"📅 Current date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    success = test_single_month_gms()
    
    if success:
        print(f"\n🎉 GMS API test completed!")
        print(f"   📂 Check app/Staging/shopee_reportoverview_raw.json")
        print(f"   💡 If successful, you can run full extraction")
    else:
        print(f"\n💥 GMS API test failed!")
        print(f"   🔍 Check error messages above")
    
    print("\n" + "=" * 60)