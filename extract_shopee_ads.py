#!/usr/bin/env python3
"""
Shopee Ads Data Extraction Script
Extracts monthly aggregate advertising data using Shopee Ads API v2.ads.get_ad_data
"""

import sys
import os
from datetime import datetime

# Add the app directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from app.Extraction.shopee_api_calls import ShopeeAPIExtractor

def main():
    """
    Extract Shopee ads data with monthly aggregation
    """
    print("🚀 Starting Shopee Ads Data Extraction")
    print("=" * 50)
    
    try:
        # Initialize the Shopee API extractor
        print("📱 Initializing Shopee API extractor...")
        extractor = ShopeeAPIExtractor()
        
        # Check if we have valid credentials
        if not hasattr(extractor, 'shop_id') or not extractor.shop_id:
            print("❌ Error: Shopee credentials not properly configured!")
            print("   Please check your tokens/shopee_tokens.json file")
            return False
        
        print(f"✅ Successfully initialized for Shop ID: {extractor.shop_id}")
        
        # Extract ads data with monthly aggregation
        print("\n📊 Starting ads data extraction...")
        print("   Method: Monthly aggregate extraction")
        print("   API: v2.ads.get_ad_data")
        print("   Range: From last extraction date to October 31, 2025")
        
        # Call the extract_traffic_metrics method with monthly_aggregate=True
        ads_data = extractor.extract_traffic_metrics(
            start_date=None,  # Will auto-detect from existing data
            end_date=None,    # Will use October 31, 2025
            start_fresh=False,  # Incremental extraction
            monthly_aggregate=True,  # Monthly aggregation as requested
            incremental=True   # Append to existing data
        )
        
        # Report results
        if ads_data:
            print(f"\n✅ Ads extraction completed successfully!")
            print(f"   📊 Total ads records extracted: {len(ads_data)}")
            print(f"   📁 Data saved to: app/Staging/shopee_reportoverview_raw.json")
            print(f"   🔄 Extraction method: Monthly aggregate")
            
            # Show sample of extracted data
            if len(ads_data) > 0:
                print(f"\n📋 Sample record:")
                sample = ads_data[0]
                print(f"   Campaign: {sample.get('campaign_name', 'N/A')}")
                print(f"   Date: {sample.get('date', 'N/A')}")
                print(f"   Impressions: {sample.get('impressions', 0):,}")
                print(f"   Clicks: {sample.get('clicks', 0):,}")
                print(f"   Spend: ${sample.get('spend', 0):.2f}")
                print(f"   ROAS: {sample.get('roas', 0):.2f}")
        else:
            print("\n⚠️ No ads data extracted")
            print("   This could be due to:")
            print("   - No active ad campaigns in the specified period")
            print("   - API rate limits reached")
            print("   - Authentication issues")
        
        # Show API usage
        print(f"\n📈 API Usage Summary:")
        print(f"   API calls made: {extractor.api_calls_made}")
        print(f"   Daily limit: {extractor.max_daily_calls}")
        print(f"   Remaining calls: {extractor.max_daily_calls - extractor.api_calls_made}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error during ads extraction: {e}")
        print(f"   Error type: {type(e).__name__}")
        import traceback
        print(f"   Traceback: {traceback.format_exc()}")
        return False

if __name__ == "__main__":
    print("🎯 Shopee Ads Data Extraction - Monthly Aggregate")
    print(f"📅 Current date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🗂️ Working directory: {os.getcwd()}")
    
    success = main()
    
    if success:
        print("\n🎉 Ads extraction process completed!")
        print("   📂 Check app/Staging/shopee_reportoverview_raw.json for results")
        print("   📊 Ready for transformation and analysis")
    else:
        print("\n💥 Ads extraction failed!")
        print("   🔍 Check error messages above for troubleshooting")
    
    print("\n" + "=" * 50)
    print("🏁 Script execution finished")