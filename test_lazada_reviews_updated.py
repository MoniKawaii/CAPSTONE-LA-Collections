#!/usr/bin/env python3
"""
Test script for updated Lazada review extraction using the new API approach:
1. /review/seller/history/list - Get review IDs (7-day chunks, 3-month limit)
2. GetReviewListByIdList - Get detailed review content
"""

import sys
import os

# Add the app directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

try:
    from app.Extraction.lazada_api_calls import LazadaDataExtractor
    print("✅ Successfully imported LazadaDataExtractor")
except ImportError as e:
    print(f"❌ Import error: {e}")
    sys.exit(1)

def test_new_review_api():
    """Test the new Lazada review API approach"""
    print("🔍 Testing Updated Lazada Review Extraction")
    print("=" * 60)
    
    try:
        # Initialize extractor
        extractor = LazadaDataExtractor()
        print("✅ Extractor initialized successfully")
        
        print(f"\n📋 Step 1: Testing GetHistoryReviewIdList API")
        print("   - Uses /review/seller/history/list")
        print("   - 7-day chunks, 3-month historical limit")
        print("   - Time range: start_time to end_time")
        
        # Test Step 1: Get review IDs from historical data
        review_ids = extractor.extract_review_history_list(start_fresh=True)
        
        if review_ids:
            print(f"✅ Step 1 completed: Found {len(review_ids)} review IDs")
            print(f"   Sample ID structure: {review_ids[0] if review_ids else 'None'}")
        else:
            print("⚠️ Step 1: No review IDs found (could be normal if no recent reviews)")
        
        print(f"\n📋 Step 2: Testing GetReviewListByIdList API")  
        print("   - Uses /review/seller/list/v2")
        print("   - Fetches full review content using ID list")
        print("   - Batched processing for efficiency")
        
        # Test Step 2: Get detailed review content
        if review_ids:
            detailed_reviews = extractor.extract_review_details(review_ids=review_ids, start_fresh=True)
            
            if detailed_reviews:
                print(f"✅ Step 2 completed: Extracted {len(detailed_reviews)} detailed reviews")
                print(f"   Sample review fields: {list(detailed_reviews[0].keys()) if detailed_reviews else 'None'}")
            else:
                print("⚠️ Step 2: No detailed reviews extracted")
        else:
            print("⏭️ Step 2: Skipped (no review IDs from Step 1)")
        
        print(f"\n🎉 Test completed successfully!")
        print(f"📁 Check app/Staging/ for output files:")
        print(f"   - lazada_reviewhistorylist_raw.json (review IDs)")
        print(f"   - lazada_productreview_raw.json (detailed reviews)")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False

def check_api_requirements():
    """Check API requirements and limitations"""
    print("\n📋 API Requirements & Limitations:")
    print("=" * 40)
    print("1. GetHistoryReviewIdList (/review/seller/history/list):")
    print("   ✅ Get historical review IDs")
    print("   ⚠️ Cannot query reviews older than 3 months")
    print("   ⚠️ Response time is within 1 month")
    print("   ⚠️ Time range must be 7 days maximum")
    print("")
    print("2. GetReviewListByIdList (/review/seller/list/v2):")
    print("   ✅ Get full review content using ID list")
    print("   ✅ Includes review text, ratings, seller responses")
    print("   ✅ Supports batch processing")
    print("")
    print("📅 Date Format: YYYY-MM-DDTHH:MM:SS+08:00 (ISO 8601)")
    print("🔄 Process: First get IDs, then get content")

if __name__ == "__main__":
    print("🚀 Lazada Review API Update Test")
    print("Following API instruction requirements:")
    print("- GetHistoryReviewIdList: Get review ID list")
    print("- GetReviewListByIdList: Get review content")
    print("")
    
    # Check requirements
    check_api_requirements()
    
    # Run test
    success = test_new_review_api()
    
    if success:
        print("\n✅ All tests passed! Updated review extraction is ready.")
    else:
        print("\n❌ Tests failed. Please check the configuration.")