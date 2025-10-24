"""
Fact Traffic Harmonization Script
Harmonizes traffic/advertising metrics from both Lazada and Shopee platforms
"""

import pandas as pd
import json
import os
import sys
from datetime import datetime

# Add the app directory to Python path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import FACT_TRAFFIC_COLUMNS, apply_data_types

def get_empty_dataframe(table_name='fact_traffic'):
    """Return an empty DataFrame with proper schema"""
    return pd.DataFrame(columns=FACT_TRAFFIC_COLUMNS)

def load_traffic_raw(platform='lazada'):
    """
    Load raw traffic data from JSON file for the specified platform
    
    Args:
        platform (str): 'lazada' or 'shopee'
        
    Returns:
        list: Traffic records from the platform
    """
    filename = f'{platform}_reportoverview_raw.json'
    filepath = os.path.join(os.path.dirname(__file__), '..', 'Raw', filename)
    
    if not os.path.exists(filepath):
        print(f"⚠️ {platform.capitalize()} traffic file not found: {filename}")
        return None
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if data:
                print(f"✓ Loaded {len(data)} {platform.capitalize()} traffic records from {filename}")
                return data
            else:
                print(f"⚠️ {platform.capitalize()} traffic file is empty: {filename}")
                return None
    except Exception as e:
        print(f"❌ Error loading {platform} traffic data: {str(e)}")
        return None

def harmonize_lazada_traffic(traffic_data):
    """
    Transform Lazada traffic records to unified schema
    
    Args:
        traffic_data (list): Raw Lazada traffic records
        
    Returns:
        DataFrame: Harmonized traffic records
    """
    if not traffic_data:
        return get_empty_dataframe()
    
    harmonized_records = []
    
    for record in traffic_data:
        try:
            harmonized = {
                'traffic_event_key': f"LZ_{record.get('time_key', 0)}",
                'time_key': int(record.get('time_key', 0)),
                'platform_key': 1,  # Lazada
                'clicks': int(record.get('clicks', 0)),
                'impressions': int(record.get('impressions', 0))
            }
            
            harmonized_records.append(harmonized)
            
        except Exception as e:
            print(f"⚠️ Warning: Error harmonizing Lazada traffic record: {str(e)}")
            continue
    
    if not harmonized_records:
        return get_empty_dataframe()
    
    df = pd.DataFrame(harmonized_records)
    
    # Ensure columns are in correct order
    df = df[FACT_TRAFFIC_COLUMNS]
    
    return df

def harmonize_shopee_traffic(traffic_data):
    """
    Transform Shopee traffic records to unified schema
    
    Args:
        traffic_data (list): Raw Shopee traffic records
        
    Returns:
        DataFrame: Harmonized traffic records
    """
    if not traffic_data:
        return get_empty_dataframe()
    
    harmonized_records = []
    
    for record in traffic_data:
        try:
            harmonized = {
                'traffic_event_key': f"SP_{record.get('time_key', 0)}",
                'time_key': int(record.get('time_key', 0)),
                'platform_key': 2,  # Shopee
                'clicks': int(record.get('clicks', 0)),
                'impressions': int(record.get('impressions', 0))
            }
            
            harmonized_records.append(harmonized)
            
        except Exception as e:
            print(f"⚠️ Warning: Error harmonizing Shopee traffic record: {str(e)}")
            continue
    
    if not harmonized_records:
        return get_empty_dataframe()
    
    df = pd.DataFrame(harmonized_records)
    
    # Ensure columns are in correct order
    df = df[FACT_TRAFFIC_COLUMNS]
    
    return df

def harmonize_fact_traffic():
    """
    Main function to harmonize Fact Traffic from both Lazada and Shopee
    
    Returns:
        DataFrame: Harmonized fact traffic table with data from both platforms
    """
    print("🚀 Starting Fact Traffic harmonization (Multi-Platform)...")
    
    all_traffic = []
    
    # Process Lazada traffic
    print("\n📊 Processing Lazada traffic metrics...")
    lazada_traffic_data = load_traffic_raw('lazada')
    
    if lazada_traffic_data:
        lazada_traffic_df = harmonize_lazada_traffic(lazada_traffic_data)
        if not lazada_traffic_df.empty:
            all_traffic.append(lazada_traffic_df)
            print(f"   ✓ Harmonized {len(lazada_traffic_df)} Lazada traffic records")
            print(f"   📊 Total impressions: {lazada_traffic_df['impressions'].sum():,}")
            print(f"   👆 Total clicks: {lazada_traffic_df['clicks'].sum():,}")
            if lazada_traffic_df['impressions'].sum() > 0:
                ctr = (lazada_traffic_df['clicks'].sum() / lazada_traffic_df['impressions'].sum()) * 100
                print(f"   📈 CTR: {ctr:.2f}%")
    else:
        print("   ⚠️ No Lazada traffic data available")
    
    # Process Shopee traffic
    print("\n🛍️ Processing Shopee traffic metrics...")
    shopee_traffic_data = load_traffic_raw('shopee')
    
    if shopee_traffic_data:
        shopee_traffic_df = harmonize_shopee_traffic(shopee_traffic_data)
        if not shopee_traffic_df.empty:
            all_traffic.append(shopee_traffic_df)
            print(f"   ✓ Harmonized {len(shopee_traffic_df)} Shopee traffic records")
            print(f"   📊 Total impressions: {shopee_traffic_df['impressions'].sum():,}")
            print(f"   👆 Total clicks: {shopee_traffic_df['clicks'].sum():,}")
            if shopee_traffic_df['impressions'].sum() > 0:
                ctr = (shopee_traffic_df['clicks'].sum() / shopee_traffic_df['impressions'].sum()) * 100
                print(f"   📈 CTR: {ctr:.2f}%")
    else:
        print("   ⚠️ No Shopee traffic data available")
    
    # Combine all traffic
    if not all_traffic:
        print("\n❌ No traffic data available from any platform")
        return get_empty_dataframe()
    
    fact_traffic_df = pd.concat(all_traffic, ignore_index=True)
    
    print(f"\n✅ Successfully harmonized {len(fact_traffic_df)} traffic records from both platforms")
    print(f"📊 Data shape: {fact_traffic_df.shape}")
    
    if len(fact_traffic_df) > 0:
        print("\n📊 Platform Breakdown:")
        lazada_count = len(fact_traffic_df[fact_traffic_df['platform_key'] == 1])
        shopee_count = len(fact_traffic_df[fact_traffic_df['platform_key'] == 2])
        print(f"   • Lazada traffic records: {lazada_count}")
        print(f"   • Shopee traffic records: {shopee_count}")
        
        print("\n📊 Overall Metrics:")
        total_impressions = fact_traffic_df['impressions'].sum()
        total_clicks = fact_traffic_df['clicks'].sum()
        print(f"   • Total impressions: {total_impressions:,}")
        print(f"   • Total clicks: {total_clicks:,}")
        if total_impressions > 0:
            overall_ctr = (total_clicks / total_impressions) * 100
            print(f"   • Overall CTR: {overall_ctr:.2f}%")
        
        print("\n📋 Sample records (both platforms):")
        print(fact_traffic_df.head(10).to_string(index=False))
    
    return fact_traffic_df

def main():
    """Main execution function"""
    print("=" * 60)
    print("🚀 FACT TRAFFIC HARMONIZATION")
    print("=" * 60)
    print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Step 1: Harmonize traffic data
        fact_traffic_df = harmonize_fact_traffic()
        
        if fact_traffic_df.empty:
            print("\n⚠️ No traffic data to save")
            return
        
        # Step 2: Apply data types
        print(f"\n🔧 Step 2: Applying data types...")
        fact_traffic_df = apply_data_types(fact_traffic_df, 'fact_traffic')
        print(f"✅ Data types applied successfully")
        
        # Step 3: Save to CSV
        output_path = os.path.join(os.path.dirname(__file__), '..', 'Transformed', 'fact_traffic.csv')
        fact_traffic_df.to_csv(output_path, index=False)
        print(f"\n💾 Step 3: Saved to {output_path}")
        
        # Summary statistics
        print(f"\n📊 SUMMARY STATISTICS:")
        print(f"   📋 Total records: {len(fact_traffic_df):,}")
        print(f"   📅 Time range: {fact_traffic_df['time_key'].min()} to {fact_traffic_df['time_key'].max()}")
        print(f"   👁️ Total impressions: {fact_traffic_df['impressions'].sum():,}")
        print(f"   👆 Total clicks: {fact_traffic_df['clicks'].sum():,}")
        
        if fact_traffic_df['impressions'].sum() > 0:
            ctr = (fact_traffic_df['clicks'].sum() / fact_traffic_df['impressions'].sum()) * 100
            print(f"   📈 Overall CTR: {ctr:.2f}%")
        
        # Platform breakdown
        print(f"\n🏪 PLATFORM BREAKDOWN:")
        for platform_key in [1, 2]:
            platform_data = fact_traffic_df[fact_traffic_df['platform_key'] == platform_key]
            if not platform_data.empty:
                platform_name = "Lazada" if platform_key == 1 else "Shopee"
                print(f"\n   {platform_name}:")
                print(f"      Records: {len(platform_data):,}")
                print(f"      Impressions: {platform_data['impressions'].sum():,}")
                print(f"      Clicks: {platform_data['clicks'].sum():,}")
                if platform_data['impressions'].sum() > 0:
                    platform_ctr = (platform_data['clicks'].sum() / platform_data['impressions'].sum()) * 100
                    print(f"      CTR: {platform_ctr:.2f}%")
        
        print(f"\n✅ Traffic harmonization completed successfully!")
        print(f"📅 Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        print(f"\n❌ Error during harmonization: {str(e)}")
        raise e

if __name__ == "__main__":
    main()
