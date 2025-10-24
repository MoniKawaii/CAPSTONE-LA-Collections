"""
Dim_Time Generator - LA Collections (Multi-Platform: Lazada & Shopee)
Generates time dimension following LA_Collections_Schema.sql specifications

Schema Requirements:
CREATE TABLE "Dim_Time" (
  "time_key" int PRIMARY KEY NOT NULL,
  "date" date UNIQUE NOT NULL,
  "year" int,
  "quarter" varchar,
  "month" int,
  "month_name" varchar,
  "week" int,
  "day" int,
  "day_of_week" int,
  "day_of_the_year" int,
  "is_weekend" boolean,
  "is_payday" boolean,
  "is_mega_sale_day" boolean
);

Business Rules:
- time_key: Format YYYYMMDD
- date: Format YYYY-MM-DD
- is_weekend: TRUE for Saturday and Sunday
- is_payday: TRUE for 15th day and 30th or 31st day, or 13th month pay
- is_mega_sale_day: TRUE for 11.11, 12.12, etc. (applies to both Lazada & Shopee)

Platform Support:
- Works with both Lazada and Shopee order data
- Dynamically adjusts date range based on actual order timestamps
- Philippines e-commerce sale events (applicable to both platforms)
"""

import pandas as pd
import sys
import os
import json
from datetime import datetime, date, timedelta
import calendar

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from config import get_empty_dataframe, apply_data_types
except ImportError:
    # Fallback for different environments
    def get_empty_dataframe(table_name):
        return pd.DataFrame()
    def apply_data_types(df, table_name):
        return df


def load_orders_for_date_range(platform='lazada'):
    """
    Load raw orders from JSON file to determine date range
    
    Args:
        platform (str): 'lazada' or 'shopee'
        
    Returns:
        tuple: (min_date, max_date) or (None, None) if no data
    """
    filename = f'{platform}_orders_raw.json'
    staging_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'Staging')
    json_path = os.path.join(staging_dir, filename)
    
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            orders_data = json.load(f)
        
        if not orders_data:
            return None, None
        
        dates = []
        
        if platform == 'lazada':
            # Lazada uses 'created_at' field with ISO format
            for order in orders_data:
                created_at_str = order.get('created_at', '')
                if created_at_str:
                    try:
                        dt = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
                        dates.append(dt.date())
                    except (ValueError, AttributeError):
                        pass
        
        elif platform == 'shopee':
            # Shopee uses 'create_time' field with Unix timestamp
            for order in orders_data:
                create_time = order.get('create_time')
                if create_time:
                    try:
                        dt = datetime.fromtimestamp(create_time)
                        dates.append(dt.date())
                    except (ValueError, TypeError, OSError):
                        pass
        
        if dates:
            return min(dates), max(dates)
        else:
            return None, None
            
    except FileNotFoundError:
        print(f"  ⚠️ {platform.capitalize()} orders file not found: {json_path}")
        return None, None
    except json.JSONDecodeError as e:
        print(f"  ❌ Error parsing {platform.capitalize()} JSON: {e}")
        return None, None


def determine_date_range(use_dynamic_range=True):
    """
    Determine date range for time dimension based on actual order data from both platforms
    
    Args:
        use_dynamic_range (bool): If True, use actual order dates; if False, use fixed range
        
    Returns:
        tuple: (start_date_str, end_date_str) in YYYY-MM-DD format
    """
    default_start = "2020-04-01"
    default_end = "2025-04-30"
    
    if not use_dynamic_range:
        return default_start, default_end
    
    print("  🔍 Analyzing order data to determine date range...")
    
    # Check Lazada orders
    lazada_min, lazada_max = load_orders_for_date_range('lazada')
    
    # Check Shopee orders
    shopee_min, shopee_max = load_orders_for_date_range('shopee')
    
    # Determine overall min and max
    all_dates = []
    
    if lazada_min and lazada_max:
        all_dates.extend([lazada_min, lazada_max])
        print(f"  ✓ Lazada orders: {lazada_min} to {lazada_max}")
    
    if shopee_min and shopee_max:
        all_dates.extend([shopee_min, shopee_max])
        print(f"  ✓ Shopee orders: {shopee_min} to {shopee_max}")
    
    if all_dates:
        actual_min = min(all_dates)
        actual_max = max(all_dates)
        
        # Add buffer: start from beginning of month, end at end of month
        buffer_start = actual_min.replace(day=1)
        
        # End at last day of max month
        last_day = calendar.monthrange(actual_max.year, actual_max.month)[1]
        buffer_end = actual_max.replace(day=last_day)
        
        print(f"  📊 Dynamic date range: {buffer_start} to {buffer_end}")
        return buffer_start.strftime('%Y-%m-%d'), buffer_end.strftime('%Y-%m-%d')
    else:
        print(f"  ⚠️ No order data found, using default range: {default_start} to {default_end}")
        return default_start, default_end

def is_mega_sale_day(check_date):
    """
    Determine if a date is a mega sale day based on Philippines e-commerce patterns
    Applies to both Lazada and Shopee platforms
    
    Args:
        check_date (datetime.date): Date to check
    
    Returns:
        bool: True if it's a mega sale day
    """
    month = check_date.month
    day = check_date.day
    
    # Major sale days in Philippines (both Lazada & Shopee)
    mega_sale_days = [
        # Double digit days (Lazada & Shopee mega sales)
        (1, 1),   # New Year
        (2, 2),   # 2.2 Sale
        (3, 3),   # 3.3 Sale
        (4, 4),   # 4.4 Sale
        (5, 5),   # 5.5 Sale
        (6, 6),   # 6.6 Sale
        (7, 7),   # 7.7 Sale
        (8, 8),   # 8.8 Sale
        (9, 9),   # 9.9 Sale (Big Sale for both platforms)
        (10, 10), # 10.10 Sale
        (11, 11), # Singles Day (BIGGEST sale for both Lazada & Shopee)
        (12, 12), # 12.12 Year End Sale (both platforms)
        
        # Major holidays and sale events (Philippines)
        (2, 14),  # Valentine's Day
        (3, 8),   # International Women's Day (Shopee Women's Festival)
        (5, 1),   # Labor Day
        (6, 12),  # Independence Day
        (8, 21),  # Ninoy Aquino Day (often sale day)
        (11, 1),  # All Saints Day
        (12, 24), # Christmas Eve
        (12, 25), # Christmas (Peak shopping season)
        (12, 26), # Boxing Day
        (12, 30), # Rizal Day
        (12, 31), # New Year's Eve
        
        # Shopee-specific mega sales
        (3, 15),  # Shopee 3.15 Consumer Day (mid-month sale)
        (4, 15),  # Shopee 4.15 Sale
        (6, 16),  # Shopee 6.16 Mid-Year Sale
        (8, 18),  # Shopee 8.18 Sale
        (10, 20), # Shopee 10.20 Sale
        
        # Lazada-specific mega sales
        (3, 27),  # Lazada Birthday Sale (end of March)
    ]
    
    # Check if it's one of the standard mega sale days
    if (month, day) in mega_sale_days:
        return True
    
    # Black Friday (last Friday of November) - both platforms
    if month == 11:
        last_day = calendar.monthrange(check_date.year, 11)[1]
        last_friday = None
        for d in range(last_day, 0, -1):
            if date(check_date.year, 11, d).weekday() == 4:  # Friday is 4
                last_friday = d
                break
        if day == last_friday:
            return True
    
    # Cyber Monday (Monday after last Thursday of November) - both platforms
    if month == 11 or (month == 12 and day <= 3):
        last_day = calendar.monthrange(check_date.year, 11)[1]
        last_thursday = None
        for d in range(last_day, 0, -1):
            if date(check_date.year, 11, d).weekday() == 3:  # Thursday is 3
                last_thursday = d
                break
        if last_thursday:
            cyber_monday = date(check_date.year, 11, last_thursday) + timedelta(days=4)
            if check_date == cyber_monday:
                return True
    
    return False

def is_payday(check_date):
    """
    Determine if a date is a payday in Philippines
    
    Args:
        check_date (datetime.date): Date to check
    
    Returns:
        bool: True if it's a payday
    """
    day = check_date.day
    month = check_date.month
    
    # Regular paydays: 15th and end of month (30th or 31st)
    if day == 15:
        return True
    
    # Last day of the month
    last_day_of_month = calendar.monthrange(check_date.year, month)[1]
    if day == last_day_of_month:
        return True
    
    # Special case: if last day is weekend, payday might be moved to Friday
    last_date = date(check_date.year, month, last_day_of_month)
    if last_date.weekday() in [5, 6]:  # Saturday or Sunday
        # Move to Friday
        friday_before = last_date - timedelta(days=last_date.weekday() - 4)
        if check_date == friday_before:
            return True
    
    # 13th month pay (usually December 15 or December salary)
    if month == 12 and (day == 15 or day == last_day_of_month):
        return True
    
    return False

def transform_dim_time(raw_data=None, start_date=None, end_date=None, use_dynamic_range=True):
    """
    Generate Dim_Time table following LA_Collections_Schema.sql
    Works with both Lazada and Shopee order data
    
    Args:
        raw_data: Optional raw data (not used in this version)
        start_date (str): Start date in YYYY-MM-DD format (optional, auto-detected if None)
        end_date (str): End date in YYYY-MM-DD format (optional, auto-detected if None)
        use_dynamic_range (bool): If True, determine date range from actual order data
    
    Returns:
        pd.DataFrame: Time dimension DataFrame
    """
    
    print("🔄 Starting Dim_Time generation for multi-platform (Lazada & Shopee)...")
    
    # Determine date range
    if start_date is None or end_date is None:
        start_date, end_date = determine_date_range(use_dynamic_range=use_dynamic_range)
    
    print(f"  📅 Generating time dimension from {start_date} to {end_date}")
    
    # Parse start and end dates
    start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
    end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
    
    # Generate date range
    date_range = []
    current_date = start_dt
    while current_date <= end_dt:
        date_range.append(current_date)
        current_date += timedelta(days=1)
    
    print(f"  📊 Generating {len(date_range)} date records...")
    
    # Create time dimension records
    time_records = []
    
    for current_date in date_range:
        # Calculate time_key as YYYYMMDD
        time_key = int(current_date.strftime('%Y%m%d'))
        
        # Calculate quarter
        quarter = f"Q{(current_date.month - 1) // 3 + 1}"
        
        # Calculate week number (ISO week)
        year, week, _ = current_date.isocalendar()
        
        # Day of week (0=Monday, 6=Sunday for Python)
        day_of_week = current_date.weekday()
        
        # Day of year
        day_of_year = current_date.timetuple().tm_yday
        
        # Is weekend (Saturday=5, Sunday=6 in Python weekday)
        is_weekend = day_of_week >= 5
        
        # Is payday
        payday = is_payday(current_date)
        
        # Is mega sale day
        mega_sale = is_mega_sale_day(current_date)
        
        # Create record
        time_record = {
            'time_key': time_key,
            'date': current_date.strftime('%Y-%m-%d'),  # Format as YYYY-MM-DD string
            'year': current_date.year,
            'quarter': quarter,
            'month': current_date.month,
            'month_name': current_date.strftime('%B'),  # Full month name
            'week': week,
            'day': current_date.day,
            'day_of_week': day_of_week,
            'day_of_the_year': day_of_year,
            'is_weekend': is_weekend,
            'is_payday': payday,
            'is_mega_sale_day': mega_sale
        }
        
        time_records.append(time_record)
    
    # Create DataFrame
    df = pd.DataFrame(time_records)
    
    # Data validation and summary
    if len(df) > 0:
        print(f"  ✅ Generated {len(df)} time dimension records")
        print(f"      Date range: {df['date'].min()} to {df['date'].max()}")
        print(f"      Years covered: {df['year'].min()} to {df['year'].max()}")
        print(f"      Weekend days: {df['is_weekend'].sum()}")
        print(f"      Paydays: {df['is_payday'].sum()}")
        print(f"      Mega sale days: {df['is_mega_sale_day'].sum()}")
        
        # Show some sample mega sale days for verification
        mega_sale_sample = df[df['is_mega_sale_day'] == True]['date'].head(10).tolist()
        if mega_sale_sample:
            print(f"      Sample mega sale days: {', '.join(mega_sale_sample[:5])}")
    else:
        print("  ⚠️ No time dimension records created")
    
    return df

def main():
    """Main function to generate and save time dimension"""
    
    print("=" * 70)
    print("🚀 Multi-Platform Time Dimension Generator (Lazada & Shopee)")
    print("=" * 70)
    
    # Generate time dimension with dynamic date range based on actual order data
    df = transform_dim_time(use_dynamic_range=True)
    
    # Display results
    if len(df) > 0:
        print("\n📋 Sample time dimension records:")
        # Show a variety of samples including special days
        sample_indices = [0, len(df)//4, len(df)//2, 3*len(df)//4, -1]
        sample_df = df.iloc[sample_indices]
        
        print("Sample records (showing key fields):")
        print(sample_df[['time_key', 'date', 'quarter', 'month_name', 'is_weekend', 'is_payday', 'is_mega_sale_day']].to_string(index=False))
        
        # Show special days
        print("\n🎯 Special days found:")
        mega_sales = df[df['is_mega_sale_day'] == True]
        if len(mega_sales) > 0:
            print(f"Mega sale days: {len(mega_sales)} total")
            print("First 15 mega sale days:")
            print(mega_sales[['date', 'month_name', 'day']].head(15).to_string(index=False))
            
            # Break down by platform-specific sales
            print("\n📊 Platform-specific mega sales included:")
            print("  • Both platforms: 1.1, 2.2, 3.3, ..., 11.11 (Singles Day), 12.12")
            print("  • Shopee-specific: 3.15, 4.15, 6.16, 8.18, 10.20")
            print("  • Lazada-specific: 3.27 (Birthday Sale)")
            print("  • Major holidays: Christmas, New Year, Independence Day, etc.")
        
        paydays = df[df['is_payday'] == True]
        if len(paydays) > 0:
            print(f"\n💰 Paydays found: {len(paydays)} (15th and end of month)")
            print("Sample paydays:")
            print(paydays[['date', 'month_name', 'day']].head(5).to_string(index=False))
        
        # Save to data folder as time_dim.csv (root data folder, not app/data)
        # Navigate from app/Transformation to root directory, then to data folder
        root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        data_dir = os.path.join(root_dir, 'data')
        os.makedirs(data_dir, exist_ok=True)
        output_file = os.path.join(data_dir, 'time_dim.csv')
        df.to_csv(output_file, index=False)
        print(f"\n💾 Saved to: {output_file}")
        
        # Data quality summary
        print(f"\n📊 Data Quality Summary:")
        print(f"  Total records: {len(df):,}")
        print(f"  Date format: {df['date'].iloc[0]} (YYYY-MM-DD)")
        print(f"  Time key format: {df['time_key'].iloc[0]} (YYYYMMDD)")
        print(f"  Unique dates: {df['date'].nunique():,}")
        print(f"  Years: {sorted(df['year'].unique())}")
        print(f"  Quarters: {sorted(df['quarter'].unique())}")
        
        # Summary by year
        print(f"\n📅 Records by year:")
        year_counts = df.groupby('year').size()
        for year, count in year_counts.items():
            print(f"    {year}: {count} days")
        
        print("\n✅ Time dimension generation complete!")
        print("   This dimension works for BOTH Lazada and Shopee data")
    else:
        print("❌ No time dimension data generated")

if __name__ == "__main__":
    main() 