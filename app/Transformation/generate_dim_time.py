"""
Dim_Time Generator - LA Collections
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
- is_mega_sale_day: TRUE for 11.11, 12.12, etc. black friday, christmas
"""

import pandas as pd
import sys
import os
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

def is_mega_sale_day(check_date):
    """
    Determine if a date is a mega sale day based on Philippines e-commerce patterns
    
    Args:
        check_date (datetime.date): Date to check
    
    Returns:
        bool: True if it's a mega sale day
    """
    month = check_date.month
    day = check_date.day
    
    # Major sale days in Philippines
    mega_sale_days = [
        # Double digit days (11.11, 12.12, etc.)
        (1, 1),   # New Year
        (2, 2),   # 2.2 Sale
        (3, 3),   # 3.3 Sale
        (4, 4),   # 4.4 Sale
        (5, 5),   # 5.5 Sale
        (6, 6),   # 6.6 Sale
        (7, 7),   # 7.7 Sale
        (8, 8),   # 8.8 Sale
        (9, 9),   # 9.9 Sale
        (10, 10), # 10.10 Sale
        (11, 11), # Singles Day (biggest sale)
        (12, 12), # 12.12 Sale
        
        # Major holidays and sale events
        (2, 14),  # Valentine's Day
        (5, 1),   # Labor Day
        (6, 12),  # Independence Day
        (8, 21),  # Ninoy Aquino Day (often sale day)
        (11, 1),  # All Saints Day
        (12, 24), # Christmas Eve
        (12, 25), # Christmas
        (12, 30), # Rizal Day
        
        # Black Friday (varies - approximate to last Friday of November)
        # We'll handle this separately
    ]
    
    # Check if it's one of the standard mega sale days
    if (month, day) in mega_sale_days:
        return True
    
    # Check for Black Friday (last Friday of November)
    if month == 11:
        # Find the last Friday of November
        last_day = calendar.monthrange(check_date.year, 11)[1]
        last_friday = None
        for d in range(last_day, 0, -1):
            if date(check_date.year, 11, d).weekday() == 4:  # Friday is 4
                last_friday = d
                break
        if day == last_friday:
            return True
    
    # Check for Cyber Monday (Monday after last Thursday of November)
    if month == 11 or (month == 12 and day <= 3):
        # Find the last Thursday of November
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

def transform_dim_time(raw_data=None, start_date="2020-04-01", end_date="2025-04-30"):
    """
    Generate Dim_Time table following LA_Collections_Schema.sql
    Fixed date range: April 2020 to April 2025
    
    Args:
        raw_data: Optional raw data (not used in this version)
        start_date (str): Start date in YYYY-MM-DD format (default: 2020-04-01)
        end_date (str): End date in YYYY-MM-DD format (default: 2025-04-30)
    
    Returns:
        pd.DataFrame: Time dimension DataFrame
    """
    
    print("🔄 Starting Dim_Time generation...")
    print(f"  📅 Fixed date range: April 2020 to April 2025")
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
    
    # Generate time dimension with fixed date range: April 2020 to April 2025
    df = transform_dim_time()
    
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
            print("Mega sale days (first 10):")
            print(mega_sales[['date', 'month_name', 'day']].head(10).to_string(index=False))
        
        paydays = df[df['is_payday'] == True]
        if len(paydays) > 0:
            print(f"\nPaydays found: {len(paydays)} (15th and end of month)")
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
    else:
        print("❌ No time dimension data generated")

if __name__ == "__main__":
    main() 