# predictive/dim_time.py
import pandas as pd
from datetime import datetime, timedelta, date
import calendar

# -----------------------------
# Business logic functions
# -----------------------------
def is_payday(check_date: date) -> bool:
    day = check_date.day
    month = check_date.month
    last_day = calendar.monthrange(check_date.year, month)[1]

    # Regular paydays
    if day == 15 or day == last_day:
        return True

    # Move payday if last day is weekend
    last_dt = date(check_date.year, month, last_day)
    if last_dt.weekday() >= 5:  # Saturday/Sunday
        friday_before = last_dt - timedelta(days=(last_dt.weekday() - 4))
        if check_date == friday_before:
            return True

    # 13th month pay (December)
    if month == 12 and (day == 15 or day == last_day):
        return True

    return False

def is_mega_sale_day(check_date: date) -> bool:
    # Fixed mega sale days
    mega_days = [
        (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6),
        (7, 7), (8, 8), (9, 9), (10, 10), (11, 11), (12, 12),
        (2, 14), (5, 1), (6, 12), (8, 21), (11, 1), 
        (12, 24), (12, 25), (12, 30)
    ]
    month, day = check_date.month, check_date.day
    if (month, day) in mega_days:
        return True

    # Black Friday: last Friday of November
    if month == 11:
        last_day = calendar.monthrange(check_date.year, 11)[1]
        for d in range(last_day, 0, -1):
            if date(check_date.year, 11, d).weekday() == 4:
                if day == d:
                    return True

    # Cyber Monday: Monday after last Thursday of November
    if month == 11:
        last_day = calendar.monthrange(check_date.year, 11)[1]
        last_thursday = max(
            d for d in range(1, last_day+1)
            if date(check_date.year, 11, d).weekday() == 3
        )
        cyber_monday = date(check_date.year, 11, last_thursday) + timedelta(days=4)
        if check_date == cyber_monday:
            return True

    return False

# -----------------------------
# Dim_Time generator
# -----------------------------
def generate_future_dim_time(last_date_str: str, horizon: int) -> pd.DataFrame:
    """
    Generate future Dim_Time table for forecasting
    
    Returns all columns your model expects:
    date, year, quarter, month, month_name, week, day,
    day_of_week, day_of_the_year, is_weekend, is_payday, is_mega_sale_day
    """
    last_date = pd.to_datetime(last_date_str).date()
    future_dates = [last_date + timedelta(days=i) for i in range(1, horizon+1)]
    
    records = []
    for d in future_dates:
        year, week, _ = d.isocalendar()
        day_of_week = d.weekday()
        record = {
            'date': d,
            'year': d.year,
            'quarter': f"Q{(d.month - 1)//3 + 1}",
            'month': d.month,
            'month_name': d.strftime('%B'),
            'week': week,
            'day': d.day,
            'day_of_week': day_of_week,
            'day_of_the_year': d.timetuple().tm_yday,
            'is_weekend': day_of_week >= 5,
            'is_payday': is_payday(d),
            'is_mega_sale_day': is_mega_sale_day(d)
        }
        records.append(record)
    
    return pd.DataFrame(records)
