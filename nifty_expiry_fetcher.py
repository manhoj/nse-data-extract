# NIFTY Expiry Day Marker Implementation
# This code identifies and marks NIFTY expiry days based on NSE rules

import pandas as pd
from datetime import datetime, timedelta
import numpy as np

def get_last_thursday_of_month(year, month):
    """Get the last Thursday of a given month"""
    # Start from the last day of the month
    if month == 12:
        next_month = datetime(year + 1, 1, 1)
    else:
        next_month = datetime(year, month + 1, 1)
    
    last_day = next_month - timedelta(days=1)
    
    # Find the last Thursday
    while last_day.weekday() != 3:  # Thursday is 3
        last_day -= timedelta(days=1)
    
    return last_day

def is_thursday(date):
    """Check if a date is Thursday"""
    return date.weekday() == 3

def is_last_thursday_of_month(date):
    """Check if a date is the last Thursday of its month"""
    last_thursday = get_last_thursday_of_month(date.year, date.month)
    return date.date() == last_thursday.date()

def mark_nifty_expiry_days(df, date_column='date', trading_day_column=None):
    """
    Mark NIFTY expiry days in a DataFrame
    
    Parameters:
    df: DataFrame with trading data
    date_column: Name of the date column
    trading_day_column: Optional column indicating if it's a trading day (1) or holiday (0)
    
    Returns:
    DataFrame with additional columns:
    - is_expiry: 1 if it's an expiry day, 0 otherwise
    - expiry_type: 'weekly', 'monthly', or None
    - adjusted_expiry: 1 if expiry was adjusted due to holiday
    """
    
    # Ensure date column is datetime
    df[date_column] = pd.to_datetime(df[date_column])
    
    # Sort by date
    df = df.sort_values(date_column).copy()
    
    # Initialize columns
    df['is_expiry'] = 0
    df['expiry_type'] = None
    df['adjusted_expiry'] = 0
    
    # Create a set of all trading dates for quick lookup
    if trading_day_column:
        trading_dates = set(df[df[trading_day_column] == 1][date_column].dt.date)
    else:
        trading_dates = set(df[date_column].dt.date)
    
    # Process each month in the date range
    start_date = df[date_column].min()
    end_date = df[date_column].max()
    
    current_date = start_date.replace(day=1)
    
    while current_date <= end_date:
        year = current_date.year
        month = current_date.month
        
        # Find all Thursdays in this month
        month_start = datetime(year, month, 1)
        if month == 12:
            month_end = datetime(year + 1, 1, 1) - timedelta(days=1)
        else:
            month_end = datetime(year, month + 1, 1) - timedelta(days=1)
        
        # Get all Thursdays in the month
        thursdays = []
        current = month_start
        while current <= month_end:
            if is_thursday(current):
                thursdays.append(current)
            current += timedelta(days=1)
        
        # Process each Thursday
        for thursday in thursdays:
            is_monthly = is_last_thursday_of_month(thursday)
            
            # Check if Thursday is a trading day
            if thursday.date() in trading_dates:
                # Mark as expiry
                mask = df[date_column].dt.date == thursday.date()
                df.loc[mask, 'is_expiry'] = 1
                df.loc[mask, 'expiry_type'] = 'monthly' if is_monthly else 'weekly'
            else:
                # Thursday is a holiday, find previous trading day
                adjusted_date = thursday - timedelta(days=1)
                while adjusted_date.date() not in trading_dates and adjusted_date >= month_start:
                    adjusted_date -= timedelta(days=1)
                
                if adjusted_date.date() in trading_dates:
                    mask = df[date_column].dt.date == adjusted_date.date()
                    df.loc[mask, 'is_expiry'] = 1
                    df.loc[mask, 'expiry_type'] = 'monthly' if is_monthly else 'weekly'
                    df.loc[mask, 'adjusted_expiry'] = 1
        
        # Move to next month
        if month == 12:
            current_date = datetime(year + 1, 1, 1)
        else:
            current_date = datetime(year, month + 1, 1)
    
    return df

# SQL Implementation for marking expiry days
sql_query = """
-- SQL query to mark NIFTY expiry days
-- This assumes you have a calendar table with trading days marked

WITH thursday_dates AS (
    -- Get all Thursdays
    SELECT 
        date,
        EXTRACT(DOW FROM date) as day_of_week,
        EXTRACT(DAY FROM date) as day_of_month,
        DATE_TRUNC('month', date) as month_start,
        DATE_TRUNC('month', date + INTERVAL '1 month') - INTERVAL '1 day' as month_end
    FROM your_nifty_table
    WHERE EXTRACT(DOW FROM date) = 4  -- Thursday
),
last_thursdays AS (
    -- Identify last Thursday of each month
    SELECT 
        date,
        CASE 
            WHEN date = MAX(date) OVER (PARTITION BY DATE_TRUNC('month', date))
            THEN 1 
            ELSE 0 
        END as is_last_thursday
    FROM thursday_dates
),
expiry_schedule AS (
    -- Create expiry schedule
    SELECT 
        t.date as scheduled_expiry,
        CASE 
            WHEN lt.is_last_thursday = 1 THEN 'monthly'
            ELSE 'weekly'
        END as expiry_type
    FROM thursday_dates t
    JOIN last_thursdays lt ON t.date = lt.date
),
adjusted_expiries AS (
    -- Adjust for holidays
    SELECT 
        es.scheduled_expiry,
        es.expiry_type,
        CASE 
            WHEN nt.is_trading_day = 1 THEN es.scheduled_expiry
            ELSE (
                -- Find previous trading day
                SELECT MAX(date) 
                FROM your_nifty_table 
                WHERE date < es.scheduled_expiry 
                AND is_trading_day = 1
            )
        END as actual_expiry_date
    FROM expiry_schedule es
    LEFT JOIN your_nifty_table nt ON es.scheduled_expiry = nt.date
)
-- Final query to update your table
UPDATE your_nifty_table
SET 
    is_expiry = CASE 
        WHEN date IN (SELECT actual_expiry_date FROM adjusted_expiries) 
        THEN 1 
        ELSE 0 
    END,
    expiry_type = (
        SELECT expiry_type 
        FROM adjusted_expiries 
        WHERE actual_expiry_date = your_nifty_table.date
    ),
    adjusted_expiry = CASE 
        WHEN date IN (
            SELECT actual_expiry_date 
            FROM adjusted_expiries 
            WHERE actual_expiry_date != scheduled_expiry
        ) THEN 1 
        ELSE 0 
    END;
"""

# Example usage
if __name__ == "__main__":
    # Example DataFrame
    dates = pd.date_range(start='2023-07-25', end='2025-07-10', freq='D')
    df = pd.DataFrame({'date': dates})
    
    # Simulate some holidays (you would use actual trading calendar)
    # Remove some Thursdays as holidays for demonstration
    df['is_trading_day'] = 1
    df.loc[df['date'].isin(['2023-08-15', '2024-03-29', '2024-08-15']), 'is_trading_day'] = 0
    
    # Mark expiry days
    result_df = mark_nifty_expiry_days(df, 'date', 'is_trading_day')
    
    # Display expiry days
    expiry_days = result_df[result_df['is_expiry'] == 1]
    print("Sample of identified expiry days:")
    print(expiry_days[['date', 'expiry_type', 'adjusted_expiry']].head(20))
    
    # Summary statistics
    print(f"\nTotal expiry days: {len(expiry_days)}")
    print(f"Weekly expiries: {len(expiry_days[expiry_days['expiry_type'] == 'weekly'])}")
    print(f"Monthly expiries: {len(expiry_days[expiry_days['expiry_type'] == 'monthly'])}")
    print(f"Adjusted expiries: {len(expiry_days[expiry_days['adjusted_expiry'] == 1])}")

# NIFTY Expiry Rules Summary:
# 1. Weekly expiry: Every Thursday of the week
# 2. Monthly expiry: Last Thursday of the month (also counts as weekly)
# 3. If Thursday is a trading holiday, expiry is on the previous trading day
# 4. These rules have been consistent for the NSE equity derivatives segment
# 5. Bank Nifty follows the same pattern but has different contract specifications