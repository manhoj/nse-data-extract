"""
Create Complete NIFTY Expiry Days Table with Holiday Adjustments
Creates a comprehensive expiry calendar with all adjustments for holidays and weekends

Usage:
python create_raw_expiry_table.py [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD]
"""

import pandas as pd
from datetime import datetime, timedelta
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import logging
import argparse

# Import database config
try:
    from database_config import DB_CONFIG
except ImportError as e:
    print(f"❌ Error: {e}")
    print("Make sure database_config.py is in the same directory")
    exit(1)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# NSE Holiday Calendar (Major holidays that affect expiry)
NSE_HOLIDAYS = {
    # Republic Day
    '01-26': 'Republic Day',
    # Holi (approximate - varies by year)
    '03-18': 'Holi',
    # Good Friday (approximate - varies by year)
    '03-29': 'Good Friday',
    # Ambedkar Jayanti
    '04-14': 'Dr. Ambedkar Jayanti',
    # May Day
    '05-01': 'May Day',
    # Independence Day
    '08-15': 'Independence Day',
    # Gandhi Jayanti
    '10-02': 'Gandhi Jayanti',
    # Diwali (approximate - varies by year)
    '11-01': 'Diwali',
    # Guru Nanak Jayanti (approximate - varies by year)
    '11-15': 'Guru Nanak Jayanti',
    # Christmas
    '12-25': 'Christmas',
}

# Year-specific holidays (add more as needed)
YEAR_SPECIFIC_HOLIDAYS = {
    '2024-03-25': 'Holi',
    '2024-03-29': 'Good Friday',
    '2024-11-01': 'Diwali',
    '2024-11-15': 'Guru Nanak Jayanti',
    '2025-03-14': 'Holi',
    '2025-04-18': 'Good Friday',
    '2025-05-01': 'Maharashtra Day',
    '2025-10-30': 'Diwali',
    '2025-11-05': 'Guru Nanak Jayanti',
}

class CompleteExpiryTableCreator:
    """Creates complete NIFTY expiry days table with all adjustments"""
    
    def __init__(self):
        """Initialize database connection following the pattern from database_handler.py"""
        self.connection = None
        self.cursor = None
        self.table_name = 'nifty_expiry_days'
        self.connect()
    
    def connect(self):
        """Establish database connection like DatabaseHandler does"""
        try:
            self.connection = psycopg2.connect(
                host=DB_CONFIG['host'],
                port=DB_CONFIG['port'],
                database=DB_CONFIG['database'],
                user=DB_CONFIG['user'],
                password=DB_CONFIG['password']
            )
            self.cursor = self.connection.cursor()
            logger.info("✅ Database connection established")
            
            # Set schema if specified
            if DB_CONFIG.get('schema') and DB_CONFIG['schema'] != 'public':
                self.cursor.execute(f"SET search_path TO {DB_CONFIG['schema']}")
                self.connection.commit()
                logger.info(f"📋 Schema set to: {DB_CONFIG['schema']}")
                
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            raise
    
    def disconnect(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("🔌 Database connection closed")
    
    def create_table(self):
        """Drop and create the nifty_expiry_days table"""
        try:
            # Drop table if exists
            drop_query = f"DROP TABLE IF EXISTS {self.table_name} CASCADE"
            self.cursor.execute(drop_query)
            self.connection.commit()
            logger.info(f"🗑️  Dropped existing table: {self.table_name}")
            
            # Create table (simplified structure as requested)
            create_query = f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    date DATE PRIMARY KEY,
                    day VARCHAR(10),
                    expiry_type VARCHAR(10) NOT NULL,
                    month INTEGER,
                    year INTEGER
                );
                
                -- Create indexes
                CREATE INDEX IF NOT EXISTS idx_{self.table_name}_year ON {self.table_name} (year);
                CREATE INDEX IF NOT EXISTS idx_{self.table_name}_month ON {self.table_name} (year, month);
                CREATE INDEX IF NOT EXISTS idx_{self.table_name}_type ON {self.table_name} (expiry_type);
            """
            
            self.cursor.execute(create_query)
            self.connection.commit()
            logger.info(f"✅ Created table: {self.table_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error creating table: {e}")
            self.connection.rollback()
            return False
    
    def is_weekend(self, date):
        """Check if date is weekend"""
        return date.weekday() in [5, 6]  # Saturday = 5, Sunday = 6
    
    def is_nse_holiday(self, date):
        """Check if date is NSE holiday"""
        # Check year-specific holidays first
        date_str = date.strftime('%Y-%m-%d')
        if date_str in YEAR_SPECIFIC_HOLIDAYS:
            return True, YEAR_SPECIFIC_HOLIDAYS[date_str]
        
        # Check recurring holidays
        month_day = date.strftime('%m-%d')
        if month_day in NSE_HOLIDAYS:
            return True, NSE_HOLIDAYS[month_day]
        
        return False, None
    
    def is_trading_day(self, date):
        """Check if date is a trading day"""
        if self.is_weekend(date):
            return False
        
        is_holiday, _ = self.is_nse_holiday(date)
        return not is_holiday
    
    def get_previous_trading_day(self, date):
        """Get previous trading day before given date"""
        current = date - timedelta(days=1)
        
        while True:
            if self.is_trading_day(current):
                return current
            current -= timedelta(days=1)
            
            # Safety check
            if (date - current).days > 10:
                logger.warning(f"⚠️  Could not find trading day within 10 days of {date}")
                return None
    
    def get_last_thursday_of_month(self, year, month):
        """Get the last Thursday of a given month"""
        # Start from last day of month
        if month == 12:
            next_month = datetime(year + 1, 1, 1)
        else:
            next_month = datetime(year, month + 1, 1)
        
        last_day = next_month - timedelta(days=1)
        
        # Find last Thursday
        while last_day.weekday() != 3:  # Thursday is 3
            last_day -= timedelta(days=1)
        
        return last_day
    
    def generate_expiry_dates(self, start_date, end_date):
        """Generate all expiry dates with adjustments"""
        expiry_data = []
        processed_dates = set()  # To avoid duplicates
        
        # Process each month
        current_date = start_date.replace(day=1)
        
        while current_date <= end_date:
            year = current_date.year
            month = current_date.month
            
            # Get all Thursdays in the month
            month_start = datetime(year, month, 1)
            if month == 12:
                month_end = datetime(year + 1, 1, 1) - timedelta(days=1)
            else:
                month_end = datetime(year, month + 1, 1) - timedelta(days=1)
            
            # Find last Thursday of month
            last_thursday = self.get_last_thursday_of_month(year, month)
            
            # Process each Thursday
            current = month_start
            while current <= month_end:
                if current.weekday() == 3:  # Thursday
                    is_monthly = (current.date() == last_thursday.date())
                    expiry_type = 'monthly' if is_monthly else 'weekly'
                    
                    # Check if Thursday is a trading day
                    if self.is_trading_day(current):
                        actual_date = current
                    else:
                        # Thursday is not a trading day, find previous trading day
                        actual_date = self.get_previous_trading_day(current)
                        
                        if actual_date is None:
                            logger.warning(f"⚠️  Skipping expiry for {current} - no trading day found")
                            current += timedelta(days=1)
                            continue
                    
                    # Only add if within our date range and not already processed
                    if start_date <= actual_date <= end_date and actual_date.date() not in processed_dates:
                        expiry_data.append({
                            'date': actual_date.date(),
                            'day': actual_date.strftime('%A'),
                            'expiry_type': expiry_type,
                            'month': actual_date.month,
                            'year': actual_date.year
                        })
                        processed_dates.add(actual_date.date())
                
                current += timedelta(days=1)
            
            # Move to next month
            if month == 12:
                current_date = datetime(year + 1, 1, 1)
            else:
                current_date = datetime(year, month + 1, 1)
        
        # Sort by date
        expiry_data.sort(key=lambda x: x['date'])
        
        logger.info(f"📊 Generated {len(expiry_data)} expiry dates")
        return expiry_data
    
    def load_data(self, expiry_data):
        """Load expiry data into database using execute_values like database_handler.py"""
        try:
            if not expiry_data:
                logger.warning("⚠️  No data to load")
                return False
            
            # Convert to list of tuples for execute_values
            columns = ['date', 'day', 'expiry_type', 'month', 'year']
            data = [[row[col] for col in columns] for row in expiry_data]
            
            # Insert using execute_values (same pattern as database_handler.py)
            insert_query = sql.SQL("""
                INSERT INTO {} ({}) 
                VALUES %s
            """).format(
                sql.Identifier(self.table_name),
                sql.SQL(', ').join(map(sql.Identifier, columns))
            )
            
            execute_values(self.cursor, insert_query, data)
            self.connection.commit()
            
            logger.info(f"✅ Loaded {len(expiry_data)} records into {self.table_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error loading data: {e}")
            self.connection.rollback()
            return False
    
    def show_summary(self):
        """Show summary of loaded data"""
        try:
            # Count summary
            summary_query = sql.SQL("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(CASE WHEN expiry_type = 'weekly' THEN 1 END) as weekly,
                    COUNT(CASE WHEN expiry_type = 'monthly' THEN 1 END) as monthly,
                    COUNT(CASE WHEN day != 'Thursday' THEN 1 END) as adjusted,
                    MIN(date) as start_date,
                    MAX(date) as end_date
                FROM {}
            """).format(sql.Identifier(self.table_name))
            
            self.cursor.execute(summary_query)
            result = self.cursor.fetchone()
            
            logger.info(f"\n📊 Summary:")
            logger.info(f"   Total Expiries: {result[0]}")
            logger.info(f"   Weekly: {result[1]}")
            logger.info(f"   Monthly: {result[2]}")
            logger.info(f"   Adjusted for holidays: {result[3]}")
            logger.info(f"   Date Range: {result[4]} to {result[5]}")
            
            # Show adjusted expiries
            adjusted_query = sql.SQL("""
                SELECT date, day, expiry_type
                FROM {} 
                WHERE day != 'Thursday'
                ORDER BY date DESC 
                LIMIT 10
            """).format(sql.Identifier(self.table_name))
            
            self.cursor.execute(adjusted_query)
            adjusted = self.cursor.fetchall()
            
            if adjusted:
                logger.info("\n📅 Recent holiday-adjusted expiries:")
                for date, day, exp_type in adjusted:
                    logger.info(f"   {date} ({day}) - {exp_type}")
            
            # Show recent expiries
            recent_query = sql.SQL("""
                SELECT date, day, expiry_type
                FROM {} 
                ORDER BY date DESC 
                LIMIT 10
            """).format(sql.Identifier(self.table_name))
            
            self.cursor.execute(recent_query)
            recent = self.cursor.fetchall()
            
            logger.info("\n📅 Recent 10 expiries:")
            for date, day, exp_type in recent:
                logger.info(f"   {date} ({day}) - {exp_type}")
            
        except Exception as e:
            logger.error(f"❌ Error showing summary: {e}")

def main():
    parser = argparse.ArgumentParser(description='Create complete NIFTY expiry days table with adjustments')
    parser.add_argument('--start-date', type=str, default='2020-01-01', 
                       help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, default='2030-12-31',
                       help='End date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    creator = None
    try:
        # Parse dates
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
        
        logger.info("🚀 Complete NIFTY Expiry Table Creator")
        logger.info("=" * 50)
        logger.info(f"📅 Creating expiries from {start_date.date()} to {end_date.date()}")
        logger.info("✅ Including holiday and weekend adjustments")
        
        # Create and run
        creator = CompleteExpiryTableCreator()
        
        # Create table
        if not creator.create_table():
            return
        
        # Generate dates
        expiry_data = creator.generate_expiry_dates(start_date, end_date)
        
        # Load data
        if creator.load_data(expiry_data):
            creator.show_summary()
            logger.info("\n✅ Done! Table 'nifty_expiry_days' created with complete expiry calendar")
            logger.info("\n📝 Sample queries:")
            logger.info("   -- Get all expiries")
            logger.info(f"   SELECT * FROM {creator.table_name} ORDER BY date DESC;")
            logger.info("   -- Get adjusted expiries")
            logger.info(f"   SELECT * FROM {creator.table_name} WHERE day != 'Thursday';")
            logger.info("   -- Get monthly expiries")
            logger.info(f"   SELECT * FROM {creator.table_name} WHERE expiry_type = 'monthly';")
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if creator:
            creator.disconnect()

if __name__ == "__main__":
    main()