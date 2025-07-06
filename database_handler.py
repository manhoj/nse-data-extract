"""
PostgreSQL Database Handler
Manages database operations for storing stock/index data

Requirements:
pip install psycopg2-binary pandas
"""

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import pandas as pd
from datetime import datetime
import logging

# Import database configuration
try:
    from database_config import DB_CONFIG, TABLE_SCHEMA, get_table_name
except ImportError:
    print("❌ Error: database_config.py not found!")
    raise

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseHandler:
    """
    Handles PostgreSQL database operations for stock data
    """
    
    def __init__(self):
        """Initialize database handler"""
        self.connection = None
        self.cursor = None
        self.connect()
    
    def connect(self):
        """Establish database connection"""
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
    
    def create_table_if_not_exists(self, table_name):
        """Create table if it doesn't exist"""
        try:
            # Format the table creation query
            create_query = TABLE_SCHEMA.format(table_name=table_name)
            
            # Execute the query
            self.cursor.execute(create_query)
            self.connection.commit()
            
            logger.info(f"✅ Table '{table_name}' ready")
            
        except Exception as e:
            logger.error(f"❌ Error creating table: {e}")
            self.connection.rollback()
            raise
    
    def get_existing_dates(self, table_name):
        """Get list of dates already in the table"""
        try:
            query = sql.SQL("SELECT DISTINCT date FROM {} ORDER BY date").format(
                sql.Identifier(table_name)
            )
            self.cursor.execute(query)
            existing_dates = [row[0] for row in self.cursor.fetchall()]
            return existing_dates
        except psycopg2.errors.UndefinedTable:
            # Table doesn't exist yet
            return []
        except Exception as e:
            logger.error(f"❌ Error fetching existing dates: {e}")
            return []
    
    def insert_data(self, df, symbol, interval, mode='append'):
        """
        Insert DataFrame data into database
        
        Args:
            df (pd.DataFrame): Data to insert
            symbol (str): Stock/Index symbol
            interval (str): Time interval
            mode (str): 'append' (skip duplicates) or 'replace' (overwrite)
        
        Returns:
            tuple: (success, records_inserted, message)
        """
        try:
            # Generate table name
            table_name = get_table_name(symbol, interval)
            logger.info(f"📊 Processing data for table: {table_name}")
            
            # Create table if it doesn't exist
            self.create_table_if_not_exists(table_name)
            
            # Prepare DataFrame
            df_copy = df.copy()
            
            # Ensure date column is properly formatted
            df_copy['date'] = pd.to_datetime(df_copy['date'])
            
            if mode == 'append':
                # Get existing dates to avoid duplicates
                existing_dates = self.get_existing_dates(table_name)
                
                if existing_dates:
                    # Convert to pandas datetime for comparison
                    existing_dates_pd = pd.to_datetime(existing_dates)
                    
                    # Filter out rows that already exist
                    initial_count = len(df_copy)
                    df_copy = df_copy[~df_copy['date'].isin(existing_dates_pd)]
                    
                    skipped_count = initial_count - len(df_copy)
                    if skipped_count > 0:
                        logger.info(f"⏭️  Skipping {skipped_count} duplicate records")
            
            elif mode == 'replace':
                # Delete existing data for the date range
                if len(df_copy) > 0:
                    min_date = df_copy['date'].min()
                    max_date = df_copy['date'].max()
                    
                    delete_query = sql.SQL(
                        "DELETE FROM {} WHERE date >= %s AND date <= %s"
                    ).format(sql.Identifier(table_name))
                    
                    self.cursor.execute(delete_query, (min_date, max_date))
                    deleted_count = self.cursor.rowcount
                    if deleted_count > 0:
                        logger.info(f"🗑️  Deleted {deleted_count} existing records")
            
            # Insert data if there are new records
            if len(df_copy) > 0:
                # Prepare data for insertion
                columns = ['date', 'open', 'high', 'low', 'close', 'volume', 
                          'price_change', 'price_change_pct', 'high_low_range', 
                          'range_pct', 'day_of_week', 'month', 'year']
                
                # Convert DataFrame to list of tuples
                data = df_copy[columns].values.tolist()
                
                # Prepare INSERT query
                insert_query = sql.SQL("""
                    INSERT INTO {} ({}) 
                    VALUES %s
                    ON CONFLICT (date) DO NOTHING
                """).format(
                    sql.Identifier(table_name),
                    sql.SQL(', ').join(map(sql.Identifier, columns))
                )
                
                # Execute bulk insert
                execute_values(self.cursor, insert_query, data)
                
                # Commit transaction
                self.connection.commit()
                
                records_inserted = self.cursor.rowcount
                logger.info(f"✅ Inserted {records_inserted} new records into {table_name}")
                
                return True, records_inserted, f"Successfully inserted {records_inserted} records"
            else:
                logger.info("ℹ️  No new records to insert (all data already exists)")
                return True, 0, "No new records to insert (all data already exists)"
            
        except Exception as e:
            logger.error(f"❌ Error inserting data: {e}")
            self.connection.rollback()
            return False, 0, f"Error: {str(e)}"
    
    def get_table_info(self, table_name):
        """Get information about a table"""
        try:
            # Check if table exists
            check_query = """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = %s 
                    AND table_name = %s
                );
            """
            self.cursor.execute(check_query, (DB_CONFIG.get('schema', 'public'), table_name))
            exists = self.cursor.fetchone()[0]
            
            if not exists:
                return None
            
            # Get record count and date range
            info_query = sql.SQL("""
                SELECT 
                    COUNT(*) as record_count,
                    MIN(date) as min_date,
                    MAX(date) as max_date
                FROM {}
            """).format(sql.Identifier(table_name))
            
            self.cursor.execute(info_query)
            result = self.cursor.fetchone()
            
            return {
                'exists': True,
                'record_count': result[0],
                'min_date': result[1],
                'max_date': result[2]
            }
            
        except Exception as e:
            logger.error(f"Error getting table info: {e}")
            return None
    
    def test_connection(self):
        """Test database connection"""
        try:
            self.cursor.execute("SELECT 1")
            result = self.cursor.fetchone()
            return result[0] == 1
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

def push_data_to_db(df, symbol, interval, mode='append'):
    """
    Convenience function to push data to database
    
    Args:
        df (pd.DataFrame): Data to push
        symbol (str): Stock/Index symbol
        interval (str): Time interval
        mode (str): 'append' or 'replace'
    
    Returns:
        tuple: (success, records_inserted, message)
    """
    db_handler = None
    try:
        # Create database handler
        db_handler = DatabaseHandler()
        
        # Test connection
        if not db_handler.test_connection():
            return False, 0, "Database connection test failed"
        
        # Insert data
        return db_handler.insert_data(df, symbol, interval, mode)
        
    except Exception as e:
        return False, 0, f"Database error: {str(e)}"
        
    finally:
        # Always close connection
        if db_handler:
            db_handler.disconnect()

# Test function
if __name__ == "__main__":
    print("🔧 Testing Database Handler...")
    
    # Test connection
    try:
        handler = DatabaseHandler()
        if handler.test_connection():
            print("✅ Database connection successful!")
            
            # Test table info
            info = handler.get_table_info("nifty_5m")
            if info:
                print(f"📊 Table info: {info}")
            else:
                print("📊 Table doesn't exist yet")
                
        handler.disconnect()
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        print("Please update database_config.py with your PostgreSQL credentials")