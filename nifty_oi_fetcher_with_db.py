"""
NIFTY OI Fetcher - Standalone Version with Database
All-in-one script for fetching and storing NIFTY options data

Usage:
    python nifty_oi_standalone.py [--create-table] [--range RANGE]
"""

import os
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from kiteconnect import KiteConnect
from datetime import datetime, timedelta
import pandas as pd
import argparse
import logging
from typing import Dict, List, Tuple

# Import authentication and config
from kite_authenticator import get_kite_token
from database_config import DB_CONFIG

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class NiftyOIFetcherStandalone:
    """All-in-one NIFTY OI fetcher with database"""
    
    def __init__(self):
        """Initialize fetcher and database connection"""
        # Initialize Kite connection
        self.access_token = get_kite_token(force_new=False)
        if not self.access_token:
            raise Exception("No valid access token available")
        
        self.api_key = "qkd6rimabtakrvea"
        self.kite = KiteConnect(api_key=self.api_key)
        self.kite.set_access_token(self.access_token)
        
        # Initialize database connection
        self.connection = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            database=DB_CONFIG['database'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        self.cursor = self.connection.cursor()
        self.table_name = "nifty_oi"
        
        logger.info("✅ NIFTY OI Fetcher initialized")
    
    def create_table(self, drop_existing=False):
        """Create the nifty_oi table"""
        try:
            if drop_existing:
                self.cursor.execute(f"DROP TABLE IF EXISTS {self.table_name}")
                logger.info(f"🗑️ Dropped existing table: {self.table_name}")
            
            create_query = f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP NOT NULL,
                    symbol VARCHAR(50) NOT NULL,
                    expiry VARCHAR(20) NOT NULL,
                    strike INTEGER,
                    option_type VARCHAR(2),
                    ltp DOUBLE PRECISION,
                    open DOUBLE PRECISION,
                    high DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    prev_close DOUBLE PRECISION,
                    oi BIGINT,
                    oi_day_high BIGINT,
                    oi_day_low BIGINT,
                    volume BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(timestamp, symbol)
                );
                
                CREATE INDEX IF NOT EXISTS idx_nifty_oi_timestamp ON {self.table_name} (timestamp);
                CREATE INDEX IF NOT EXISTS idx_nifty_oi_strike ON {self.table_name} (strike);
                CREATE INDEX IF NOT EXISTS idx_nifty_oi_option_type ON {self.table_name} (option_type);
            """
            
            self.cursor.execute(create_query)
            self.connection.commit()
            logger.info(f"✅ Created table: {self.table_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error creating table: {e}")
            self.connection.rollback()
            return False
    
    def get_nifty_spot_price(self) -> float:
        """Get current NIFTY spot price"""
        try:
            quote = self.kite.quote(["NSE:NIFTY 50"])
            spot_price = quote["NSE:NIFTY 50"]["last_price"]
            logger.info(f"📊 NIFTY Spot Price: {spot_price}")
            return spot_price
        except Exception as e:
            logger.error(f"❌ Error fetching spot price: {e}")
            raise
    
    def get_next_expiries(self, num_expiries: int = 2) -> List[Tuple[datetime, str, str]]:
        """Get next N expiries (weekly expiries every Thursday)"""
        today = datetime.now()
        expiries = []
        
        # Find next Thursdays
        current_date = today
        
        while len(expiries) < num_expiries:
            # If it's Thursday and after current time, or any day after today
            if current_date.weekday() == 3:  # Thursday
                if current_date.date() > today.date() or (current_date.date() == today.date() and today.hour < 15):
                    # Format: YY + M + DD where M is month number (1-9 for Jan-Sep, O for Oct, N for Nov, D for Dec)
                    year = current_date.strftime("%y")
                    month = current_date.month
                    day = current_date.strftime("%d")
                    
                    # Month mapping for Kite
                    month_map = {
                        1: '1', 2: '2', 3: '3', 4: '4', 5: '5', 6: '6',
                        7: '7', 8: '8', 9: '9', 10: 'O', 11: 'N', 12: 'D'
                    }
                    
                    month_code = month_map[month]
                    expiry_code = f"{year}{month_code}{day}"
                    
                    # Format date as DD-MMM-YYYY
                    expiry_date_str = current_date.strftime("%d-%b-%Y").upper()
                    
                    expiries.append((current_date, expiry_code, expiry_date_str))
            
            current_date += timedelta(days=1)
        
        return expiries
    
    def fetch_and_store_options(self, range_value: int = 1000):
        """Fetch options data and store in database"""
        try:
            # Check if table exists, create if not
            self.cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                );
            """, (self.table_name,))
            
            table_exists = self.cursor.fetchone()[0]
            
            if not table_exists:
                logger.warning(f"⚠️ Table {self.table_name} doesn't exist. Creating it now...")
                self.create_table()
                logger.info("✅ Table created successfully")
            
            # Get spot price
            spot_price = self.get_nifty_spot_price()
            
            # Get next two expiries
            expiries = self.get_next_expiries(num_expiries=2)
            logger.info(f"📅 Found {len(expiries)} expiries:")
            for exp_date, exp_code, exp_date_str in expiries:
                logger.info(f"   - {exp_date_str} (Code: {exp_code})")
            
            # Generate strikes
            start_strike = int((spot_price - range_value) // 50) * 50
            end_strike = int((spot_price + range_value) // 50 + 1) * 50
            strikes = list(range(start_strike, end_strike + 50, 50))
            
            logger.info(f"📊 Fetching {len(strikes)} strikes from {start_strike} to {end_strike}")
            
            # Prepare data for database
            all_data = []
            timestamp = datetime.now()
            
            # Fetch data for each expiry
            for expiry_date, expiry_code, expiry_date_str in expiries:
                logger.info(f"\n📥 Fetching data for expiry: {expiry_date_str}")
                
                # Build symbols for this expiry
                symbols = []
                for strike in strikes:
                    for opt_type in ["CE", "PE"]:
                        symbol = f"NIFTY{expiry_code}{strike}{opt_type}"
                        symbols.append(symbol)
                
                # Fetch in batches
                batch_size = 200
                for i in range(0, len(symbols), batch_size):
                    batch = symbols[i:i + batch_size]
                    full_symbols = [f"NFO:{sym}" for sym in batch]
                    
                    logger.info(f"📥 Fetching batch {i//batch_size + 1} for {expiry_date_str}")
                    
                    try:
                        quotes = self.kite.quote(full_symbols)
                        
                        for sym in batch:
                            full_sym = f"NFO:{sym}"
                            data = quotes.get(full_sym, {})
                            
                            if data and "last_price" in data:
                                ohlc = data.get("ohlc", {})
                                
                                # Extract strike and option type
                                if expiry_code in sym:
                                    remaining = sym.replace(f"NIFTY{expiry_code}", "")
                                    strike = int(remaining[:-2])
                                    option_type = remaining[-2:]
                                else:
                                    continue
                                
                                row = (
                                    timestamp,
                                    sym,
                                    expiry_date_str,  # Use formatted date instead of code
                                    strike,
                                    option_type,
                                    data.get("last_price", 0),
                                    ohlc.get("open", 0),
                                    ohlc.get("high", 0),
                                    ohlc.get("low", 0),
                                    ohlc.get("close", 0),
                                    data.get("oi", 0),
                                    data.get("oi_day_high", 0),
                                    data.get("oi_day_low", 0),
                                    data.get("volume", 0)
                                )
                                all_data.append(row)
                    
                    except Exception as e:
                        logger.error(f"❌ Error in batch: {e}")
            
            # Insert into database
            if all_data:
                insert_query = f"""
                    INSERT INTO {self.table_name} 
                    (timestamp, symbol, expiry, strike, option_type, ltp, open, high, low, prev_close, 
                     oi, oi_day_high, oi_day_low, volume)
                    VALUES %s
                    ON CONFLICT (timestamp, symbol) 
                    DO UPDATE SET
                        ltp = EXCLUDED.ltp,
                        oi = EXCLUDED.oi,
                        volume = EXCLUDED.volume
                """
                
                execute_values(self.cursor, insert_query, all_data)
                self.connection.commit()
                
                logger.info(f"✅ Inserted {len(all_data)} records")
                
                # Display summary for each expiry
                self.display_summary_multi_expiry(all_data, spot_price, expiries)
                
                return True
            
        except Exception as e:
            logger.error(f"❌ Error: {e}")
            self.connection.rollback()
            return False
    
    def display_summary_multi_expiry(self, data: List, spot_price: float, expiries: List):
        """Display summary for multiple expiries"""
        df = pd.DataFrame(data, columns=[
            'timestamp', 'symbol', 'expiry', 'strike', 'option_type',
            'ltp', 'open', 'high', 'low', 'prev_close',
            'oi', 'oi_day_high', 'oi_day_low', 'volume'
        ])
        
        print("\n" + "="*80)
        print(f"📊 NIFTY OI DATA SUMMARY")
        print(f"🎯 Spot Price: {spot_price}")
        print(f"📅 Timestamp: {df['timestamp'].iloc[0]}")
        print("="*80)
        
        # Summary for each expiry
        for expiry_date, expiry_code, expiry_date_str in expiries:
            expiry_df = df[df['expiry'] == expiry_date_str]
            
            if not expiry_df.empty:
                print(f"\n📅 EXPIRY: {expiry_date_str}")
                print("-" * 50)
                
                # Calculate totals
                total_ce_oi = expiry_df[expiry_df['option_type'] == 'CE']['oi'].sum()
                total_pe_oi = expiry_df[expiry_df['option_type'] == 'PE']['oi'].sum()
                pcr = total_pe_oi / total_ce_oi if total_ce_oi > 0 else 0
                
                print(f"Total CE OI: {total_ce_oi:,}")
                print(f"Total PE OI: {total_pe_oi:,}")
                print(f"PCR (OI): {pcr:.2f}")
                
                # Max OI strikes
                max_ce_oi = expiry_df[expiry_df['option_type'] == 'CE'].nlargest(1, 'oi')
                max_pe_oi = expiry_df[expiry_df['option_type'] == 'PE'].nlargest(1, 'oi')
                
                if not max_ce_oi.empty:
                    print(f"Max CE OI Strike (Resistance): {max_ce_oi.iloc[0]['strike']}")
                    print(f"Max PE OI Strike (Support): {max_pe_oi.iloc[0]['strike']}")
        
        # Overall summary
        print(f"\n📈 OVERALL STATISTICS:")
        print(f"Total Records: {len(df)}")
        print(f"Total Expiries: {len(df['expiry'].unique())}")
        print(f"Total CE OI (All Expiries): {df[df['option_type'] == 'CE']['oi'].sum():,}")
        print(f"Total PE OI (All Expiries): {df[df['option_type'] == 'PE']['oi'].sum():,}")
    
    def close(self):
        """Close connections"""
        self.cursor.close()
        self.connection.close()
        logger.info("🔌 Connections closed")

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='NIFTY OI Fetcher - Standalone')
    parser.add_argument('--create-table', action='store_true',
                       help='Create database table')
    parser.add_argument('--drop-existing', action='store_true',
                       help='Drop existing table before creating')
    parser.add_argument('--range', type=int, default=1000,
                       help='Range from spot price (default: 1000)')
    
    args = parser.parse_args()
    
    try:
        # Initialize fetcher
        fetcher = NiftyOIFetcherStandalone()
        
        # Create table if requested
        if args.create_table:
            success = fetcher.create_table(drop_existing=args.drop_existing)
            if not success:
                return
        
        # Fetch and store data
        fetcher.fetch_and_store_options(range_value=args.range)
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if 'fetcher' in locals():
            fetcher.close()

if __name__ == "__main__":
    main()