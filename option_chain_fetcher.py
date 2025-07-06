"""
Option Chain Data Fetcher - Final Working Version
Fetches NIFTY option prices with Greeks using the correct spot price method

This version uses NSE:NIFTY 50 LTP method which works correctly
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time
from kiteconnect import KiteConnect
import logging
import psycopg2
from psycopg2.extras import execute_values
import json
import time as time_module
from dotenv import load_dotenv
import os

# Import existing modules
try:
    from kite_authenticator import get_kite_token
    from database_handler import DatabaseHandler
    from database_config import DB_CONFIG
except ImportError:
    print("❌ Error: Required modules not found!")
    exit(1)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

class OptionChainFetcher:
    """
    Fetches option chain data with Greeks for NIFTY
    """
    
    def __init__(self):
        """Initialize the option chain fetcher"""
        logger.info("🔐 Initializing Option Chain Fetcher...")
        
        # Get access token
        self.access_token = get_kite_token(force_new=False)
        
        if not self.access_token:
            logger.error("❌ Failed to get access token!")
            raise Exception("No valid access token available")
        
        # Initialize Kite API
        self.api_key = os.environ["KITE_API_KEY"]
        self.kite = KiteConnect(api_key=self.api_key)
        self.kite.set_access_token(self.access_token)
        
        # Test connection
        try:
            profile = self.kite.profile()
            logger.info(f"✅ Connected as: {profile.get('user_name', 'Unknown')}")
        except Exception as e:
            logger.error(f"❌ Connection test failed: {e}")
            raise
        
        # Initialize database handler
        self.db_handler = DatabaseHandler()
        
        # Cache for instruments
        self._instruments_cache = None
        self._cache_timestamp = None
        
        logger.info("✅ Option Chain Fetcher initialized successfully!")
    
    def get_current_expiry(self):
        """Get the current weekly expiry date (Thursday)"""
        today = datetime.now().date()
        days_until_thursday = (3 - today.weekday()) % 7
        
        if days_until_thursday == 0:
            # Today is Thursday
            if datetime.now().time() > time(15, 30):
                # After market close, get next Thursday
                days_until_thursday = 7
        
        next_expiry = today + timedelta(days=days_until_thursday)
        return next_expiry
    
    def get_nifty_options(self, force_refresh=False):
        """Get all NIFTY option instruments"""
        # Check cache (valid for 1 hour)
        if (not force_refresh and 
            self._instruments_cache is not None and 
            self._cache_timestamp and 
            (datetime.now() - self._cache_timestamp).seconds < 3600):
            return self._instruments_cache
        
        try:
            logger.info("🔍 Fetching NIFTY option instruments...")
            
            # Get all NFO instruments
            all_instruments = self.kite.instruments("NFO")
            
            # Filter NIFTY options
            nifty_options = []
            for instrument in all_instruments:
                if (instrument['name'] == 'NIFTY' and 
                    instrument['instrument_type'] in ['CE', 'PE'] and
                    instrument['segment'] == 'NFO-OPT'):
                    nifty_options.append(instrument)
            
            # Update cache
            self._instruments_cache = nifty_options
            self._cache_timestamp = datetime.now()
            
            logger.info(f"✅ Found {len(nifty_options)} NIFTY option instruments")
            
            return nifty_options
            
        except Exception as e:
            logger.error(f"❌ Error fetching option instruments: {e}")
            return []
    
    def get_option_strikes(self, spot_price, expiry_date):
        """Get relevant option strikes based on spot price"""
        # Round to nearest 50 for NIFTY
        atm_strike = int(round(spot_price / 50) * 50)
        
        logger.info(f"📊 Spot Price: {spot_price:.2f}, ATM Strike: {atm_strike}")
        
        # For CE: ITM strikes are lower, OTM strikes are higher
        # For PE: ITM strikes are higher, OTM strikes are lower
        ce_strikes = {
            'ATM': atm_strike,
            'ITM_1': atm_strike - 50,
            'ITM_2': atm_strike - 100,
            'OTM_1': atm_strike + 50,
            'OTM_2': atm_strike + 100
        }
        
        pe_strikes = {
            'ATM': atm_strike,
            'ITM_1': atm_strike + 50,
            'ITM_2': atm_strike + 100,
            'OTM_1': atm_strike - 50,
            'OTM_2': atm_strike - 100
        }
        
        return {'CE': ce_strikes, 'PE': pe_strikes}
    
    def find_option_instrument(self, strike, option_type, expiry_date, instruments):
        """Find specific option instrument"""
        for instrument in instruments:
            if (instrument['strike'] == strike and 
                instrument['instrument_type'] == option_type and
                instrument['expiry'] == expiry_date):
                return instrument
        
        logger.warning(f"⚠️ Instrument not found: {option_type} {strike} {expiry_date}")
        return None
    
    def fetch_option_data(self, instrument_token, interval="5minute"):
        """Fetch historical data for an option"""
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(minutes=30)  # Get more data
            
            historical_data = self.kite.historical_data(
                instrument_token=instrument_token,
                from_date=start_date,
                to_date=end_date,
                interval=interval
            )
            
            if historical_data:
                df = pd.DataFrame(historical_data)
                # Ensure timezone-naive
                if 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)
                return df
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.debug(f"No historical data for option token {instrument_token}: {e}")
            return pd.DataFrame()
    
    def calculate_greeks_approximation(self, option_price, spot_price, strike, option_type, days_to_expiry):
        """Calculate approximate Greeks using simple formulas"""
        try:
            if days_to_expiry <= 0:
                days_to_expiry = 0.1  # Avoid division by zero
            
            # Calculate moneyness
            if option_type == 'CE':
                moneyness = (spot_price - strike) / spot_price
                intrinsic_value = max(0, spot_price - strike)
            else:  # PE
                moneyness = (strike - spot_price) / spot_price
                intrinsic_value = max(0, strike - spot_price)
            
            time_value = option_price - intrinsic_value
            
            # Approximate Delta
            if option_type == 'CE':
                if moneyness > 0.02:  # ITM
                    delta = 0.5 + min(0.4, moneyness * 10)
                elif moneyness < -0.02:  # OTM
                    delta = 0.5 - min(0.4, abs(moneyness) * 10)
                else:  # ATM
                    delta = 0.5
            else:  # PE
                if moneyness > 0.02:  # ITM
                    delta = -0.5 - min(0.4, moneyness * 10)
                elif moneyness < -0.02:  # OTM
                    delta = -0.5 + min(0.4, abs(moneyness) * 10)
                else:  # ATM
                    delta = -0.5
            
            # Approximate Gamma (highest at ATM)
            atm_factor = np.exp(-((moneyness * 100) ** 2) / 50)
            gamma = 0.001 * atm_factor / np.sqrt(days_to_expiry)
            
            # Approximate Theta (time decay)
            theta = -time_value / days_to_expiry if days_to_expiry > 0 else -time_value
            
            # Approximate Vega
            vega = option_price * 0.001 * np.sqrt(days_to_expiry) * atm_factor
            
            # Approximate IV (using price as proxy)
            iv_proxy = (option_price / spot_price) * np.sqrt(365 / days_to_expiry) * 100
            iv = min(100, max(5, iv_proxy))  # Cap between 5% and 100%
            
            return {
                'delta': round(delta, 4),
                'gamma': round(gamma, 6),
                'theta': round(theta, 2),
                'vega': round(vega, 4),
                'iv': round(iv, 2)
            }
            
        except Exception as e:
            logger.error(f"Error calculating Greeks: {e}")
            return {
                'delta': 0.5 if option_type == 'CE' else -0.5,
                'gamma': 0.001,
                'theta': -1,
                'vega': 0.01,
                'iv': 15.0
            }
    
    def create_options_table(self):
        """Create options data table if it doesn't exist"""
        create_query = """
        CREATE TABLE IF NOT EXISTS nifty_options_5m (
            timestamp           TIMESTAMP,
            spot_price          DOUBLE PRECISION,
            strike              INTEGER,
            option_type         VARCHAR(2),
            expiry_date         DATE,
            moneyness          VARCHAR(10),
            open               DOUBLE PRECISION,
            high               DOUBLE PRECISION,
            low                DOUBLE PRECISION,
            close              DOUBLE PRECISION,
            volume             BIGINT,
            oi                 BIGINT,
            delta              DOUBLE PRECISION,
            gamma              DOUBLE PRECISION,
            theta              DOUBLE PRECISION,
            vega               DOUBLE PRECISION,
            iv                 DOUBLE PRECISION,
            bid_price          DOUBLE PRECISION,
            ask_price          DOUBLE PRECISION,
            bid_qty            INTEGER,
            ask_qty            INTEGER,
            last_price         DOUBLE PRECISION,
            change             DOUBLE PRECISION,
            change_pct         DOUBLE PRECISION,
            PRIMARY KEY (timestamp, strike, option_type, expiry_date)
        );
        
        -- Create indexes for faster queries
        CREATE INDEX IF NOT EXISTS idx_nifty_options_timestamp ON nifty_options_5m(timestamp);
        CREATE INDEX IF NOT EXISTS idx_nifty_options_strike ON nifty_options_5m(strike);
        CREATE INDEX IF NOT EXISTS idx_nifty_options_type ON nifty_options_5m(option_type);
        CREATE INDEX IF NOT EXISTS idx_nifty_options_expiry ON nifty_options_5m(expiry_date);
        """
        
        try:
            self.db_handler.cursor.execute(create_query)
            self.db_handler.connection.commit()
            logger.info("✅ Options table ready")
        except Exception as e:
            logger.error(f"❌ Error creating options table: {e}")
            self.db_handler.connection.rollback()
    
    def fetch_spot_price(self):
        """Fetch current NIFTY spot price using the working method"""
        try:
            # Use the working method: NSE:NIFTY 50 with LTP
            logger.info("🔍 Fetching NIFTY spot price...")
            
            # Get both LTP and quote for complete data
            ltp_data = self.kite.ltp(["NSE:NIFTY 50"])
            quote_data = self.kite.quote(["NSE:NIFTY 50"])
            
            if ltp_data and "NSE:NIFTY 50" in ltp_data:
                spot_price = ltp_data["NSE:NIFTY 50"]["last_price"]
                
                # Get OHLC from quote if available
                ohlc = {'open': spot_price, 'high': spot_price, 'low': spot_price, 'close': spot_price}
                if quote_data and "NSE:NIFTY 50" in quote_data:
                    quote = quote_data["NSE:NIFTY 50"]
                    ohlc = quote.get('ohlc', ohlc)
                
                logger.info(f"✅ NIFTY Spot Price: {spot_price}")
                
                return {
                    'timestamp': datetime.now(),
                    'open': ohlc.get('open', spot_price),
                    'high': ohlc.get('high', spot_price),
                    'low': ohlc.get('low', spot_price),
                    'close': spot_price,
                    'volume': quote.get('volume', 0) if 'quote' in locals() else 0
                }
            else:
                logger.error("❌ Failed to fetch NIFTY spot price")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error fetching spot price: {e}")
            return None
    
    def fetch_and_store_option_chain(self):
        """Main function to fetch and store option chain data"""
        try:
            logger.info("🚀 Starting option chain data fetch...")
            
            # Create table if needed
            self.create_options_table()
            
            # 1. Get NIFTY spot price
            spot_data = self.fetch_spot_price()
            if not spot_data:
                logger.error("❌ Failed to fetch spot price")
                return False
            
            spot_price = spot_data['close']
            timestamp = spot_data['timestamp']
            
            # 2. Get current expiry
            expiry_date = self.get_current_expiry()
            logger.info(f"📅 Target Expiry: {expiry_date}")
            
            # 3. Get all option instruments
            options = self.get_nifty_options()
            if not options:
                logger.error("❌ No option instruments found")
                return False
            
            # Get available expiries
            available_expiries = sorted(set(opt['expiry'] for opt in options))
            logger.info(f"📅 Available expiries: {available_expiries[:5]}")
            
            # If current expiry not found, use the nearest available
            if expiry_date not in available_expiries and available_expiries:
                # For current day, if market is closed, use next expiry
                if datetime.now().time() > time(15, 30):
                    expiry_date = min([e for e in available_expiries if e > datetime.now().date()], 
                                    default=available_expiries[0])
                else:
                    expiry_date = min(available_expiries, key=lambda x: abs((x - expiry_date).days))
                logger.info(f"📅 Using available expiry: {expiry_date}")
            
            # 4. Determine strikes to fetch
            strikes_dict = self.get_option_strikes(spot_price, expiry_date)
            
            # 5. Fetch data for each strike
            option_data_list = []
            
            for option_type in ['CE', 'PE']:
                strikes = strikes_dict[option_type]
                
                for moneyness, strike in strikes.items():
                    logger.info(f"📈 Processing {option_type} {strike} ({moneyness})...")
                    
                    # Find instrument
                    instrument = self.find_option_instrument(strike, option_type, expiry_date, options)
                    
                    if not instrument:
                        logger.warning(f"⚠️ Instrument not found: {option_type} {strike}")
                        continue
                    
                    # Get quote data for current prices
                    try:
                        symbol = f"NFO:{instrument['tradingsymbol']}"
                        ltp_data = self.kite.ltp([symbol])
                        quote_data = self.kite.quote([symbol])
                        
                        if ltp_data and symbol in ltp_data:
                            last_price = ltp_data[symbol]['last_price']
                        else:
                            last_price = 0
                            
                        quote = quote_data.get(symbol, {}) if quote_data else {}
                        
                    except Exception as e:
                        logger.warning(f"Failed to get quote for {symbol}: {e}")
                        quote = {}
                        last_price = 0
                    
                    # If we have a valid price, calculate Greeks and store
                    if last_price > 0:
                        # Calculate days to expiry
                        days_to_expiry = (expiry_date - datetime.now().date()).days
                        
                        # Calculate Greeks
                        greeks = self.calculate_greeks_approximation(
                            last_price, spot_price, strike, option_type, days_to_expiry
                        )
                        
                        # Get OHLC from quote or use last price
                        ohlc = quote.get('ohlc', {})
                        open_price = ohlc.get('open', last_price)
                        high_price = ohlc.get('high', last_price)
                        low_price = ohlc.get('low', last_price)
                        close_price = ohlc.get('close', last_price)
                        
                        # Prepare record
                        option_record = {
                            'timestamp': timestamp,
                            'spot_price': spot_price,
                            'strike': strike,
                            'option_type': option_type,
                            'expiry_date': expiry_date,
                            'moneyness': moneyness,
                            'open': open_price,
                            'high': high_price,
                            'low': low_price,
                            'close': close_price,
                            'volume': quote.get('volume', 0),
                            'oi': quote.get('oi', 0),
                            'delta': greeks['delta'],
                            'gamma': greeks['gamma'],
                            'theta': greeks['theta'],
                            'vega': greeks['vega'],
                            'iv': greeks['iv'],
                            'bid_price': quote.get('depth', {}).get('buy', [{}])[0].get('price', None) if quote.get('depth') else None,
                            'ask_price': quote.get('depth', {}).get('sell', [{}])[0].get('price', None) if quote.get('depth') else None,
                            'bid_qty': quote.get('depth', {}).get('buy', [{}])[0].get('quantity', None) if quote.get('depth') else None,
                            'ask_qty': quote.get('depth', {}).get('sell', [{}])[0].get('quantity', None) if quote.get('depth') else None,
                            'last_price': last_price,
                            'change': quote.get('change', 0),
                            'change_pct': quote.get('change_percent', 0)
                        }
                        
                        option_data_list.append(option_record)
                        logger.info(f"   ✅ Price: {last_price:.2f}, Greeks calculated")
                    else:
                        logger.warning(f"   ⚠️ No valid price for {option_type} {strike}")
                    
                    # Small delay to avoid rate limits
                    time_module.sleep(0.1)
            
            # 6. Store all data in database
            if option_data_list:
                logger.info(f"💾 Storing {len(option_data_list)} option records...")
                
                # Convert to DataFrame
                df = pd.DataFrame(option_data_list)
                
                # Prepare for bulk insert
                columns = list(df.columns)
                values = df.values.tolist()
                
                insert_query = f"""
                    INSERT INTO nifty_options_5m ({','.join(columns)})
                    VALUES %s
                    ON CONFLICT (timestamp, strike, option_type, expiry_date)
                    DO UPDATE SET
                        spot_price = EXCLUDED.spot_price,
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume,
                        oi = EXCLUDED.oi,
                        delta = EXCLUDED.delta,
                        gamma = EXCLUDED.gamma,
                        theta = EXCLUDED.theta,
                        vega = EXCLUDED.vega,
                        iv = EXCLUDED.iv,
                        bid_price = EXCLUDED.bid_price,
                        ask_price = EXCLUDED.ask_price,
                        bid_qty = EXCLUDED.bid_qty,
                        ask_qty = EXCLUDED.ask_qty,
                        last_price = EXCLUDED.last_price,
                        change = EXCLUDED.change,
                        change_pct = EXCLUDED.change_pct
                """
                
                execute_values(self.db_handler.cursor, insert_query, values)
                self.db_handler.connection.commit()
                
                logger.info("✅ Option chain data stored successfully!")
                
                # Display summary
                self.display_summary(df, spot_price)
                
                return True
            else:
                logger.warning("⚠️ No option data to store")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error in fetch_and_store_option_chain: {e}")
            import traceback
            traceback.print_exc()
            self.db_handler.connection.rollback()
            return False
    
    def display_summary(self, df, spot_price):
        """Display summary of fetched option data"""
        print("\n" + "="*80)
        print(f"📊 OPTION CHAIN SUMMARY - SPOT: {spot_price:.2f}")
        print("="*80)
        
        # Group by option type
        for option_type in ['CE', 'PE']:
            type_data = df[df['option_type'] == option_type]
            
            if not type_data.empty:
                print(f"\n{option_type} Options:")
                print("-" * 60)
                print(f"{'Strike':<8} {'Moneyness':<10} {'Price':<10} {'IV':<8} {'Delta':<8} {'Theta':<8}")
                print("-" * 60)
                
                for _, row in type_data.iterrows():
                    print(f"{row['strike']:<8} {row['moneyness']:<10} "
                          f"{row['close']:<10.2f} {row['iv']:<8.1f} "
                          f"{row['delta']:<8.3f} {row['theta']:<8.2f}")
        
        print("\n" + "="*80)
    
    def run_continuous(self, interval_minutes=5):
        """Run continuously every N minutes during market hours"""
        logger.info(f"🔄 Starting continuous fetch every {interval_minutes} minutes...")
        
        while True:
            current_time = datetime.now()
            
            # Check if market hours (9:15 AM to 3:30 PM)
            if (current_time.weekday() < 5 and  # Monday to Friday
                time(9, 15) <= current_time.time() <= time(15, 30)):
                
                logger.info(f"⏰ Running fetch at {current_time.strftime('%H:%M:%S')}")
                self.fetch_and_store_option_chain()
                
            else:
                logger.info(f"📅 Market closed. Current time: {current_time.strftime('%H:%M:%S')}")
            
            # Wait for next interval
            time_module.sleep(interval_minutes * 60)

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='NIFTY Option Chain Data Fetcher')
    parser.add_argument('--continuous', action='store_true', 
                       help='Run continuously every 5 minutes during market hours')
    parser.add_argument('--interval', type=int, default=5,
                       help='Interval in minutes for continuous mode (default: 5)')
    
    args = parser.parse_args()
    
    try:
        # Initialize fetcher
        fetcher = OptionChainFetcher()
        
        if args.continuous:
            # Run continuously
            fetcher.run_continuous(interval_minutes=args.interval)
        else:
            # Run once
            success = fetcher.fetch_and_store_option_chain()
            if success:
                logger.info("✅ Option chain fetch completed successfully!")
            else:
                logger.error("❌ Option chain fetch failed!")
                
    except KeyboardInterrupt:
        logger.info("\n⏹️ Process interrupted by user")
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Clean up
        if 'fetcher' in locals() and hasattr(fetcher, 'db_handler'):
            fetcher.db_handler.disconnect()

if __name__ == "__main__":
    main()