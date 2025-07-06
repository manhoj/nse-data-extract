"""
Data Extractor Module - Cleaned Version
Main application for extracting stock/index data using Kite API

Requirements:
- kiteconnect
- pandas

Install requirements:
pip install kiteconnect pandas

Important Notes:
- Configured for Indian Stock Market (NSE/BSE)
- Market hours: 9:15 AM to 3:30 PM IST
- All datetime data is converted to timezone-naive for consistency
- Data after market close (3:30 PM) for the current day is automatically filtered
"""

import pandas as pd
from datetime import datetime, timedelta
from kiteconnect import KiteConnect
import os
import time
from dotenv import load_dotenv

# Import our authentication module
try:
    from kite_authenticator import get_kite_token
except ImportError:
    print("❌ Error: kite_authenticator.py not found!")
    print("Make sure both files are in the same directory")
    exit(1)

load_dotenv()

class DataExtractor:
    """
    Main Data Extraction Class
    
    Note on Timezone Handling:
    - Kite API returns timezone-aware datetime objects (UTC+05:30 for IST)
    - This class converts all datetimes to timezone-naive for consistent processing
    - Market hours are enforced as 9:15 AM to 3:30 PM IST
    - Data after market close for the current day is automatically filtered out
    """
    
    def __init__(self):
        """Initialize the data extractor"""
        # Get access token from authenticator - don't force new by default
        print("🔐 Initializing data extractor...")
        self.access_token = get_kite_token(force_new=False)
        
        if not self.access_token:
            print("❌ Failed to get access token!")
            print("Authentication required before data extraction")
            raise Exception("No valid access token available")
        
        # Initialize Kite API
        self.api_key = os.environ["KITE_API_KEY"]
        self.kite = KiteConnect(api_key=self.api_key)
        self.kite.set_access_token(self.access_token)
        
        # Create output directory
        self.output_dir = os.getenv("OUTPUT_DIR", os.path.join(os.getcwd(), "kite_data_output"))
        self._create_output_directory()
        
        print("✅ Data Extractor initialized successfully!")
        print(f"📁 Output directory: {os.path.abspath(self.output_dir)}")
    
    def _create_output_directory(self):
        """Create output directory if it doesn't exist"""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            print(f"📁 Created output directory: {self.output_dir}")
        else:
            print(f"📁 Using existing output directory: {self.output_dir}")
    
    def verify_connection(self):
        """Verify API connection"""
        try:
            profile = self.kite.profile()
            print(f"👤 Connected as: {profile.get('user_name', 'Unknown')}")
            print(f"📧 Email: {profile.get('email', 'N/A')}")
        except Exception as e:
            print(f"❌ Connection verification failed: {e}")
    
    def search_instrument(self, symbol, exchange="NSE"):
        """
        Search for instrument by symbol
        
        Args:
            symbol (str): Stock/Index symbol
            exchange (str): Exchange (NSE, BSE)
            
        Returns:
            dict: Instrument details or None
        """
        try:
            print(f"🔍 Searching for {symbol} on {exchange}...")
            instruments = self.kite.instruments(exchange)
            
            # Search for exact match first
            for instrument in instruments:
                if instrument['tradingsymbol'] == symbol:
                    print(f"✅ Found exact match: {instrument['name']} (Token: {instrument['instrument_token']})")
                    return instrument
            
            # Search for partial match
            matches = []
            for instrument in instruments:
                if symbol.lower() in instrument['tradingsymbol'].lower():
                    matches.append(instrument)
            
            if matches:
                print(f"📋 Found {len(matches)} partial matches:")
                for i, match in enumerate(matches[:5]):  # Show first 5 matches
                    print(f"  {i+1}. {match['tradingsymbol']} - {match['name']}")
                
                if len(matches) == 1:
                    return matches[0]
                else:
                    print(f"💡 Try exact symbol name for better results")
            else:
                print(f"❌ No matches found for {symbol}")
            
            return None
            
        except Exception as e:
            print(f"❌ Error searching instrument: {e}")
            return None
    
    def get_nifty50_token(self):
        """Get NIFTY 50 instrument token"""
        try:
            instruments = self.kite.instruments("INDICES")
            for instrument in instruments:
                if "NIFTY 50" in instrument['name'] or instrument['tradingsymbol'] == "NIFTY 50":
                    print(f"✅ Found NIFTY 50: Token = {instrument['instrument_token']}")
                    return instrument['instrument_token']
            
            # Fallback to standard token
            print("⚠️  Using standard NIFTY 50 token: 256265")
            return 256265
            
        except Exception as e:
            print(f"❌ Error finding NIFTY 50: {e}")
            return 256265
    
    def get_banknifty_token(self):
        """Get BANK NIFTY instrument token"""
        try:
            instruments = self.kite.instruments("INDICES")
            for instrument in instruments:
                if "NIFTY BANK" in instrument['name'] or "BANKNIFTY" in instrument['tradingsymbol']:
                    print(f"✅ Found BANK NIFTY: Token = {instrument['instrument_token']}")
                    return instrument['instrument_token']
            
            # Fallback to standard token
            print("⚠️  Using standard BANK NIFTY token: 260105")
            return 260105
            
        except Exception as e:
            print(f"❌ Error finding BANK NIFTY: {e}")
            return 260105
    
    def get_indiavix_token(self):
        """Get INDIA VIX instrument token"""
        try:
            # Check NSE first
            instruments = self.kite.instruments("NSE")
            for instrument in instruments:
                if "INDIAVIX" in instrument['tradingsymbol'] or "VIX" in instrument['tradingsymbol']:
                    print(f"✅ Found INDIA VIX: Token = {instrument['instrument_token']}")
                    return instrument['instrument_token']
            
            # Check INDICES
            instruments = self.kite.instruments("INDICES")
            for instrument in instruments:
                if "INDIAVIX" in instrument['name'] or "VIX" in instrument['name']:
                    print(f"✅ Found INDIA VIX: Token = {instrument['instrument_token']}")
                    return instrument['instrument_token']
            
            # Fallback to standard token
            print("⚠️  Using standard INDIA VIX token: 264969")
            return 264969
            
        except Exception as e:
            print(f"❌ Error finding INDIA VIX: {e}")
            return 264969
    
    def get_market_end_time(self, date):
        """
        Get market end time for a given date (3:30 PM IST)
        
        Args:
            date (datetime): Date to check
            
        Returns:
            datetime: Market end time for that date
        """
        # Set time to 3:30 PM (15:30 in 24-hour format)
        # This assumes the system time is in IST or properly configured
        market_close = date.replace(hour=15, minute=30, second=0, microsecond=0)
        return market_close
    
    def ensure_timezone_naive(self, df):
        """
        Ensure DataFrame date column is timezone-naive
        
        Args:
            df (pd.DataFrame): DataFrame with date column
            
        Returns:
            pd.DataFrame: DataFrame with timezone-naive dates
        """
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            if df['date'].dt.tz is not None:
                df['date'] = df['date'].dt.tz_localize(None)
        return df
    
    def fetch_historical_data_chunked(self, instrument_token, from_date, to_date, interval="day"):
        """
        Fetch historical data in chunks for large datasets
        
        Args:
            instrument_token (int): Instrument token
            from_date (datetime): Start date
            to_date (datetime): End date
            interval (str): Data interval
            
        Returns:
            pd.DataFrame: Combined historical data
        """
        # For intraday data, don't fetch beyond market close time for today
        current_time = datetime.now()
        if interval != "day" and to_date.date() >= current_time.date():
            # If end date is today or future, cap it at current time or market close
            market_close_today = self.get_market_end_time(current_time)
            if current_time.time() > market_close_today.time():
                # After market hours, set end time to market close
                to_date = market_close_today
            else:
                # During market hours, use current time
                to_date = current_time
            print(f"⏰ Adjusted end time for intraday data: {to_date}")
            print(f"📍 Note: Indian stock market closes at 3:30 PM IST")
        
        # Determine chunk size based on interval
        if interval in ['minute']:
            chunk_days = 5  # 5 days at a time for minute data
        elif interval in ['3minute', '5minute']:
            chunk_days = 60  # 60 days at a time for 5-minute data
        elif interval in ['10minute', '15minute']:
            chunk_days = 60
        elif interval in ['30minute']:
            chunk_days = 100
        elif interval in ['hour']:
            chunk_days = 365
        else:  # daily
            chunk_days = 2000
        
        total_days = (to_date - from_date).days
        
        if total_days <= chunk_days:
            # Single request
            return self.fetch_historical_data(instrument_token, from_date, to_date, interval)
        
        # Multiple chunks needed
        print(f"📦 Large dataset detected: {total_days} days")
        print(f"🔄 Will fetch in chunks of {chunk_days} days each")
        
        all_data = []
        current_start = from_date
        chunk_count = 0
        
        while current_start < to_date:
            chunk_count += 1
            current_end = min(current_start + timedelta(days=chunk_days), to_date)
            
            print(f"📥 Fetching chunk {chunk_count}: {current_start.date()} to {current_end.date()}")
            
            try:
                chunk_data = self.fetch_historical_data(instrument_token, current_start, current_end, interval)
                
                if not chunk_data.empty:
                    all_data.append(chunk_data)
                    print(f"✅ Chunk {chunk_count}: {len(chunk_data)} records")
                else:
                    print(f"⚠️  Chunk {chunk_count}: No data returned")
                
                # Rate limiting - wait between requests
                time.sleep(0.5)
                
            except Exception as e:
                print(f"❌ Error fetching chunk {chunk_count}: {e}")
                # Continue with next chunk
            
            current_start = current_end + timedelta(days=1)
        
        # Combine all chunks
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            # Remove duplicates that might occur at chunk boundaries
            combined_df = combined_df.drop_duplicates(subset=['date'])
            combined_df = combined_df.sort_values('date').reset_index(drop=True)
            
            # Filter out any data after market close for today
            if interval != "day":
                # Ensure timezone-naive for comparison
                combined_df = self.ensure_timezone_naive(combined_df)
                
                current_date = datetime.now().date()
                market_close_today = self.get_market_end_time(datetime.now())
                
                # Filter out future data or data after market close today
                initial_count = len(combined_df)
                combined_df = combined_df[
                    (combined_df['date'].dt.date < current_date) | 
                    ((combined_df['date'].dt.date == current_date) & (combined_df['date'] <= market_close_today))
                ]
                
                filtered_count = initial_count - len(combined_df)
                if filtered_count > 0:
                    print(f"⏰ Filtered out {filtered_count} records after market close (3:30 PM)")
            
            print(f"🎉 Successfully combined {len(all_data)} chunks into {len(combined_df)} total records")
            return combined_df
        else:
            print("❌ No data retrieved from any chunk")
            return pd.DataFrame()
    
    def fetch_historical_data(self, instrument_token, from_date, to_date, interval="day"):
        """
        Fetch historical data (single request)
        
        Args:
            instrument_token (int): Instrument token
            from_date (datetime): Start date
            to_date (datetime): End date
            interval (str): Data interval
            
        Returns:
            pd.DataFrame: Historical data
        """
        try:
            # For intraday data, don't fetch beyond market close time for today
            current_time = datetime.now()
            if interval != "day" and to_date.date() >= current_time.date():
                market_close_today = self.get_market_end_time(current_time)
                if current_time.time() > market_close_today.time():
                    # After market hours, set end time to market close
                    to_date = market_close_today
                else:
                    # During market hours, use current time
                    to_date = current_time
            
            print(f"🔗 API call: Token={instrument_token}, From={from_date.date()}, To={to_date.date()}, Interval={interval}")
            
            # Note about timezone handling
            if interval != "day":
                print(f"📍 Note: Fetching intraday data (market hours: 9:15 AM - 3:30 PM IST)")
            
            historical_data = self.kite.historical_data(
                instrument_token=instrument_token,
                from_date=from_date,
                to_date=to_date,
                interval=interval
            )
            
            if historical_data:
                df = pd.DataFrame(historical_data)
                print(f"✅ API returned {len(df)} records")
                print(f"📊 Columns: {list(df.columns)}")
                if len(df) > 0:
                    print(f"📅 Date range in data: {df['date'].min()} to {df['date'].max()}")
                
                # Additional filter to ensure no data after market close for today
                if interval != "day" and len(df) > 0:
                    # Ensure timezone-naive for comparison
                    df = self.ensure_timezone_naive(df)
                    
                    current_date = datetime.now().date()
                    market_close = self.get_market_end_time(datetime.now())
                    
                    initial_count = len(df)
                    df = df[
                        (df['date'].dt.date < current_date) | 
                        ((df['date'].dt.date == current_date) & (df['date'] <= market_close))
                    ]
                    
                    filtered_count = initial_count - len(df)
                    if filtered_count > 0:
                        print(f"⏰ Filtered out {filtered_count} records after market close (3:30 PM)")
                
                return df
            else:
                print("⚠️  API returned empty data")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"❌ API Error: {e}")
            print(f"🔍 Check if:")
            print(f"   - Instrument token {instrument_token} is valid")
            print(f"   - Date range {from_date.date()} to {to_date.date()} has trading days")
            print(f"   - Access token is still valid")
            return pd.DataFrame()
    
    def process_data(self, df, symbol_name="Stock"):
        """
        Process and enhance the raw data
        
        Args:
            df (pd.DataFrame): Raw historical data
            symbol_name (str): Name for display
            
        Returns:
            pd.DataFrame: Processed data
        """
        if df.empty:
            return df
        
        # Ensure timezone-naive dates for all comparisons
        df = self.ensure_timezone_naive(df)
        
        # Filter out any data after market close (3:30 PM) for today
        current_date = datetime.now().date()
        market_close_today = self.get_market_end_time(datetime.now())
        
        # Remove any data after market close for today
        initial_count = len(df)
        df = df[
            (df['date'].dt.date < current_date) | 
            ((df['date'].dt.date == current_date) & (df['date'] <= market_close_today))
        ]
        
        filtered_count = initial_count - len(df)
        if filtered_count > 0:
            print(f"⏰ Filtered out {filtered_count} records after market close time (3:30 PM)")
        
        # Add calculated fields
        df['price_change'] = df['close'] - df['open']
        df['price_change_pct'] = (df['price_change'] / df['open']) * 100
        df['high_low_range'] = df['high'] - df['low']
        df['range_pct'] = (df['high_low_range'] / df['open']) * 100
        
        # Add time-based fields
        df['day_of_week'] = df['date'].dt.day_name()
        df['month'] = df['date'].dt.month
        df['year'] = df['date'].dt.year
        
        # Round numerical columns
        numeric_cols = ['open', 'high', 'low', 'close', 'price_change', 'price_change_pct', 'high_low_range', 'range_pct']
        df[numeric_cols] = df[numeric_cols].round(2)
        
        # Sort by date
        df = df.sort_values('date').reset_index(drop=True)
        
        print(f"📊 {symbol_name} Data Summary:")
        print(f"   📈 Records: {len(df)}")
        print(f"   📅 Date Range: {df['date'].min().date()} to {df['date'].max().date()}")
        print(f"   💰 Price Range: ₹{df['low'].min()} - ₹{df['high'].max()}")
        print(f"   📊 Avg Volume: {df['volume'].mean():,.0f}")
        
        return df
    
    def save_data(self, df, filename, symbol_name="Data"):
        """Save data to CSV file in output directory"""
        
        # Check if DataFrame is empty
        if df.empty:
            print(f"❌ Cannot save {symbol_name}: DataFrame is empty!")
            return False
            
        print(f"💾 Saving {len(df)} records to CSV...")
        
        try:
            # Ensure timezone-naive dates before saving
            df = self.ensure_timezone_naive(df)
            
            # Ensure .csv extension
            csv_filename = filename.replace('.xlsx', '.csv')
            
            # Create full path in output directory
            output_path = os.path.join(self.output_dir, csv_filename)
            
            # Save to CSV
            df.to_csv(output_path, index=False)
            
            # Get file size
            file_size = os.path.getsize(output_path)
            
            print(f"✅ {symbol_name} data saved to: {output_path}")
            print(f"📁 File size: {file_size:,} bytes")
            print(f"📊 Records: {len(df)} | Columns: {len(df.columns)}")
            
            # Show summary stats
            if len(df) > 0:
                print(f"📈 Data Summary:")
                print(f"   📅 Date Range: {df['date'].min().strftime('%Y-%m-%d %H:%M:%S')} to {df['date'].max().strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"   💰 Price Range: ₹{df['low'].min():.2f} - ₹{df['high'].max():.2f}")
                print(f"   📊 Avg Volume: {df['volume'].mean():,.0f}")
            
            return True
            
        except Exception as e:
            print(f"❌ Error saving CSV file: {e}")
            return False
    
    def display_data_sample(self, df, symbol_name="Stock", num_rows=5):
        """Display sample of the data"""
        if df.empty:
            print("❌ No data to display")
            return
        
        print(f"\n📈 {symbol_name} - Sample Data (First {num_rows} rows)")
        print("=" * 100)
        
        # Ensure timezone-naive for display
        df = self.ensure_timezone_naive(df)
        
        # Select display columns
        display_cols = ['date', 'open', 'high', 'low', 'close', 'volume', 'price_change', 'price_change_pct']
        sample_df = df[display_cols].head(num_rows)
        
        for _, row in sample_df.iterrows():
            date_str = row['date'].strftime('%Y-%m-%d %H:%M:%S')
            change_emoji = "📈" if row['price_change'] >= 0 else "📉"
            print(f"{date_str} | O:{row['open']:8.2f} | H:{row['high']:8.2f} | L:{row['low']:8.2f} | "
                  f"C:{row['close']:8.2f} | Vol:{row['volume']:12,} | {change_emoji} {row['price_change']:+7.2f} ({row['price_change_pct']:+6.2f}%)")
        
        print("=" * 100)
    
    def get_time_frame_input(self):
        """
        Interactive time frame selection with validation
        
        Returns:
            tuple: (days, interval, description)
        """
        print("\n" + "="*60)
        print("⏰ TIME FRAME SELECTION")
        print("="*60)
        
        # Available intervals with descriptions
        intervals = {
            "1": {"interval": "minute", "name": "1 Minute", "desc": "Every minute data", "max_days": 60, "chunk_days": 5},
            "2": {"interval": "3minute", "name": "3 Minutes", "desc": "Every 3 minutes", "max_days": 200, "chunk_days": 15},
            "3": {"interval": "5minute", "name": "5 Minutes", "desc": "Every 5 minutes", "max_days": 2000, "chunk_days": 60},
            "4": {"interval": "10minute", "name": "10 Minutes", "desc": "Every 10 minutes", "max_days": 2000, "chunk_days": 60},
            "5": {"interval": "15minute", "name": "15 Minutes", "desc": "Every 15 minutes", "max_days": 2000, "chunk_days": 60},
            "6": {"interval": "30minute", "name": "30 Minutes", "desc": "Every 30 minutes", "max_days": 2000, "chunk_days": 100},
            "7": {"interval": "hour", "name": "1 Hour", "desc": "Hourly data", "max_days": 2000, "chunk_days": 365},
            "8": {"interval": "day", "name": "1 Day", "desc": "Daily OHLC data", "max_days": 2000, "chunk_days": 2000}
        }
        
        print("📊 AVAILABLE TIME INTERVALS:")
        print("-" * 80)
        for key, info in intervals.items():
            chunk_info = f"(Fetched in {info['chunk_days']}-day chunks)" if info['chunk_days'] < info['max_days'] else ""
            print(f"  {key}. {info['name']:<12} - {info['desc']:<25} Max: {info['max_days']} days {chunk_info}")
        print("-" * 80)
        print("💡 For large datasets, data will be automatically fetched in chunks and combined")
        print("⚡ Recommended: 5-minute interval for detailed intraday analysis")
        
        # Get interval selection
        while True:
            choice = input("\n🕐 Select time interval (1-8): ").strip()
            if choice in intervals:
                selected_interval = intervals[choice]
                break
            else:
                print("❌ Invalid choice! Please select 1-8")
        
        print(f"✅ Selected: {selected_interval['name']} ({selected_interval['desc']})")
        
        # Get duration based on selected interval
        print(f"\n📅 DURATION SELECTION")
        print(f"Maximum allowed days for {selected_interval['name']}: {selected_interval['max_days']}")
        
        # Suggest common durations based on interval
        if selected_interval['interval'] in ['minute', '3minute']:
            suggestions = [1, 3, 7, 15, 30, 60]
            default_days = 7
        elif selected_interval['interval'] in ['5minute', '10minute', '15minute']:
            suggestions = [7, 30, 60, 180, 365, 730]  # Including 2 years
            default_days = 30
        elif selected_interval['interval'] in ['30minute', 'hour']:
            suggestions = [30, 90, 180, 365, 730, 1000]
            default_days = 90
        else:  # daily
            suggestions = [30, 90, 180, 365, 730, 1000, 2000]
            default_days = 90
        
        print(f"💡 Suggested durations: {suggestions} (including 730 days = 2 years)")
        
        # Special highlight for 2 years
        if selected_interval['interval'] in ['5minute', '10minute', '15minute']:
            print(f"🎯 Popular choice: 730 days (2 years) for comprehensive backtesting")
        
        while True:
            days_input = input(f"\n📆 Enter number of days (default {default_days}): ").strip()
            
            if not days_input:
                days = default_days
                break
            
            try:
                days = int(days_input)
                if days <= 0:
                    print("❌ Days must be positive!")
                    continue
                elif days > selected_interval['max_days']:
                    print(f"❌ Maximum {selected_interval['max_days']} days allowed for {selected_interval['name']}")
                    continue
                else:
                    break
            except ValueError:
                print("❌ Please enter a valid number!")
        
        # Calculate estimated data points
        if selected_interval['interval'] == 'minute':
            estimated_points = days * 375  # ~375 minutes per trading day
        elif selected_interval['interval'] == '3minute':
            estimated_points = days * 125
        elif selected_interval['interval'] == '5minute':
            estimated_points = days * 75
        elif selected_interval['interval'] == '10minute':
            estimated_points = days * 37
        elif selected_interval['interval'] == '15minute':
            estimated_points = days * 25
        elif selected_interval['interval'] == '30minute':
            estimated_points = days * 12
        elif selected_interval['interval'] == 'hour':
            estimated_points = days * 6
        else:  # daily
            estimated_points = days
        
        description = f"{days} days of {selected_interval['name']} data (~{estimated_points:,} data points)"
        
        print(f"\n📋 SELECTION SUMMARY:")
        print(f"   ⏰ Interval: {selected_interval['name']} ({selected_interval['interval']})")
        print(f"   📅 Duration: {days} days")
        print(f"   📊 Estimated data points: ~{estimated_points:,}")
        
        return days, selected_interval['interval'], description
    
    def extract_stock_data(self, symbol, exchange="NSE", days=30, interval="day"):
        """
        Extract data for a specific stock
        
        Args:
            symbol (str): Stock symbol (e.g., "ADANIPORTS")
            exchange (str): Exchange (NSE, BSE)
            days (int): Number of days of data
            interval (str): Data interval
            
        Returns:
            pd.DataFrame: Stock data
        """
        print(f"\n🚀 EXTRACTING {symbol} DATA")
        print("=" * 50)
        
        # Find instrument
        instrument = self.search_instrument(symbol, exchange)
        if not instrument:
            print(f"❌ Could not find instrument for {symbol}")
            return pd.DataFrame()
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days + 10)  # Extra days for weekends/holidays
        
        print(f"📅 Date range: {start_date.date()} to {end_date.date()} ({(end_date-start_date).days} days)")
        
        # Fetch data using chunked method for large datasets
        df = self.fetch_historical_data_chunked(
            instrument_token=instrument['instrument_token'],
            from_date=start_date,
            to_date=end_date,
            interval=interval
        )
        
        if not df.empty:
            # Process data
            print(f"🔄 Processing {len(df)} raw records...")
            df = self.process_data(df, symbol)
            
            # Display sample
            self.display_data_sample(df, symbol)
            
            # Save data with validation
            filename = f"{symbol.lower()}_{days}days_{interval}.csv"
            if self.save_data(df, filename, symbol):
                print(f"✅ {symbol} data extraction completed successfully!")
            else:
                print(f"❌ Failed to save {symbol} data")
            
            return df
        else:
            print(f"❌ Failed to extract data for {symbol} - No data returned from API")
            print("🔍 Possible reasons:")
            print("   - Stock symbol not found")
            print("   - No trading data for the selected date range")
            print("   - API connection issues")
            print("   - Market holidays/weekends only in date range")
            return pd.DataFrame()
    
    def extract_nifty50_data(self, days=30, interval="day"):
        """
        Extract NIFTY 50 index data
        
        Args:
            days (int): Number of days of data
            interval (str): Data interval (day, 5minute, etc.)
            
        Returns:
            pd.DataFrame: NIFTY 50 data
        """
        print(f"\n🚀 EXTRACTING NIFTY 50 DATA")
        print("=" * 50)
        
        # Get NIFTY 50 token
        nifty_token = self.get_nifty50_token()
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days + 10)  # Extra days for weekends/holidays
        
        print(f"📅 Date range: {start_date.date()} to {end_date.date()} ({(end_date-start_date).days} days)")
        
        # Fetch data using chunked method for large datasets
        df = self.fetch_historical_data_chunked(
            instrument_token=nifty_token,
            from_date=start_date,
            to_date=end_date,
            interval=interval
        )
        
        if not df.empty:
            # Process data
            print(f"🔄 Processing {len(df)} raw records...")
            df = self.process_data(df, "NIFTY 50")
            
            # Display sample
            self.display_data_sample(df, "NIFTY 50")
            
            # Save data with validation
            filename = f"nifty50_{days}days_{interval}.csv"
            if self.save_data(df, filename, "NIFTY 50"):
                print(f"✅ NIFTY 50 data extraction completed successfully!")
            else:
                print(f"❌ Failed to save NIFTY 50 data")
            
            return df
        else:
            print(f"❌ Failed to extract NIFTY 50 data - No data returned from API")
            print("🔍 Possible reasons:")
            print("   - NIFTY 50 instrument token incorrect")
            print("   - No trading data for the selected date range")
            print("   - API connection issues")
            print("   - Market holidays/weekends only in date range")
            return pd.DataFrame()
    
    def test_simple_extraction(self):
        """
        Simple test function to debug data extraction
        """
        print("\n🧪 RUNNING SIMPLE TEST EXTRACTION")
        print("=" * 50)
        
        # Test with a simple stock for just 1 day
        print("🔍 Testing RELIANCE stock for 1 day of daily data...")
        
        try:
            # Find RELIANCE
            instrument = self.search_instrument("RELIANCE", "NSE")
            if not instrument:
                print("❌ Could not find RELIANCE")
                return
            
            # Simple date range - last 5 days to ensure we get at least 1 trading day
            end_date = datetime.now()
            start_date = end_date - timedelta(days=5)
            
            print(f"📅 Fetching from {start_date.date()} to {end_date.date()}")
            
            # Direct API call (no chunking)
            df = self.fetch_historical_data(
                instrument_token=instrument['instrument_token'],
                from_date=start_date,
                to_date=end_date,
                interval="day"
            )
            
            if not df.empty:
                print(f"✅ Test successful! Got {len(df)} records")
                print("📊 Sample data:")
                print(df.head().to_string())
                
                # Test saving to CSV
                test_filename = "test_reliance.csv"
                if self.save_data(df, test_filename, "RELIANCE_TEST"):
                    print(f"✅ CSV save test successful!")
                    print(f"🔍 Check the file: {os.path.join(self.output_dir, test_filename)}")
                else:
                    print(f"❌ CSV save test failed")
                    
                # Test with 5-minute data
                print("\n🔍 Testing 5-minute data extraction...")
                df_5min = self.fetch_historical_data(
                    instrument_token=instrument['instrument_token'],
                    from_date=end_date - timedelta(days=1),
                    to_date=end_date,
                    interval="5minute"
                )
                
                if not df_5min.empty:
                    print(f"✅ 5-minute test successful! Got {len(df_5min)} records")
                    
                    # Ensure timezone-naive for display and comparison
                    df_5min = self.ensure_timezone_naive(df_5min)
                    
                    print(f"📅 Latest timestamp: {df_5min['date'].max()}")
                    
                    # Check if any data is after market close
                    market_close = self.get_market_end_time(datetime.now())
                    after_close = df_5min[df_5min['date'] > market_close]
                    if len(after_close) > 0:
                        print(f"⚠️  Found {len(after_close)} records after market close!")
                    else:
                        print("✅ No data after market close (3:30 PM)")
                        
            else:
                print("❌ Test failed - no data returned")
                
        except Exception as e:
            print(f"❌ Test failed with error: {e}")
            import traceback
            traceback.print_exc()

def main():
    """
    Main function - Entry point of the application
    
    Note: This application is configured for Indian Stock Market (NSE/BSE)
    Market Hours: 9:15 AM to 3:30 PM IST
    All times are handled in IST (Indian Standard Time)
    """
    print("🚀 KITE DATA EXTRACTOR")
    print("=" * 50)
    print("📍 Market Hours: 9:15 AM to 3:30 PM IST")
    print("⏰ Current Time:", datetime.now().strftime('%Y-%m-%d %H:%M:%S IST'))
    
    try:
        # Initialize extractor
        extractor = DataExtractor()
        
        print("\n📋 EXTRACTION OPTIONS:")
        print("=" * 30)
        print("1. 📈 Extract specific stock data")
        print("2. 📊 Extract NIFTY 50 index data")
        print("3. 🔧 Batch extraction (multiple stocks)")
        print("4. 💼 Popular stocks quick extraction")
        print("5. 🧪 Test extraction (debug mode)")
        
        choice = input("\nEnter your choice (1-5): ").strip()
        
        if choice == "1":
            # Individual stock data extraction
            print("\n" + "="*50)
            print("📈 INDIVIDUAL STOCK DATA EXTRACTION")
            print("="*50)
            
            symbol = input("📝 Enter stock symbol (e.g., ADANIPORTS, RELIANCE, TCS): ").strip().upper()
            if not symbol:
                print("❌ Stock symbol cannot be empty!")
                return
            
            # Get time frame
            days, interval, description = extractor.get_time_frame_input()
            
            # Confirm extraction
            print(f"\n🔍 EXTRACTION PREVIEW:")
            print(f"   🏢 Stock: {symbol}")
            print(f"   ⏰ Time Frame: {description}")
            
            confirm = input(f"\n❓ Proceed with extraction? (y/n): ").strip().lower()
            if confirm != 'y':
                print("⏹️  Extraction cancelled")
                return
            
            df = extractor.extract_stock_data(symbol, days=days, interval=interval)
            
        elif choice == "2":
            # NIFTY 50 data extraction
            print("\n" + "="*50)
            print("📊 NIFTY 50 INDEX DATA EXTRACTION")
            print("="*50)
            
            # Get time frame
            days, interval, description = extractor.get_time_frame_input()
            
            # Confirm extraction
            print(f"\n🔍 EXTRACTION PREVIEW:")
            print(f"   📊 Index: NIFTY 50")
            print(f"   ⏰ Time Frame: {description}")
            
            confirm = input(f"\n❓ Proceed with extraction? (y/n): ").strip().lower()
            if confirm != 'y':
                print("⏹️  Extraction cancelled")
                return
            
            df = extractor.extract_nifty50_data(days=days, interval=interval)
            
        elif choice == "3":
            # Batch extraction
            print("\n" + "="*50)
            print("🔧 BATCH EXTRACTION (MULTIPLE STOCKS)")
            print("="*50)
            
            symbols_input = input("📝 Enter stock symbols separated by commas (e.g., ADANIPORTS,RELIANCE,TCS): ").strip().upper()
            if not symbols_input:
                print("❌ No symbols provided!")
                return
            
            symbols = [s.strip() for s in symbols_input.split(',')]
            print(f"📋 Stocks to extract: {symbols}")
            
            # Get time frame
            days, interval, description = extractor.get_time_frame_input()
            
            # Confirm extraction
            print(f"\n🔍 BATCH EXTRACTION PREVIEW:")
            print(f"   🏢 Stocks: {len(symbols)} stocks ({', '.join(symbols)})")
            print(f"   ⏰ Time Frame: {description}")
            
            confirm = input(f"\n❓ Proceed with batch extraction? (y/n): ").strip().lower()
            if confirm != 'y':
                print("⏹️  Extraction cancelled")
                return
            
            # Extract data for each stock
            for i, symbol in enumerate(symbols, 1):
                print(f"\n🔄 Extracting {i}/{len(symbols)}: {symbol}")
                df = extractor.extract_stock_data(symbol, days=days, interval=interval)
                if df.empty:
                    print(f"⚠️  Failed to extract data for {symbol}")
            
            print(f"\n✅ BATCH EXTRACTION COMPLETED!")
            
        elif choice == "4":
            # Popular stocks quick extraction
            print("\n" + "="*50)
            print("💼 POPULAR STOCKS QUICK EXTRACTION")
            print("="*50)
            
            popular_stocks = {
                "1": {"symbol": "RELIANCE", "name": "Reliance Industries"},
                "2": {"symbol": "TCS", "name": "Tata Consultancy Services"},
                "3": {"symbol": "HDFCBANK", "name": "HDFC Bank"},
                "4": {"symbol": "INFY", "name": "Infosys"},
                "5": {"symbol": "ADANIPORTS", "name": "Adani Ports"},
                "6": {"symbol": "SBIN", "name": "State Bank of India"},
                "7": {"symbol": "ITC", "name": "ITC Limited"},
                "8": {"symbol": "LT", "name": "Larsen & Toubro"}
            }
            
            print("📋 Popular Stocks:")
            for key, stock in popular_stocks.items():
                print(f"  {key}. {stock['symbol']:<12} - {stock['name']}")
            
            stock_choice = input("\n📈 Select a stock (1-8): ").strip()
            if stock_choice not in popular_stocks:
                print("❌ Invalid choice!")
                return
            
            selected_stock = popular_stocks[stock_choice]
            symbol = selected_stock['symbol']
            
            # Get time frame
            days, interval, description = extractor.get_time_frame_input()
            
            # Confirm extraction
            print(f"\n🔍 EXTRACTION PREVIEW:")
            print(f"   🏢 Stock: {symbol} ({selected_stock['name']})")
            print(f"   ⏰ Time Frame: {description}")
            
            confirm = input(f"\n❓ Proceed with extraction? (y/n): ").strip().lower()
            if confirm != 'y':
                print("⏹️  Extraction cancelled")
                return
            
            df = extractor.extract_stock_data(symbol, days=days, interval=interval)
            
        elif choice == "5":
            # Test extraction
            print("\n" + "="*50)
            print("🧪 DEBUG TEST EXTRACTION")
            print("="*50)
            print("This will test basic data extraction to help debug issues")
            
            confirm = input("❓ Run debug test? (y/n): ").strip().lower()
            if confirm == 'y':
                extractor.test_simple_extraction()
            else:
                print("⏹️  Test cancelled")
                return
            
        else:
            print("❌ Invalid choice! Please select 1-5")
            return
        
        if choice in ["1", "2", "4"] and 'df' in locals() and not df.empty:
            print(f"\n✅ DATA EXTRACTION COMPLETED SUCCESSFULLY!")
            print(f"📊 Total records extracted: {len(df)}")
            print(f"📄 Data saved to CSV file in: {os.path.abspath(extractor.output_dir)}")
        elif choice == "3":
            print(f"🎉 All extractions completed! Check individual CSV files in: {os.path.abspath(extractor.output_dir)}")
        elif choice == "5":
            print(f"🧪 Debug test completed! Check the output above for details.")
    
    except KeyboardInterrupt:
        print(f"\n\n⏹️  Process interrupted by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()