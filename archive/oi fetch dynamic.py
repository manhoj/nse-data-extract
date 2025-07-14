"""
Dynamic NIFTY Options Fetcher
Fetches option data for strikes from -1000 to +1000 of current spot price

Usage:
    python dynamic_options_fetcher.py [--range RANGE] [--interval INTERVAL]
"""

import os
from kiteconnect import KiteConnect
from datetime import datetime, timedelta
import pandas as pd
import argparse
from typing import Dict, List, Tuple
import logging

# Import authentication
from kite_authenticator import get_kite_token

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DynamicOptionsDataFetcher:
    """Fetches NIFTY options data dynamically based on spot price"""
    
    def __init__(self):
        """Initialize the fetcher"""
        # Get access token
        self.access_token = get_kite_token(force_new=False)
        
        if not self.access_token:
            logger.error("❌ Failed to get access token!")
            raise Exception("No valid access token available")
        
        # Initialize Kite API
        self.api_key = "qkd6rimabtakrvea"
        self.kite = KiteConnect(api_key=self.api_key)
        self.kite.set_access_token(self.access_token)
        
        logger.info("✅ Dynamic Options Fetcher initialized successfully!")
    
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
    
    def get_current_expiry(self) -> Tuple[datetime, str]:
        """Get current month expiry date and format"""
        today = datetime.now()
        
        # Get last Thursday of current month
        year = today.year
        month = today.month
        
        # Find last Thursday
        if month == 12:
            next_month = datetime(year + 1, 1, 1)
        else:
            next_month = datetime(year, month + 1, 1)
        
        last_day = next_month - timedelta(days=1)
        
        while last_day.weekday() != 3:  # Thursday is 3
            last_day -= timedelta(days=1)
        
        # If expiry has passed, get next month's expiry
        if today.date() > last_day.date():
            if month == 12:
                month = 1
                year += 1
            else:
                month += 1
            
            # Recalculate for next month
            if month == 12:
                next_month = datetime(year + 1, 1, 1)
            else:
                next_month = datetime(year, month + 1, 1)
            
            last_day = next_month - timedelta(days=1)
            while last_day.weekday() != 3:
                last_day -= timedelta(days=1)
        
        # Format: 24JAN, 24FEB, etc.
        expiry_format = last_day.strftime("%y%b").upper()
        
        return last_day, expiry_format
    
    def generate_strike_range(self, spot_price: float, range_value: int = 1000, interval: int = 50) -> List[int]:
        """
        Generate strike prices based on spot price
        
        Args:
            spot_price: Current NIFTY spot price
            range_value: Range from spot price (default: 1000)
            interval: Strike interval (default: 50)
        
        Returns:
            List of strike prices
        """
        # Calculate start and end strikes
        start_strike = int((spot_price - range_value) // interval) * interval
        end_strike = int((spot_price + range_value) // interval + 1) * interval
        
        # Generate strikes
        strikes = list(range(start_strike, end_strike + interval, interval))
        
        logger.info(f"📊 Generated {len(strikes)} strikes from {start_strike} to {end_strike}")
        return strikes
    
    def fetch_options_data(self, strikes: List[int], expiry_format: str) -> pd.DataFrame:
        """
        Fetch options data for given strikes
        
        Args:
            strikes: List of strike prices
            expiry_format: Expiry format (e.g., "24JAN")
        
        Returns:
            DataFrame with options data
        """
        all_data = []
        timestamp = datetime.now()
        
        # Build option symbols
        option_symbols = []
        for strike in strikes:
            for opt_type in ["CE", "PE"]:
                symbol = f"NIFTY{expiry_format}{strike}{opt_type}"
                option_symbols.append(symbol)
        
        # Fetch data in batches (Kite API has limits)
        batch_size = 200  # Kite allows max 500 symbols per request
        
        for i in range(0, len(option_symbols), batch_size):
            batch_symbols = option_symbols[i:i + batch_size]
            full_symbols = [f"NFO:{sym}" for sym in batch_symbols]
            
            try:
                logger.info(f"📥 Fetching batch {i//batch_size + 1}/{(len(option_symbols)-1)//batch_size + 1}")
                quote_data = self.kite.quote(full_symbols)
                
                # Process each symbol
                for sym in batch_symbols:
                    full_sym = f"NFO:{sym}"
                    data = quote_data.get(full_sym, {})
                    
                    if data and "last_price" in data:
                        ohlc = data.get("ohlc", {})
                        
                        row = {
                            "timestamp": timestamp,
                            "symbol": sym,
                            "expiry": expiry_format,
                            "ltp": data.get("last_price", 0),
                            "open": ohlc.get("open", 0),
                            "high": ohlc.get("high", 0),
                            "low": ohlc.get("low", 0),
                            "prev_close": ohlc.get("close", 0),
                            "oi": data.get("oi", 0),
                            "oi_day_high": data.get("oi_day_high", 0),
                            "oi_day_low": data.get("oi_day_low", 0),
                            "volume": data.get("volume", 0)
                        }
                        all_data.append(row)
                    else:
                        logger.warning(f"⚠️ No data for {sym}")
                
            except Exception as e:
                logger.error(f"❌ Error fetching batch: {e}")
                continue
        
        # Create DataFrame
        df = pd.DataFrame(all_data)
        logger.info(f"✅ Fetched data for {len(df)} options")
        
        return df
    
    def display_summary(self, df: pd.DataFrame, spot_price: float):
        """Display summary of fetched data"""
        if df.empty:
            logger.warning("No data to display")
            return
        
        print("\n" + "="*80)
        print(f"📊 NIFTY OPTIONS DATA SUMMARY")
        print(f"🎯 Spot Price: {spot_price}")
        print(f"📅 Timestamp: {df['timestamp'].iloc[0]}")
        print(f"📆 Expiry: {df['expiry'].iloc[0]}")
        print("="*80)
        
        # Separate CE and PE
        ce_df = df[df['symbol'].str.endswith('CE')].copy()
        pe_df = df[df['symbol'].str.endswith('PE')].copy()
        
        # Extract strike prices
        ce_df['strike'] = ce_df['symbol'].str.extract(r'(\d+)CE').astype(int)
        pe_df['strike'] = pe_df['symbol'].str.extract(r'(\d+)PE').astype(int)
        
        # Find ATM strike
        atm_strike = int(round(spot_price / 50) * 50)
        
        print(f"\n🎯 ATM Strike: {atm_strike}")
        
        # Display ATM and nearby strikes
        nearby_strikes = [atm_strike - 100, atm_strike - 50, atm_strike, atm_strike + 50, atm_strike + 100]
        
        print("\n📊 OPTION CHAIN (ATM ± 2 strikes):")
        print("-"*80)
        print(f"{'Strike':>8} | {'CE LTP':>8} | {'CE OI':>12} | {'PE LTP':>8} | {'PE OI':>12}")
        print("-"*80)
        
        for strike in nearby_strikes:
            ce_row = ce_df[ce_df['strike'] == strike]
            pe_row = pe_df[pe_df['strike'] == strike]
            
            ce_ltp = ce_row['ltp'].iloc[0] if not ce_row.empty else 0
            ce_oi = ce_row['oi'].iloc[0] if not ce_row.empty else 0
            pe_ltp = pe_row['ltp'].iloc[0] if not pe_row.empty else 0
            pe_oi = pe_row['oi'].iloc[0] if not pe_row.empty else 0
            
            marker = " <<<" if strike == atm_strike else ""
            print(f"{strike:>8} | {ce_ltp:>8.2f} | {ce_oi:>12,} | {pe_ltp:>8.2f} | {pe_oi:>12,}{marker}")
        
        # Overall statistics
        print("\n📈 OVERALL STATISTICS:")
        print(f"Total Options: {len(df)}")
        print(f"CE Options: {len(ce_df)}")
        print(f"PE Options: {len(pe_df)}")
        print(f"Total CE OI: {ce_df['oi'].sum():,}")
        print(f"Total PE OI: {pe_df['oi'].sum():,}")
        print(f"PCR (OI): {pe_df['oi'].sum() / ce_df['oi'].sum():.2f}")
        
        # Find max OI strikes
        max_ce_oi_strike = ce_df.loc[ce_df['oi'].idxmax(), 'strike'] if not ce_df.empty else 0
        max_pe_oi_strike = pe_df.loc[pe_df['oi'].idxmax(), 'strike'] if not pe_df.empty else 0
        
        print(f"\n🎯 KEY LEVELS:")
        print(f"Max CE OI Strike (Resistance): {max_ce_oi_strike}")
        print(f"Max PE OI Strike (Support): {max_pe_oi_strike}")
    
    def save_to_csv(self, df: pd.DataFrame, filename: str = None):
        """Save data to CSV file"""
        if filename is None:
            filename = f"nifty_options_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        df.to_csv(filename, index=False)
        logger.info(f"💾 Data saved to {filename}")
        return filename

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Dynamic NIFTY Options Data Fetcher')
    parser.add_argument('--range', type=int, default=1000,
                       help='Range from spot price (default: 1000)')
    parser.add_argument('--interval', type=int, default=50,
                       help='Strike interval (default: 50)')
    parser.add_argument('--save', action='store_true',
                       help='Save data to CSV file')
    
    args = parser.parse_args()
    
    try:
        # Initialize fetcher
        fetcher = DynamicOptionsDataFetcher()
        
        # Get spot price
        spot_price = fetcher.get_nifty_spot_price()
        
        # Get current expiry
        expiry_date, expiry_format = fetcher.get_current_expiry()
        logger.info(f"📅 Current expiry: {expiry_date.date()} ({expiry_format})")
        
        # Generate strike range
        strikes = fetcher.generate_strike_range(spot_price, args.range, args.interval)
        
        # Fetch options data
        logger.info("📥 Fetching options data...")
        df = fetcher.fetch_options_data(strikes, expiry_format)
        
        # Display summary
        fetcher.display_summary(df, spot_price)
        
        # Save if requested
        if args.save:
            filename = fetcher.save_to_csv(df)
            print(f"\n✅ Data saved to: {filename}")
        
    except KeyboardInterrupt:
        logger.info("\n⏹️ Process interrupted by user")
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()