"""
NIFTY First Hour Movement Analyzer - Corrected Version
Uses the same data fetching logic as the Streamlit app for accurate results

Key changes:
1. Uses fetch_historical_data_chunked method
2. Applies same timezone handling
3. Uses same data processing pipeline
"""

import pandas as pd
from datetime import datetime, timedelta, time
from kiteconnect import KiteConnect
import logging
import sys
import argparse
import schedule
import time as time_module
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Import required modules
try:
    from kite_authenticator import get_kite_token
    from telegram_config import send_telegram_message
    from data_extractor import DataExtractor
except ImportError:
    print("❌ Error: Required modules not found!")
    print("Make sure you have:")
    print("  - kite_authenticator.py")
    print("  - telegram_config.py")
    print("  - data_extractor.py")
    exit(1)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('nifty_first_hour.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class NiftyFirstHourAnalyzer(DataExtractor):
    """
    Analyzes NIFTY's first hour movement using the same logic as data_extractor
    Inherits from DataExtractor to use the same data fetching methods
    """
    
    def __init__(self, debug=False):
        """Initialize the analyzer"""
        logger.info("🔐 Initializing NIFTY First Hour Analyzer...")
        
        self.debug = debug
        
        # Initialize parent class (DataExtractor)
        super().__init__()
        
        # Verify connection
        try:
            profile = self.kite.profile()
            logger.info(f"✅ Connected as: {profile.get('user_name', 'Unknown')}")
        except Exception as e:
            logger.error(f"❌ Connection verification failed: {e}")
            raise
    
    def calculate_first_hour_movement(self, target_date=None):
        """
        Calculate NIFTY's movement in the first hour of trading
        Using the same logic as streamlit_app extract_data function
        
        Args:
            target_date (datetime, optional): Specific date to analyze
        
        Returns:
            dict: Movement details and trading signal
        """
        try:
            if target_date:
                current_time = target_date
                today = target_date.date()
            else:
                current_time = datetime.now()
                today = current_time.date()
            
            # Check if it's a weekday
            if today.weekday() > 4:  # Saturday = 5, Sunday = 6
                logger.warning("📅 Target date is weekend. Markets are closed.")
                return None
            
            # For live analysis, check if current time is after 10:15 AM
            if not target_date and current_time.time() < time(10, 15):
                logger.warning(f"⏰ Too early! Current time: {current_time.strftime('%H:%M')}. Wait until 10:15 AM.")
                return None
            
            # Define time windows (same as market hours)
            market_open = datetime.combine(today, time(9, 15))
            first_hour_end = datetime.combine(today, time(10, 15))
            
            # Get NIFTY token
            nifty_token = self.get_nifty50_token()
            
            # Calculate date range with buffer (same as streamlit logic)
            # For intraday intervals, include buffer and current day
            end_date = first_hour_end + timedelta(minutes=5)  # Small buffer after 10:15
            start_date = market_open - timedelta(minutes=5)   # Small buffer before 9:15
            
            logger.info(f"📈 Fetching NIFTY data for first hour analysis...")
            logger.info(f"📅 Date range: {start_date} to {end_date}")
            
            # Use the same fetch method as streamlit app
            df = self.fetch_historical_data_chunked(
                instrument_token=nifty_token,
                from_date=start_date,
                to_date=end_date,
                interval="minute"
            )
            
            if df.empty:
                logger.error("❌ No data returned from API")
                return None
            
            # Process data (same as streamlit)
            df = self.process_data(df, "NIFTY 50")
            
            # Filter for exact first hour window
            # The data should already be timezone-naive from process_data
            df_first_hour = df[
                (df['date'] >= market_open) & 
                (df['date'] <= first_hour_end)
            ].copy()
            
            if df_first_hour.empty:
                logger.error("❌ No data for first hour window")
                return None
            
            # Sort by date to ensure correct order
            df_first_hour = df_first_hour.sort_values('date').reset_index(drop=True)
            
            if self.debug:
                logger.info(f"🔍 Debug Info - First Hour Data:")
                logger.info(f"   Total records: {len(df_first_hour)}")
                logger.info(f"   Time range: {df_first_hour['date'].min()} to {df_first_hour['date'].max()}")
                
                # Show first few and last few records
                logger.info("📊 First 3 records:")
                for idx in range(min(3, len(df_first_hour))):
                    row = df_first_hour.iloc[idx]
                    logger.info(f"   {row['date']} - O:{row['open']:.2f}, H:{row['high']:.2f}, L:{row['low']:.2f}, C:{row['close']:.2f}")
                
                logger.info("📊 Last 3 records:")
                for idx in range(max(0, len(df_first_hour)-3), len(df_first_hour)):
                    row = df_first_hour.iloc[idx]
                    logger.info(f"   {row['date']} - O:{row['open']:.2f}, H:{row['high']:.2f}, L:{row['low']:.2f}, C:{row['close']:.2f}")
            
            # Get values for analysis
            # First row's open price (9:15 open)
            open_price = df_first_hour.iloc[0]['open']
            open_time = df_first_hour.iloc[0]['date']
            
            # Last row's close price (10:15 close)
            close_price = df_first_hour.iloc[-1]['close']
            close_time = df_first_hour.iloc[-1]['date']
            
            # High and low for the entire first hour
            high_price = df_first_hour['high'].max()
            low_price = df_first_hour['low'].min()
            
            # Calculate movement
            price_movement = close_price - open_price
            price_movement_pct = (price_movement / open_price) * 100
            
            # Volume analysis
            total_volume = df_first_hour['volume'].sum()
            avg_volume = df_first_hour['volume'].mean()
            
            # Determine trading signal based on movement
            if price_movement > 50:
                signal = "CE_BUYING"
                action = "🟢 Look for CE buying setup"
                emoji = "📈"
            elif price_movement < -50:
                signal = "PE_BUYING"
                action = "🔴 Look for PE buying setup"
                emoji = "📉"
            elif -25 <= price_movement <= 25:
                signal = "AVOID"
                action = "❌ Avoid (likely sideways/range-bound)"
                emoji = "➡️"
            elif 25 < price_movement <= 50:
                signal = "CE_CONFIRMATION"
                action = "🟡 Trade only with other confirmation (Bullish bias)"
                emoji = "⚡"
            else:  # -50 < price_movement < -25
                signal = "PE_CONFIRMATION"
                action = "🟡 Trade only with other confirmation (Bearish bias)"
                emoji = "⚡"
            
            result = {
                'timestamp': current_time,
                'analysis_date': today,
                'open_time': open_time,
                'close_time': close_time,
                'open_price': open_price,
                'current_price': close_price,
                'high_price': high_price,
                'low_price': low_price,
                'price_movement': price_movement,
                'price_movement_pct': price_movement_pct,
                'total_volume': total_volume,
                'avg_volume': avg_volume,
                'signal': signal,
                'action': action,
                'emoji': emoji,
                'data_points': len(df_first_hour)
            }
            
            logger.info(f"✅ Analysis complete:")
            logger.info(f"   Signal: {signal}")
            logger.info(f"   Movement: {price_movement:.2f} points ({price_movement_pct:.2f}%)")
            logger.info(f"   Open: {open_price:.2f} at {open_time}")
            logger.info(f"   Close: {close_price:.2f} at {close_time}")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Error in analysis: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def format_telegram_message(self, analysis):
        """
        Format analysis results for Telegram
        
        Args:
            analysis (dict): Analysis results
            
        Returns:
            str: Formatted message
        """
        if not analysis:
            return "❌ Analysis failed. Check logs for details."
        
        # Format timestamp
        time_str = analysis['timestamp'].strftime('%d-%b-%Y %I:%M %p')
        analysis_date_str = analysis['analysis_date'].strftime('%d-%b-%Y')
        
        # Create message
        message = f"""
{analysis['emoji']} *NIFTY First Hour Analysis*
📅 {time_str}
📊 Analysis Date: {analysis_date_str}

*First Hour Movement (9:15 AM - 10:15 AM):*
• Open: ₹{analysis['open_price']:.2f} (at {analysis['open_time'].strftime('%H:%M:%S')})
• Close: ₹{analysis['current_price']:.2f} (at {analysis['close_time'].strftime('%H:%M:%S')})
• High: ₹{analysis['high_price']:.2f}
• Low: ₹{analysis['low_price']:.2f}

*Movement: {analysis['price_movement']:+.2f} points ({analysis['price_movement_pct']:+.2f}%)*

📊 *Volume Analysis:*
• Total Volume: {analysis['total_volume']:,}
• Avg Volume/min: {analysis['avg_volume']:,.0f}

✅ *Suggested Action:*
{analysis['action']}

📊 _Data points analyzed: {analysis['data_points']}_

#NIFTY #FirstHour #TradingSignal
"""
        
        return message
    
    def run_analysis_and_notify(self, target_date=None):
        """Run the analysis and send Telegram notification"""
        logger.info("🚀 Starting NIFTY first hour analysis...")
        
        try:
            # Run analysis
            analysis = self.calculate_first_hour_movement(target_date)
            
            if analysis:
                # Format message
                message = self.format_telegram_message(analysis)
                
                # Send to Telegram
                success = send_telegram_message(message, parse_mode='Markdown')
                
                if success:
                    logger.info("✅ Telegram notification sent successfully!")
                else:
                    logger.error("❌ Failed to send Telegram notification")
                
                # Log to file for record
                with open('nifty_first_hour_signals.csv', 'a') as f:
                    if f.tell() == 0:  # File is empty, write header
                        f.write("timestamp,analysis_date,open_time,close_time,open,close,high,low,movement,movement_pct,volume,signal,action\n")
                    f.write(f"{analysis['timestamp']},{analysis['analysis_date']},{analysis['open_time']},{analysis['close_time']},"
                           f"{analysis['open_price']},{analysis['current_price']},{analysis['high_price']},{analysis['low_price']},"
                           f"{analysis['price_movement']},{analysis['price_movement_pct']:.2f},{analysis['total_volume']},"
                           f"{analysis['signal']},{analysis['action'].replace(',', ';')}\n")
                
                return True
            else:
                logger.warning("⚠️ No analysis results to send")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error in run_analysis_and_notify: {e}")
            # Send error notification
            error_msg = f"❌ *NIFTY Analysis Error*\n\nError: {str(e)}\n\nCheck logs for details."
            send_telegram_message(error_msg, parse_mode='Markdown')
            return False
    
    def scheduled_run(self):
        """Run the analysis on schedule (10:16 AM on weekdays)"""
        logger.info("📅 Scheduler started. Waiting for 10:16 AM on weekdays...")
        
        # Schedule at 10:16 to ensure 10:15 candle is complete
        schedule.every().monday.at("10:16").do(self.run_analysis_and_notify)
        schedule.every().tuesday.at("10:16").do(self.run_analysis_and_notify)
        schedule.every().wednesday.at("10:16").do(self.run_analysis_and_notify)
        schedule.every().thursday.at("10:16").do(self.run_analysis_and_notify)
        schedule.every().friday.at("10:16").do(self.run_analysis_and_notify)
        
        # Keep running
        while True:
            schedule.run_pending()
            time_module.sleep(30)  # Check every 30 seconds

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='NIFTY First Hour Movement Analyzer')
    parser.add_argument('--scheduled', action='store_true', 
                       help='Run on schedule (10:16 AM every weekday)')
    parser.add_argument('--test', action='store_true',
                       help='Run analysis immediately (for testing)')
    parser.add_argument('--debug', action='store_true',
                       help='Enable debug mode with detailed logging')
    parser.add_argument('--date', type=str,
                       help='Analyze specific date (YYYY-MM-DD format)')
    
    args = parser.parse_args()
    
    try:
        # Initialize analyzer with debug mode if specified
        analyzer = NiftyFirstHourAnalyzer(debug=args.debug)
        
        # Parse target date if provided
        target_date = None
        if args.date:
            try:
                # Parse date and set time to after first hour for analysis
                parsed_date = datetime.strptime(args.date, '%Y-%m-%d')
                target_date = parsed_date.replace(hour=10, minute=30)
                logger.info(f"📅 Analyzing specific date: {args.date}")
            except ValueError:
                logger.error("❌ Invalid date format. Use YYYY-MM-DD")
                return
        
        if args.scheduled:
            # Run on schedule
            analyzer.scheduled_run()
        else:
            # Run immediately
            analyzer.run_analysis_and_notify(target_date)
            
    except KeyboardInterrupt:
        logger.info("\n⏹️ Process interrupted by user")
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()