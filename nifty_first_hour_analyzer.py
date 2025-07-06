"""
NIFTY First Hour Movement Analyzer
Analyzes NIFTY's first hour movement (9:15 AM - 10:15 AM) and sends Telegram alerts

Requirements:
- kiteconnect
- pandas
- python-telegram-bot
- schedule (for automation)
- python-dotenv (for environment variables)

Install requirements:
pip install kiteconnect pandas python-telegram-bot schedule python-dotenv

Usage:
1. Update telegram_config.py with your bot token and chat ID (or use .env file)
2. Run manually at 10:15 AM or set up as a scheduled task
3. Can also run with scheduler: python nifty_first_hour_analyzer.py --scheduled
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

# Import authentication and telegram modules
try:
    from kite_authenticator import get_kite_token
    from telegram_config import send_telegram_message, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
except ImportError:
    print("❌ Error: Required modules not found!")
    print("Make sure you have:")
    print("  - kite_authenticator.py (from your existing setup)")
    print("  - telegram_config.py (create this with your Telegram bot details)")
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

class NiftyFirstHourAnalyzer:
    """
    Analyzes NIFTY's first hour movement and sends trading signals
    """
    
    def __init__(self):
        """Initialize the analyzer"""
        logger.info("🔐 Initializing NIFTY First Hour Analyzer...")
        
        # Get access token
        self.access_token = get_kite_token(force_new=False)
        
        if not self.access_token:
            logger.error("❌ Failed to get access token!")
            raise Exception("No valid access token available")
        
        # Initialize Kite API
        self.api_key = os.environ["KITE_API_KEY"]
        self.kite = KiteConnect(api_key=self.api_key)
        self.kite.set_access_token(self.access_token)
        
        # NIFTY 50 token
        self.nifty_token = 256265  # Standard NIFTY 50 token
        
        logger.info("✅ Analyzer initialized successfully!")
    
    def get_market_open_time(self, date):
        """Get market open time (9:15 AM IST)"""
        return date.replace(hour=9, minute=15, second=0, microsecond=0)
    
    def get_first_hour_end_time(self, date):
        """Get first hour end time (10:15 AM IST)"""
        return date.replace(hour=10, minute=15, second=0, microsecond=0)
    
    def ensure_timezone_naive(self, df):
        """Ensure DataFrame date column is timezone-naive"""
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            if df['date'].dt.tz is not None:
                df['date'] = df['date'].dt.tz_localize(None)
        return df
    
    def fetch_nifty_data(self, from_time, to_time, interval="minute"):
        """
        Fetch NIFTY data for specified time range
        
        Args:
            from_time (datetime): Start time
            to_time (datetime): End time
            interval (str): Data interval
            
        Returns:
            pd.DataFrame: NIFTY data
        """
        try:
            logger.info(f"📊 Fetching NIFTY data from {from_time} to {to_time}")
            
            historical_data = self.kite.historical_data(
                instrument_token=self.nifty_token,
                from_date=from_time,
                to_date=to_time,
                interval=interval
            )
            
            if historical_data:
                df = pd.DataFrame(historical_data)
                df = self.ensure_timezone_naive(df)
                logger.info(f"✅ Fetched {len(df)} records")
                return df
            else:
                logger.warning("⚠️ No data returned")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"❌ Error fetching data: {e}")
            return pd.DataFrame()
    
    def calculate_first_hour_movement(self):
        """
        Calculate NIFTY's movement in the first hour of trading
        
        Returns:
            dict: Movement details and trading signal
        """
        try:
            current_time = datetime.now()
            today = current_time.date()
            
            # Check if it's a weekday
            if current_time.weekday() > 4:  # Saturday = 5, Sunday = 6
                logger.warning("📅 Today is weekend. Markets are closed.")
                return None
            
            # Define time windows
            market_open = self.get_market_open_time(datetime.combine(today, time()))
            first_hour_end = self.get_first_hour_end_time(datetime.combine(today, time()))
            
            # Check if current time is appropriate (should be after 10:15 AM)
            if current_time < first_hour_end:
                logger.warning(f"⏰ Too early! Current time: {current_time.strftime('%H:%M')}. Wait until 10:15 AM.")
                return None
            
            # Fetch minute data for the first hour
            logger.info("📈 Analyzing first hour movement...")
            df_minute = self.fetch_nifty_data(
                from_time=market_open - timedelta(minutes=5),  # Buffer for data availability
                to_time=first_hour_end + timedelta(minutes=5),  # Buffer
                interval="minute"
            )
            
            if df_minute.empty:
                logger.error("❌ No minute data available")
                return None
            
            # Filter data for exact first hour window
            df_first_hour = df_minute[
                (df_minute['date'] >= market_open) & 
                (df_minute['date'] <= first_hour_end)
            ].copy()
            
            if df_first_hour.empty:
                logger.error("❌ No data for first hour window")
                return None
            
            # Get opening and current values
            open_price = df_first_hour.iloc[0]['open']  # First minute open
            close_price = df_first_hour.iloc[-1]['close']  # Last available close
            high_price = df_first_hour['high'].max()
            low_price = df_first_hour['low'].min()
            
            # Calculate movement
            price_movement = close_price - open_price
            price_movement_pct = (price_movement / open_price) * 100
            
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
                'open_price': open_price,
                'current_price': close_price,
                'high_price': high_price,
                'low_price': low_price,
                'price_movement': price_movement,
                'price_movement_pct': price_movement_pct,
                'signal': signal,
                'action': action,
                'emoji': emoji,
                'data_points': len(df_first_hour)
            }
            
            logger.info(f"✅ Analysis complete: {signal} | Movement: {price_movement:.2f} points ({price_movement_pct:.2f}%)")
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
        
        # Create message
        message = f"""
{analysis['emoji']} *NIFTY First Hour Analysis*
📅 {time_str}

*First Hour Movement (9:15 AM - 10:15 AM):*
• Open: {analysis['open_price']:.2f}
• Current: {analysis['current_price']:.2f}
• High: {analysis['high_price']:.2f}
• Low: {analysis['low_price']:.2f}

*Movement: {analysis['price_movement']:+.2f} points ({analysis['price_movement_pct']:+.2f}%)*

✅ *Suggested Action:*
{analysis['action']}

📊 _Data points analyzed: {analysis['data_points']}_

#NIFTY #FirstHour #TradingSignal
"""
        
        return message
    
    def run_analysis_and_notify(self):
        """Run the analysis and send Telegram notification"""
        logger.info("🚀 Starting NIFTY first hour analysis...")
        
        try:
            # Run analysis
            analysis = self.calculate_first_hour_movement()
            
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
                        f.write("timestamp,open,close,movement,movement_pct,signal,action\n")
                    f.write(f"{analysis['timestamp']},{analysis['open_price']},{analysis['current_price']},"
                           f"{analysis['price_movement']},{analysis['price_movement_pct']:.2f},"
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
        """Run the analysis on schedule (10:15 AM on weekdays)"""
        logger.info("📅 Scheduler started. Waiting for 10:15 AM on weekdays...")
        
        # Schedule the job
        schedule.every().monday.at("10:15").do(self.run_analysis_and_notify)
        schedule.every().tuesday.at("10:15").do(self.run_analysis_and_notify)
        schedule.every().wednesday.at("10:15").do(self.run_analysis_and_notify)
        schedule.every().thursday.at("10:15").do(self.run_analysis_and_notify)
        schedule.every().friday.at("10:15").do(self.run_analysis_and_notify)
        
        # Keep running
        while True:
            schedule.run_pending()
            time_module.sleep(30)  # Check every 30 seconds

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='NIFTY First Hour Movement Analyzer')
    parser.add_argument('--scheduled', action='store_true', 
                       help='Run on schedule (10:15 AM every weekday)')
    parser.add_argument('--test', action='store_true',
                       help='Run analysis immediately (for testing)')
    
    args = parser.parse_args()
    
    try:
        # Initialize analyzer
        analyzer = NiftyFirstHourAnalyzer()
        
        if args.scheduled:
            # Run on schedule
            analyzer.scheduled_run()
        else:
            # Run immediately
            analyzer.run_analysis_and_notify()
            
    except KeyboardInterrupt:
        logger.info("\n⏹️ Process interrupted by user")
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
