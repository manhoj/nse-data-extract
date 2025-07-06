"""
NIFTY 50 Expiry Dates Fetcher
Fetches all available expiry dates for NIFTY 50 options

Requirements:
- kiteconnect
- pandas

Run with:
python nifty_expiry_fetcher.py
"""

import pandas as pd
from datetime import datetime, timedelta
from kiteconnect import KiteConnect
import calendar
from dotenv import load_dotenv
import os

# Import authentication module
try:
    from kite_authenticator import get_kite_token
except ImportError:
    print("❌ Error: kite_authenticator.py not found!")
    exit(1)

load_dotenv()

class NiftyExpiryFetcher:
    """
    Fetches and analyzes NIFTY 50 option expiry dates
    """
    
    def __init__(self):
        """Initialize the expiry fetcher"""
        print("🔐 Initializing NIFTY Expiry Fetcher...")
        self.access_token = get_kite_token(force_new=False)
        
        if not self.access_token:
            print("❌ Failed to get access token!")
            raise Exception("No valid access token available")
        
        # Initialize Kite API
        self.api_key = os.environ["KITE_API_KEY"]
        self.kite = KiteConnect(api_key=self.api_key)
        self.kite.set_access_token(self.access_token)
        
        print("✅ NIFTY Expiry Fetcher initialized successfully!")
    
    def get_all_nifty_expiries(self):
        """
        Get all available expiry dates for NIFTY 50 options
        
        Returns:
            dict: Categorized expiry dates (weekly, monthly, etc.)
        """
        try:
            print("📅 Fetching all NIFTY 50 expiry dates...")
            
            # Get all NFO instruments
            instruments = self.kite.instruments("NFO")
            
            # Filter for NIFTY options
            nifty_expiries = set()
            
            for instrument in instruments:
                if (instrument['name'] == 'NIFTY' and 
                    instrument['instrument_type'] in ['CE', 'PE'] and
                    instrument['segment'] == 'NFO-OPT'):
                    nifty_expiries.add(instrument['expiry'])
            
            # Convert to sorted list
            expiry_list = sorted(list(nifty_expiries))
            
            print(f"✅ Found {len(expiry_list)} unique expiry dates")
            
            # Categorize expiries
            categorized = self.categorize_expiries(expiry_list)
            
            return categorized
            
        except Exception as e:
            print(f"❌ Error fetching expiry dates: {e}")
            return None
    
    def categorize_expiries(self, expiry_list):
        """
        Categorize expiry dates into weekly and monthly
        
        Args:
            expiry_list (list): List of expiry dates
            
        Returns:
            dict: Categorized expiry dates
        """
        weekly_expiries = []
        monthly_expiries = []
        
        for expiry in expiry_list:
            # Check if it's the last Thursday of the month (monthly expiry)
            if self.is_monthly_expiry(expiry):
                monthly_expiries.append(expiry)
            else:
                weekly_expiries.append(expiry)
        
        return {
            'all': expiry_list,
            'weekly': weekly_expiries,
            'monthly': monthly_expiries,
            'upcoming': expiry_list[:5] if len(expiry_list) >= 5 else expiry_list
        }
    
    def is_monthly_expiry(self, expiry_date):
        """
        Check if the given date is a monthly expiry (last Thursday of month)
        
        Args:
            expiry_date (date): Expiry date to check
            
        Returns:
            bool: True if monthly expiry, False otherwise
        """
        # Get the last day of the month
        last_day = calendar.monthrange(expiry_date.year, expiry_date.month)[1]
        last_date = datetime(expiry_date.year, expiry_date.month, last_day).date()
        
        # Find the last Thursday
        while last_date.weekday() != 3:  # 3 = Thursday
            last_date -= timedelta(days=1)
        
        return expiry_date == last_date
    
    def get_current_and_next_expiry(self):
        """
        Get current and next expiry dates
        
        Returns:
            tuple: (current_expiry, next_expiry)
        """
        today = datetime.now().date()
        all_expiries = self.get_all_nifty_expiries()
        
        if not all_expiries:
            return None, None
        
        expiry_list = all_expiries['all']
        
        # Find current expiry (first expiry >= today)
        current_expiry = None
        next_expiry = None
        
        for i, expiry in enumerate(expiry_list):
            if expiry >= today:
                current_expiry = expiry
                if i + 1 < len(expiry_list):
                    next_expiry = expiry_list[i + 1]
                break
        
        return current_expiry, next_expiry
    
    def display_expiry_calendar(self, expiries_data):
        """
        Display expiry dates in a formatted calendar view
        
        Args:
            expiries_data (dict): Categorized expiry data
        """
        if not expiries_data:
            print("❌ No expiry data to display")
            return
        
        print("\n" + "="*60)
        print("📅 NIFTY 50 OPTION EXPIRY CALENDAR")
        print("="*60)
        
        # Display upcoming expiries
        print("\n🔜 UPCOMING EXPIRIES:")
        print("-"*40)
        for expiry in expiries_data['upcoming']:
            expiry_type = "Monthly" if expiry in expiries_data['monthly'] else "Weekly"
            days_to_expiry = (expiry - datetime.now().date()).days
            day_name = expiry.strftime('%A')
            
            if days_to_expiry == 0:
                status = "📍 TODAY!"
            elif days_to_expiry < 0:
                status = "✅ Expired"
            else:
                status = f"📆 {days_to_expiry} days"
            
            print(f"{expiry.strftime('%Y-%m-%d')} ({day_name}) - {expiry_type:<8} {status}")
        
        # Display monthly expiries
        print("\n📆 MONTHLY EXPIRIES:")
        print("-"*40)
        for expiry in expiries_data['monthly'][:6]:  # Show next 6 monthly expiries
            days_to_expiry = (expiry - datetime.now().date()).days
            if days_to_expiry >= 0:
                print(f"{expiry.strftime('%Y-%m-%d')} - {expiry.strftime('%B %Y')} ({days_to_expiry} days)")
        
        # Summary statistics
        print("\n📊 SUMMARY:")
        print("-"*40)
        print(f"Total Expiries Available: {len(expiries_data['all'])}")
        print(f"Weekly Expiries: {len(expiries_data['weekly'])}")
        print(f"Monthly Expiries: {len(expiries_data['monthly'])}")
        
        # Next expiry info
        current_expiry, next_expiry = self.get_current_and_next_expiry()
        if current_expiry:
            print(f"\n🎯 Current Expiry: {current_expiry.strftime('%Y-%m-%d (%A)')}")
            days_to_current = (current_expiry - datetime.now().date()).days
            print(f"   Days to expiry: {days_to_current}")
        
        if next_expiry:
            print(f"\n📍 Next Expiry: {next_expiry.strftime('%Y-%m-%d (%A)')}")
            days_to_next = (next_expiry - datetime.now().date()).days
            print(f"   Days to expiry: {days_to_next}")
    
    def get_strikes_for_expiry(self, expiry_date):
        """
        Get all available strikes for a specific expiry date
        
        Args:
            expiry_date (date): Expiry date
            
        Returns:
            list: Available strike prices
        """
        try:
            instruments = self.kite.instruments("NFO")
            strikes = set()
            
            for instrument in instruments:
                if (instrument['name'] == 'NIFTY' and 
                    instrument['instrument_type'] in ['CE', 'PE'] and
                    instrument['expiry'] == expiry_date):
                    strikes.add(instrument['strike'])
            
            return sorted(list(strikes))
            
        except Exception as e:
            print(f"❌ Error fetching strikes: {e}")
            return []
    
    def analyze_expiry_pattern(self):
        """
        Analyze expiry patterns and provide insights
        """
        expiries_data = self.get_all_nifty_expiries()
        
        if not expiries_data:
            print("❌ No expiry data available")
            return
        
        print("\n" + "="*60)
        print("📊 EXPIRY PATTERN ANALYSIS")
        print("="*60)
        
        # Analyze weekly expiries
        weekly_expiries = expiries_data['weekly']
        if weekly_expiries:
            print("\n📈 WEEKLY EXPIRY PATTERN:")
            print("-"*40)
            
            # Check day of week
            thursday_count = sum(1 for exp in weekly_expiries if exp.weekday() == 3)
            print(f"Thursday Expiries: {thursday_count}/{len(weekly_expiries)} ({thursday_count/len(weekly_expiries)*100:.1f}%)")
            
            # Check if there are non-Thursday expiries (holidays)
            non_thursday = [exp for exp in weekly_expiries if exp.weekday() != 3]
            if non_thursday:
                print(f"\n⚠️  Non-Thursday Expiries (Holiday adjustments):")
                for exp in non_thursday[:5]:  # Show first 5
                    print(f"   {exp.strftime('%Y-%m-%d (%A)')}")
        
        # Analyze monthly expiries
        monthly_expiries = expiries_data['monthly']
        if monthly_expiries:
            print("\n📈 MONTHLY EXPIRY PATTERN:")
            print("-"*40)
            
            # Group by month
            months_covered = set(exp.strftime('%Y-%m') for exp in monthly_expiries)
            print(f"Months with monthly expiries: {len(months_covered)}")
            
            # Check continuity
            print(f"Continuous monthly expiries available: {'Yes' if len(months_covered) >= 12 else 'No'}")

def main():
    """Main function"""
    print("🔷 NIFTY 50 EXPIRY DATES FETCHER")
    print("="*50)
    
    try:
        # Initialize fetcher
        fetcher = NiftyExpiryFetcher()
        
        while True:
            print("\n📋 OPTIONS:")
            print("1. 📅 View All Expiry Dates")
            print("2. 🎯 Get Current & Next Expiry")
            print("3. 📊 Analyze Expiry Patterns")
            print("4. 🔢 Get Strikes for Specific Expiry")
            print("5. 📆 Export Expiry Calendar")
            print("6. ❌ Exit")
            
            choice = input("\nEnter your choice (1-6): ").strip()
            
            if choice == "1":
                expiries_data = fetcher.get_all_nifty_expiries()
                if expiries_data:
                    fetcher.display_expiry_calendar(expiries_data)
                    
            elif choice == "2":
                current, next_exp = fetcher.get_current_and_next_expiry()
                
                print("\n🎯 CURRENT & NEXT EXPIRY")
                print("-"*40)
                
                if current:
                    days_to_current = (current - datetime.now().date()).days
                    print(f"Current Expiry: {current.strftime('%Y-%m-%d (%A)')}")
                    print(f"Days to expiry: {days_to_current}")
                    
                    # Get strikes for current expiry
                    strikes = fetcher.get_strikes_for_expiry(current)
                    if strikes:
                        print(f"Available strikes: {len(strikes)}")
                        print(f"Strike range: {min(strikes)} - {max(strikes)}")
                
                if next_exp:
                    days_to_next = (next_exp - datetime.now().date()).days
                    print(f"\nNext Expiry: {next_exp.strftime('%Y-%m-%d (%A)')}")
                    print(f"Days to expiry: {days_to_next}")
                    
            elif choice == "3":
                fetcher.analyze_expiry_pattern()
                
            elif choice == "4":
                expiries_data = fetcher.get_all_nifty_expiries()
                if expiries_data:
                    print("\n📅 Available Expiries:")
                    for i, exp in enumerate(expiries_data['upcoming'], 1):
                        print(f"{i}. {exp.strftime('%Y-%m-%d (%A)')}")
                    
                    exp_choice = input("\nSelect expiry number: ").strip()
                    try:
                        idx = int(exp_choice) - 1
                        if 0 <= idx < len(expiries_data['upcoming']):
                            selected_expiry = expiries_data['upcoming'][idx]
                            strikes = fetcher.get_strikes_for_expiry(selected_expiry)
                            
                            print(f"\n🔢 Strikes for {selected_expiry.strftime('%Y-%m-%d')}:")
                            print("-"*40)
                            print(f"Total strikes: {len(strikes)}")
                            
                            if strikes:
                                print(f"Range: {min(strikes)} - {max(strikes)}")
                                print(f"Gap: {strikes[1] - strikes[0] if len(strikes) > 1 else 'N/A'}")
                                
                                # Show sample strikes
                                print("\nSample strikes:")
                                sample_strikes = strikes[::max(1, len(strikes)//10)][:10]
                                print(", ".join(str(s) for s in sample_strikes))
                    except (ValueError, IndexError):
                        print("❌ Invalid selection")
                        
            elif choice == "5":
                expiries_data = fetcher.get_all_nifty_expiries()
                if expiries_data:
                    # Create DataFrame
                    df_data = []
                    for exp in expiries_data['all']:
                        exp_type = "Monthly" if exp in expiries_data['monthly'] else "Weekly"
                        days_to_exp = (exp - datetime.now().date()).days
                        
                        df_data.append({
                            'expiry_date': exp,
                            'day_name': exp.strftime('%A'),
                            'type': exp_type,
                            'days_to_expiry': days_to_exp,
                            'month': exp.strftime('%B %Y')
                        })
                    
                    df = pd.DataFrame(df_data)
                    
                    # Save to CSV
                    filename = f"nifty_expiry_calendar_{datetime.now().strftime('%Y%m%d')}.csv"
                    df.to_csv(filename, index=False)
                    print(f"✅ Expiry calendar exported to: {filename}")
                    
            elif choice == "6":
                print("👋 Exiting...")
                break
                
            else:
                print("❌ Invalid choice! Please try again.")
                
    except KeyboardInterrupt:
        print("\n\n⏹️ Process interrupted by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
