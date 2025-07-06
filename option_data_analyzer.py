"""
Option Data Analyzer
Provides analysis and querying capabilities for stored option chain data

Features:
- Query option data by various filters
- Analyze Greeks patterns
- Generate option chain snapshots
- Export data for further analysis
"""

import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
from tabulate import tabulate
import numpy as np

# Import database configuration
try:
    from database_config import DB_CONFIG
except ImportError:
    print("❌ Error: database_config.py not found!")
    exit(1)

class OptionDataAnalyzer:
    """Analyze and query option chain data"""
    
    def __init__(self):
        """Initialize analyzer"""
        self.connection = None
        self.connect_db()
    
    def connect_db(self):
        """Connect to PostgreSQL database"""
        try:
            self.connection = psycopg2.connect(
                host=DB_CONFIG['host'],
                port=DB_CONFIG['port'],
                database=DB_CONFIG['database'],
                user=DB_CONFIG['user'],
                password=DB_CONFIG['password']
            )
            print("✅ Connected to database")
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            raise
    
    def disconnect(self):
        """Disconnect from database"""
        if self.connection:
            self.connection.close()
            print("🔌 Disconnected from database")
    
    def get_latest_option_chain(self):
        """Get the latest option chain snapshot"""
        query = """
        SELECT DISTINCT ON (strike, option_type)
            timestamp, spot_price, strike, option_type, moneyness,
            close as price, volume, oi, 
            delta, gamma, theta, vega, iv,
            bid_price, ask_price, change_pct
        FROM nifty_options_5m
        WHERE timestamp = (SELECT MAX(timestamp) FROM nifty_options_5m)
        ORDER BY strike, option_type, timestamp DESC
        """
        
        df = pd.read_sql(query, self.connection)
        return df
    
    def get_option_history(self, strike, option_type, hours=24):
        """Get historical data for specific option"""
        query = """
        SELECT timestamp, spot_price, open, high, low, close, volume, oi,
               delta, gamma, theta, vega, iv
        FROM nifty_options_5m
        WHERE strike = %s 
        AND option_type = %s
        AND timestamp >= %s
        ORDER BY timestamp
        """
        
        start_time = datetime.now() - timedelta(hours=hours)
        df = pd.read_sql(query, self.connection, params=(strike, option_type, start_time))
        return df
    
    def get_greeks_summary(self):
        """Get summary of Greeks for all active options"""
        query = """
        WITH latest_data AS (
            SELECT DISTINCT ON (strike, option_type)
                *
            FROM nifty_options_5m
            WHERE timestamp = (SELECT MAX(timestamp) FROM nifty_options_5m)
            ORDER BY strike, option_type, timestamp DESC
        )
        SELECT 
            option_type,
            moneyness,
            strike,
            close as price,
            delta,
            gamma,
            theta,
            vega,
            iv,
            volume,
            oi
        FROM latest_data
        WHERE delta IS NOT NULL
        ORDER BY option_type, strike
        """
        
        df = pd.read_sql(query, self.connection)
        return df
    
    def get_high_theta_options(self, min_theta=-50):
        """Find options with high theta decay"""
        query = """
        WITH latest_data AS (
            SELECT DISTINCT ON (strike, option_type)
                *
            FROM nifty_options_5m
            WHERE timestamp = (SELECT MAX(timestamp) FROM nifty_options_5m)
            ORDER BY strike, option_type, timestamp DESC
        )
        SELECT 
            strike,
            option_type,
            moneyness,
            close as price,
            theta,
            delta,
            iv,
            volume,
            oi
        FROM latest_data
        WHERE theta < %s  -- Negative theta means decay
        ORDER BY theta
        """
        
        df = pd.read_sql(query, self.connection, params=(min_theta,))
        return df
    
    def get_iv_analysis(self):
        """Analyze implied volatility across strikes"""
        query = """
        WITH latest_data AS (
            SELECT DISTINCT ON (strike, option_type)
                *
            FROM nifty_options_5m
            WHERE timestamp = (SELECT MAX(timestamp) FROM nifty_options_5m)
            ORDER BY strike, option_type, timestamp DESC
        )
        SELECT 
            strike,
            MAX(CASE WHEN option_type = 'CE' THEN iv END) as ce_iv,
            MAX(CASE WHEN option_type = 'PE' THEN iv END) as pe_iv,
            MAX(CASE WHEN option_type = 'CE' THEN close END) as ce_price,
            MAX(CASE WHEN option_type = 'PE' THEN close END) as pe_price,
            MAX(spot_price) as spot_price
        FROM latest_data
        WHERE iv IS NOT NULL
        GROUP BY strike
        ORDER BY strike
        """
        
        df = pd.read_sql(query, self.connection)
        return df
    
    def display_option_chain(self):
        """Display formatted option chain"""
        df = self.get_latest_option_chain()
        
        if df.empty:
            print("❌ No option data available")
            return
        
        spot_price = df['spot_price'].iloc[0]
        timestamp = df['timestamp'].iloc[0]
        
        print(f"\n{'='*100}")
        print(f"📊 NIFTY OPTION CHAIN - SPOT: {spot_price:.2f}")
        print(f"⏰ Last Updated: {timestamp}")
        print(f"{'='*100}")
        
        # Separate CE and PE
        ce_df = df[df['option_type'] == 'CE'].set_index('strike')
        pe_df = df[df['option_type'] == 'PE'].set_index('strike')
        
        # Merge for display
        display_df = pd.DataFrame(index=sorted(df['strike'].unique()))
        
        # CE columns
        display_df['CE_Price'] = ce_df['price']
        display_df['CE_IV'] = ce_df['iv']
        display_df['CE_Delta'] = ce_df['delta']
        display_df['CE_Theta'] = ce_df['theta']
        display_df['CE_Volume'] = ce_df['volume']
        
        # PE columns
        display_df['PE_Price'] = pe_df['price']
        display_df['PE_IV'] = pe_df['iv']
        display_df['PE_Delta'] = pe_df['delta']
        display_df['PE_Theta'] = pe_df['theta']
        display_df['PE_Volume'] = pe_df['volume']
        
        # Format for display
        display_df = display_df.fillna('-')
        
        # Round numeric columns
        numeric_cols = ['CE_Price', 'CE_IV', 'CE_Delta', 'CE_Theta', 
                       'PE_Price', 'PE_IV', 'PE_Delta', 'PE_Theta']
        
        for col in numeric_cols:
            if col in display_df.columns:
                display_df[col] = display_df[col].apply(
                    lambda x: f"{x:.2f}" if isinstance(x, (int, float)) else x
                )
        
        print(tabulate(display_df, headers=display_df.columns, tablefmt='grid'))
    
    def plot_iv_smile(self):
        """Plot IV smile curve"""
        df = self.get_iv_analysis()
        
        if df.empty:
            print("❌ No IV data available for plotting")
            return
        
        plt.figure(figsize=(12, 6))
        
        # Plot CE and PE IV
        plt.plot(df['strike'], df['ce_iv'], 'b-o', label='CE IV', markersize=8)
        plt.plot(df['strike'], df['pe_iv'], 'r-o', label='PE IV', markersize=8)
        
        # Add spot price line
        spot_price = df['spot_price'].iloc[0]
        plt.axvline(x=spot_price, color='green', linestyle='--', label=f'Spot: {spot_price:.0f}')
        
        plt.xlabel('Strike Price')
        plt.ylabel('Implied Volatility (%)')
        plt.title('NIFTY Option IV Smile')
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.show()
    
    def export_to_csv(self, filename=None):
        """Export latest option chain to CSV"""
        df = self.get_latest_option_chain()
        
        if df.empty:
            print("❌ No data to export")
            return
        
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"nifty_option_chain_{timestamp}.csv"
        
        df.to_csv(filename, index=False)
        print(f"✅ Data exported to {filename}")
    
    def get_option_flow_summary(self, hours=1):
        """Get summary of option flow in last N hours"""
        query = """
        SELECT 
            option_type,
            SUM(CASE WHEN moneyness LIKE 'ITM%%' THEN volume ELSE 0 END) as itm_volume,
            SUM(CASE WHEN moneyness = 'ATM' THEN volume ELSE 0 END) as atm_volume,
            SUM(CASE WHEN moneyness LIKE 'OTM%%' THEN volume ELSE 0 END) as otm_volume,
            SUM(volume) as total_volume,
            AVG(iv) as avg_iv,
            COUNT(DISTINCT strike) as active_strikes
        FROM nifty_options_5m
        WHERE timestamp >= %s
        GROUP BY option_type
        """
        
        start_time = datetime.now() - timedelta(hours=hours)
        df = pd.read_sql(query, self.connection, params=(start_time,))
        
        print(f"\n📊 Option Flow Summary (Last {hours} hour(s))")
        print("="*60)
        print(tabulate(df, headers=df.columns, tablefmt='grid', showindex=False))

def main():
    """Main function with menu"""
    analyzer = OptionDataAnalyzer()
    
    while True:
        print("\n" + "="*50)
        print("📊 NIFTY OPTION DATA ANALYZER")
        print("="*50)
        print("1. Display Current Option Chain")
        print("2. Show Greeks Summary")
        print("3. Find High Theta Decay Options")
        print("4. Plot IV Smile")
        print("5. Export Option Chain to CSV")
        print("6. Show Option Flow Summary")
        print("7. Exit")
        
        choice = input("\nSelect option (1-7): ").strip()
        
        try:
            if choice == '1':
                analyzer.display_option_chain()
            
            elif choice == '2':
                df = analyzer.get_greeks_summary()
                if not df.empty:
                    print("\n📊 Greeks Summary")
                    print(tabulate(df, headers=df.columns, tablefmt='grid', showindex=False))
                else:
                    print("❌ No Greeks data available")
            
            elif choice == '3':
                df = analyzer.get_high_theta_options()
                if not df.empty:
                    print("\n⏰ High Theta Decay Options")
                    print(tabulate(df, headers=df.columns, tablefmt='grid', showindex=False))
                else:
                    print("❌ No high theta options found")
            
            elif choice == '4':
                analyzer.plot_iv_smile()
            
            elif choice == '5':
                analyzer.export_to_csv()
            
            elif choice == '6':
                hours = int(input("Enter number of hours to analyze (default 1): ") or "1")
                analyzer.get_option_flow_summary(hours)
            
            elif choice == '7':
                print("👋 Exiting...")
                break
            
            else:
                print("❌ Invalid choice")
                
        except Exception as e:
            print(f"❌ Error: {e}")
            import traceback
            traceback.print_exc()
    
    analyzer.disconnect()

if __name__ == "__main__":
    main()
