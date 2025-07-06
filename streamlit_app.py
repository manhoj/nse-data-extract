"""
Streamlit Web App for Kite Data Extractor
Web interface for extracting stock/index data using Kite API

Requirements:
pip install streamlit kiteconnect pandas plotly

Run with:
streamlit run streamlit_app.py
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import time
import os
import plotly.graph_objects as go
import plotly.express as px
from dotenv import load_dotenv

# Import our existing modules
try:
    from kite_authenticator import get_kite_token
    from data_extractor import DataExtractor as BaseDataExtractor
    from kiteconnect import KiteConnect
    from database_handler import push_data_to_db, DatabaseHandler
    from database_config import get_table_name, DB_CONFIG
except ImportError as e:
    st.error(f"❌ Required files not found! {str(e)}")
    st.stop()

# Page configuration
st.set_page_config(
    page_title="Kite Data Extractor",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    /* Main header styling */
    .main-header {
        font-size: 3.5rem;
        font-weight: 800;
        text-align: center;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 2rem;
        padding: 1rem;
    }
    
    /* Card-like containers */
    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 10px;
    }
    
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        padding-left: 24px;
        padding-right: 24px;
        background-color: white;
        border-radius: 8px;
        border: 2px solid #e0e0e0;
        font-weight: 600;
        transition: all 0.3s ease;
    }
    
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
    }
    
    /* Metric cards */
    div[data-testid="metric-container"] {
        background-color: #ffffff;
        border: 1px solid #f0f0f0;
        padding: 1rem;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        transition: transform 0.2s;
    }
    
    div[data-testid="metric-container"]:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.1);
    }
    
    /* Buttons styling */
    .stButton > button {
        border-radius: 8px;
        font-weight: 600;
        transition: all 0.3s ease;
        border: none;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.15);
    }
    
    /* Radio buttons */
    .stRadio > div {
        background-color: #f8f9fa;
        padding: 0.5rem;
        border-radius: 8px;
    }
    
    /* Select boxes */
    .stSelectbox > div > div {
        border-radius: 8px;
        border: 2px solid #e0e0e0;
    }
    
    /* Success/Error/Info boxes */
    .success-box {
        padding: 1.2rem;
        border-radius: 10px;
        background: linear-gradient(135deg, #84fab0 0%, #8fd3f4 100%);
        border: none;
        color: #1a5e3a;
        font-weight: 600;
    }
    
    .error-box {
        padding: 1.2rem;
        border-radius: 10px;
        background: linear-gradient(135deg, #fa709a 0%, #fee140 100%);
        border: none;
        color: #7a1a2e;
        font-weight: 600;
    }
    
    .info-box {
        padding: 1.2rem;
        border-radius: 10px;
        background: linear-gradient(135deg, #a8edea 0%, #fed6e3 100%);
        border: none;
        color: #2c5f7c;
        font-weight: 600;
    }
    
    /* Progress bar */
    .stProgress > div > div > div {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
    }
    
    /* Sidebar styling */
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #f8f9fa 0%, #e9ecef 100%);
    }
    
    /* Expander styling */
    .streamlit-expanderHeader {
        background-color: #f8f9fa;
        border-radius: 8px;
        font-weight: 600;
    }
    
    /* Input fields */
    .stTextInput > div > div {
        border-radius: 8px;
        border: 2px solid #e0e0e0;
    }
    
    .stNumberInput > div > div {
        border-radius: 8px;
        border: 2px solid #e0e0e0;
    }
    
    /* Data table styling */
    .dataframe {
        border: none !important;
        border-radius: 10px;
        overflow: hidden;
    }
    
    /* Download button special styling */
    .stDownloadButton > button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
    }
    
    .stDownloadButton > button:hover {
        background: linear-gradient(135deg, #764ba2 0%, #667eea 100%);
    }
    
    /* Divider styling */
    hr {
        margin: 2rem 0;
        border: none;
        height: 2px;
        background: linear-gradient(90deg, transparent, #e0e0e0, transparent);
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state with proper defaults
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False
if 'extractor' not in st.session_state:
    st.session_state.extractor = None
if 'extraction_complete' not in st.session_state:
    st.session_state.extraction_complete = False
if 'last_extracted_data' not in st.session_state:
    st.session_state.last_extracted_data = None
if 'last_extracted_symbol' not in st.session_state:
    st.session_state.last_extracted_symbol = None
if 'last_extracted_filename' not in st.session_state:
    st.session_state.last_extracted_filename = None
if 'nse_stocks' not in st.session_state:
    st.session_state.nse_stocks = None
if 'nse_indices' not in st.session_state:
    st.session_state.nse_indices = None
if 'auth_error_count' not in st.session_state:
    st.session_state.auth_error_count = 0
if 'auth_checked' not in st.session_state:
    st.session_state.auth_checked = False
if 'output_dir' not in st.session_state:
    st.session_state.output_dir = os.getenv("OUTPUT_DIR", os.path.join(os.getcwd(), "datafiles"))

class StreamlitDataExtractor(BaseDataExtractor):
    """Streamlit-optimized version of DataExtractor"""
    def __init__(self, access_token, output_dir=None):
        self.access_token = access_token
        self.api_key = os.environ["KITE_API_KEY"]
        self.kite = KiteConnect(api_key=self.api_key)
        self.kite.set_access_token(self.access_token)
        self.output_dir = output_dir or os.getenv("OUTPUT_DIR", os.path.join(os.getcwd(), "datafiles"))
        self._expiry_cache = {}
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            print(f"📁 Created output directory: {self.output_dir}")

def authenticate_user():
    """Handle user authentication with smart token management"""
    st.markdown("""
        <div style="text-align: center; padding: 3rem; background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%); border-radius: 15px; margin: 2rem 0;">
            <h2 style="color: #2c3e50; margin-bottom: 1rem;">🔐 Authentication Required</h2>
            <p style="color: #5a6c7d; font-size: 1.1rem;">Connect to your Zerodha Kite account to start extracting data</p>
        </div>
    """, unsafe_allow_html=True)
    
    # Show current token status
    if st.session_state.authenticated:
        st.success("✅ Already authenticated! You can proceed to extract data.")
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            if st.button("🔄 Force Re-authentication", help="Only use if experiencing issues", use_container_width=True):
                st.session_state.authenticated = False
                st.session_state.extractor = None
                st.session_state.auth_checked = False
                st.rerun()
        return
    
    # Check error count
    if st.session_state.auth_error_count >= 3:
        st.error("❌ Too many authentication attempts. Please refresh the page.")
        if st.button("🔄 Reset & Try Again", use_container_width=True):
            st.session_state.auth_error_count = 0
            st.rerun()
        return
    
    # Authentication instructions
    with st.expander("📖 How Authentication Works", expanded=False):
        st.markdown("""
        1. Click the **Authenticate** button below
        2. A browser window will open with Zerodha login
        3. Enter your Kite credentials and complete 2FA
        4. You'll be redirected back automatically
        5. The app will save your session for convenience
        """)
    
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        if st.button("🚀 Authenticate with Kite API", type="primary", use_container_width=True):
            with st.spinner("🔄 Connecting to Zerodha Kite..."):
                try:
                    # Get fresh token
                    access_token = get_kite_token(force_new=True)
                    
                    if access_token:
                        temp_extractor = StreamlitDataExtractor(access_token, output_dir=st.session_state.output_dir)
                        profile = temp_extractor.kite.profile()
                        
                        st.session_state.extractor = temp_extractor
                        st.session_state.authenticated = True
                        st.session_state.auth_error_count = 0
                        
                        st.success(f"✅ Authentication successful! Welcome **{profile.get('user_name', 'User')}**")
                        time.sleep(1)
                        st.rerun()
                    else:
                        raise Exception("No access token received")
                        
                except Exception as e:
                    error_msg = str(e)
                    if "Address already in use" in error_msg or "Errno 48" in error_msg:
                        st.error("❌ Authentication server conflict! Please refresh this page (F5) and try again")
                    else:
                        st.error(f"❌ Authentication failed: {error_msg}")
                    
                    st.session_state.auth_error_count += 1

@st.cache_data(ttl=1800)
def load_instruments_safe():
    """Load and cache NSE stocks and indices with better error handling"""
    # Note: This function is cached, so it won't have access to session state
    # We'll need to pass the extractor as parameter or handle differently
    return True

def load_instruments_from_extractor():
    """Load instruments using the current extractor"""
    if not st.session_state.authenticated or not st.session_state.extractor:
        return False
    
    try:
        # Get NSE stocks
        try:
            nse_instruments = st.session_state.extractor.kite.instruments("NSE")
            
            stocks = []
            for instrument in nse_instruments:
                # Only include proper equity stocks, exclude derivatives/bonds
                if (instrument['segment'] == 'NSE' and 
                    instrument['instrument_type'] == 'EQ' and
                    instrument['name'] and  # Must have a name
                    not any(char.isdigit() for char in instrument['tradingsymbol'][-3:]) and  # Exclude numbered series
                    '-' not in instrument['tradingsymbol'] and  # Exclude hyphenated symbols (usually derivatives)
                    len(instrument['tradingsymbol']) <= 15):  # Reasonable length for stock symbols
                    
                    stocks.append({
                        'symbol': instrument['tradingsymbol'],
                        'name': instrument['name'],
                        'token': instrument['instrument_token'],
                        'display': f"{instrument['tradingsymbol']} - {instrument['name']}"
                    })
            
            # Filter out any remaining invalid entries
            stocks = [s for s in stocks if s['name'] and s['name'] != '-' and len(s['name']) > 2]
            stocks = sorted(stocks, key=lambda x: x['symbol'])
            st.session_state.nse_stocks = stocks
            
        except Exception:
            st.session_state.nse_stocks = []
        
        # Get indices with fallback
        fallback_indices = [
            {'symbol': 'NIFTY 50', 'name': 'NIFTY 50 Index', 'token': 256265, 'display': 'NIFTY 50'},
            {'symbol': 'NIFTY BANK', 'name': 'NIFTY BANK Index', 'token': 260105, 'display': 'NIFTY BANK'},
            {'symbol': 'SENSEX', 'name': 'SENSEX Index', 'token': 265, 'display': 'SENSEX'},
        ]
        
        st.session_state.nse_indices = fallback_indices
        
        return True
        
    except Exception:
        st.session_state.nse_stocks = []
        st.session_state.nse_indices = fallback_indices
        return False

def get_popular_stocks():
    """Get popular stocks list"""
    return [
        {'symbol': 'RELIANCE', 'name': 'Reliance Industries'},
        {'symbol': 'TCS', 'name': 'Tata Consultancy Services'},
        {'symbol': 'HDFCBANK', 'name': 'HDFC Bank'},
        {'symbol': 'INFY', 'name': 'Infosys'},
        {'symbol': 'HINDUNILVR', 'name': 'Hindustan Unilever'},
        {'symbol': 'ICICIBANK', 'name': 'ICICI Bank'},
        {'symbol': 'SBIN', 'name': 'State Bank of India'},
        {'symbol': 'BHARTIARTL', 'name': 'Bharti Airtel'},
        {'symbol': 'ITC', 'name': 'ITC Limited'},
        {'symbol': 'KOTAKBANK', 'name': 'Kotak Mahindra Bank'},
        {'symbol': 'LT', 'name': 'Larsen & Toubro'},
        {'symbol': 'AXISBANK', 'name': 'Axis Bank'},
        {'symbol': 'ADANIPORTS', 'name': 'Adani Ports'},
        {'symbol': 'ASIANPAINT', 'name': 'Asian Paints'},
        {'symbol': 'MARUTI', 'name': 'Maruti Suzuki'},
        {'symbol': 'SUNPHARMA', 'name': 'Sun Pharmaceutical'},
        {'symbol': 'TITAN', 'name': 'Titan Company'},
        {'symbol': 'WIPRO', 'name': 'Wipro'},
        {'symbol': 'ULTRACEMCO', 'name': 'UltraTech Cement'},
        {'symbol': 'NESTLEIND', 'name': 'Nestle India'}
    ]

def show_time_frame_selection():
    """Show time frame selection interface"""
    st.markdown("#### ⏰ Time Frame Selection")
    
    intervals = {
        "1 Minute": {"interval": "minute", "max_days": 60, "chunk_days": 5, "icon": "⚡"},
        "3 Minutes": {"interval": "3minute", "max_days": 200, "chunk_days": 15, "icon": "🔥"},
        "5 Minutes": {"interval": "5minute", "max_days": 2000, "chunk_days": 60, "icon": "⭐"},
        "10 Minutes": {"interval": "10minute", "max_days": 2000, "chunk_days": 60, "icon": "📊"},
        "15 Minutes": {"interval": "15minute", "max_days": 2000, "chunk_days": 60, "icon": "📈"},
        "30 Minutes": {"interval": "30minute", "max_days": 2000, "chunk_days": 100, "icon": "📉"},
        "1 Hour": {"interval": "hour", "max_days": 2000, "chunk_days": 365, "icon": "🕐"},
        "1 Day": {"interval": "day", "max_days": 2000, "chunk_days": 2000, "icon": "📅"}
    }
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Use regular names without icons for selection
        selected_interval_name = st.selectbox(
            "📊 Select Time Interval",
            list(intervals.keys()),
            index=2,  # Default to 5 minutes
            help="Choose the data granularity",
            format_func=lambda x: f"{intervals[x]['icon']} {x}"
        )
        
        selected_interval = intervals[selected_interval_name]
        
        st.info(f"📌 Maximum **{selected_interval['max_days']:,}** days available for {selected_interval_name}")
    
    with col2:
        max_days = selected_interval['max_days']
        
        # Smart defaults based on interval
        if 'minute' in selected_interval['interval']:
            default_days = 7 if selected_interval['interval'] == 'minute' else 30
        else:
            default_days = 90
        
        days = st.number_input(
            "📅 Number of Days",
            min_value=1,
            max_value=max_days,
            value=default_days,
            help=f"Maximum allowed: {max_days:,} days"
        )
    
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
    
    # Show estimation
    st.info(f"📊 Estimated data points: **{estimated_points:,}** records")
    
    return days, selected_interval['interval'], f"{days} days of {selected_interval_name} data"

def extract_data(symbol, days, interval, data_type="stock", instrument_token=None, save_mode="csv"):
    """Extract data with progress tracking and save to CSV or Database"""
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    try:
        status_text.text("🔍 Initializing extraction...")
        progress_bar.progress(10)
        
        # Get output directory for CSV mode
        output_dir = st.session_state.extractor.output_dir if save_mode == "csv" else None
        
        if data_type == "index" or data_type == "special":
            status_text.text(f"📊 Extracting {symbol} data...")
            progress_bar.progress(30)
            
            end_date = datetime.now()
            # For intraday intervals, ensure we include current day
            if interval != "day":
                end_date = end_date + timedelta(days=1)
            start_date = end_date - timedelta(days=days + 10)
            
            if instrument_token:
                token = instrument_token
            else:
                # Special handling for INDIAVIX
                if symbol == "INDIAVIX" or "VIX" in symbol.upper():
                    # Try to find INDIAVIX token using the dedicated method
                    try:
                        token = st.session_state.extractor.get_indiavix_token()
                    except:
                        token = 264969  # Fallback token
                elif symbol == "NIFTY 50":
                    token = 256265
                elif symbol == "NIFTY BANK":
                    token = 260105
                elif symbol == "SENSEX":
                    token = 265
                else:
                    token = 256265
            
            # Fetch the data
            df = st.session_state.extractor.fetch_historical_data_chunked(
                instrument_token=token,
                from_date=start_date,
                to_date=end_date,
                interval=interval
            )
            
            if not df.empty:
                # Process the data
                progress_bar.progress(60)
                status_text.text("🔄 Processing data...")
                df = st.session_state.extractor.process_data(df, symbol)
                
                progress_bar.progress(80)
                
                if save_mode == "csv":
                    # Save to CSV
                    status_text.text("💾 Saving to CSV...")
                    
                    # Create safe filename
                    safe_symbol = symbol.replace(" ", "_").replace("/", "_").lower()
                    filename = f"{safe_symbol}_{days}days_{interval}.csv"
                    
                    # Use save_data method which handles output directory
                    success = st.session_state.extractor.save_data(df, filename, symbol)
                    
                    if success:
                        # Store filename for display
                        st.session_state.last_extracted_filename = filename
                        full_path = os.path.join(output_dir, filename)
                        
                        # Show success message with file info
                        col1, col2 = st.columns(2)
                        with col1:
                            st.success(f"✅ Data saved to: {output_dir}/{filename}")
                        with col2:
                            if os.path.exists(full_path):
                                file_size = os.path.getsize(full_path)
                                st.info(f"📁 Size: {file_size:,} bytes")
                        
                        # Also show in the status text
                        status_text.text(f"✅ Saved to {output_dir}/{filename}")
                    else:
                        st.warning("⚠️ Could not save to file")
                        status_text.text("⚠️ Data extracted but file save failed")
                
                else:  # Database mode
                    status_text.text("🗄️ Saving to database...")
                    
                    # Push to database
                    success, records_inserted, message = push_data_to_db(df, symbol, interval, mode='append')
                    
                    if success:
                        table_name = get_table_name(symbol, interval)
                        st.session_state.last_extracted_filename = table_name
                        
                        # Show success message
                        col1, col2 = st.columns(2)
                        with col1:
                            st.success(f"✅ Data saved to table: {table_name}")
                        with col2:
                            st.info(f"📊 Records inserted: {records_inserted}")
                        
                        status_text.text(f"✅ Saved to database table: {table_name}")
                        
                        # Show additional info
                        if records_inserted == 0:
                            st.info("ℹ️ No new records inserted (data already exists in database)")
                    else:
                        st.error(f"❌ Database save failed: {message}")
                        status_text.text("❌ Database save failed")
            
        else:
            # For stocks
            status_text.text(f"📈 Extracting {symbol} stock data...")
            progress_bar.progress(30)
            
            if save_mode == "csv":
                # Use existing method which saves to CSV
                df = st.session_state.extractor.extract_stock_data(symbol, days=days, interval=interval)
                
                # Store filename for display
                filename = f"{symbol.lower()}_{days}days_{interval}.csv"
                st.session_state.last_extracted_filename = filename
            else:
                # For database mode, we need to extract without saving to CSV
                # Find instrument
                instrument = st.session_state.extractor.search_instrument(symbol, "NSE")
                if not instrument:
                    st.error(f"❌ Could not find instrument for {symbol}")
                    return None
                
                # Calculate date range
                end_date = datetime.now()
                # Include current day for intraday
                if interval != "day":
                    end_date = end_date + timedelta(days=1)
                start_date = end_date - timedelta(days=days + 10)
                
                # Fetch data
                df = st.session_state.extractor.fetch_historical_data_chunked(
                    instrument_token=instrument['instrument_token'],
                    from_date=start_date,
                    to_date=end_date,
                    interval=interval
                )
                
                if not df.empty:
                    # Process data
                    df = st.session_state.extractor.process_data(df, symbol)
                    
                    # Push to database
                    progress_bar.progress(80)
                    status_text.text("🗄️ Saving to database...")
                    
                    success, records_inserted, message = push_data_to_db(df, symbol, interval, mode='append')
                    
                    if success:
                        table_name = get_table_name(symbol, interval)
                        st.session_state.last_extracted_filename = table_name
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            st.success(f"✅ Data saved to table: {table_name}")
                        with col2:
                            st.info(f"📊 Records inserted: {records_inserted}")
                        
                        status_text.text(f"✅ Saved to database table: {table_name}")
                        
                        if records_inserted == 0:
                            st.info("ℹ️ No new records inserted (data already exists in database)")
                    else:
                        st.error(f"❌ Database save failed: {message}")
                        status_text.text("❌ Database save failed")
                        df = None
        
        progress_bar.progress(90)
        status_text.text("🎯 Finalizing...")
        
        if df is not None and not df.empty:
            progress_bar.progress(100)
            status_text.text("✅ Extraction completed!")
            
            # Store in session state for viewing
            st.session_state.last_extracted_data = df
            st.session_state.last_extracted_symbol = symbol
            st.session_state.extraction_complete = True
            
            # Display extraction summary
            st.markdown("---")
            st.markdown("### 📊 Extraction Summary")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Symbol", symbol)
            with col2:
                st.metric("Records", f"{len(df):,}")
            with col3:
                if save_mode == "csv":
                    csv_file = st.session_state.last_extracted_filename
                    st.metric("File", csv_file)
                else:
                    table_name = st.session_state.last_extracted_filename
                    st.metric("Table", table_name)
            
            # Show location info
            if save_mode == "csv":
                st.info(f"📁 All files are saved in: **{os.path.abspath(output_dir)}**")
            else:
                st.info(f"🗄️ Data saved to PostgreSQL database: **{DB_CONFIG['database']}**")
            
            return df
        else:
            progress_bar.progress(100)
            status_text.text("❌ No data returned")
            return None
            
    except Exception as e:
        progress_bar.progress(100)
        status_text.text(f"❌ Error: {str(e)}")
        st.error(f"Error details: {str(e)}")
        import traceback
        st.text(traceback.format_exc())
        return None

def show_data_summary(df, symbol_name):
    """Show data summary and statistics with enhanced design"""
    if df is None or df.empty:
        return
    
    st.markdown("### 📊 Data Summary")
    
    # Create metric cards with better styling
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("📈 Total Records", f"{len(df):,}")
    
    with col2:
        start_date = df['date'].min().strftime('%Y-%m-%d')
        end_date = df['date'].max().strftime('%Y-%m-%d')
        st.metric("📅 Date Range", f"{start_date}")
        st.caption(f"to {end_date}")
    
    with col3:
        price_change = df['close'].iloc[-1] - df['close'].iloc[0]
        price_change_pct = (price_change / df['close'].iloc[0]) * 100
        delta_color = "normal" if price_change >= 0 else "inverse"
        st.metric("💰 Price Change", f"₹{price_change:.2f}", f"{price_change_pct:.2f}%", delta_color=delta_color)
    
    with col4:
        st.metric("📊 Avg Volume", f"{df['volume'].mean():,.0f}")
    
    # Price statistics in an expandable section
    with st.expander("📈 Detailed Price Statistics", expanded=True):
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown(f"""
            <div style="text-align: center; padding: 1rem;">
                <h4 style="color: #00cc96;">📈 Highest</h4>
                <p style="font-size: 1.5rem; font-weight: bold;">₹{df['high'].max():.2f}</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown(f"""
            <div style="text-align: center; padding: 1rem;">
                <h4 style="color: #ff3366;">📉 Lowest</h4>
                <p style="font-size: 1.5rem; font-weight: bold;">₹{df['low'].min():.2f}</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown(f"""
            <div style="text-align: center; padding: 1rem;">
                <h4 style="color: #667eea;">💵 Latest Close</h4>
                <p style="font-size: 1.5rem; font-weight: bold;">₹{df['close'].iloc[-1]:.2f}</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            volatility = df['close'].std()
            st.markdown(f"""
            <div style="text-align: center; padding: 1rem;">
                <h4 style="color: #764ba2;">📊 Volatility</h4>
                <p style="font-size: 1.5rem; font-weight: bold;">₹{volatility:.2f}</p>
            </div>
            """, unsafe_allow_html=True)

def create_price_chart(df, symbol_name):
    """Create interactive price chart with enhanced aesthetics"""
    if df is None or df.empty:
        return
    
    st.markdown("### 📈 Price Chart")
    
    chart_type = st.selectbox("Chart Type", ["Candlestick", "Line Chart", "OHLC"], key="chart_type")
    
    if chart_type == "Candlestick":
        fig = go.Figure(data=go.Candlestick(
            x=df['date'],
            open=df['open'],
            high=df['high'],
            low=df['low'],
            close=df['close'],
            name=symbol_name,
            increasing_line_color='#00cc96',
            decreasing_line_color='#ff3366'
        ))
    elif chart_type == "Line Chart":
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df['date'], 
            y=df['close'], 
            mode='lines', 
            name='Close Price',
            line=dict(color='#667eea', width=2)
        ))
    else:
        fig = go.Figure(data=go.Ohlc(
            x=df['date'],
            open=df['open'],
            high=df['high'],
            low=df['low'],
            close=df['close'],
            name=symbol_name,
            increasing_line_color='#00cc96',
            decreasing_line_color='#ff3366'
        ))
    
    fig.update_layout(
        title=dict(
            text=f"{symbol_name} Price Chart",
            font=dict(size=24, color='#2c3e50')
        ),
        xaxis_title="Date",
        yaxis_title="Price (₹)",
        height=500,
        template="plotly_white",
        hovermode='x unified',
        xaxis=dict(
            rangeslider=dict(visible=False),
            showgrid=True,
            gridcolor='rgba(0,0,0,0.1)'
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor='rgba(0,0,0,0.1)'
        ),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )
    
    st.plotly_chart(fig, use_container_width=True)

def create_volume_chart(df):
    """Create volume chart with enhanced aesthetics"""
    if df is None or df.empty or df['volume'].sum() == 0:
        return
    
    st.markdown("### 📊 Volume Analysis")
    
    # Color based on price change
    colors = ['#ff3366' if row['close'] < row['open'] else '#00cc96' for _, row in df.iterrows()]
    
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df['date'], 
        y=df['volume'], 
        name='Volume',
        marker_color=colors,
        opacity=0.7
    ))
    
    fig.update_layout(
        title=dict(
            text="Volume Chart",
            font=dict(size=20, color='#2c3e50')
        ),
        xaxis_title="Date",
        yaxis_title="Volume",
        height=300,
        template="plotly_white",
        hovermode='x unified',
        xaxis=dict(
            showgrid=True,
            gridcolor='rgba(0,0,0,0.1)'
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor='rgba(0,0,0,0.1)'
        ),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )
    
    st.plotly_chart(fig, use_container_width=True)

def show_data_table(df):
    """Show data table with filtering and enhanced design"""
    if df is None or df.empty:
        return
    
    st.markdown("### 📋 Data Table")
    
    # Show output directory info with better styling
    if st.session_state.extractor and hasattr(st.session_state.extractor, 'output_dir'):
        output_dir = st.session_state.extractor.output_dir
        if st.session_state.last_extracted_filename:
            full_path = os.path.join(output_dir, st.session_state.last_extracted_filename)
            st.info(f"📁 **Saved file location:** `{os.path.abspath(full_path)}`")
    
    # Row selection with better UI
    col1, col2 = st.columns([3, 1])
    with col1:
        num_rows = st.selectbox("Rows to display:", [10, 25, 50, 100, "All"], key="row_selector")
    
    if num_rows == "All":
        display_df = df
    else:
        display_df = df.head(num_rows)
    
    # Style the dataframe
    st.dataframe(
        display_df, 
        use_container_width=True,
        hide_index=True,
        column_config={
            "date": st.column_config.DatetimeColumn(
                "Date",
                format="DD-MM-YYYY HH:mm",
                width="medium",
            ),
            "open": st.column_config.NumberColumn(
                "Open",
                format="₹%.2f",
            ),
            "high": st.column_config.NumberColumn(
                "High",
                format="₹%.2f",
            ),
            "low": st.column_config.NumberColumn(
                "Low",
                format="₹%.2f",
            ),
            "close": st.column_config.NumberColumn(
                "Close",
                format="₹%.2f",
            ),
            "volume": st.column_config.NumberColumn(
                "Volume",
                format="%d",
            ),
            "price_change": st.column_config.NumberColumn(
                "Change",
                format="₹%.2f",
            ),
            "price_change_pct": st.column_config.NumberColumn(
                "Change %",
                format="%.2f%%",
            ),
        }
    )
    
    # Download button with better styling
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        csv = df.to_csv(index=False)
        st.download_button(
            label="📥 Download Complete Dataset as CSV",
            data=csv,
            file_name=f"{st.session_state.last_extracted_symbol}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            type="primary",
            use_container_width=True
        )

def main():
    """Main Streamlit app"""
    
    # Header with enhanced design
    st.markdown("""
        <div style="text-align: center; padding: 2rem 0;">
            <h1 class="main-header">📊 Kite Data Extractor Pro</h1>
            <p style="font-size: 1.2rem; color: #666; margin-top: -1rem;">
                Extract historical market data from Zerodha Kite API
            </p>
        </div>
    """, unsafe_allow_html=True)
    
    # Check authentication once at startup
    if not st.session_state.auth_checked and not st.session_state.authenticated:
        try:
            access_token = get_kite_token(force_new=False)
            if access_token:
                temp_extractor = StreamlitDataExtractor(access_token, output_dir=st.session_state.output_dir)
                profile = temp_extractor.kite.profile()
                st.session_state.extractor = temp_extractor
                st.session_state.authenticated = True
        except Exception:
            pass
        st.session_state.auth_checked = True
    
    # Sidebar with enhanced design
    with st.sidebar:
        st.markdown("""
            <div style="text-align: center; padding: 1rem;">
                <h2 style="color: #667eea;">⚙️ Control Panel</h2>
            </div>
        """, unsafe_allow_html=True)
        
        if not st.session_state.authenticated:
            st.warning("⚠️ Please authenticate first")
        else:
            st.success("✅ Connected to Kite API")
            
            # Show output directory
            if st.session_state.extractor and hasattr(st.session_state.extractor, 'output_dir'):
                with st.expander("📁 Output Settings", expanded=False):
                    new_output_dir = st.text_input(
                        "Output folder for CSV files:",
                        value=st.session_state.output_dir,
                        help="Enter the local path where CSV files will be saved."
                    )
                    if new_output_dir != st.session_state.output_dir:
                        st.session_state.output_dir = new_output_dir
                    st.info(f"**Output folder:** `{st.session_state.output_dir}`")
            
            # Load instruments if needed
            if st.session_state.nse_stocks is None or st.session_state.nse_indices is None:
                load_instruments_from_extractor()
            
            # Quick stats only if data exists
            if st.session_state.last_extracted_data is not None:
                st.divider()
                st.markdown("### 📊 Last Extraction")
                df = st.session_state.last_extracted_data
                
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Records", f"{len(df):,}")
                with col2:
                    if st.session_state.last_extracted_symbol:
                        st.metric("Symbol", st.session_state.last_extracted_symbol)
                
                csv = df.to_csv(index=False)
                st.download_button(
                    label="💾 Quick Download",
                    data=csv,
                    file_name=f"data_{datetime.now().strftime('%H%M%S')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
    
    # Main content
    if not st.session_state.authenticated:
        authenticate_user()
    else:
        tab1, tab2, tab3 = st.tabs(["📊 Extract Data", "📈 View Results", "ℹ️ Help & Info"])
        
        with tab1:
            st.markdown("### 🎯 Data Extraction Center")
            
            # Save mode selection with better styling
            col1, col2 = st.columns([1, 2])
            with col1:
                st.markdown("#### 💾 Save Mode")
            with col2:
                save_mode = st.radio(
                    "",
                    ["📄 CSV File", "🗄️ PostgreSQL Database"],
                    horizontal=True,
                    help="Choose where to save the extracted data"
                )
            
            # Map display names to internal values
            save_mode_value = "csv" if "CSV" in save_mode else "database"
            
            # Show database connection status if database mode selected
            if save_mode_value == "database":
                with st.expander("🔧 Database Configuration", expanded=False):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.info(f"**Host:** `{DB_CONFIG['host']}:{DB_CONFIG['port']}`")
                        st.info(f"**Database:** `{DB_CONFIG['database']}`")
                    with col2:
                        st.info(f"**Schema:** `{DB_CONFIG['schema']}`")
                        st.info(f"**User:** `{DB_CONFIG['user']}`")
                    
                    # Test database connection
                    if st.button("🔌 Test Database Connection", use_container_width=True):
                        try:
                            db_handler = DatabaseHandler()
                            if db_handler.test_connection():
                                st.success("✅ Database connection successful!")
                            else:
                                st.error("❌ Database connection failed!")
                            db_handler.disconnect()
                        except Exception as e:
                            st.error(f"❌ Connection error: {str(e)}")
                            st.info("Please check your database configuration in `database_config.py`")
            
            st.divider()
            
            # Data type selection with icons
            st.markdown("#### 🔍 Select Data Type")
            data_type = st.selectbox(
                "",
                ["⭐ Popular Stocks", "📈 Search All Stocks", "📊 Major Indices", "📉 Special Symbols (VIX)", "✏️ Manual Entry"],
                help="Choose what type of data to extract"
            )
            
            symbol = None
            instrument_token = None
            selected_data_type = "stock"
            
            if data_type == "⭐ Popular Stocks":
                st.markdown("##### ⭐ Popular Stock Selection")
                
                popular_stocks = get_popular_stocks()
                
                # Display in a grid
                cols = st.columns(3)
                selected_popular = st.selectbox(
                    "Select a popular stock:",
                    popular_stocks,
                    format_func=lambda x: f"{x['symbol']} - {x['name']}"
                )
                
                symbol = selected_popular['symbol']
                
            elif data_type == "📈 Search All Stocks":
                st.markdown("##### 📈 Stock Search")
                
                # Show some quick examples first
                st.info("💡 **Quick examples:** RELIANCE, TCS, INFY, HDFCBANK, SBIN, ITC")
                
                if st.session_state.nse_stocks and len(st.session_state.nse_stocks) > 0:
                    search_term = st.text_input("🔍 Search stocks", placeholder="Type to search (e.g., RELIANCE, TCS)")
                    
                    if search_term:
                        filtered_stocks = [
                            stock for stock in st.session_state.nse_stocks 
                            if (search_term.upper() in stock['symbol'].upper() or 
                                search_term.upper() in stock['name'].upper()) and
                                stock['name'] and stock['name'] != '-'
                        ]
                    else:
                        filtered_stocks = [s for s in st.session_state.nse_stocks[:100] 
                                         if s['name'] and s['name'] != '-' and len(s['name']) > 2]
                    
                    if filtered_stocks:
                        def format_stock(stock):
                            name = stock['name'] if stock['name'] and stock['name'] != '-' else 'Unknown'
                            return f"{stock['symbol']} - {name}"
                        
                        selected_stock = st.selectbox(
                            "Select Stock:",
                            filtered_stocks,
                            format_func=format_stock,
                            help="Use search box to find specific stocks"
                        )
                        symbol = selected_stock['symbol']
                    else:
                        st.warning("No stocks found. Try 'Popular Stocks' or 'Manual Entry' option.")
                else:
                    st.warning("Use 'Popular Stocks' or 'Manual Entry' option instead")
                    
            elif data_type == "📊 Major Indices":
                st.markdown("##### 📊 Major Index Selection")
                
                major_indices = [
                    {'symbol': 'NIFTY 50', 'name': 'NIFTY 50', 'token': 256265, 'icon': '📈'},
                    {'symbol': 'NIFTY BANK', 'name': 'NIFTY BANK', 'token': 260105, 'icon': '🏦'},
                    {'symbol': 'SENSEX', 'name': 'SENSEX', 'token': 265, 'icon': '📊'}
                ]
                
                selected_index = st.selectbox(
                    "Select Index:",
                    major_indices,
                    format_func=lambda x: f"{x['icon']} {x['name']}"
                )
                
                symbol = selected_index['symbol']
                instrument_token = selected_index['token']
                selected_data_type = "index"
                
            elif data_type == "📉 Special Symbols (VIX)":
                st.markdown("##### 📉 Special Symbol Selection")
                
                special_symbols = [
                    {'symbol': 'INDIAVIX', 'name': 'India VIX (Volatility Index)', 'icon': '📉'}
                ]
                
                selected_special = st.selectbox(
                    "Select Special Symbol:",
                    special_symbols,
                    format_func=lambda x: f"{x['icon']} {x['name']}"
                )
                
                symbol = selected_special['symbol']
                selected_data_type = "special"
                st.info("ℹ️ **India VIX** measures market volatility expectations")
                
            elif data_type == "✏️ Manual Entry":
                st.markdown("##### ✏️ Manual Symbol Entry")
                
                entry_type = st.radio("Entry Type:", ["Stock", "Index", "Special (VIX)"], horizontal=True)
                
                if entry_type == "Stock":
                    symbol = st.text_input(
                        "📝 Enter Stock Symbol",
                        placeholder="e.g., RELIANCE, TCS, ADANIPORTS",
                        help="Enter the exact NSE trading symbol"
                    ).upper().strip()
                    selected_data_type = "stock"
                elif entry_type == "Index":
                    manual_indices = {
                        "NIFTY 50": 256265,
                        "NIFTY BANK": 260105, 
                        "SENSEX": 265
                    }
                    
                    selected_manual_index = st.selectbox("Select Index:", list(manual_indices.keys()))
                    symbol = selected_manual_index
                    instrument_token = manual_indices[selected_manual_index]
                    selected_data_type = "index"
                else:  # Special (VIX)
                    symbol = st.text_input(
                        "📝 Enter Special Symbol",
                        value="INDIAVIX",
                        placeholder="e.g., INDIAVIX",
                        help="Enter special symbols like INDIAVIX"
                    ).upper().strip()
                    selected_data_type = "special"
            
            # Time frame selection
            if symbol:
                st.divider()
                days, interval, description = show_time_frame_selection()
                
                # Show expected table name for database mode
                if save_mode_value == "database":
                    expected_table = get_table_name(symbol, interval)
                    st.info(f"🗄️ Data will be saved to table: **`{expected_table}`**")
                
                # Extract button with enhanced styling
                col1, col2, col3 = st.columns([1, 2, 1])
                with col2:
                    if st.button("🚀 Extract Data", type="primary", use_container_width=True):
                        df = extract_data(symbol, days, interval, selected_data_type, instrument_token, save_mode_value)
                        
                        if df is not None and not df.empty:
                            st.success(f"✅ Successfully extracted **{len(df):,}** records for **{symbol}**!")
                            st.balloons()
                        else:
                            st.error("❌ Data extraction failed! Please check the symbol and try again.")
            else:
                st.warning("⚠️ Please select or enter a stock/index symbol to continue")
        
        with tab2:
            if st.session_state.extraction_complete and st.session_state.last_extracted_data is not None:
                df = st.session_state.last_extracted_data
                symbol_name = st.session_state.last_extracted_symbol or "Extracted Data"
                
                show_data_summary(df, symbol_name)
                create_price_chart(df, symbol_name)
                create_volume_chart(df)
                show_data_table(df)
            else:
                # Empty state with better design
                st.markdown("""
                    <div style="text-align: center; padding: 4rem 2rem;">
                        <h2 style="color: #999;">📊 No Data Yet</h2>
                        <p style="color: #666; font-size: 1.1rem;">
                            Extract some data first to see charts and analysis here
                        </p>
                    </div>
                """, unsafe_allow_html=True)
        
        with tab3:
            st.markdown("""
            <div style="background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%); padding: 2rem; border-radius: 10px;">
                <h2 style="color: #2c3e50;">ℹ️ Help & Information</h2>
            </div>
            """, unsafe_allow_html=True)
            
            st.markdown("""
            ### 📖 How to Use
            
            1. **🔐 Authentication**: Click "Authenticate with Kite API" to connect
            2. **💾 Select Save Mode**: Choose between CSV file or PostgreSQL database
            3. **🔍 Select Data Type**: Choose from available options (including VIX)
            4. **⏰ Configure Time Frame**: Select interval and duration
            5. **🚀 Extract Data**: Click "Extract Data" to fetch historical data
            6. **📈 View Results**: Check the "View Results" tab for charts and data
            7. **📥 Download**: Use the download button to save data as CSV
            
            ### 💾 Save Modes
            
            #### 📄 CSV File Mode
            - Saves data to CSV files in the output directory
            - File naming: `symbol_days_interval.csv`
            - Location: `${os.getenv('OUTPUT_DIR', os.path.join(os.getcwd(), 'datafiles'))}`
            
            #### 🗄️ PostgreSQL Database Mode
            - Saves data directly to PostgreSQL database
            - Table naming convention:
                - NIFTY 50 → `nifty_1m`, `nifty_5m`, `nifty_day`, etc.
                - RELIANCE → `reliance_1m`, `reliance_5m`, etc.
                - INDIAVIX → `indiavix_1m`, `indiavix_5m`, etc.
            - Automatically creates tables if they don't exist
            - Appends new data and skips duplicates
            
            ### 📉 Special Symbols
            
            #### India VIX (INDIAVIX)
            - Measures 30-day implied volatility of NIFTY 50 options
            - Higher values indicate higher expected volatility
            - Useful for understanding market sentiment
            - Available in "Special Symbols" section
            
            ### 🔧 Database Configuration
            
            Update `database_config.py` with your PostgreSQL credentials:
            ```python
            DB_CONFIG = {
                "host": "your_host",
                "port": 5432,
                "database": "your_database",
                "user": "your_username",
                "password": "your_password",
                "schema": "public"
            }
            ```
            
            ### ⚡ Quick Tips
            - ✅ Use search box to find specific stocks quickly
            - ✅ Popular stocks option for commonly traded securities
            - ✅ Special symbols section for VIX data
            - ✅ Database mode automatically handles duplicate data
            - ✅ CSV mode saves to local files
            - ✅ Current day data is included for all extractions
            
            ### 🔧 Troubleshooting
            - 🔄 If authentication loops, refresh the browser (F5)
            - 🧹 Clear browser cache if issues persist
            - ✅ Make sure you have a valid Kite account
            - 🗄️ For database errors, check PostgreSQL credentials
            - 🖥️ Ensure PostgreSQL server is running and accessible
            """)

if __name__ == "__main__":
    main()