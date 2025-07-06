"""
PostgreSQL Database Configuration
Stores database connection details

Update these values with your actual database credentials
"""

from dotenv import load_dotenv
import os
load_dotenv()

# Database connection parameters
DB_CONFIG = {
    "host": os.environ["DB_HOST"],
    "port": int(os.environ["DB_PORT"]),
    "database": os.environ["DB_NAME"],
    "user": os.environ["DB_USER"],
    "password": os.environ["DB_PASSWORD"],
    "schema": os.getenv("DB_SCHEMA", "public")
}

# Connection pool settings
POOL_CONFIG = {
    "minconn": 1,
    "maxconn": 10
}

# Table naming configuration
TABLE_NAME_MAPPING = {
    "minute": "1m",
    "3minute": "3m",
    "5minute": "5m",
    "10minute": "10m",
    "15minute": "15m",
    "30minute": "30m",
    "hour": "hour",
    "60minute": "hour",  # Alternative naming
    "day": "day"
}

def get_table_name(symbol, interval):
    """
    Generate table name based on symbol and interval
    
    Args:
        symbol (str): Stock/Index symbol (e.g., "NIFTY 50", "RELIANCE", "INDIAVIX")
        interval (str): Time interval (e.g., "5minute", "day")
    
    Returns:
        str: Table name (e.g., "nifty_5m", "reliance_day", "indiavix_5m")
    """
    # Clean symbol name
    clean_symbol = symbol.replace(" ", "").replace("-", "").replace("&", "").lower()
    
    # Special handling for indices and special symbols
    if "nifty50" in clean_symbol or "nifty" in clean_symbol:
        clean_symbol = "nifty"
    elif "sensex" in clean_symbol:
        clean_symbol = "sensex"
    elif "banknifty" in clean_symbol or "niftybank" in clean_symbol:
        clean_symbol = "banknifty"
    elif "indiavix" in clean_symbol or "vix" in clean_symbol:
        clean_symbol = "indiavix"
    
    # Get interval suffix
    interval_suffix = TABLE_NAME_MAPPING.get(interval, interval)
    
    return f"{clean_symbol}_{interval_suffix}"

# Table schema definition (without expiry columns)
TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS {table_name} (
    date             TIMESTAMP PRIMARY KEY,
    open             DOUBLE PRECISION,
    high             DOUBLE PRECISION,
    low              DOUBLE PRECISION,
    close            DOUBLE PRECISION,
    volume           BIGINT,
    price_change     DOUBLE PRECISION,
    price_change_pct DOUBLE PRECISION,
    high_low_range   DOUBLE PRECISION,
    range_pct        DOUBLE PRECISION,
    day_of_week      TEXT,
    month            INTEGER,
    year             INTEGER
);

-- Create index on date for faster queries
CREATE INDEX IF NOT EXISTS idx_{table_name}_date ON {table_name}(date);
"""