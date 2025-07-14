"""
NIFTY Options Explorer - Debug Tool
Explores all available NIFTY options to understand the format

Usage:
    python testoifetch.py
"""

import os
from kiteconnect import KiteConnect
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import authentication
from kite_authenticator import get_kite_token

def get_env_var(var_name):
    value = os.environ.get(var_name)
    if not value:
        raise EnvironmentError(f"Environment variable '{var_name}' not set.")
    return value

def main():
    api_key = get_env_var("KITE_API_KEY")
    access_token = get_kite_token(force_new=False)

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)

    # Step 1: Get NIFTY Spot Price
    try:
        spot_data = kite.ltp(["NSE:NIFTY 50"])
        spot_price = spot_data["NSE:NIFTY 50"]["last_price"]
    except Exception as e:
        print(f"Error fetching NIFTY spot price: {e}")
        return

    # Step 2: Derive ATM strike rounded to nearest 50
    atm_strike = int(round(spot_price / 50.0) * 50)

    # Step 3: Generate 5 ITM and 5 OTM strikes
    strikes = [atm_strike + i * 50 for i in range(-5, 6)]
    strikes = sorted(set(strikes))

    # Step 4: Build option symbols (CE and PE)
    expiry_code = os.environ.get("NIFTY_EXPIRY_CODE", "25717")  # Default: 17th July 2025
    option_symbols = [
        f"NIFTY{expiry_code}{strike}{opt_type}"
        for strike in strikes
        for opt_type in ("CE", "PE")
    ]
    full_symbols = [f"NFO:{sym}" for sym in option_symbols]

    # Step 5: Fetch quote data
    try:
        quote_data = kite.quote(full_symbols)
    except Exception as e:
        print(f"Error fetching option quotes: {e}")
        return

    # Step 6: Display required fields
    print(f"\n📊 NIFTY Spot: {spot_price} | ATM Strike: {atm_strike}")
    print(f"🔍 Option Chain Snapshot for expiry code {expiry_code}\n")

    for sym in option_symbols:
        full_sym = f"NFO:{sym}"
        data = quote_data.get(full_sym, {})

        if not data or "last_price" not in data:
            print(f"{sym} - ❌ Data not available")
            continue

        ltp = data.get("last_price")
        ohlc = data.get("ohlc", {})
        oi = data.get("oi", "N/A")
        oi_high = data.get("oi_day_high", "N/A")
        oi_low = data.get("oi_day_low", "N/A")

        print(f"{sym}")
        print(f"  ➤ LTP           : ₹{ltp}")
        print(f"  ➤ Open          : ₹{ohlc.get('open')}")
        print(f"  ➤ High          : ₹{ohlc.get('high')}")
        print(f"  ➤ Low           : ₹{ohlc.get('low')}")
        print(f"  ➤ Prev Close    : ₹{ohlc.get('close')}")
        print(f"  ➤ OI            : {oi:,}" if isinstance(oi, int) else f"  ➤ OI            : {oi}")
        print(f"  ➤ OI Day High   : {oi_high:,}" if isinstance(oi_high, int) else f"  ➤ OI Day High   : {oi_high}")
        print(f"  ➤ OI Day Low    : {oi_low:,}" if isinstance(oi_low, int) else f"  ➤ OI Day Low    : {oi_low}")
        volume = data.get("volume", "N/A")
        print(f"  ➤ Volume        : {volume}\n")

if __name__ == "__main__":
    main()