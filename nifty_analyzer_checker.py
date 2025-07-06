"""
Configuration Checker for NIFTY First Hour Analyzer
Validates all settings and connections before running the main analyzer

Usage:
python check_nifty_analyzer.py
"""

import sys
import os
from datetime import datetime, time
import importlib.util

def check_module(module_name, file_name=None):
    """Check if a Python module exists"""
    if file_name:
        # Check for local file
        if os.path.exists(file_name):
            print(f"✅ {file_name} found")
            return True
        else:
            print(f"❌ {file_name} not found")
            return False
    else:
        # Check for installed package
        spec = importlib.util.find_spec(module_name)
        if spec is not None:
            print(f"✅ {module_name} is installed")
            return True
        else:
            print(f"❌ {module_name} is not installed")
            return False

def check_time_validity():
    """Check if current time is appropriate for analysis"""
    current_time = datetime.now()
    current_hour_min = current_time.time()
    market_start = time(9, 15)
    analysis_time = time(10, 15)
    market_end = time(15, 30)
    
    print(f"\n📅 Current date: {current_time.strftime('%d-%b-%Y')}")
    print(f"⏰ Current time: {current_time.strftime('%I:%M %p')}")
    
    # Check if weekday
    if current_time.weekday() > 4:
        print("⚠️  Today is weekend. Markets are closed.")
        return False
    
    # Check time
    if current_hour_min < analysis_time:
        print(f"⚠️  Too early! Analysis runs at 10:15 AM. Current: {current_time.strftime('%I:%M %p')}")
        return False
    elif current_hour_min > market_end:
        print("ℹ️  Market has closed for today. Analysis can still run for today's data.")
    else:
        print("✅ Time is valid for analysis")
    
    return True

def main():
    print("🔍 NIFTY First Hour Analyzer - Configuration Check")
    print("=" * 50)
    
    all_checks_passed = True
    
    # 1. Check Python version
    print("\n1️⃣ Checking Python version...")
    python_version = sys.version_info
    if python_version.major == 3 and python_version.minor >= 7:
        print(f"✅ Python {python_version.major}.{python_version.minor}.{python_version.micro}")
    else:
        print(f"❌ Python {python_version.major}.{python_version.minor} (Need 3.7+)")
        all_checks_passed = False
    
    # 2. Check required packages
    print("\n2️⃣ Checking required packages...")
    packages = [
        ('kiteconnect', None),
        ('pandas', None),
        ('telegram', None),  # python-telegram-bot
        ('schedule', None),
        ('requests', None)
    ]
    
    for package, _ in packages:
        if not check_module(package):
            all_checks_passed = False
    
    # 3. Check required files
    print("\n3️⃣ Checking required files...")
    files = [
        'kite_authenticator.py',
        'telegram_config.py',
        'nifty_first_hour_analyzer.py'
    ]
    
    for file in files:
        if not check_module(None, file):
            all_checks_passed = False
    
    # 4. Check Telegram configuration
    print("\n4️⃣ Checking Telegram configuration...")
    try:
        from telegram_config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
        
        if TELEGRAM_BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
            print("❌ TELEGRAM_BOT_TOKEN not configured")
            all_checks_passed = False
        else:
            print(f"✅ Telegram bot token configured ({TELEGRAM_BOT_TOKEN[:10]}...)")
        
        if TELEGRAM_CHAT_ID == "YOUR_CHAT_ID_HERE":
            print("❌ TELEGRAM_CHAT_ID not configured")
            all_checks_passed = False
        else:
            print(f"✅ Telegram chat ID configured ({TELEGRAM_CHAT_ID})")
            
    except ImportError as e:
        print(f"❌ Error importing telegram_config: {e}")
        all_checks_passed = False
    
    # 5. Check Kite authentication
    print("\n5️⃣ Checking Kite authentication...")
    try:
        from kite_authenticator import get_kite_token
        
        # Try to get token (don't force new)
        token = get_kite_token(force_new=False)
        if token:
            print(f"✅ Kite access token available ({token[:10]}...)")
        else:
            print("⚠️  No saved Kite token. Will need to authenticate on first run.")
            
    except ImportError as e:
        print(f"❌ Error importing kite_authenticator: {e}")
        all_checks_passed = False
    except Exception as e:
        print(f"⚠️  Kite authentication check error: {e}")
    
    # 6. Check time validity
    print("\n6️⃣ Checking time validity...")
    check_time_validity()
    
    # 7. Check output directory
    print("\n7️⃣ Checking output files...")
    if os.path.exists('nifty_first_hour.log'):
        print("✅ Log file exists")
    else:
        print("ℹ️  Log file will be created on first run")
    
    if os.path.exists('nifty_first_hour_signals.csv'):
        print("✅ Signal history file exists")
        # Show last few signals
        try:
            with open('nifty_first_hour_signals.csv', 'r') as f:
                lines = f.readlines()
                if len(lines) > 1:
                    print(f"   Last signal: {lines[-1].strip()}")
        except:
            pass
    else:
        print("ℹ️  Signal history file will be created on first run")
    
    # Summary
    print("\n" + "=" * 50)
    if all_checks_passed:
        print("✅ All checks passed! System is ready.")
        print("\nTo run the analyzer:")
        print("  - Immediate: python nifty_first_hour_analyzer.py")
        print("  - Scheduled: python nifty_first_hour_analyzer.py --scheduled")
    else:
        print("❌ Some checks failed. Please fix the issues above.")
        print("\nQuick fixes:")
        print("  1. Install packages: pip install kiteconnect pandas python-telegram-bot schedule requests")
        print("  2. Update telegram_config.py with your bot token and chat ID")
        print("  3. Ensure all required files are in the same directory")

if __name__ == "__main__":
    main()
