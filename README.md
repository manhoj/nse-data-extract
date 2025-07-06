# Kite Data Extractor 

A comprehensive tool for extracting and analyzing Indian stock market data using Zerodha Kite API. This project provides both a web interface (Streamlit) and command-line tools for data extraction, analysis, and automated trading insights.

## 🚀 Features

- **📊 Historical Data Extraction**: Extract historical data for stocks, indices, and special symbols (VIX)
- **📈 Real-time Analysis**: NIFTY first hour movement analysis with automated alerts
- **🔄 Option Chain Analysis**: Comprehensive option chain data and Greeks analysis
- **🌐 Web Interface**: User-friendly Streamlit web app for data extraction and visualization
- **🗄️ Database Integration**: PostgreSQL support for data storage and management
- **📱 Telegram Notifications**: Automated alerts and notifications via Telegram bot
- **⚙️ Flexible Output**: Save data as CSV files or directly to database
- **🔐 Secure Authentication**: Environment-based configuration for API credentials

## 📋 Prerequisites

- Python 3.7 or higher
- Zerodha Kite account with API access
- PostgreSQL database (optional, for database mode)
- Telegram bot (optional, for notifications)

## 🛠️ Installation

1. **Clone the repository**
   ```bash
   git clone <your-repo-url>
   cd raw_data_extract
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment variables**
   Create a `.env` file in the project root:
   ```env
   # Kite API credentials
   KITE_API_KEY=your_kite_api_key
   KITE_API_SECRET=your_kite_api_secret
   
   # Database configuration (optional)
   DB_HOST=localhost
   DB_PORT=5432
   DB_NAME=your_database
   DB_USER=your_username
   DB_PASSWORD=your_password
   DB_SCHEMA=public
   
   # Output directory (optional)
   OUTPUT_DIR=/path/to/your/output/directory
   
   # Telegram configuration (optional)
   TELEGRAM_BOT_TOKEN=your_bot_token
   TELEGRAM_CHAT_ID=your_chat_id
   ```

## 🚀 Usage

### Web Interface (Recommended)

Start the Streamlit web app:
```bash
streamlit run streamlit_app.py
```

The web interface provides:
- **Authentication**: Connect to your Zerodha Kite account
- **Data Extraction**: Extract historical data for stocks, indices, and VIX
- **Visualization**: Interactive charts and data analysis
- **Configuration**: Set output directories and database settings

### Command Line Tools

#### Data Extractor
```bash
python data_extractor.py
```

#### NIFTY First Hour Analyzer
```bash
python nifty_first_hour_analyzer.py
```

#### Option Chain Fetcher
```bash
python option_chain_fetcher.py
```

#### NIFTY Expiry Fetcher
```bash
python nifty_expiry_fetcher.py
```

## 📁 Project Structure

```
raw_data_extract/
├── streamlit_app.py          # Main web interface
├── data_extractor.py         # Core data extraction logic
├── kite_authenticator.py     # Kite API authentication
├── database_handler.py       # Database operations
├── database_config.py        # Database configuration
├── nifty_first_hour_analyzer.py  # NIFTY analysis tool
├── option_chain_fetcher.py   # Option chain data
├── nifty_expiry_fetcher.py   # Expiry date fetcher
├── telegram_config.py        # Telegram bot configuration
├── requirements.txt          # Python dependencies
├── .env                      # Environment variables (not in git)
└── .gitignore               # Git ignore rules
```

## 🔧 Configuration

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `KITE_API_KEY` | Your Zerodha API key | Yes |
| `KITE_API_SECRET` | Your Zerodha API secret | Yes |
| `DB_HOST` | PostgreSQL host | No (for database mode) |
| `DB_PORT` | PostgreSQL port | No (default: 5432) |
| `DB_NAME` | Database name | No (for database mode) |
| `DB_USER` | Database username | No (for database mode) |
| `DB_PASSWORD` | Database password | No (for database mode) |
| `OUTPUT_DIR` | CSV output directory | No (default: ./datafiles) |
| `TELEGRAM_BOT_TOKEN` | Telegram bot token | No (for notifications) |
| `TELEGRAM_CHAT_ID` | Telegram chat ID | No (for notifications) |

### Database Mode

When using database mode, data is stored in PostgreSQL with the following table naming convention:
- NIFTY 50 → `nifty_1m`, `nifty_5m`, `nifty_day`, etc.
- RELIANCE → `reliance_1m`, `reliance_5m`, etc.
- INDIAVIX → `indiavix_1m`, `indiavix_5m`, etc.

## 📊 Data Formats

### CSV Output
Files are saved as: `{symbol}_{days}days_{interval}.csv`

### Database Schema
```sql
CREATE TABLE {table_name} (
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
```

## 🔒 Security

- **No hardcoded credentials**: All sensitive information is stored in environment variables
- **Secure token management**: Access tokens are stored locally and validated
- **Environment-based config**: Different configurations for development and production

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## 📝 License

This project is for educational and personal use. Please ensure compliance with Zerodha's API terms of service.

## ⚠️ Disclaimer

This tool is for educational purposes only. Trading involves risk, and past performance does not guarantee future results. Always do your own research and consider consulting with financial advisors.

## 🆘 Support

For issues and questions:
1. Check the documentation above
2. Review the error logs
3. Ensure all environment variables are set correctly
4. Verify your Kite API,database and other credentials are valid

## 🔄 Updates

- **v1.0**: Initial release with basic data extraction
- **v1.1**: Added Streamlit web interface
- **v1.2**: Added database integration and Telegram notifications
- **v1.3**: Enhanced security and environment variable support 