import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine
import mysql.connector
from datetime import datetime
import os 
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database connection using environment variables
db_config = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAME')
}



engine = create_engine(f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}")

# List of Nifty Fifty stock symbols
nifty_fifty_symbols = [
    'ADANIPORTS.NS', 'ASIANPAINT.NS', 'AXISBANK.NS', 'BAJAJ-AUTO.NS', 'BAJAJFINSV.NS', 
    'BAJFINANCE.NS', 'BHARTIARTL.NS', 'BPCL.NS', 'BRITANNIA.NS', 'CIPLA.NS', 'COALINDIA.NS', 
    'DIVISLAB.NS', 'DRREDDY.NS', 'EICHERMOT.NS', 'GAIL.NS', 'GRASIM.NS', 'HCLTECH.NS', 
    'HDFC.NS', 'HDFCBANK.NS', 'HDFCLIFE.NS', 'HEROMOTOCO.NS', 'HINDALCO.NS', 'HINDUNILVR.NS', 
    'ICICIBANK.NS', 'INDUSINDBK.NS', 'INFY.NS', 'IOC.NS', 'ITC.NS', 'JSWSTEEL.NS', 'KOTAKBANK.NS', 
    'LT.NS', 'M&M.NS', 'MARUTI.NS', 'NESTLEIND.NS', 'NTPC.NS', 'ONGC.NS', 'POWERGRID.NS', 
    'RELIANCE.NS', 'SBILIFE.NS', 'SBIN.NS', 'SHREECEM.NS', 'SUNPHARMA.NS', 'TATAMOTORS.NS', 
    'TATASTEEL.NS', 'TCS.NS', 'TECHM.NS', 'TITAN.NS', 'ULTRACEMCO.NS', 'UPL.NS', 'WIPRO.NS']

def fetch_and_store_data():
    for symbol in nifty_fifty_symbols:
        try:
            print(f"Fetching data for {symbol}...")
            stock_data = yf.download(symbol, period="1d", interval="1m")
            if stock_data.empty:
                print(f"No data fetched for {symbol}")
                continue
            
            # Resample to daily frequency
            daily_data = stock_data.resample('D').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'})
            daily_data['Symbol'] = symbol
            daily_data.reset_index(inplace=True)
            
            # Check for existing data in the database
            existing_records_query = f"SELECT * FROM nifty_fifty WHERE Symbol = '{symbol}' AND DATE(date) = CURDATE()"
            existing_records = pd.read_sql(existing_records_query, engine)
            
            if not existing_records.empty:
                print(f"Data for {symbol} already exists for today.")
                continue
            
            # Rename 'Datetime' to 'date' to match SQL table
            daily_data.rename(columns={'Datetime': 'date'}, inplace=True)
            
            # Write to MySQL database
            daily_data.to_sql('nifty_fifty', con=engine, if_exists='append', index=False)
            print(f"Data for {symbol} stored successfully.")
        
        except Exception as e:
            print(f"Error fetching or storing data for {symbol}: {e}")

def analyze_and_export_to_excel():
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor(dictionary=True)
        
        cursor.execute("SELECT MIN(id) as id, date, open, high, low, close, SUM(volume) as volume, Symbol FROM nifty_fifty GROUP BY Symbol, date, open, high, low, close")
        rows = cursor.fetchall()
        
        df = pd.DataFrame(rows)
        
        # Convert 'date' column to datetime
        df['date'] = pd.to_datetime(df['date'])
        
        # Group by 'Symbol' and 'date' and aggregate to one row per day
        df_grouped = df.groupby(['Symbol', pd.Grouper(key='date', freq='D')]).agg({
            'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
        }).reset_index()
        
        # Calculate returns
        df_grouped['Returns'] = df_grouped.groupby('Symbol')['close'].pct_change() * 100
        df_grouped['Daily_Returns'] = df_grouped.groupby('Symbol')['close'].pct_change() * 100
        df_grouped['Weekly_Returns'] = df_grouped.groupby('Symbol')['close'].pct_change(periods=5) * 100
        df_grouped['Monthly_Returns'] = df_grouped.groupby('Symbol')['close'].pct_change(periods=21) * 100
        df_grouped['Yearly_Returns'] = df_grouped.groupby('Symbol')['close'].pct_change(periods=252) * 100

        # Export to Excel
        excel_filename = f"nifty_fifty_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        df_grouped.to_excel(excel_filename, index=False)
        print(f"Data exported to {excel_filename}")
        
        cursor.close()
        connection.close()
    
    except Exception as e:
        print(f"Error analyzing or exporting data: {e}")

# Fetch and store data
fetch_and_store_data()

# Analyze and export data to Excel
analyze_and_export_to_excel()



