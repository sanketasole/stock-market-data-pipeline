
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