import time
import threading
import ccxt
import sys
import os
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from function import ccxt_function

def analyze_and_record():
    try:
        # Initialize Binance Futures API
        exchange = ccxt.binanceusdm()
        os.makedirs("./ltsm", exist_ok=True)  # Create directory if it doesn't exist

        # Store last analyzed timestamps for each symbol
        last_timestamps = {}

        while True:
            top_coins = ccxt_function.get_top_5_coins()

            for symbol in top_coins:
                try:
                    # Fetch 5-minute interval data
                    data = ccxt_function.fetch_data(symbol, exchange, timeframe="5m")
                    data.dropna(inplace=True)

                    # Get the most recent timestamp
                    latest_timestamp = data.iloc[-1]['timestamp']

                    # Check if data is new
                    if symbol in last_timestamps and latest_timestamp == last_timestamps[symbol]:
                        print(f"No new data for {symbol}. Skipping analysis.")
                        continue

                    # Update the last analyzed timestamp
                    last_timestamps[symbol] = latest_timestamp

                    # Perform LSTM analysis
                    lstm_result = ccxt_function.lstm_analyze_coin(symbol, exchange)

                    # Log the results per coin
                    log_file = f"./ltsm/{symbol.replace('/', '_')}_log.txt"
                    timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    with open(log_file, "a") as file:
                        file.write(f"[{timestamp}] LSTM Result: {lstm_result}\n")

                except Exception as e:
                    print(f"Error analyzing symbol {symbol}: {e}")
                    if 'data' in locals():
                        print(f"Problematic data for {symbol}:\n{data}")
                    else:
                        print("Data could not be fetched or initialized.")

            # Wait briefly before checking for updates again
            time.sleep(10)  # Check for updates every 10 seconds

    except Exception as e:
        print(f"Error fetching top coins: {e}")

if __name__ == "__main__":
    analyze_and_record()
