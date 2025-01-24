import time
import threading
import ccxt
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from function import ccxt_function

coin_data_cache = {}
log_file_path = "trading_log.txt"

def ensure_analysis_keys(analysis):
    required_keys = ["market_flow", "entry_price", "target_price", "stop_loss"]
    for key in required_keys:
        if key not in analysis:
            analysis[key] = None
    return analysis

def analyze_coins():
    global coin_data_cache
    cache_lock = threading.Lock()  # Lock for thread safety
    exchange = ccxt.binanceusdm()

    while True:
        try:
            top_coins = ccxt_function.get_top_5_coins()
            print("5개 분석 coin: {}".format(top_coins))
            for symbol in top_coins:
                
                try:
                    analysis = ccxt_function.analyze_coin(symbol, exchange)
                    analysis = ensure_analysis_keys(analysis)
                    
                    # Update cache safely
                    with cache_lock:
                        coin_data_cache[symbol] = analysis

                    # Log analysis data
                    try:
                        with open(log_file_path, "a") as log_file:
                            log_file.write(f"{symbol}: {analysis} - Time: {time.ctime()}\n")
                    except IOError as log_error:
                        print(f"Log file error: {log_error}")
                except Exception as e:
                    with cache_lock:
                        coin_data_cache[symbol] = {"error": str(e)}
        except Exception as e:
            print(f"Error during coin analysis: {e}")

        time.sleep(1)  # Adjust interval as necessary
