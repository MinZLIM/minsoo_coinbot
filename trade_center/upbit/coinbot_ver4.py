import ccxt
import pandas as pd
import numpy as np
from flask import Flask, jsonify, render_template_string
from prophet import Prophet
from keras.models import Sequential
from keras.layers import LSTM, Dense, Input
from sklearn.preprocessing import MinMaxScaler
import threading
import time
from fucntion import ccxt_function
from urllib.parse import unquote, quote


app = Flask(__name__)

# Global cache for pre-analyzed coin data
coin_data_cache = {}


# Pre-analyze coin data
def pre_analyze_coins():
    global coin_data_cache
    exchange = ccxt.binanceusdm()
    while True:
        try:
            top_coins = ccxt_function.get_top_30_coins()
            for symbol in top_coins:
                try:
                    data = ccxt_function.fetch_data(f"{symbol}", exchange, timeframe="3m")

                    # LSTM Flow Prediction
                    X, y, scaler = ccxt_function.prepare_lstm_data(data)
                    X = np.reshape(X, (X.shape[0], X.shape[1], 1))
                    lstm_model = ccxt_function.train_lstm_model(X, y)
                    predicted = lstm_model.predict(X)
                    predicted_prices = scaler.inverse_transform(predicted)

                    # Prophet Flow Prediction
                    prophet_model = ccxt_function.train_prophet_model(data)
                    future = prophet_model.make_future_dataframe(periods=24, freq='H')
                    forecast = prophet_model.predict(future)

                    # Bollinger Bands for Stop Loss and Target Prices
                    data = ccxt_function.calculate_bollinger_bands(data)
                    last_row = data.iloc[-1]
                    target_price, stop_loss = ccxt_function.update_target_price(last_row['close'], last_row['upper_band'], last_row['lower_band'])

                    # Market flow analysis for Long/Short signal
                    market_flow = ccxt_function.analyze_market_flow(data)

                    # Cache the analysis with flow signal
                    coin_data_cache[symbol] = {
                        "entry_price": last_row['close'],
                        "target_price": target_price,
                        "stop_loss": stop_loss,
                        "market_flow": market_flow
                    }
                except Exception as e:
                    coin_data_cache[symbol] = {"error": str(e)}
            time.sleep(3600)  # Update every hour
        except Exception as e:
            print(f"Error in pre_analyze_coins: {e}")

# Flask Routes
@app.route("/coins", methods=['GET'])
def get_coins():
    try:
        # Filter coins with buy signals
        coins_with_signals = {symbol: data for symbol, data in coin_data_cache.items() if data.get("market_flow") in ["long", "short"]}
        html_template = """
        <html>
        <head><title>Coin Trading Signals</title></head>
        <body>
            <h1>Coins with Trading Signals</h1>
            <ul>
            {% for symbol, data in coins.items() %}
                <li>
                    <a href="/coins/{{ symbol | urlencode }}">{{ symbol }}</a> - {{ data["market_flow"] | capitalize }}
                </li>
            {% endfor %}
            </ul>
        </body>
        </html>
        """
        return render_template_string(html_template, coins=coins_with_signals)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/coins/<path:symbol>", methods=['GET'])
def get_coin_data(symbol):
    try:
        symbol = unquote(symbol)  # Decode the URL-encoded symbol
        if symbol in coin_data_cache:
            coin_data = coin_data_cache[symbol]
            return jsonify({
                "entry_price": coin_data.get("entry_price"),
                "target_price": coin_data.get("target_price"),
                "stop_loss": coin_data.get("stop_loss"),
                "market_flow": coin_data.get("market_flow")
            })
        else:
            return jsonify({"error": f"Coin data for {symbol} not found."}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500
        
if __name__ == "__main__":
    # Start pre-analysis in a separate thread
    analysis_thread = threading.Thread(target=pre_analyze_coins, daemon=True)
    analysis_thread.start()
    app.run(host="0.0.0.0", port=5000)
