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

app = Flask(__name__)

# Global cache for pre-analyzed coin data
coin_data_cache = {}

# Step 1: Collect top 30 coins by trading volume using CCXT
def get_top_30_coins():
    exchange = ccxt.binance()
    tickers = exchange.fetch_tickers()
    trading_volume = {
        symbol: tickers[symbol]['quoteVolume']
        for symbol in tickers if symbol.endswith('/USDT')
    }
    top_30 = sorted(trading_volume, key=trading_volume.get, reverse=True)[:30]
    return top_30

def fetch_data(symbol, exchange):
    ohlcv = exchange.fetch_ohlcv(symbol, timeframe='1h', limit=500)
    df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    return df

# Step 2: LSTM-based flow prediction
def prepare_lstm_data(data, look_back=60):
    scaler = MinMaxScaler(feature_range=(0, 1))
    scaled_data = scaler.fit_transform(data['close'].values.reshape(-1, 1))
    X, y = [], []
    for i in range(look_back, len(scaled_data)):
        X.append(scaled_data[i-look_back:i, 0])
        y.append(scaled_data[i, 0])
    return np.array(X), np.array(y), scaler

def train_lstm_model(X, y):
    model = Sequential()
    model.add(Input(shape=(X.shape[1], 1)))
    model.add(LSTM(units=50, return_sequences=False))
    model.add(Dense(units=1))
    model.compile(optimizer='adam', loss='mean_squared_error')
    model.fit(X, y, epochs=10, batch_size=32)
    return model

# Step 3: Prophet-based flow prediction
def train_prophet_model(data):
    prophet_data = data[['timestamp', 'close']].rename(columns={'timestamp': 'ds', 'close': 'y'})
    model = Prophet()
    model.fit(prophet_data)
    return model

# Step 4: Bollinger Bands for Stop Loss and Target Prices
def calculate_bollinger_bands(data, window=20):
    data['moving_avg'] = data['close'].rolling(window=window).mean()
    data['std_dev'] = data['close'].rolling(window=window).std()
    data['upper_band'] = data['moving_avg'] + (2 * data['std_dev'])
    data['lower_band'] = data['moving_avg'] - (2 * data['std_dev'])
    return data

def update_target_price(current_price, upper_band, lower_band):
    target_price = max(current_price, upper_band)
    stop_loss = min(current_price, lower_band)
    return target_price, stop_loss

# Pre-analyze coin data
def pre_analyze_coins():
    global coin_data_cache
    exchange = ccxt.binance()
    while True:
        try:
            top_coins = get_top_30_coins()
            for symbol in top_coins:
                try:
                    data = fetch_data(f"{symbol}/USDT", exchange)

                    # LSTM Flow Prediction
                    X, y, scaler = prepare_lstm_data(data)
                    X = np.reshape(X, (X.shape[0], X.shape[1], 1))
                    lstm_model = train_lstm_model(X, y)
                    predicted = lstm_model.predict(X)
                    predicted_prices = scaler.inverse_transform(predicted)

                    # Prophet Flow Prediction
                    prophet_model = train_prophet_model(data)
                    future = prophet_model.make_future_dataframe(periods=24, freq='H')
                    forecast = prophet_model.predict(future)

                    # Bollinger Bands for Stop Loss and Target Prices
                    data = calculate_bollinger_bands(data)
                    last_row = data.iloc[-1]
                    target_price, stop_loss = update_target_price(last_row['close'], last_row['upper_band'], last_row['lower_band'])

                    # Determine buy signal (example logic: if close price < lower band)
                    buy_signal = last_row['close'] < last_row['lower_band']

                    # Cache the analysis if buy signal is true
                    if buy_signal:
                        coin_data_cache[symbol] = {
                            "target_price": target_price,
                            "stop_loss": stop_loss,
                            "predicted_prices": predicted_prices.tolist(),
                            "prophet_forecast": forecast[['ds', 'yhat']].tail(24).to_dict(orient='records'),
                            "buy_signal": buy_signal
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
        coins_with_buy_signal = {symbol: data for symbol, data in coin_data_cache.items() if data.get("buy_signal")}
        html_template = """
        <html>
        <head><title>Coin Buy Signals</title></head>
        <body>
            <h1>Coins with Buy Signals</h1>
            <ul>
            {% for symbol in coins %}
                <li>
                    <a href="/coins/{{ symbol }}">{{ symbol }}</a>
                </li>
            {% endfor %}
            </ul>
        </body>
        </html>
        """
        return render_template_string(html_template, coins=list(coins_with_buy_signal.keys()))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/coins/<symbol>", methods=['GET'])
def get_coin_data(symbol):
    try:
        if symbol in coin_data_cache:
            return jsonify(coin_data_cache[symbol])
        else:
            return jsonify({"error": "Coin data not found."}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    # Start pre-analysis in a separate thread
    analysis_thread = threading.Thread(target=pre_analyze_coins, daemon=True)
    analysis_thread.start()
    app.run(host="0.0.0.0", port=5000)
