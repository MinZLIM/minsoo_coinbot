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


# Step 1: Collect top 30 coins by trading volume using Binance Futures
def get_top_30_coins():
    exchange = ccxt.binanceusdm()  # Binance Futures API
    tickers = exchange.fetch_tickers()
    trading_volume = {
        symbol: tickers[symbol]['quoteVolume']
        for symbol in tickers if symbol.endswith('USDT')
    }
    top_30 = sorted(trading_volume, key=trading_volume.get, reverse=True)[:30]
    return top_30

def fetch_data(symbol, exchange, timeframe="3m"):
    ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=500)
    df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
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
    
def update_target_price(entry_price, upper_band, lower_band, market_flow):
    if market_flow == "long":
        # For long, target above entry and stop loss below
        target_price = upper_band  # Or a percentage above the entry price
        stop_loss = lower_band  # Or a percentage below the entry price
    elif market_flow == "short":
        # For short, target below entry and stop loss above
        target_price = lower_band  # Or a percentage below the entry price
        stop_loss = upper_band  # Or a percentage above the entry price
    else:
        raise ValueError("Invalid market flow direction")
    return target_price, stop_loss


# Step 5: Analyze market flow to determine Long/Short signals
def analyze_market_flow(data):
    # Calculate short-term and long-term moving averages
    data['short_term_ma'] = data['close'].rolling(window=20).mean()
    data['long_term_ma'] = data['close'].rolling(window=50).mean()

    # Determine flow direction
    last_short_ma = data['short_term_ma'].iloc[-1]
    last_long_ma = data['long_term_ma'].iloc[-1]

    if last_short_ma > last_long_ma:
        return "long"  # Upward trend
    elif last_short_ma < last_long_ma:
        return "short"  # Downward trend
    else:
        return "neutral"


# Utility Functions
def analyze_coin(symbol, exchange, timeframe="3m"):
    """
    Analyzes a single coin to generate trading signals.
    """
    data = fetch_data(symbol, exchange, timeframe=timeframe)

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

    # Market flow analysis for Long/Short signal
    market_flow = analyze_market_flow(data)

    return {
        "entry_price": last_row['close'],
        "target_price": target_price,
        "stop_loss": stop_loss,
        "market_flow": market_flow
    }
