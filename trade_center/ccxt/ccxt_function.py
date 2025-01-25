import ccxt
import pandas as pd
import numpy as np
from flask import Flask, jsonify
from prophet import Prophet
from keras.models import Sequential
from keras.layers import LSTM, Dense, Input
from sklearn.preprocessing import MinMaxScaler
import time
import talib

# Step 1: Collect top 5 coins by trading volume using Binance Futures
def get_top_5_coins():
    exchange = ccxt.binanceusdm()  # Binance Futures API
    tickers = exchange.fetch_tickers()
    trading_volume = {
        symbol: tickers[symbol]['quoteVolume']
        for symbol in tickers if symbol.endswith('USDT')
    }
    top_5 = sorted(trading_volume, key=trading_volume.get, reverse=True)[:5]
    return top_5

def fetch_data(symbol, exchange, timeframe="5m"):
    ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=500)
    df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms").astype(str)
    df[["open", "high", "low", "close", "volume"]] = df[["open", "high", "low", "close", "volume"]].astype(float)
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

    # Training the model
    history = model.fit(X, y, epochs=10, batch_size=128, verbose=1)

    # Output final loss
    final_loss = history.history['loss'][-1]
    print(f"LSTM Training Completed. Final Loss: {final_loss:.6f}")

    return model

def convert_to_kst(forecast):
    forecast['ds'] = forecast['ds'].dt.tz_localize('UTC').dt.tz_convert('Asia/Seoul')
    return forecast

# Step 3: Prophet-based flow prediction
def train_prophet_model(data):
    prophet_data = data[['timestamp', 'close']].rename(columns={'timestamp': 'ds', 'close': 'y'})
    model = Prophet()
    model.fit(prophet_data)

    # Generate future predictions
    future = model.make_future_dataframe(periods=24, freq='15min')
    forecast = model.predict(future)
    forecast = convert_to_kst(forecast)

    # Print forecast summary
    print("\nProphet Forecast Sample:")
    print(forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(5))  # Print last 5 predictions

    return model

def lstm_analyze_coin(symbol, exchange, timeframe="3m"):
    print(f"Symbol: {symbol}")

    # Fetch data
    data = fetch_data(symbol, exchange, timeframe)

    # LSTM Flow Prediction
    X, y, scaler = prepare_lstm_data(data)
    X = np.reshape(X, (X.shape[0], X.shape[1], 1))
    lstm_model = train_lstm_model(X, y)
    predicted = lstm_model.predict(X)
    predicted_prices = scaler.inverse_transform(predicted)
    print(f"Predicted Prices (Original Scale): {predicted_prices[:5].flatten()}")  # Print first 5 predictions

def prophet_analyze_coin(symbol, exchange, timeframe="1H"):
    print(f"Symbol: {symbol}")

    # Fetch data
    data = fetch_data(symbol, exchange, timeframe)

    # Prophet Flow Prediction
    prophet_model = train_prophet_model(data)
    future = prophet_model.make_future_dataframe(periods=24, freq='H')
    forecast = prophet_model.predict(future)
    print(f"Forecast Data: {forecast[['ds', 'yhat']].tail(5)}")  # Print last 5 forecasts

def calculate_indicators(df, bollinger_period=20, bollinger_std_dev=2, rsi_period=14, stochastic_k_period=14, stochastic_d_period=3, adx_period=14):
    """
    Calculate Bollinger Bands, RSI, Stochastic, and ADX indicators.
    """
    # Bollinger Bands
    df['upper_band'], df['middle_band'], df['lower_band'] = talib.BBANDS(
        df['close'],
        timeperiod=bollinger_period,
        nbdevup=bollinger_std_dev,
        nbdevdn=bollinger_std_dev,
        matype=0
    )

    # RSI
    df['rsi'] = talib.RSI(df['close'], timeperiod=rsi_period)

    # Stochastic Oscillator
    df['slowk'], df['slowd'] = talib.STOCH(
        df['high'],
        df['low'],
        df['close'],
        fastk_period=stochastic_k_period,
        slowk_period=stochastic_d_period,
        slowk_matype=0,
        slowd_period=stochastic_d_period,
        slowd_matype=0
    )

    # ADX
    df['adx'] = talib.ADX(df['high'], df['low'], df['close'], timeperiod=adx_period)

    return df