import ccxt
import pandas as pd
import numpy as np
from flask import Flask, jsonify
from prophet import Prophet
from keras.models import Sequential
from keras.layers import LSTM, Dense, Input
from sklearn.preprocessing import MinMaxScaler
import time
import pandas_ta as ta

# Step 1: Collect top 5 coins by trading volume using Binance Futures
def get_top_30_coins():
    exchange = ccxt.binanceusdm()  # Binance Futures API
    tickers = exchange.fetch_tickers()
    trading_volume = {
        symbol: tickers[symbol]['quoteVolume']
        for symbol in tickers if symbol.endswith('USDT')
    }
    top_30 = sorted(trading_volume, key=trading_volume.get, reverse=True)[:30]
    return top_30

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

def calculate_indicators(symbol, exchange, bollinger_period=20, bollinger_std_dev=2, rsi_period=14, stochastic_k_period=14, stochastic_d_period=3, adx_period=14):
    """
    Calculate Bollinger Bands, RSI, Stochastic, and ADX indicators.
    """

    df = fetch_data(symbol, exchange)
    # Bollinger Bands
    bb = ta.bbands(df['close'], length=bollinger_period, std=bollinger_std_dev)
    df['upper_band'] = bb['BBU_20_2.0']
    df['middle_band'] = bb['BBM_20_2.0']
    df['lower_band'] = bb['BBL_20_2.0']

    # RSI
    df['rsi'] = ta.rsi(df['close'], length=rsi_period)

    # Stochastic Oscillator
    stoch = ta.stoch(df['high'], df['low'], df['close'], k=stochastic_k_period, d=stochastic_d_period)
    df['slowk'] = stoch['STOCHk_14_3_3']
    df['slowd'] = stoch['STOCHd_14_3_3']

    # ADX
    adx = ta.adx(df['high'], df['low'], df['close'], length=adx_period)
    df['adx'] = adx['ADX_14']

    return df

def calculate_position_size(account_balance, risk_percentage):
    """
    Calculate position size based on account balance and risk percentage.
    """
    return account_balance * (risk_percentage / 100)

def calculate_targets(entry_price, risk_reward_ratio, stop_loss_percentage, action):
    """
    Calculate target price and stop loss price based on action (long or short).
    """
    if action == "long":
        stop_loss = entry_price * (1 - stop_loss_percentage / 100)
        target = entry_price + (entry_price - stop_loss) * risk_reward_ratio
    elif action == "short":
        stop_loss = entry_price * (1 + stop_loss_percentage / 100)
        target = entry_price - (stop_loss - entry_price) * risk_reward_ratio
    else:
        raise ValueError("Invalid action. Must be 'long' or 'short'.")

    return target, stop_loss

def decide_trade_action(indicators):
    """
    Decide whether to take a long, short, or no position based on indicators.
    """
    rsi = indicators['rsi'].iloc[-1]
    upper_band = indicators['upper_band'].iloc[-1]
    lower_band = indicators['lower_band'].iloc[-1]
    close_price = indicators['close'].iloc[-1]
    adx = indicators['adx'].iloc[-1]

    if rsi < 30 and close_price < lower_band and adx > 25:
        return "long"
    elif rsi > 70 and close_price > upper_band and adx > 25:
        return "short"
    else:
        return "none"
    