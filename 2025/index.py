import ccxt
import pandas as pd
import numpy as np

# 바이낸스 API 객체
binance = ccxt.binance({'enableRateLimit': True})

# ================================
# 1. 거래 심볼 관련
# ================================
def fetch_futures_top_symbols(limit=20):
    """
    바이낸스 선물 시장에서 24시간 거래대금 상위 심볼 가져오기 (USDT 무기한 기준)
    """
    futures_tickers = binance.fapiPublicGetTicker24hr()
    usdt_perpetual = [
        t for t in futures_tickers if t['symbol'].endswith('USDT') and not t['symbol'].endswith('BUSDUSDT')
    ]
    sorted_symbols = sorted(usdt_perpetual, key=lambda x: float(x['quoteVolume']), reverse=True)
    return [t['symbol'] for t in sorted_symbols[:limit]]

# ================================
# 2. 데이터 수집
# ================================
def fetch_futures_ohlcv(symbol, timeframe='1d', limit=30):
    """
    바이낸스 선물 심볼의 OHLCV 데이터 (기본 1일봉 기준)
    """
    market_symbol = symbol.replace('USDT', '/USDT')  # ex) BTCUSDT -> BTC/USDT
    ohlcv = binance.fapiPublicGetKlines({
        'symbol': symbol,
        'interval': timeframe,
        'limit': limit
    })
    df = pd.DataFrame(ohlcv, columns=[
        'timestamp', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'num_trades',
        'taker_buy_base_volume', 'taker_buy_quote_volume', 'ignore'
    ])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
    return df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]

# ================================
# 3. 기술 지표 계산 - 볼린저 밴드
# ================================
def calculate_bollinger_bands(df, period=20, std_dev=2):
    """
    볼린저 밴드 계산
    """
    df = df.copy()
    df['ma'] = df['close'].rolling(window=period).mean()
    df['std'] = df['close'].rolling(window=period).std()
    df['upper'] = df['ma'] + std_dev * df['std']
    df['lower'] = df['ma'] - std_dev * df['std']
    df['band_width'] = df['upper'] - df['lower']
    return df

# ================================
# 4. 추세 판단 (1일봉 기준)
# ================================
def get_trend_from_daily(symbol):
    """
    1일봉 기준 현재가가 전일 종가보다 높은지 낮은지로 추세 판단
    """
    df = fetch_futures_ohlcv(symbol, timeframe='1d', limit=2)
    if df.empty or len(df) < 2:
        return None  # 데이터 부족

    prev_close = df.iloc[-2]['close']
    curr_close = df.iloc[-1]['close']
    return 'uptrend' if curr_close > prev_close else 'downtrend'

# ================================
# 5. 15분봉 볼린저 밴드 분석
# ================================
def get_recent_bollinger_band(symbol, period=20, std_dev=2):
    """
    15분봉 기준 현재가의 볼린저 밴드 내 상대 위치 및 밴드 폭 계산
    """
    df = fetch_futures_ohlcv(symbol, timeframe='15m', limit=period + 1)
    if df.empty or len(df) < period:
        return None

    df = calculate_bollinger_bands(df, period=period, std_dev=std_dev)
    latest = df.iloc[-1]
    current_price = latest['close']
    
    band_range = latest['upper'] - latest['lower']
    if band_range == 0:
        return None  # 밴드폭 0이면 무효

    position_ratio = (current_price - latest['lower']) / band_range  # 밴드 내 위치 (0~1)
    return {
        'price': current_price,
        'upper': latest['upper'],
        'lower': latest['lower'],
        'ma': latest['ma'],
        'position_ratio': position_ratio,
        'band_width': band_range
    }

# ================================
# 6. 진입 조건 판단 함수
# ================================

def should_enter_short(symbol, band_info, min_target_profit=0.003, leverage=10):
    """
    숏 진입 조건:
    - 추세: 하락
    - 볼린저 밴드 상단 90% 이상 진입
    - 예상 이익이 수수료보다 커야 함
    """
    trend = get_trend_from_daily(symbol)
    if trend != 'downtrend' or band_info is None:
        return False

    position_ratio = band_info['position_ratio']
    band_width = band_info['band_width']
    entry_price = band_info['price']

    # 목표가 (하단 밴드) 기준 예상 수익률
    target_price = band_info['lower']
    expected_profit = (entry_price - target_price) / entry_price * leverage

    return position_ratio >= 0.9 and expected_profit >= min_target_profit


def should_enter_long(symbol, band_info, min_target_profit=0.003, leverage=10):
    """
    롱 진입 조건:
    - 추세: 상승
    - 볼린저 밴드 하단 10% 이하 진입
    - 예상 이익이 수수료보다 커야 함
    """
    trend = get_trend_from_daily(symbol)
    if trend != 'uptrend' or band_info is None:
        return False

    position_ratio = band_info['position_ratio']
    band_width = band_info['band_width']
    entry_price = band_info['price']

    # 목표가 (상단 밴드) 기준 예상 수익률
    target_price = band_info['upper']
    expected_profit = (target_price - entry_price) / entry_price * leverage

    return position_ratio <= 0.1 and expected_profit >= min_target_profit
