# -*- coding: utf-8 -*-
import ccxt
import pandas as pd
import pandas_ta as ta
import time
import logging
import os
import json
import websocket # websocket-client
from threading import Thread, Lock
from datetime import datetime, timezone, timedelta
import math
import uuid # 고유 ID 생성을 위해 추가
from functools import partial # for callback arguments
import numpy as np # NaN 처리를 위해 추가

# zoneinfo는 Python 3.9 이상, 하위 버전은 pytz 필요
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from pytz import timezone as ZoneInfo # Fallback for older Python

# ==============================================================================
# 사용자 설정 값
# ==============================================================================
# !!! 실제 거래용 API 키 및 시크릿 키 !!!
API_KEY = ""
API_SECRET = ""

# --- 실제 거래 설정 ---
SIMULATION_MODE = False
# ---------------------

LEVERAGE = 10
MAX_OPEN_POSITIONS = 4 # 최대 4개 포지션 (마진 기반 동적 크기)
TOP_N_SYMBOLS = 30
TIMEFRAME = '15m'
TIMEFRAME_MINUTES = 15
TARGET_ASSET = 'USDT'

# 지표 설정
BBANDS_PERIOD = 20
BBANDS_STDDEV = 2.0
STOCH_RSI_PERIOD = 14
STOCH_RSI_K = 3
STOCH_RSI_D = 3
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
CCI_PERIOD = 14

# 포지션 조건 설정 (*** Stoch 임계값 변경, 실시간 진입 ***)
STOCH_RSI_LONG_ENTRY = 10   # <<< --- 롱 진입 Stoch RSI 상한선 10으로 변경
STOCH_RSI_SHORT_ENTRY = 90  # <<< --- 숏 진입 Stoch RSI 하한선 90으로 변경
LONG_STOP_LOSS_FACTOR = 0.98
SHORT_STOP_LOSS_FACTOR = 1.02
POSITION_MONITORING_DELAY_MINUTES = 5
POSITION_TIMEOUT_HOURS = 1
WHIPSAW_BLACKLIST_HOURS = 1

# 수수료 설정 (참고용)
FEE_RATE = 0.0005

# 데이터 관리 설정
INITIAL_CANDLE_FETCH_LIMIT = 100
MAX_CANDLE_HISTORY = 200

# 시간대 설정
KST = ZoneInfo("Asia/Seoul")
UTC = timezone.utc

# Pandas 출력 옵션
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

# ==============================================================================
# 로깅 설정
# ==============================================================================
log_dir = os.path.dirname(os.path.abspath(__file__))
# 운영 로그
op_logger = logging.getLogger('operation')
op_logger.setLevel(logging.INFO)
op_formatter = logging.Formatter('%(asctime)s - %(levelname)s - [REAL_WS_RT] - %(message)s') # Prefix 변경 (RT: RealTime)
op_handler = logging.FileHandler(os.path.join(log_dir, 'operation_real_ws_rt.log')) # 파일명 변경
op_handler.setFormatter(op_formatter)
op_logger.addHandler(op_handler)
op_logger.addHandler(logging.StreamHandler())

# 매매 로그
trade_logger = logging.getLogger('trade')
trade_logger.setLevel(logging.INFO)
trade_formatter = logging.Formatter('%(asctime)s - [REAL_WS_RT] - %(message)s')
trade_handler = logging.FileHandler(os.path.join(log_dir, 'trade_real_ws_rt.log'))
trade_handler.setFormatter(trade_formatter)
trade_logger.addHandler(trade_handler)

# 자산 로그
asset_logger = logging.getLogger('asset')
asset_logger.setLevel(logging.INFO)
asset_formatter = logging.Formatter('%(asctime)s - [REAL_WS_RT] - %(message)s')
asset_handler = logging.FileHandler(os.path.join(log_dir, 'asset_real_ws_rt.log'))
asset_handler.setFormatter(asset_formatter)
asset_logger.addHandler(asset_handler)

# ==============================================================================
# 전역 변수 및 동기화 객체
# ==============================================================================
real_positions = {}
real_positions_lock = Lock()
total_trades = 0
winning_trades = 0
stats_lock = Lock()
historical_data = {}
data_lock = Lock()
blacklist = {}
blacklist_lock = Lock()
last_asset_log_time = datetime.now(UTC)
websocket_running = True
subscribed_symbols = set()
binance_rest = None

# ==============================================================================
# API 및 데이터 처리 함수 (이전과 동일)
# ==============================================================================
def initialize_binance_rest():
    global binance_rest; op_logger.info("Initializing CCXT REST...")
    try:
        binance_rest = ccxt.binance({'apiKey': API_KEY, 'secret': API_SECRET, 'enableRateLimit': True,'options': {'defaultType': 'future', 'adjustForTimeDifference': True}}); binance_rest.load_markets(); op_logger.info("CCXT REST initialized."); return True
    except ccxt.AuthenticationError: op_logger.error("REST API Auth Error!"); return False
    except Exception as e: op_logger.error(f"Failed init CCXT REST: {e}"); return False

def get_current_balance(asset=TARGET_ASSET):
    if not binance_rest: return 0.0
    try: balance = binance_rest.fetch_balance(params={'type': 'future'}); available = balance['free'].get(asset, 0.0); return float(available) if available else 0.0
    except Exception as e: op_logger.error(f"Error fetching balance: {e}"); return 0.0

def get_top_volume_symbols(n=TOP_N_SYMBOLS):
    if not binance_rest: return []
    op_logger.info(f"Fetching top {n} symbols...")
    try:
        tickers = binance_rest.fetch_tickers(); futures_tickers = {s: t for s, t in tickers.items() if '/' in s and s.endswith(f"/{TARGET_ASSET}:{TARGET_ASSET}")}; sorted_tickers = sorted(futures_tickers.values(), key=lambda x: x.get('quoteVolume', 0) or 0, reverse=True); top_symbols_ccxt = [t['symbol'].split(':')[0] for t in sorted_tickers[:n]]; return top_symbols_ccxt
    except Exception as e: op_logger.error(f"Error fetching top symbols: {e}"); return []

def fetch_initial_ohlcv(symbol_ccxt, timeframe=TIMEFRAME, limit=INITIAL_CANDLE_FETCH_LIMIT):
     if not binance_rest: return None
     try:
        ohlcv = binance_rest.fetch_ohlcv(symbol_ccxt, timeframe=timeframe, limit=limit);
        if not ohlcv: return None
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']); df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True); df.set_index('timestamp', inplace=True); return df
     except Exception as e: op_logger.error(f"Error fetching initial OHLCV for {symbol_ccxt}: {e}"); return None

def calculate_indicators(df):
    if df is None or len(df) < max(BBANDS_PERIOD, MACD_SLOW, CCI_PERIOD, STOCH_RSI_PERIOD + STOCH_RSI_K): return None
    try:
        df_copy = df.copy(); df_copy.ta.bbands(length=BBANDS_PERIOD, std=BBANDS_STDDEV, append=True)
        if len(df_copy) >= STOCH_RSI_PERIOD * 2: df_copy.ta.stochrsi(length=STOCH_RSI_PERIOD, rsi_length=STOCH_RSI_PERIOD, k=STOCH_RSI_K, d=STOCH_RSI_D, append=True)
        df_copy.ta.macd(fast=MACD_FAST, slow=MACD_SLOW, signal=MACD_SIGNAL, append=True); df_copy.ta.cci(length=CCI_PERIOD, append=True)
        rename_map = {f'BBL_{BBANDS_PERIOD}_{BBANDS_STDDEV}': 'BBL', f'BBM_{BBANDS_PERIOD}_{BBANDS_STDDEV}': 'BBM', f'BBU_{BBANDS_PERIOD}_{BBANDS_STDDEV}': 'BBU', f'STOCHRSIk_{STOCH_RSI_PERIOD}_{STOCH_RSI_PERIOD}_{STOCH_RSI_K}_{STOCH_RSI_D}': 'STOCHk', f'STOCHRSId_{STOCH_RSI_PERIOD}_{STOCH_RSI_PERIOD}_{STOCH_RSI_K}_{STOCH_RSI_D}': 'STOCHd', f'MACD_{MACD_FAST}_{MACD_SLOW}_{MACD_SIGNAL}': 'MACD', f'MACDs_{MACD_FAST}_{MACD_SLOW}_{MACD_SIGNAL}': 'MACD_signal', f'MACDh_{MACD_FAST}_{MACD_SLOW}_{MACD_SIGNAL}': 'MACD_hist', f'CCI_{CCI_PERIOD}_0.015': 'CCI'}
        existing_rename_map = {k: v for k, v in rename_map.items() if k in df_copy.columns}; df_copy.rename(columns=existing_rename_map, inplace=True)
        required_cols = ['BBL', 'BBU', 'STOCHk', 'STOCHd'] # Essential for logic
        if not all(col in df_copy.columns for col in required_cols) or df_copy[required_cols].isnull().any().any(): return None # Check NaN too
        return df_copy
    except Exception as e: op_logger.error(f"Indicator Calc Error: {e}"); return None

def set_isolated_margin(symbol_ccxt, leverage):
    if not binance_rest: return False
    op_logger.info(f"Setting ISOLATED margin {symbol_ccxt}/{leverage}x...")
    try:
        binance_rest.set_margin_mode('ISOLATED', symbol_ccxt, params={}); time.sleep(0.2)
        binance_rest.set_leverage(leverage, symbol_ccxt, params={}); op_logger.info(f"Set ISOLATED/{leverage}x OK."); return True
# 수정된 코드
    except ccxt.ExchangeError as e:
        if 'Margin type already set to ISOLATED' in str(e) or 'No need to change margin type' in str(e) :
             # op_logger.warning 코드를 다음 줄로 내리고 들여쓰기
            op_logger.warning(f"{symbol_ccxt} already ISOLATED.")
            try:
                binance_rest.set_leverage(leverage, symbol_ccxt, params={})
                op_logger.info(f"Set Leverage {leverage}x OK (was isolated).")
                return True
            except Exception as le:
                op_logger.error(f"Failed set leverage (already isolated) {symbol_ccxt}: {le}")
                return False
        elif 'position exists' in str(e):
            op_logger.warning(f"Can't change margin/lev, position exists? {symbol_ccxt}.")
            return False
        else:
            op_logger.error(f"Failed set ISOLATED {symbol_ccxt}: {e}")
            return False
    except Exception as e: op_logger.error(f"Unexpected error setting isolated {symbol_ccxt}: {e}"); return False

def place_market_order_real(symbol_ccxt, side, amount, current_price):
    if not binance_rest: op_logger.error("CCXT instance needed."); return None
    op_logger.info(f"[REAL ORDER] Attempt {side.upper()} {amount:.8f} {symbol_ccxt} @ market (approx {current_price:.4f})")
    try:
        market = binance_rest.market(symbol_ccxt); adjusted_amount = binance_rest.amount_to_precision(symbol_ccxt, amount)
        # op_logger.info(f"Adjusted amount: {adjusted_amount}") # Reduce verbosity
        if float(adjusted_amount) <= 0: op_logger.error(f"Adj amount <= 0."); return None
        order = binance_rest.create_market_order(symbol_ccxt, side, adjusted_amount)
        op_logger.info(f"[REAL ORDER PLACED] ID:{order.get('id')} Sym:{symbol_ccxt} Side:{side} Amt:{adjusted_amount}")
        trade_logger.info(f"REAL EXECUTE: {side.upper()} {symbol_ccxt}, OrdAmt:{adjusted_amount}, ID:{order.get('id')}")
        filled_amount, avg_price, ts_ms = order.get('filled'), order.get('average'), order.get('timestamp')
        if filled_amount is not None and avg_price is not None: op_logger.info(f"[REAL ORDER FILLED] {symbol_ccxt} Amt:{filled_amount}, AvgPx:{avg_price}"); return {'symbol': symbol_ccxt, 'average': avg_price, 'filled': filled_amount, 'timestamp': ts_ms}
        else: op_logger.warning(f"[REAL ORDER] Placed {symbol_ccxt} but fill confirmation pending."); return {'symbol': symbol_ccxt, 'average': current_price, 'filled': adjusted_amount, 'timestamp': int(datetime.now(UTC).timestamp()*1000)}
    except ccxt.InsufficientFunds as e: op_logger.error(f"[ORDER FAILED] Insufficient funds {symbol_ccxt}: {e}"); log_asset_status(); return None
    except ccxt.ExchangeError as e: op_logger.error(f"[ORDER FAILED] Exchange error {symbol_ccxt}: {e}"); return None
    except Exception as e: op_logger.error(f"[ORDER FAILED] Unexpected error {symbol_ccxt}: {e}", exc_info=True); return None

def check_symbol_in_blacklist(symbol_ws):
    with blacklist_lock: expiry = blacklist.get(symbol_ws);
    if expiry and datetime.now(UTC) < expiry: return True
    elif expiry: del blacklist[symbol_ws]; return False
    return False

def add_to_blacklist(symbol_ws):
    with blacklist_lock: expiry_time = datetime.now(UTC) + timedelta(hours=WHIPSAW_BLACKLIST_HOURS); blacklist[symbol_ws] = expiry_time; op_logger.warning(f"Blacklisted {symbol_ws} until {expiry_time.astimezone(KST).strftime('%H:%M:%S KST')}")

def log_asset_status():
    global last_asset_log_time, total_trades, winning_trades
    now = datetime.now(UTC);
    if now - last_asset_log_time >= timedelta(hours=1): balance = get_current_balance();
    with stats_lock: cur_trades, cur_wins = total_trades, winning_trades
    win_rate = (cur_wins / cur_trades * 100) if cur_trades > 0 else 0; asset_logger.info(f"Balance:{balance:.2f}, Trades:{cur_trades}, Wins:{cur_wins}(Inaccurate), WinRate:{win_rate:.2f}%"); last_asset_log_time = now

# ==============================================================================
# 웹소켓 처리 로직 (*** 실시간 진입 조건 체크 ***)
# ==============================================================================

def update_historical_data(symbol_ws, kline_data):
    global historical_data
    with data_lock:
        if symbol_ws not in historical_data: return False
        df = historical_data[symbol_ws]; kline_start_time = pd.to_datetime(kline_data['t'], unit='ms', utc=True)
        new_data = pd.DataFrame([{'timestamp': kline_start_time, 'open': float(kline_data['o']), 'high': float(kline_data['h']), 'low': float(kline_data['l']), 'close': float(kline_data['c']), 'volume': float(kline_data['v'])}]).set_index('timestamp')
        if kline_start_time in df.index: df.loc[kline_start_time] = new_data.iloc[0]
        else: df = pd.concat([df, new_data]);
        if len(df) > MAX_CANDLE_HISTORY: df = df.iloc[-MAX_CANDLE_HISTORY:]
        historical_data[symbol_ws] = df; return True

def process_kline_message(symbol_ws, kline_data):
    global total_trades, winning_trades, real_positions

    if not update_historical_data(symbol_ws, kline_data): return
    # is_candle_closed = kline_data['x'] # <<< --- 진입 시 이 플래그 사용 안 함!

    with data_lock: current_df = historical_data.get(symbol_ws)
    if current_df is None: return
    indicator_df = calculate_indicators(current_df)
    if indicator_df is None or indicator_df.empty: return # 지표 계산 불가 시 중단

    try:
        last_candle = indicator_df.iloc[-1]
        current_price = last_candle['close']
        # Get indicators, handle potential NaN
        stoch_k = last_candle.get('STOCHk', np.nan)
        stoch_d = last_candle.get('STOCHd', np.nan)
        bbl = last_candle.get('BBL', np.nan)
        bbu = last_candle.get('BBU', np.nan)
        if pd.isna(stoch_k) or pd.isna(stoch_d) or pd.isna(bbl) or pd.isna(bbu):
            # op_logger.debug(f"[{symbol_ws}] NaN found in indicators. K:{stoch_k}, D:{stoch_d}, BBL:{bbl}, BBU:{bbu}")
            return # 필수 지표 없으면 진행 불가
    except IndexError: return
    except Exception as e: op_logger.error(f"[{symbol_ws}] Indicator access error: {e}"); return

    now_utc = datetime.now(UTC)
    symbol_ccxt = symbol_ws.replace('USDT', '/USDT')

    positions_to_check = {}
    with real_positions_lock: positions_to_check = real_positions.copy()
    position_info = positions_to_check.get(symbol_ws)

    # === 1. 열린 포지션 정리 조건 확인 (변경 없음 - 5분 딜레이 후 실시간 체크) ===
    closed_symbols_in_loop = []
    for open_symbol, pos_info in positions_to_check.items():
        side, entry_time, entry_price, amount = pos_info['side'], pos_info['entry_time'], pos_info['entry_price'], pos_info['amount']
        should_close, close_reason = False, ""
        open_symbol_ccxt = open_symbol.replace('USDT','/USDT')
        time_since_entry = now_utc - entry_time

        if time_since_entry < timedelta(minutes=POSITION_MONITORING_DELAY_MINUTES): continue # 5분 딜레이 체크

        if time_since_entry >= timedelta(hours=POSITION_TIMEOUT_HOURS):
            should_close, close_reason = True, "Timeout"; op_logger.info(f"[{open_symbol}] Timeout detected.")

        # Stoch 정리 조건은 현재 업데이트된 심볼 기준 실시간 지표 사용
        if not should_close and open_symbol == symbol_ws:
            if side == 'long' and stoch_k < stoch_d: should_close, close_reason = True, "Stoch K < D"; op_logger.info(f"[{symbol_ws}] Stoch LONG exit met (RT).")
            elif side == 'short' and stoch_k > stoch_d: should_close, close_reason = True, "Stoch K > D"; op_logger.info(f"[{symbol_ws}] Stoch SHORT exit met (RT).")

        if should_close:
            op_logger.info(f"[{open_symbol}] Attempting close ({close_reason})...")
            close_price = current_price if open_symbol == symbol_ws else entry_price # 가격 결정
            order_result = place_market_order_real(open_symbol_ccxt, 'sell' if side == 'long' else 'buy', amount, close_price)
            if order_result:
                closed_symbols_in_loop.append(open_symbol)
                with real_positions_lock:
                    if open_symbol in real_positions: del real_positions[open_symbol]
                with stats_lock: total_trades += 1 # PNL 확인 로직 필요
                log_asset_status()
                if time_since_entry < timedelta(hours=WHIPSAW_BLACKLIST_HOURS): # 손실 여부 확인 필요
                     op_logger.warning(f"[{open_symbol}] Closed within {WHIPSAW_BLACKLIST_HOURS}h. Check PNL for blacklist.")
                     # add_to_blacklist(open_symbol)
            else: op_logger.error(f"[{open_symbol}] CLOSE ORDER FAILED ({close_reason}). Manual check needed!")

    # === 2. 신규 진입 조건 확인 (*** 실시간 체크로 변경 ***) ===
    with real_positions_lock: # Lock 필요
        position_info_after_exit = real_positions.get(symbol_ws) # 최신 상태 다시 확인
        current_open_count = len(real_positions)

        # 변경: is_candle_closed 조건 삭제! -> 포지션 없고, 블랙리스트 아니면 항상 체크
        if position_info_after_exit is None:
            if check_symbol_in_blacklist(symbol_ws): return
            if current_open_count >= MAX_OPEN_POSITIONS: return # 최대 포지션 개수(4개) 체크

            # --- 마진 기반 동적 크기 조정 로직 ---
            dynamic_margin_percentage = 0.0
            if current_open_count == 0: dynamic_margin_percentage = 0.25
            elif current_open_count == 1: dynamic_margin_percentage = 1/3
            elif current_open_count == 2: dynamic_margin_percentage = 0.50
            elif current_open_count == 3: dynamic_margin_percentage = 1.00
            else: op_logger.error(f"[{symbol_ws}] Invalid open count {current_open_count}."); return

            if dynamic_margin_percentage > 0:
                available_balance = get_current_balance()
                if available_balance <= 0: op_logger.warning(f"[{symbol_ws}] Zero/Negative balance."); return
                target_margin = available_balance * dynamic_margin_percentage
                if target_margin <= 0: op_logger.warning(f"[{symbol_ws}] Target margin <= 0."); return
                target_notional_value = target_margin * LEVERAGE
                # op_logger.info(f"[{symbol_ws}] Sizing: {current_open_count} open -> Target Margin: {target_margin:.2f} -> Target Notional: {target_notional_value:.2f}") # Reduce verbosity

                MIN_NOTIONAL_VALUE = 5.0
                if target_notional_value < MIN_NOTIONAL_VALUE:
                    # op_logger.warning(f"[{symbol_ws}] Target Notional ({target_notional_value:.2f}) < Min ({MIN_NOTIONAL_VALUE}). Skip.") # Reduce verbosity
                    return # *** 실시간 체크 시 이 로그가 매우 자주 나올 수 있음 ***

                # --- 실시간 진입 조건 체크 (Stoch, 강화된 임계값) ---
                target_side, stop_loss_price, take_profit_price = None, None, None
                assumed_entry_price = current_price

                # 롱 조건 (Stoch 임계값 10)
                long_stoch_cond = stoch_k <= STOCH_RSI_LONG_ENTRY and stoch_d <= STOCH_RSI_LONG_ENTRY and stoch_k > stoch_d
                if long_stoch_cond:
                    target_side = 'buy'; stop_loss_price = bbl * LONG_STOP_LOSS_FACTOR if bbl > 0 else None; take_profit_price = bbu if bbu > 0 else None

                # 숏 조건 (Stoch 임계값 90)
                short_stoch_cond = stoch_k >= STOCH_RSI_SHORT_ENTRY and stoch_d >= STOCH_RSI_SHORT_ENTRY and stoch_k < stoch_d
                if not target_side and short_stoch_cond:
                    target_side = 'sell'; stop_loss_price = assumed_entry_price * SHORT_STOP_LOSS_FACTOR if assumed_entry_price > 0 else None; take_profit_price = bbl if bbl > 0 else None

                # --- 진입 시도 ---
                if target_side and stop_loss_price is not None and take_profit_price is not None:
                    order_amount = target_notional_value / assumed_entry_price if assumed_entry_price > 0 else 0
                    if order_amount > 0:
                        potential_profit = 0
                        if target_side == 'buy': potential_profit = (take_profit_price - assumed_entry_price) * order_amount if take_profit_price > assumed_entry_price else 0
                        else: potential_profit = (assumed_entry_price - take_profit_price) * order_amount if assumed_entry_price > take_profit_price else 0
                        est_fee = (target_notional_value + abs(take_profit_price * order_amount)) * FEE_RATE

                        if potential_profit > est_fee:
                            # *** 실제 진입 시 매우 주의! 같은 캔들 내에서 반복 진입 시도 가능성 있음! ***
                            # -> 진입 시도 자체를 추적하는 플래그/타임스탬프 필요 가능성
                            op_logger.info(f"[{symbol_ws}] Entry condition MET (RT) for {target_side.upper()}. Preparing real order...")
                            if set_isolated_margin(symbol_ccxt, LEVERAGE):
                                order_result = place_market_order_real(symbol_ccxt, target_side, order_amount, assumed_entry_price)
                                if order_result:
                                    avg_price = order_result.get('average', assumed_entry_price)
                                    filled_amount = order_result.get('filled')
                                    entry_time_utc = datetime.fromtimestamp(order_result.get('timestamp', datetime.now(UTC).timestamp()*1000) / 1000, tz=UTC)
                                    if filled_amount and float(filled_amount) > 0:
                                        # Lock is held
                                        if len(real_positions) < MAX_OPEN_POSITIONS:
                                            real_positions[symbol_ws] = {'side': 'long' if target_side == 'buy' else 'short', 'entry_price': avg_price, 'amount': float(filled_amount), 'entry_time': entry_time_utc}
                                            op_logger.info(f"[{symbol_ws}] Entered & Tracking (RT). Active: {len(real_positions)}")
                                        else: # Safeguard
                                            op_logger.warning(f"[{symbol_ws}] Entered but max pos! Closing immediately!")
                                            place_market_order_real(symbol_ccxt, 'sell' if target_side == 'buy' else 'buy', filled_amount, avg_price)
                                    else: op_logger.error(f"[{symbol_ws}] Entry order zero/missing fill amt.")


# ==============================================================================
# 웹소켓 콜백 함수
# ==============================================================================
def on_message(wsapp, message):
    try:
        data = json.loads(message)
        if 'stream' in data and 'data' in data:
            stream_name, payload = data['stream'], data['data']
            if '@kline_' in stream_name:
                symbol_lower = stream_name.split('@')[0]; symbol_upper = symbol_lower.upper()
                if payload.get('e') == 'kline': process_kline_message(symbol_upper, payload['k'])
        elif 'result' in data and data.get('result') is None: pass
        elif 'e' in data and data['e'] == 'error': op_logger.error(f"WS API Error: {data.get('m')}")
    except json.JSONDecodeError: op_logger.error(f"JSON Decode Err: {message[:100]}")
    except Exception as e: op_logger.error(f"Msg Proc Error: {e}", exc_info=True)

def on_error(wsapp, error): op_logger.error(f"WebSocket Error: {error}")
def on_close(wsapp, close_status_code, close_msg):
    global websocket_running; op_logger.info(f"WebSocket closed. Code:{close_status_code}, Msg:{close_msg}"); websocket_running = False

def on_open(wsapp, symbols_ws, timeframe):
    global subscribed_symbols, historical_data
    op_logger.info("WebSocket connection opened.")
    op_logger.info(f"Subscribing to {len(symbols_ws)} kline streams ({timeframe})...")
    streams = [f"{s.lower()}@kline_{timeframe}" for s in symbols_ws]; subscribe_message = {"method": "SUBSCRIBE", "params": streams, "id": 1}
    wsapp.send(json.dumps(subscribe_message)); time.sleep(1); subscribed_symbols = set(symbols_ws); op_logger.info("Subscription message sent.")
    op_logger.info("Fetching initial historical data...")
    if not binance_rest: op_logger.error("REST instance failed."); wsapp.close(); return
    with data_lock:
        historical_data.clear(); fetch_errors = 0
        for symbol_upper in symbols_ws:
            symbol_ccxt = symbol_upper.replace('USDT', '/USDT')
            initial_df = fetch_initial_ohlcv(symbol_ccxt, timeframe)
            if initial_df is not None: historical_data[symbol_upper] = initial_df
            else: op_logger.warning(f"No initial data for {symbol_upper}."); fetch_errors += 1
            time.sleep(0.3)
    op_logger.info(f"Initial data fetch complete ({len(historical_data)} OK, {fetch_errors} errors).")
    print("-" * 80 + "\nWebSocket connected. Listening for REAL-TIME TRADING signals...\n" + "-" * 80)

# ==============================================================================
# 메인 실행 로직
# ==============================================================================
if __name__ == "__main__":
    start_time_str = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S %Z")
    op_logger.info(f"Bot starting at: {start_time_str}")

    if SIMULATION_MODE: op_logger.error("Set SIMULATION_MODE to False."); exit()
    if not API_KEY or not API_SECRET or API_KEY == "YOUR_BINANCE_API_KEY": op_logger.error("API Key/Secret needed!"); exit()

    op_logger.warning("="*30 + " REAL TRADING MODE ACTIVE - REAL-TIME ENTRY " + "="*30)
    op_logger.warning("!!! Entry decisions are now based on FORMING candle data !!!")
    op_logger.warning("!!! This significantly increases risk of false signals and whipsaws !!!")
    op_logger.warning(f"Max positions: {MAX_OPEN_POSITIONS}, Stoch Entry Thresh: {STOCH_RSI_LONG_ENTRY}/{STOCH_RSI_SHORT_ENTRY}, Dynamic MARGIN Sizing.")
    op_logger.warning("!!! MONITOR CLOSELY AND USE EXTREME CAUTION !!!")
    op_logger.warning("="*80)
    for i in range(7, 0, -1): print(f"Starting in {i}...", end='\r'); time.sleep(1)
    print("Starting now!      ")

    if not initialize_binance_rest(): op_logger.error("Exiting due to REST API failure."); exit()

    top_symbols_ccxt = get_top_volume_symbols(TOP_N_SYMBOLS)
    if not top_symbols_ccxt: op_logger.error("Could not fetch top symbols."); exit()
    top_symbols_ws = [s.replace('/', '') for s in top_symbols_ccxt]

    ws_url = f"wss://fstream.binance.com/stream?streams={'/'.join([f'{s.lower()}@kline_{TIMEFRAME}' for s in top_symbols_ws])}"
    op_logger.info(f"Connecting to WebSocket...")

    on_open_with_args = partial(on_open, symbols_ws=top_symbols_ws, timeframe=TIMEFRAME)
    wsapp = websocket.WebSocketApp(ws_url, on_open=on_open_with_args, on_message=on_message, on_error=on_error, on_close=on_close)

    try: wsapp.run_forever()
    except KeyboardInterrupt: op_logger.info("Keyboard interrupt.")
    finally:
        if websocket_running: wsapp.close()
        final_balance = get_current_balance()
        with stats_lock: final_trades, final_wins = total_trades, winning_trades
        final_win_rate = (final_wins / final_trades * 100) if final_trades > 0 else 0
        final_msg = f"Final Balance:{final_balance:.2f}, Trades:{final_trades}, Wins:{final_wins}(Inaccurate), WinRate:{final_win_rate:.2f}%"
        op_logger.info(final_msg); asset_logger.info(final_msg)
        op_logger.info("Real trading bot shutdown complete.")