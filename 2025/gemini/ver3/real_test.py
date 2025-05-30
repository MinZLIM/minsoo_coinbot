# -*- coding: utf-8 -*-
import ccxt
from ccxt.base.errors import OrderNotFound, RateLimitExceeded, ExchangeNotAvailable, OnMaintenance, RequestTimeout
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
import ssl # For WebSocket connection robustness

# zoneinfo는 Python 3.9 이상, 하위 버전은 pytz 필요
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from pytz import timezone as ZoneInfo # Fallback for older Python

# ==============================================================================
# 사용자 설정 값
# ==============================================================================
# !!! 실제 거래용 API 키 및 시크릿 키 !!!
API_KEY = "YOUR_BINANCE_API_KEY"
API_SECRET = "YOUR_BINANCE_API_SECRET"

# --- 실제 거래 설정 ---
SIMULATION_MODE = False
# ---------------------

LEVERAGE = 10
MAX_OPEN_POSITIONS = 4
TOP_N_SYMBOLS = 30
TIMEFRAME = '15m'
TIMEFRAME_MINUTES = 15
TARGET_ASSET = 'USDT'

# 지표 설정 (Stoch 단일 설정)
BBANDS_PERIOD = 20
BBANDS_STDDEV = 2.0
STOCH_RSI_PERIOD = 21
STOCH_RSI_K = 10
STOCH_RSI_D = 10

# 포지션 조건 설정
STOCH_RSI_LONG_ENTRY_THRESH = 15
STOCH_RSI_SHORT_ENTRY_THRESH = 85
LONG_STOP_LOSS_FACTOR = 0.99
SHORT_STOP_LOSS_FACTOR = 1.01
POSITION_MONITORING_DELAY_MINUTES = 5
WHIPSAW_BLACKLIST_HOURS = 2
TP_UPDATE_THRESHOLD_PERCENT = 0.1

# 동기화 및 재연결 설정
SYNC_INTERVAL_MINUTES = 5
RECONNECT_DELAY_SECONDS = 15        # <<< --- 재연결 변수 정의
MAX_RECONNECT_ATTEMPTS = 10         # <<< --- 재연결 변수 정의 (None이면 무한)

# 수수료 설정
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
# 로깅 설정 (*** 운영 로그 DEBUG로 변경 ***)
# ==============================================================================
log_dir = os.path.dirname(os.path.abspath(__file__))
# 운영 로그
op_logger = logging.getLogger('operation')
op_logger.setLevel(logging.DEBUG) # <<< --- 로그 레벨 DEBUG 설정됨
op_formatter = logging.Formatter('%(asctime)s - %(levelname)s - [REAL_WS_DEBUG] - %(message)s') # Prefix 변경
op_handler = logging.FileHandler(os.path.join(log_dir, 'operation_real_ws_debug.log')) # 파일명 변경
op_handler.setFormatter(op_formatter)
op_logger.addHandler(op_handler)
op_logger.addHandler(logging.StreamHandler()) # 콘솔에도 DEBUG 레벨까지 출력

# 매매 로그, 자산 로그 설정 (INFO 유지)
trade_logger = logging.getLogger('trade')
trade_logger.setLevel(logging.INFO)
trade_formatter = logging.Formatter('%(asctime)s - [REAL_WS_DEBUG] - %(message)s')
trade_handler = logging.FileHandler(os.path.join(log_dir, 'trade_real_ws_debug.log'))
trade_handler.setFormatter(trade_formatter)
trade_logger.addHandler(trade_handler)
asset_logger = logging.getLogger('asset')
asset_logger.setLevel(logging.INFO)
asset_formatter = logging.Formatter('%(asctime)s - [REAL_WS_DEBUG] - %(message)s')
asset_handler = logging.FileHandler(os.path.join(log_dir, 'asset_real_ws_debug.log'))
asset_handler.setFormatter(asset_formatter)
asset_logger.addHandler(asset_handler)

# ==============================================================================
# 전역 변수 및 동기화 객체
# ==============================================================================
real_positions = {}
real_positions_lock = Lock()
total_trades = 0; winning_trades = 0; stats_lock = Lock()
historical_data = {}; data_lock = Lock()
blacklist = {}; blacklist_lock = Lock()
entry_in_progress = {}; entry_lock = Lock()
last_asset_log_time = datetime.now(UTC)
websocket_running = True
subscribed_symbols = set()
binance_rest = None
wsapp = None

# ==============================================================================
# API 및 데이터 처리 함수 (set_isolated_margin 최종 수정본 포함)
# ==============================================================================

def initialize_binance_rest():
    global binance_rest; op_logger.info("Initializing CCXT REST...")
    try:
        options = {'defaultType': 'future', 'adjustForTimeDifference': True, "warnOnFetchOpenOrdersWithoutSymbol": False}
        binance_rest = ccxt.binance({'apiKey': API_KEY, 'secret': API_SECRET, 'enableRateLimit': True,'options': options});
        binance_rest.load_markets(); op_logger.info("CCXT REST initialized."); return True
    except ccxt.AuthenticationError: op_logger.error("REST API Auth Error!"); return False
    except Exception as e: op_logger.error(f"Failed init CCXT REST: {e}"); return False

def get_current_balance(asset=TARGET_ASSET):
    if not binance_rest: op_logger.error("CCXT not ready for balance check."); return 0.0
    try:
        # op_logger.debug("Fetching balance...") # DEBUG 로그 추가 가능
        balance = binance_rest.fetch_balance(params={'type': 'future'});
        available = balance['free'].get(asset, 0.0);
        # op_logger.debug(f"Available balance: {available}") # DEBUG 로그 추가 가능
        return float(available) if available else 0.0
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
        actual_limit = max(limit, STOCH_RSI_PERIOD * 2 + 50)
        ohlcv = binance_rest.fetch_ohlcv(symbol_ccxt, timeframe=timeframe, limit=actual_limit);
        if not ohlcv: return None
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']); df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True); df.set_index('timestamp', inplace=True); return df
     except Exception as e: op_logger.error(f"Error fetching initial OHLCV for {symbol_ccxt}: {e}"); return None

def calculate_indicators(df):
    required_len = max(BBANDS_PERIOD, STOCH_RSI_PERIOD * 2)
    if df is None or len(df) < required_len: return None
    try:
        df_copy = df.copy(); df_copy.ta.bbands(length=BBANDS_PERIOD, std=BBANDS_STDDEV, append=True)
        df_copy.ta.stochrsi(length=STOCH_RSI_PERIOD, rsi_length=STOCH_RSI_PERIOD, k=STOCH_RSI_K, d=STOCH_RSI_D, append=True)
        rename_map = {f'BBL_{BBANDS_PERIOD}_{BBANDS_STDDEV}': 'BBL', f'BBM_{BBANDS_PERIOD}_{BBANDS_STDDEV}': 'BBM', f'BBU_{BBANDS_PERIOD}_{BBANDS_STDDEV}': 'BBU', f'STOCHRSIk_{STOCH_RSI_PERIOD}_{STOCH_RSI_PERIOD}_{STOCH_RSI_K}_{STOCH_RSI_D}': 'STOCHk', f'STOCHRSId_{STOCH_RSI_PERIOD}_{STOCH_RSI_PERIOD}_{STOCH_RSI_K}_{STOCH_RSI_D}': 'STOCHd'}
        existing_rename_map = {k: v for k, v in rename_map.items() if k in df_copy.columns}; df_copy.rename(columns=existing_rename_map, inplace=True)
        required_cols = ['BBL', 'BBU', 'STOCHk', 'STOCHd']
        if not all(col in df_copy.columns for col in required_cols) or df_copy[required_cols].iloc[-1].isnull().any():
             # op_logger.debug(f"Indicator calc failed: Missing cols or NaN in last row. Cols: {df_copy.columns}") # DEBUG 로그
             return None
        return df_copy
    except Exception as e: op_logger.error(f"Indicator Calc Error: {e}"); return None

# --- 격리 마진 설정 (*** 최종 수정된 버전 ***) ---
def set_isolated_margin(symbol_ccxt, leverage):
    if not binance_rest: op_logger.error(f"[{symbol_ccxt}] CCXT instance not ready."); return False
    op_logger.info(f"Setting ISOLATED margin {symbol_ccxt}/{leverage}x...")
    try:
        binance_rest.set_margin_mode('ISOLATED', symbol_ccxt, params={}); time.sleep(0.2)
        binance_rest.set_leverage(leverage, symbol_ccxt, params={}); op_logger.info(f"Set ISOLATED/{leverage}x for {symbol_ccxt} OK."); return True
    except ccxt.ExchangeError as e:
        error_msg = str(e)
        if 'Margin type already set' in error_msg or 'No need to change margin type' in error_msg:
             op_logger.warning(f"{symbol_ccxt} already ISOLATED.")
             try: binance_rest.set_leverage(leverage, symbol_ccxt, params={}); op_logger.info(f"Set Leverage {leverage}x OK (was isolated)."); return True
             except Exception as le: op_logger.error(f"Failed set leverage (already iso) {symbol_ccxt}: {le}"); return False
        elif 'position exists' in error_msg: op_logger.warning(f"Can't change margin/lev, position exists? {symbol_ccxt}."); return False
        else: op_logger.error(f"Failed set ISOLATED margin/leverage for {symbol_ccxt}: {e}"); return False
    except Exception as e: op_logger.error(f"Unexpected error setting isolated {symbol_ccxt}: {e}", exc_info=True); return False

def place_market_order_real(symbol_ccxt, side, amount, current_price):
    if not binance_rest: op_logger.error("CCXT instance needed."); return None
    op_logger.info(f"[REAL ORDER] Attempt {side.upper()} {amount:.8f} {symbol_ccxt} @ market (approx {current_price:.4f})")
    try:
        market = binance_rest.market(symbol_ccxt); adjusted_amount = binance_rest.amount_to_precision(symbol_ccxt, amount)
        if float(adjusted_amount) <= 0: op_logger.error(f"[{symbol_ccxt}] Adj amount <= 0."); return None
        order = binance_rest.create_market_order(symbol_ccxt, side, adjusted_amount)
        op_logger.info(f"[REAL ORDER PLACED] ID:{order.get('id')} Sym:{symbol_ccxt} Side:{side} Amt:{adjusted_amount}")
        trade_logger.info(f"REAL EXECUTE: {side.upper()} {symbol_ccxt}, OrdAmt:{adjusted_amount}, ID:{order.get('id')}")
        filled_amount, avg_price, ts_ms = order.get('filled'), order.get('average'), order.get('timestamp')
        if filled_amount is not None and avg_price is not None: op_logger.info(f"[REAL ORDER FILLED] {symbol_ccxt} Amt:{filled_amount}, AvgPx:{avg_price}"); return {'symbol': symbol_ccxt, 'average': avg_price, 'filled': filled_amount, 'timestamp': ts_ms, 'id': order.get('id')}
        else: op_logger.warning(f"[REAL ORDER] Placed {symbol_ccxt} but fill confirmation pending."); return {'symbol': symbol_ccxt, 'average': current_price, 'filled': adjusted_amount, 'timestamp': int(datetime.now(UTC).timestamp()*1000), 'id': order.get('id')}
    except ccxt.InsufficientFunds as e: op_logger.error(f"[ORDER FAILED] Insufficient funds {symbol_ccxt}: {e}"); log_asset_status(); return None
    except ccxt.ExchangeError as e: op_logger.error(f"[ORDER FAILED] Exchange error {symbol_ccxt}: {e}"); return None
    except Exception as e: op_logger.error(f"[ORDER FAILED] Unexpected error {symbol_ccxt}: {e}", exc_info=True); return None

# --- SL/TP 주문 함수 (첫 라인 수정됨) ---
def place_stop_market_order(symbol_ccxt, side, stop_price, amount):
    if not binance_rest: op_logger.error(f"[{symbol_ccxt}] CCXT needed for SL."); return None
    op_logger.info(f"[REAL SL ORDER] Attempt {side.upper()} {amount:.8f} {symbol_ccxt} if price hits {stop_price:.4f}")
    try:
        market = binance_rest.market(symbol_ccxt); adjusted_amount = binance_rest.amount_to_precision(symbol_ccxt, amount); stop_price_str = binance_rest.price_to_precision(symbol_ccxt, stop_price)
        if float(adjusted_amount) <= 0: op_logger.error(f"[{symbol_ccxt}] SL Adj amount <= 0."); return None
        params = {'reduceOnly': True, 'stopPrice': stop_price_str}; order = binance_rest.create_order(symbol_ccxt, 'STOP_MARKET', side, adjusted_amount, None, params); order_id = order.get('id')
        op_logger.info(f"[REAL SL ORDER PLACED] ID:{order_id} Sym:{symbol_ccxt} Side:{side} StopPx:{stop_price_str} Amt:{adjusted_amount}"); trade_logger.info(f"REAL SL SET: {side.upper()} {symbol_ccxt}, Amt:{adjusted_amount}, StopPx:{stop_price_str}, ID:{order_id}"); return order_id
    except ccxt.ExchangeError as e: op_logger.error(f"[SL ORDER FAILED] Exch error {symbol_ccxt}: {e}"); return None
    except Exception as e: op_logger.error(f"[SL ORDER FAILED] Unexp error {symbol_ccxt}: {e}", exc_info=True); return None

def place_take_profit_market_order(symbol_ccxt, side, stop_price, amount):
    if not binance_rest: op_logger.error(f"[{symbol_ccxt}] CCXT needed for TP."); return None
    op_logger.info(f"[REAL TP ORDER] Attempt {side.upper()} {amount:.8f} {symbol_ccxt} if price hits {stop_price:.4f}")
    try:
        market = binance_rest.market(symbol_ccxt); adjusted_amount = binance_rest.amount_to_precision(symbol_ccxt, amount); stop_price_str = binance_rest.price_to_precision(symbol_ccxt, stop_price)
        if float(adjusted_amount) <= 0: op_logger.error(f"[{symbol_ccxt}] TP Adj amount <= 0."); return None
        params = {'reduceOnly': True, 'stopPrice': stop_price_str}; order = binance_rest.create_order(symbol_ccxt, 'TAKE_PROFIT_MARKET', side, adjusted_amount, None, params); order_id = order.get('id')
        op_logger.info(f"[REAL TP ORDER PLACED] ID:{order_id} Sym:{symbol_ccxt} Side:{side} StopPx:{stop_price_str} Amt:{adjusted_amount}"); trade_logger.info(f"REAL TP SET: {side.upper()} {symbol_ccxt}, Amt:{adjusted_amount}, StopPx:{stop_price_str}, ID:{order_id}"); return order_id
    except ccxt.ExchangeError as e: op_logger.error(f"[TP ORDER FAILED] Exch error {symbol_ccxt}: {e}"); return None
    except Exception as e: op_logger.error(f"[TP ORDER FAILED] Unexp error {symbol_ccxt}: {e}", exc_info=True); return None

def cancel_order(symbol_ccxt, order_id):
    if not binance_rest or not order_id: op_logger.debug(f"Cancel skipped {symbol_ccxt}, no id."); return True
    op_logger.info(f"Attempting to cancel order {order_id} for {symbol_ccxt}...")
    try: binance_rest.cancel_order(order_id, symbol_ccxt); op_logger.info(f"Successfully cancelled order {order_id}."); return True
    except OrderNotFound: op_logger.warning(f"Order {order_id} not found for {symbol_ccxt} (already exec/cancel?)."); return True
    except (ExchangeNotAvailable, OnMaintenance) as e: op_logger.error(f"Cannot cancel {order_id} due to exchange issue: {e}"); return False
    except RateLimitExceeded as e: op_logger.error(f"Rate limit cancelling {order_id}: {e}"); return False
    except ccxt.ExchangeError as e: op_logger.error(f"Failed cancel {order_id} for {symbol_ccxt}: {e}"); return False
    except Exception as e: op_logger.error(f"Unexp error cancelling {order_id} for {symbol_ccxt}: {e}"); return False

def check_symbol_in_blacklist(symbol_ws):
    with blacklist_lock: expiry = blacklist.get(symbol_ws);
    if expiry and datetime.now(UTC) < expiry: return True
    elif expiry: del blacklist[symbol_ws]; return False
    return False

def add_to_blacklist(symbol_ws, reason=""):
    with blacklist_lock: expiry_time = datetime.now(UTC) + timedelta(hours=WHIPSAW_BLACKLIST_HOURS); blacklist[symbol_ws] = expiry_time; op_logger.warning(f"Blacklisted {symbol_ws} until {expiry_time.astimezone(KST).strftime('%H:%M:%S KST')}. Reason: {reason}")

def log_asset_status():
    global last_asset_log_time, total_trades, winning_trades; now = datetime.now(UTC);
    if now - last_asset_log_time >= timedelta(hours=1):
        balance = get_current_balance()
        with stats_lock: cur_trades, cur_wins = total_trades, winning_trades
        win_rate = (cur_wins / cur_trades * 100) if cur_trades > 0 else 0.0
        asset_logger.info(f"Balance:{balance:.2f}, Trades:{cur_trades}, Wins:{cur_wins}(Inaccurate), WinRate:{win_rate:.2f}%")
        last_asset_log_time = now

# ==============================================================================
# 상태 동기화 로직 (fetch_open_orders 수정됨, 블랙리스트 로직 수정됨)
# ==============================================================================
def sync_positions_with_exchange():
    global real_positions, total_trades
    op_logger.info("[SYNC] Starting state synchronization...")
    if not binance_rest: op_logger.error("[SYNC] CCXT instance not ready."); return
    orders_to_cancel_sync = []; positions_to_close_untracked_sync = []
    try:
        exchange_pos = binance_rest.fetch_positions(); time.sleep(0.5)

        # *** 미체결 주문 조회 (심볼별 조회) ***
        op_logger.info(f"[SYNC] Fetching open orders for relevant symbols...")
        all_open_orders = []
        symbols_to_fetch_orders = set()
        with real_positions_lock: symbols_to_fetch_orders.update(real_positions.keys())
        symbols_to_fetch_orders.update(subscribed_symbols)
        for symbol_ws in list(symbols_to_fetch_orders):
            symbol_ccxt = symbol_ws.replace('USDT','/USDT')
            try:
                orders = binance_rest.fetch_open_orders(symbol=symbol_ccxt)
                if orders: all_open_orders.extend(orders)
                time.sleep(0.2)
            except RateLimitExceeded as e: op_logger.warning(f"[SYNC] Rate limit fetching orders for {symbol_ccxt}. Skipping rest."); break
            except Exception as e: op_logger.error(f"[SYNC] Error fetching open orders for {symbol_ccxt}: {e}")
        op_logger.info(f"[SYNC] Fetched total {len(all_open_orders)} open orders.")
        # *** 조회 방식 변경 완료 ***

        # *** 포지션 파싱 로직 수정됨 ***
        current_exchange_pos_dict = {}
        for pos in exchange_pos:
            try:
                amount = float(pos.get('info', {}).get('positionAmt', 0))
                if abs(amount) > 1e-9: # 유효한 포지션 수량 확인
                    symbol_ccxt = pos.get('symbol')
                    if symbol_ccxt: # 심볼 이름 확인
                        symbol_ws = symbol_ccxt.replace('/USDT', 'USDT')
                        current_exchange_pos_dict[symbol_ws] = {
                            'side': 'long' if amount > 0 else 'short',
                            'amount': abs(amount),
                            'entry_price': float(pos.get('entryPrice', 0)),
                            'symbol_ccxt': symbol_ccxt
                        }
            except Exception as parse_err:
                op_logger.error(f"[SYNC] Error parsing position data: {pos.get('info')}, Err: {parse_err}")
        # *** 파싱 로직 수정 완료 ***

        open_sl_tp_order_ids = {o['id'] for o in all_open_orders if o.get('type') in ['STOP_MARKET', 'TAKE_PROFIT_MARKET'] and o.get('status') == 'open'}
        open_orders_by_symbol = {}
        for o in all_open_orders:
             if o.get('status') == 'open':
                  symbol_ccxt = o.get('symbol');
                  if symbol_ccxt: symbol_ws = symbol_ccxt.replace('/USDT', 'USDT');
                  if symbol_ws not in open_orders_by_symbol: open_orders_by_symbol[symbol_ws] = []
                  open_orders_by_symbol[symbol_ws].append(o['id'])

        with real_positions_lock:
            local_pos_symbols = set(real_positions.keys()); exchange_pos_symbols = set(current_exchange_pos_dict.keys())
            symbols_to_remove_local = local_pos_symbols - exchange_pos_symbols
            for symbol_ws in symbols_to_remove_local:
                op_logger.warning(f"[SYNC] Local pos {symbol_ws} not on exchange. Removing."); local_pos_info = real_positions.pop(symbol_ws)
                sl_id, tp_id = local_pos_info.get('sl_order_id'), local_pos_info.get('tp_order_id'); symbol_ccxt = symbol_ws.replace('USDT', '/USDT')
                if sl_id and sl_id not in open_sl_tp_order_ids: op_logger.warning(f"[SYNC] Pos {symbol_ws} closed, SL {sl_id} gone -> Blacklisting."); add_to_blacklist(symbol_ws, reason="Closed on exchange, SL likely hit")
                else:
                    if sl_id in open_sl_tp_order_ids: orders_to_cancel_sync.append({'symbol_ccxt': symbol_ccxt, 'id': sl_id})
                if tp_id and tp_id in open_sl_tp_order_ids: orders_to_cancel_sync.append({'symbol_ccxt': symbol_ccxt, 'id': tp_id})

            symbols_to_add_or_close = exchange_pos_symbols - local_pos_symbols
            for symbol_ws in symbols_to_add_or_close:
                op_logger.warning(f"[SYNC] Untracked pos {symbol_ws} on exchange. Closing."); pos_info = current_exchange_pos_dict[symbol_ws]
                positions_to_close_untracked_sync.append({'symbol_ccxt': pos_info['symbol_ccxt'], 'side_to_close': 'sell' if pos_info['side'] == 'long' else 'buy', 'amount': pos_info['amount']})
                symbol_open_orders = open_orders_by_symbol.get(symbol_ws, []); symbol_ccxt = pos_info['symbol_ccxt']
                for order_id in symbol_open_orders: op_logger.warning(f"[SYNC] Marking related order {order_id} for cancel."); orders_to_cancel_sync.append({'symbol_ccxt': symbol_ccxt, 'id': order_id})

            symbols_to_check = local_pos_symbols.intersection(exchange_pos_symbols)
            for symbol_ws in symbols_to_check:
                local_info = real_positions[symbol_ws]; sl_id_local, tp_id_local = local_info.get('sl_order_id'), local_info.get('tp_order_id'); symbol_ccxt = symbol_ws.replace('USDT', '/USDT')
                if sl_id_local and sl_id_local not in open_sl_tp_order_ids: op_logger.warning(f"[SYNC] Local SL {sl_id_local} for {symbol_ws} not open. Clearing ID."); real_positions[symbol_ws]['sl_order_id'] = None
                if tp_id_local and tp_id_local not in open_sl_tp_order_ids: op_logger.warning(f"[SYNC] Local TP {tp_id_local} for {symbol_ws} not open. Clearing ID."); real_positions[symbol_ws]['tp_order_id'] = None; real_positions[symbol_ws]['current_tp_price'] = None
                symbol_open_orders = open_orders_by_symbol.get(symbol_ws, [])
                for order_id in symbol_open_orders:
                     if order_id not in [sl_id_local, tp_id_local]: op_logger.warning(f"[SYNC] Orphaned order {order_id} for {symbol_ws}? Marking cancel."); orders_to_cancel_sync.append({'symbol_ccxt': symbol_ccxt, 'id': order_id})

        op_logger.info(f"[SYNC] Actions: Close {len(positions_to_close_untracked_sync)}, Cancel {len(orders_to_cancel_sync)} orders.")
        for pos_data in positions_to_close_untracked_sync:
            symbol_ccxt = pos_data['symbol_ccxt']; op_logger.warning(f"[SYNC] Closing untracked {symbol_ccxt}...")
            try: ticker = binance_rest.fetch_ticker(symbol_ccxt); price = ticker['last']; place_market_order_real(symbol_ccxt, pos_data['side_to_close'], pos_data['amount'], price)
            except Exception as close_err: op_logger.error(f"[SYNC] Failed close untracked {symbol_ccxt}: {close_err}")
            time.sleep(0.5)

        unique_orders_to_cancel = {f"{o['symbol_ccxt']}_{o['id']}": o for o in orders_to_cancel_sync}.values()
        for order_info in unique_orders_to_cancel: cancel_order(order_info['symbol_ccxt'], order_info['id']); time.sleep(0.3)
        op_logger.info("[SYNC] State synchronization finished.")
    except RateLimitExceeded as e: op_logger.warning(f"[SYNC] Rate limit: {e}."); return
    except (ExchangeNotAvailable, OnMaintenance) as e: op_logger.warning(f"[SYNC] Exchange unavailable: {e}."); return
    except Exception as e: op_logger.error(f"[SYNC] Error: {e}", exc_info=True); return

# *** sync_state_periodically 함수 수정됨 (global 선언 추가, 들여쓰기 수정) ***
def sync_state_periodically(interval_seconds):
    global websocket_running # <<< --- global 선언 추가됨
    op_logger.info(f"State synchronization thread starting. Interval: {interval_seconds} seconds.")
    while websocket_running:
        try:
            time.sleep(interval_seconds)
            if websocket_running:
                sync_positions_with_exchange() # <<< --- 올바른 들여쓰기
        except Exception as e:
             op_logger.error(f"Error in sync_state_periodically loop: {e}", exc_info=True)
             time.sleep(60)
    op_logger.info("State synchronization thread finished.")

# ==============================================================================
# 웹소켓 처리 로직 (*** 상세 디버그 로그 추가됨 ***)
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

def try_update_tp(open_symbol, open_symbol_ccxt, side, amount, tp_order_id, current_tp_price, new_tp_target):
    price_diff_percent = abs(new_tp_target - current_tp_price) / current_tp_price * 100 if current_tp_price > 0 else 0
    if price_diff_percent >= TP_UPDATE_THRESHOLD_PERCENT:
        op_logger.info(f"[{open_symbol}] TP target changed: {current_tp_price:.4f} -> {new_tp_target:.4f}. Updating TP order...")
        if cancel_order(open_symbol_ccxt, tp_order_id):
            tp_side = 'sell' if side == 'long' else 'buy'; new_tp_id, new_tp_actual_price = None, None
            try: adj_amt_str = binance_rest.amount_to_precision(open_symbol_ccxt, amount); new_tp_str = binance_rest.price_to_precision(open_symbol_ccxt, new_tp_target); new_tp_actual_price = float(new_tp_str); new_tp_id = place_take_profit_market_order(open_symbol_ccxt, tp_side, new_tp_actual_price, float(adj_amt_str))
            except Exception as tp_e: op_logger.error(f"[{open_symbol}] Error placing new TP order: {tp_e}")
            if new_tp_id:
                with real_positions_lock:
                    if open_symbol in real_positions: real_positions[open_symbol]['tp_order_id'] = new_tp_id; real_positions[open_symbol]['current_tp_price'] = new_tp_actual_price; op_logger.info(f"[{open_symbol}] TP order updated to {new_tp_actual_price:.4f} (ID: {new_tp_id})."); return True
            else: op_logger.error(f"[{open_symbol}] Failed place new TP order.")
    return False

def process_kline_message(symbol_ws, kline_data):
    global total_trades, winning_trades, real_positions, entry_in_progress

    if not update_historical_data(symbol_ws, kline_data): return
    is_candle_closed = kline_data['x']

    with data_lock: current_df = historical_data.get(symbol_ws)
    if current_df is None: return
    indicator_df = calculate_indicators(current_df)
    if indicator_df is None or indicator_df.empty: return

    try: # 지표 추출
        last_candle = indicator_df.iloc[-1]; current_price = last_candle['close']
        stoch_k = last_candle.get('STOCHk', np.nan); stoch_d = last_candle.get('STOCHd', np.nan)
        bbl = last_candle.get('BBL', np.nan); bbu = last_candle.get('BBU', np.nan)
        if pd.isna(stoch_k) or pd.isna(stoch_d) or pd.isna(bbl) or pd.isna(bbu) or pd.isna(current_price): return
    except IndexError: return
    except Exception as e: op_logger.error(f"[{symbol_ws}] Indicator access error: {e}"); return

    now_utc = datetime.now(UTC)
    symbol_ccxt = symbol_ws.replace('USDT', '/USDT')

    positions_to_check = {}
    with real_positions_lock: positions_to_check = real_positions.copy()

    # === 1. 열린 포지션 관리 (TP 업데이트만 수행) ===
    processed_symbols_in_loop = set()
    for open_symbol, pos_info in positions_to_check.items():
        if open_symbol in processed_symbols_in_loop: continue
        side, entry_time, amount = pos_info['side'], pos_info['entry_time'], pos_info['amount']
        tp_order_id, current_tp_price = pos_info.get('tp_order_id'), pos_info.get('current_tp_price')
        open_symbol_ccxt = open_symbol.replace('USDT','/USDT')
        time_since_entry = now_utc - entry_time

        if time_since_entry < timedelta(minutes=POSITION_MONITORING_DELAY_MINUTES): continue

        # TP 업데이트 로직 (현재 메시지 심볼에 대해서만)
        if open_symbol == symbol_ws and tp_order_id and current_tp_price:
            new_tp_target = bbu if side == 'long' else bbl
            if new_tp_target and new_tp_target > 0:
                 if try_update_tp(open_symbol, open_symbol_ccxt, side, amount, tp_order_id, current_tp_price, new_tp_target):
                      processed_symbols_in_loop.add(open_symbol)

    # === 2. 신규 진입 조건 확인 (캔들 종료 시) ===
    with real_positions_lock, entry_lock:
        if symbol_ws in processed_symbols_in_loop: return

        position_info_after_exit = real_positions.get(symbol_ws)
        is_entry_attempted = entry_in_progress.get(symbol_ws, False)
        current_open_count = len(real_positions)

        if position_info_after_exit is None and not is_entry_attempted and is_candle_closed:
            if check_symbol_in_blacklist(symbol_ws):
                op_logger.debug(f"[{symbol_ws}] Skipping entry: Blacklisted.") # 디버그 로그
                return
            if current_open_count >= MAX_OPEN_POSITIONS:
                op_logger.debug(f"[{symbol_ws}] Skipping entry: Max positions reached ({current_open_count}/{MAX_OPEN_POSITIONS}).") # 디버그 로그
                return

            # --- 현재 지표 값 로깅 ---
            op_logger.debug(f"[{symbol_ws}] Checking Entry: Price={current_price:.4f}, K={stoch_k:.2f}, D={stoch_d:.2f}, BBL={bbl:.4f}, BBU={bbu:.4f}")

            # --- 진입 조건 체크 ---
            target_side, stop_loss_price_target, take_profit_price_target = None, None, None
            entry_trigger_price = current_price
            long_stoch_cond = stoch_k <= STOCH_RSI_LONG_ENTRY_THRESH and stoch_d <= STOCH_RSI_LONG_ENTRY_THRESH and stoch_k > stoch_d
            short_stoch_cond = stoch_k >= STOCH_RSI_SHORT_ENTRY_THRESH and stoch_d >= STOCH_RSI_SHORT_ENTRY_THRESH and stoch_k < stoch_d
            op_logger.debug(f"[{symbol_ws}] Condition Check: LongStoch={long_stoch_cond}, ShortStoch={short_stoch_cond}") # 조건 결과 로깅

            if long_stoch_cond: target_side = 'buy'; take_profit_price_target = bbu if bbu > 0 else None
            elif short_stoch_cond: target_side = 'sell'; take_profit_price_target = bbl if bbl > 0 else None

            if target_side and take_profit_price_target is not None:
                entry_in_progress[symbol_ws] = True
                try: # --- Start of entry process ---
                    available_balance = get_current_balance()
                    if available_balance <= 0: raise Exception("Zero balance")
                    dynamic_margin_percentage = 0.0
                    if current_open_count == 0: dynamic_margin_percentage = 0.25
                    elif current_open_count == 1: dynamic_margin_percentage = 1/3
                    elif current_open_count == 2: dynamic_margin_percentage = 0.50
                    elif current_open_count == 3: dynamic_margin_percentage = 1.00
                    else: raise Exception(f"Invalid open count {current_open_count}")
                    if dynamic_margin_percentage <= 0: raise Exception("Dynamic percentage <= 0")

                    target_margin = available_balance * dynamic_margin_percentage
                    if target_margin <= 0: raise Exception("Target margin <= 0")
                    target_notional_value = target_margin * LEVERAGE
                    op_logger.debug(f"[{symbol_ws}] Sizing: TargetMargin={target_margin:.2f}, TargetNotional={target_notional_value:.2f}")

                    MIN_NOTIONAL_VALUE = 5.0
                    if target_notional_value < MIN_NOTIONAL_VALUE: raise Exception(f"Target Notional ({target_notional_value:.2f}) < Min")
                    order_amount = target_notional_value / entry_trigger_price if entry_trigger_price > 0 else 0
                    if order_amount <= 0: raise Exception("Order amount <= 0")

                    temp_sl_price = entry_trigger_price * LONG_STOP_LOSS_FACTOR if target_side == 'buy' else entry_trigger_price * SHORT_STOP_LOSS_FACTOR
                    potential_profit = 0
                    if target_side == 'buy': potential_profit = (take_profit_price_target - entry_trigger_price) * order_amount if take_profit_price_target > entry_trigger_price else 0
                    else: potential_profit = (entry_trigger_price - take_profit_price_target) * order_amount if entry_trigger_price > take_profit_price_target else 0
                    est_fee = (target_notional_value + abs(take_profit_price_target * order_amount)) * FEE_RATE
                    op_logger.debug(f"[{symbol_ws}] Profitability: Potential={potential_profit:.4f}, Fee={est_fee:.4f}")

                    if potential_profit <= est_fee: raise Exception("Profitability check failed")

                    op_logger.info(f"[{symbol_ws}] Entry condition MET for {target_side.upper()}. Preparing order...")
                    if not set_isolated_margin(symbol_ccxt, LEVERAGE): raise Exception("Set isolated margin failed")

                    entry_order_result = place_market_order_real(symbol_ccxt, target_side, order_amount, entry_trigger_price)
                    if not entry_order_result: raise Exception("Entry market order failed")

                    entry_price = entry_order_result.get('average', entry_trigger_price)
                    filled_amount = entry_order_result.get('filled')
                    ts_ms = entry_order_result.get('timestamp');
                    if not ts_ms: ts_ms = int(datetime.now(UTC).timestamp() * 1000)
                    entry_time_utc = datetime.fromtimestamp(ts_ms / 1000, tz=UTC)

                    if not (filled_amount and float(filled_amount) > 0): raise Exception("Zero/missing fill amount")

                    op_logger.info(f"[{symbol_ws}] Entry filled. Placing SL/TP...")
                    final_sl_price = entry_price * LONG_STOP_LOSS_FACTOR if target_side == 'buy' else entry_price * SHORT_STOP_LOSS_FACTOR
                    final_tp_price = take_profit_price_target

                    sl_order_id, tp_order_id = None, None
                    adjusted_filled_amount = None
                    try: # SL/TP 주문
                        adjusted_filled_amount_str = binance_rest.amount_to_precision(symbol_ccxt, filled_amount)
                        final_sl_price_str = binance_rest.price_to_precision(symbol_ccxt, final_sl_price)
                        final_tp_price_str = binance_rest.price_to_precision(symbol_ccxt, final_tp_price)
                        adjusted_filled_amount = float(adjusted_filled_amount_str)
                        sl_side = 'sell' if target_side == 'buy' else 'buy'; tp_side = sl_side

                        if final_sl_price_str and adjusted_filled_amount > 0: sl_order_id = place_stop_market_order(symbol_ccxt, sl_side, float(final_sl_price_str), adjusted_filled_amount)
                        time.sleep(0.1)
                        if final_tp_price_str and adjusted_filled_amount > 0: tp_order_id = place_take_profit_market_order(symbol_ccxt, tp_side, float(final_tp_price_str), adjusted_filled_amount)

                        if sl_order_id is None or tp_order_id is None: raise Exception(f"SL ({sl_order_id}) or TP ({tp_order_id}) failed.")
                    except Exception as pe: # SL/TP 실패 시 롤백
                        op_logger.error(f"[{symbol_ws}] Error placing SL/TP: {pe}. ROLLING BACK ENTRY!")
                        if entry_order_result: place_market_order_real(symbol_ccxt, 'sell' if target_side == 'buy' else 'buy', filled_amount, entry_price)
                        cancel_order(symbol_ccxt, sl_order_id); cancel_order(symbol_ccxt, tp_order_id)
                        raise pe

                    # 포지션 정보 저장
                    current_open_count_final = len(real_positions) # Lock은 잡혀 있음
                    if current_open_count_final < MAX_OPEN_POSITIONS:
                        real_positions[symbol_ws] = {'side': 'long' if target_side == 'buy' else 'short', 'entry_price': entry_price, 'amount': float(filled_amount), 'entry_time': entry_time_utc, 'sl_order_id': sl_order_id, 'tp_order_id': tp_order_id, 'current_tp_price': float(final_tp_price_str) if final_tp_price_str else None}
                        op_logger.info(f"[{symbol_ws}] Entered & Tracking with SL/TP. Active: {len(real_positions)}")
                    else: # Safeguard
                        op_logger.warning(f"[{symbol_ws}] Entered & SL/TP OK, but max pos! Closing!")
                        cancel_order(symbol_ccxt, sl_order_id); cancel_order(symbol_ccxt, tp_order_id)
                        place_market_order_real(symbol_ccxt, 'sell' if target_side == 'buy' else 'buy', filled_amount, entry_price)

                except Exception as entry_err:
                    op_logger.info(f"[{symbol_ws}] Entry process skipped: {entry_err}") # INFO 레벨로 변경
                finally:
                    entry_in_progress[symbol_ws] = False # 중복 방지 해제


# ==============================================================================
# 웹소켓 콜백 함수
# ==============================================================================
def on_message(wsapp, message):
    # *** 들여쓰기 수정된 버전 ***
    try:
        data = json.loads(message)
        if 'stream' in data and 'data' in data:
            stream_name, payload = data['stream'], data['data']
            if '@kline_' in stream_name: # <<<--- 올바른 들여쓰기
                symbol_lower = stream_name.split('@')[0]
                symbol_upper = symbol_lower.upper()
                if payload.get('e') == 'kline': # <<<--- 올바른 들여쓰기
                    process_kline_message(symbol_upper, payload['k']) # <<<--- 올바른 위치
        elif 'result' in data and data.get('result') is None: pass
        elif 'e' in data and data['e'] == 'error': op_logger.error(f"WS API Error: {data.get('m')}")
    except json.JSONDecodeError: op_logger.error(f"JSON Decode Err: {message[:100]}")
    except Exception as e: op_logger.error(f"Msg Proc Error: {e}", exc_info=True)

def on_error(wsapp, error): op_logger.error(f"WebSocket Error: {error}")
def on_close(wsapp, close_status_code, close_msg):
    op_logger.info(f"WebSocket closed. Code:{close_status_code}, Msg:{close_msg}")

def on_open(wsapp, symbols_ws, timeframe):
    global subscribed_symbols, historical_data; op_logger.info("WebSocket connection opened."); op_logger.info(f"Subscribing to {len(symbols_ws)} streams ({timeframe})...")
    streams = [f"{s.lower()}@kline_{timeframe}" for s in symbols_ws]; subscribe_message = {"method": "SUBSCRIBE", "params": streams, "id": 1}
    wsapp.send(json.dumps(subscribe_message)); time.sleep(1); subscribed_symbols = set(symbols_ws); op_logger.info("Subscription message sent.")
    op_logger.info("Fetching initial historical data...")
    if not binance_rest: op_logger.error("REST instance failed."); wsapp.close(); return
    with data_lock: historical_data.clear(); fetch_errors = 0
    for symbol_upper in symbols_ws: symbol_ccxt = symbol_upper.replace('USDT', '/USDT'); initial_df = fetch_initial_ohlcv(symbol_ccxt, timeframe, limit=max(INITIAL_CANDLE_FETCH_LIMIT, STOCH_RSI_PERIOD*2));
    if initial_df is not None: historical_data[symbol_upper] = initial_df
    else: op_logger.warning(f"No initial data for {symbol_upper}."); fetch_errors += 1; time.sleep(0.3)
    op_logger.info(f"Initial data fetch complete ({len(historical_data)} OK, {fetch_errors} errors).")
    print("-" * 80 + "\nWebSocket connected. Listening for REAL TRADING signals...\n" + "-" * 80)


# ==============================================================================
# 메인 실행 로직 (재연결 로직 포함, wsapp 초기화 수정됨)
# ==============================================================================
if __name__ == "__main__":
    start_time_str = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S %Z")
    op_logger.info(f"Bot starting at: {start_time_str}")

    if SIMULATION_MODE: op_logger.error("Set SIMULATION_MODE to False."); exit()
    if not API_KEY or not API_SECRET or API_KEY == "YOUR_BINANCE_API_KEY": op_logger.error("API Key/Secret needed!"); exit()

    op_logger.warning("="*30 + " REAL TRADING MODE ACTIVE - Final Check V3 " + "="*30) # 제목 수정
    op_logger.warning("!!! Entry: Candle Close + Stoch | Exit: SL/TP Orders (TP Updated) OR Stoch Cross !!!")
    op_logger.warning(f"MaxPos:{MAX_OPEN_POSITIONS}, Stoch(P:{STOCH_RSI_PERIOD},K:{STOCH_RSI_K},D:{STOCH_RSI_D}) EntryThresh:{STOCH_RSI_LONG_ENTRY_THRESH}/{STOCH_RSI_SHORT_ENTRY_THRESH}")
    op_logger.warning("!!! MONITOR CLOSELY AND USE EXTREME CAUTION !!!")
    op_logger.warning("="*80)
    for i in range(7, 0, -1): print(f"Starting in {i}...", end='\r'); time.sleep(1)
    print("Starting now!      ")

    if not initialize_binance_rest(): op_logger.error("Exiting due to REST API failure."); exit()

    op_logger.info("Running initial state synchronization...")
    sync_positions_with_exchange()
    op_logger.info("Initial sync complete.")

    sync_thread = Thread(target=sync_state_periodically, args=(SYNC_INTERVAL_MINUTES * 60,), daemon=True)
    sync_thread.start()

    top_symbols_ccxt = get_top_volume_symbols(TOP_N_SYMBOLS)
    if not top_symbols_ccxt: op_logger.error("Could not fetch top symbols."); exit()
    top_symbols_ws = [s.replace('/', '') for s in top_symbols_ccxt]

    reconnect_attempts = 0
    wsapp = None # <<< --- wsapp None으로 초기화
    while websocket_running:
        if reconnect_attempts > 0:
            delay = min(RECONNECT_DELAY_SECONDS * (2**(reconnect_attempts-1)), 300)
            op_logger.info(f"Attempting reconnect #{reconnect_attempts} in {delay} seconds...")
            time.sleep(delay)

        if MAX_RECONNECT_ATTEMPTS is not None and reconnect_attempts >= MAX_RECONNECT_ATTEMPTS:
            op_logger.error("Maximum reconnect attempts reached. Stopping bot.")
            websocket_running = False
            break

        ws_url = f"wss://fstream.binance.com/stream?streams={'/'.join([f'{s.lower()}@kline_{TIMEFRAME}' for s in top_symbols_ws])}"
        op_logger.info(f"Connecting/Reconnecting to WebSocket...")

        on_open_with_args = partial(on_open, symbols_ws=top_symbols_ws, timeframe=TIMEFRAME)
        # *** 전역 변수 wsapp에 할당 ***
        wsapp = websocket.WebSocketApp(ws_url, on_open=on_open_with_args, on_message=on_message, on_error=on_error, on_close=on_close)

        try:
            wsapp.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
            op_logger.warning("WebSocket run_forever loop exited.")
            if not websocket_running: op_logger.info("Stop signal received."); break
            reconnect_attempts += 1

        except KeyboardInterrupt:
            op_logger.info("Keyboard interrupt detected. Stopping bot.")
            websocket_running = False
            # if wsapp and wsapp.sock and wsapp.sock.connected: wsapp.close() # finally에서 처리
            break
        except Exception as e:
            op_logger.error(f"Unhandled exception in WebSocket run_forever loop: {e}", exc_info=True)
            reconnect_attempts += 1

        op_logger.info("WebSocket connection attempt finished. Looping...")


    # --- 최종 정리 ---
    op_logger.info("Shutting down...")
    if sync_thread.is_alive():
         op_logger.info("Waiting for sync thread to finish...")
         sync_thread.join(timeout=5)

    # *** finally 블록 수정: wsapp None 체크 추가 ***
    if wsapp and wsapp.sock and wsapp.sock.connected:
        op_logger.info("Closing WebSocket connection explicitly...")
        wsapp.close()

    op_logger.info("Attempting to fetch final balance...")
    time.sleep(1)
    final_balance = get_current_balance()
    op_logger.info("Attempting to cancel remaining open orders...")
    try:
        open_orders = binance_rest.fetch_open_orders()
        if open_orders:
             op_logger.info(f"Found {len(open_orders)} open orders to potentially cancel.")
             for order in open_orders:
                 try: cancel_order(order['symbol'], order['id']); time.sleep(0.3)
                 except Exception as cancel_e: op_logger.error(f"Err cancelling {order['id']} for {order['symbol']}: {cancel_e}")
        else: op_logger.info("No open orders found to cancel.")
    except Exception as fetch_e: op_logger.error(f"Could not fetch/cancel open orders on shutdown: {fetch_e}")

    with stats_lock: final_trades, final_wins = total_trades, winning_trades
    final_win_rate = (final_wins / final_trades * 100) if final_trades > 0 else 0
    final_msg = f"Final Balance:{final_balance:.2f}, Trades:{final_trades}, Wins:{final_wins}(Inaccurate), WinRate:{final_win_rate:.2f}%"
    op_logger.info(final_msg); asset_logger.info(final_msg)
    op_logger.info("Real trading bot shutdown complete.")
