# -*- coding: utf-8 -*-
# Imports
import ccxt
from ccxt.base.errors import OrderNotFound, RateLimitExceeded, ExchangeNotAvailable, OnMaintenance, InvalidNonce, RequestTimeout, AuthenticationError
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
import uuid
from functools import partial
import numpy as np

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from pytz import timezone as ZoneInfo

# ==============================================================================
# 사용자 설정 값
# ==============================================================================
API_KEY = ""
API_SECRET = ""
SIMULATION_MODE = False
LEVERAGE = 10
MAX_OPEN_POSITIONS = 4
TOP_N_SYMBOLS = 30
TIMEFRAME = '15m'
TIMEFRAME_MINUTES = 15
TARGET_ASSET = 'USDT'
BBANDS_PERIOD = 20
BBANDS_STDDEV = 2.0
STOCH_RSI_PERIOD = 21
STOCH_RSI_K = 10
STOCH_RSI_D = 10
STOCH_RSI_LONG_ENTRY_THRESH = 15
STOCH_RSI_SHORT_ENTRY_THRESH = 85
LONG_STOP_LOSS_FACTOR = 0.99
SHORT_STOP_LOSS_FACTOR = 1.01
POSITION_MONITORING_DELAY_MINUTES = 5
WHIPSAW_BLACKLIST_HOURS = 2
TP_UPDATE_THRESHOLD_PERCENT = 0.1
REST_SYNC_INTERVAL_MINUTES = 30
SYMBOL_UPDATE_INTERVAL_HOURS = 2
LISTEN_KEY_REFRESH_INTERVAL_MINUTES = 30
FEE_RATE = 0.0005
INITIAL_CANDLE_FETCH_LIMIT = 100
MAX_CANDLE_HISTORY = 200
KST = ZoneInfo("Asia/Seoul")
UTC = timezone.utc
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

# ==============================================================================
# 로깅 설정
# ==============================================================================
log_dir = os.path.dirname(os.path.abspath(__file__))
log_prefix = "[REAL_UDS_AUTO_SYM_RECON]"
# 운영 로그
op_logger = logging.getLogger('operation')
op_logger.setLevel(logging.INFO)
op_formatter = logging.Formatter(f'%(asctime)s - %(levelname)s - {log_prefix} - %(message)s')
op_handler = logging.FileHandler(os.path.join(log_dir, 'operation_real_uds_auto_sym_recon.log'))
op_handler.setFormatter(op_formatter)
op_logger.addHandler(op_handler)
op_logger.addHandler(logging.StreamHandler())
# 매매 로그
trade_logger = logging.getLogger('trade')
trade_logger.setLevel(logging.INFO)
trade_formatter = logging.Formatter(f'%(asctime)s - {log_prefix} - %(message)s')
trade_handler = logging.FileHandler(os.path.join(log_dir, 'trade_real_uds_auto_sym_recon.log'))
trade_handler.setFormatter(trade_formatter)
trade_logger.addHandler(trade_handler)
# 자산 로그
asset_logger = logging.getLogger('asset')
asset_logger.setLevel(logging.INFO)
asset_formatter = logging.Formatter(f'%(asctime)s - {log_prefix} - %(message)s')
asset_handler = logging.FileHandler(os.path.join(log_dir, 'asset_real_uds_auto_sym_recon.log'))
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
kline_websocket_running = False
user_websocket_running = False
kline_wsapp = None
user_wsapp = None
listen_key = None
listen_key_lock = Lock()
subscribed_symbols = set()
subscribed_symbols_lock = Lock()
shutdown_requested = False
kline_thread = None
binance_rest = None

# ==============================================================================
# API 및 데이터 처리 함수 (모든 함수 정의 포함)
# ==============================================================================
def initialize_binance_rest():
    global binance_rest; op_logger.info("Initializing CCXT REST...")
    if not API_KEY or API_KEY == "YOUR_BINANCE_API_KEY" or not API_SECRET:
        op_logger.error("API Key/Secret not configured properly!"); return False
    try:
        binance_rest = ccxt.binance({
            'apiKey': API_KEY, 'secret': API_SECRET, 'enableRateLimit': True,
            'options': { 'defaultType': 'future', 'adjustForTimeDifference': True }})
        binance_rest.load_markets()
        try:
            server_time = binance_rest.fetch_time()
            op_logger.info(f"Server time: {datetime.fromtimestamp(server_time / 1000, tz=UTC)}")
        except Exception as time_err: op_logger.warning(f"Failed to fetch server time: {time_err}")
        op_logger.info("CCXT REST initialized successfully."); return True
    except AuthenticationError: op_logger.error("REST API Authentication Error!"); return False
    except RequestTimeout: op_logger.error("REST API connection timed out."); return False
    except ExchangeNotAvailable: op_logger.error("Exchange is currently unavailable."); return False
    except Exception as e: op_logger.error(f"Failed to initialize CCXT REST: {e}", exc_info=True); return False

def get_current_balance(asset=TARGET_ASSET):
    if not binance_rest: return 0.0
    try:
        balance = binance_rest.fetch_balance(params={'type': 'future'})
        available = balance['free'].get(asset, 0.0)
        return float(available) if available else 0.0
    except (RequestTimeout, ExchangeNotAvailable, OnMaintenance) as e:
        op_logger.warning(f"Error fetching balance (Network/Exchange Issue): {e}"); return 0.0
    except Exception as e: op_logger.error(f"Unexpected error fetching balance: {e}"); return 0.0

def get_top_volume_symbols(n=TOP_N_SYMBOLS):
    if not binance_rest: return []
    op_logger.info(f"Fetching top {n} symbols by quote volume...")
    try:
        tickers = binance_rest.fetch_tickers()
        futures_tickers = {s: t for s, t in tickers.items() if '/' in s and s.endswith(f"/{TARGET_ASSET}:{TARGET_ASSET}") and t.get('quoteVolume') is not None}
        if not futures_tickers: op_logger.warning("No USDT perpetual futures tickers found."); return []
        sorted_tickers = sorted(futures_tickers.values(), key=lambda x: x.get('quoteVolume', 0), reverse=True)
        top_symbols_ccxt = [t['symbol'].split(':')[0] for t in sorted_tickers[:n]]
        op_logger.info(f"Fetched top {len(top_symbols_ccxt)} symbols: {top_symbols_ccxt[:5]}...")
        return top_symbols_ccxt
    except (RequestTimeout, ExchangeNotAvailable, OnMaintenance) as e:
        op_logger.warning(f"Error fetching top symbols (Network/Exchange Issue): {e}"); return []
    except Exception as e: op_logger.error(f"Error fetching top symbols: {e}"); return []

def fetch_initial_ohlcv(symbol_ccxt, timeframe=TIMEFRAME, limit=INITIAL_CANDLE_FETCH_LIMIT):
     if not binance_rest: return None
     try:
         actual_limit = max(limit, STOCH_RSI_PERIOD * 2 + 50)
         op_logger.debug(f"Fetching initial {actual_limit} candles for {symbol_ccxt} ({timeframe})...")
         ohlcv = binance_rest.fetch_ohlcv(symbol_ccxt, timeframe=timeframe, limit=actual_limit)
         if not ohlcv: op_logger.warning(f"No OHLCV data returned for {symbol_ccxt}."); return None
         df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
         df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True); df.set_index('timestamp', inplace=True)
         op_logger.debug(f"Successfully fetched {len(df)} candles for {symbol_ccxt}.")
         return df
     except (RequestTimeout, ExchangeNotAvailable, OnMaintenance) as e:
        op_logger.warning(f"Error fetching initial OHLCV for {symbol_ccxt} (Network/Exchange Issue): {e}"); return None
     except Exception as e: op_logger.error(f"Error fetching initial OHLCV for {symbol_ccxt}: {e}"); return None

def calculate_indicators(df):
    required_len = max(BBANDS_PERIOD, STOCH_RSI_PERIOD * 2)
    if df is None or len(df) < required_len: return None
    try:
        df_copy = df.copy()
        df_copy.ta.bbands(length=BBANDS_PERIOD, std=BBANDS_STDDEV, append=True)
        df_copy.ta.stochrsi(length=STOCH_RSI_PERIOD, rsi_length=STOCH_RSI_PERIOD, k=STOCH_RSI_K, d=STOCH_RSI_D, append=True)
        bbl_col = f'BBL_{BBANDS_PERIOD}_{float(BBANDS_STDDEV)}'; bbm_col = f'BBM_{BBANDS_PERIOD}_{float(BBANDS_STDDEV)}'; bbu_col = f'BBU_{BBANDS_PERIOD}_{float(BBANDS_STDDEV)}'
        stochk_col = f'STOCHRSIk_{STOCH_RSI_PERIOD}_{STOCH_RSI_PERIOD}_{STOCH_RSI_K}_{STOCH_RSI_D}'; stochd_col = f'STOCHRSId_{STOCH_RSI_PERIOD}_{STOCH_RSI_PERIOD}_{STOCH_RSI_K}_{STOCH_RSI_D}'
        rename_map = {bbl_col: 'BBL', bbm_col: 'BBM', bbu_col: 'BBU', stochk_col: 'STOCHk', stochd_col: 'STOCHd'}
        existing_rename_map = {k: v for k, v in rename_map.items() if k in df_copy.columns}
        if len(existing_rename_map) < 5: return None
        df_copy.rename(columns=existing_rename_map, inplace=True)
        required_cols = ['BBL', 'BBU', 'STOCHk', 'STOCHd']
        if not all(col in df_copy.columns for col in required_cols): return None
        if df_copy[required_cols].iloc[-1].isnull().any(): return None
        return df_copy
    except Exception as e: op_logger.error(f"Indicator Calculation Error: {e}", exc_info=True); return None

def set_isolated_margin(symbol_ccxt, leverage):
    if not binance_rest: return False
    op_logger.info(f"Setting ISOLATED margin for {symbol_ccxt} with {leverage}x leverage...")
    try:
        try:
            binance_rest.set_margin_mode('ISOLATED', symbol_ccxt, params={}); op_logger.info(f"Margin mode set to ISOLATED for {symbol_ccxt}."); time.sleep(0.2)
        except ccxt.ExchangeError as e:
            if 'No need to change margin type' in str(e) or 'Margin type already set to ISOLATED' in str(e): op_logger.warning(f"{symbol_ccxt} is already in ISOLATED margin mode.")
            elif 'position exists' in str(e): op_logger.error(f"Cannot change margin mode for {symbol_ccxt}, position exists."); return False
            else: op_logger.error(f"Failed to set margin mode for {symbol_ccxt}: {e}"); return False
        try:
            binance_rest.set_leverage(leverage, symbol_ccxt, params={}); op_logger.info(f"Leverage set to {leverage}x for {symbol_ccxt}."); return True
        except ccxt.ExchangeError as e:
             if 'No need to change leverage' in str(e): op_logger.warning(f"Leverage for {symbol_ccxt} is already {leverage}x."); return True
             else: op_logger.error(f"Failed to set leverage for {symbol_ccxt}: {e}"); return False
    except (RequestTimeout, ExchangeNotAvailable, OnMaintenance) as e: op_logger.warning(f"Failed set iso margin for {symbol_ccxt}: {e}"); return False
    except Exception as e: op_logger.error(f"Unexpected error setting iso margin for {symbol_ccxt}: {e}", exc_info=True); return False

def place_market_order_real(symbol_ccxt, side, amount, current_price=None):
    if not binance_rest: op_logger.error("CCXT instance not ready."); return None
    if amount <= 0: op_logger.error(f"[{symbol_ccxt}] Invalid amount: {amount}"); return None
    try:
        market = binance_rest.market(symbol_ccxt)
        adjusted_amount_str = binance_rest.amount_to_precision(symbol_ccxt, amount)
        adjusted_amount = float(adjusted_amount_str)
        if adjusted_amount <= 0: op_logger.error(f"[{symbol_ccxt}] Adjusted amount {adjusted_amount_str} <= 0."); return None
        min_notional = market.get('limits', {}).get('cost', {}).get('min', 5.0)
        if current_price and adjusted_amount * current_price < min_notional:
             op_logger.error(f"[{symbol_ccxt}] Order value < min notional ({min_notional}). Amt: {adjusted_amount_str}"); return None
        op_logger.info(f"[REAL ORDER] Attempting {side.upper()} {adjusted_amount_str} {symbol_ccxt} @ MARKET")
        client_order_id = f"bot_{uuid.uuid4().hex[:16]}"
        params = {'newClientOrderId': client_order_id}
        order = binance_rest.create_market_order(symbol_ccxt, side, adjusted_amount, params=params)
        op_logger.info(f"[REAL ORDER PLACED] ID:{order.get('id')} ClientID:{client_order_id} Sym:{symbol_ccxt} Side:{side} ReqAmt:{adjusted_amount_str}")
        trade_logger.info(f"REAL MARKET EXECUTE: {side.upper()} {symbol_ccxt}, ReqAmt:{adjusted_amount_str}, OrdID:{order.get('id')}, CliOrdID:{client_order_id}")
        filled_amount = order.get('filled'); avg_price = order.get('average'); ts_ms = order.get('timestamp'); order_id = order.get('id')
        if filled_amount is not None and avg_price is not None:
            op_logger.info(f"[REAL ORDER FILLED IMMED] {symbol_ccxt} FillAmt:{filled_amount}, AvgPx:{avg_price}, OrdID:{order_id}")
            return {'symbol': symbol_ccxt, 'average': avg_price, 'filled': filled_amount, 'timestamp': ts_ms, 'id': order_id, 'clientOrderId': client_order_id, 'status': 'closed'}
        else:
            op_logger.warning(f"[REAL ORDER] Placed {symbol_ccxt} (ID:{order_id}) but immediate fill info missing. Relying on UDS.")
            return {'symbol': symbol_ccxt, 'id': order_id, 'clientOrderId': client_order_id, 'status': 'open'}
    except ccxt.InsufficientFunds as e: op_logger.error(f"[ORDER FAILED] Insufficient funds {symbol_ccxt}: {e}"); log_asset_status(); return None
    except ccxt.ExchangeError as e: op_logger.error(f"[ORDER FAILED] Exchange error {symbol_ccxt}: {e}"); return None
    except (RequestTimeout, ExchangeNotAvailable, OnMaintenance) as e: op_logger.warning(f"[ORDER FAILED] Network/Exchange issue {symbol_ccxt}: {e}"); return None
    except Exception as e: op_logger.error(f"[ORDER FAILED] Unexpected error {symbol_ccxt}: {e}", exc_info=True); return None

def place_stop_market_order(symbol_ccxt, side, stop_price, amount):
    if not binance_rest: op_logger.error(f"[{symbol_ccxt}] CCXT needed for SL."); return None
    if amount <= 0 or stop_price <= 0: op_logger.error(f"[{symbol_ccxt}] Invalid SL amount/stop_price."); return None
    try:
        adjusted_amount_str = binance_rest.amount_to_precision(symbol_ccxt, amount); stop_price_str = binance_rest.price_to_precision(symbol_ccxt, stop_price)
        adjusted_amount = float(adjusted_amount_str)
        if adjusted_amount <= 0: op_logger.error(f"[{symbol_ccxt}] SL Adjusted amount <= 0."); return None
        op_logger.info(f"[REAL SL ORDER] Attempting {side.upper()} {adjusted_amount_str} {symbol_ccxt} if price hits {stop_price_str} (STOP_MARKET)")
        params = {'stopPrice': stop_price_str, 'reduceOnly': True}; client_order_id = f"sl_{uuid.uuid4().hex[:16]}"; params['newClientOrderId'] = client_order_id
        order = binance_rest.create_order(symbol_ccxt, 'STOP_MARKET', side, adjusted_amount, None, params)
        order_id = order.get('id')
        op_logger.info(f"[REAL SL ORDER PLACED] ID:{order_id} ClientID:{client_order_id} Sym:{symbol_ccxt} Side:{side} StopPx:{stop_price_str} Amt:{adjusted_amount_str}")
        trade_logger.info(f"REAL SL SET: {side.upper()} {symbol_ccxt}, Amt:{adjusted_amount_str}, StopPx:{stop_price_str}, OrdID:{order_id}, CliOrdID:{client_order_id}")
        return {'id': order_id, 'clientOrderId': client_order_id}
    except ccxt.ExchangeError as e: op_logger.error(f"[SL ORDER FAILED] Exch error {symbol_ccxt}: {e}"); return None
    except (RequestTimeout, ExchangeNotAvailable, OnMaintenance) as e: op_logger.warning(f"[SL ORDER FAILED] Network/Exch issue {symbol_ccxt}: {e}"); return None
    except Exception as e: op_logger.error(f"[SL ORDER FAILED] Unexp error {symbol_ccxt}: {e}", exc_info=True); return None

def place_take_profit_market_order(symbol_ccxt, side, stop_price, amount):
    if not binance_rest: op_logger.error(f"[{symbol_ccxt}] CCXT needed for TP."); return None
    if amount <= 0 or stop_price <= 0: op_logger.error(f"[{symbol_ccxt}] Invalid TP amount/stop_price."); return None
    try:
        adjusted_amount_str = binance_rest.amount_to_precision(symbol_ccxt, amount); stop_price_str = binance_rest.price_to_precision(symbol_ccxt, stop_price)
        adjusted_amount = float(adjusted_amount_str)
        if adjusted_amount <= 0: op_logger.error(f"[{symbol_ccxt}] TP Adjusted amount <= 0."); return None
        op_logger.info(f"[REAL TP ORDER] Attempting {side.upper()} {adjusted_amount_str} {symbol_ccxt} if price hits {stop_price_str} (TAKE_PROFIT_MARKET)")
        params = {'stopPrice': stop_price_str, 'reduceOnly': True}; client_order_id = f"tp_{uuid.uuid4().hex[:16]}"; params['newClientOrderId'] = client_order_id
        order = binance_rest.create_order(symbol_ccxt, 'TAKE_PROFIT_MARKET', side, adjusted_amount, None, params)
        order_id = order.get('id')
        op_logger.info(f"[REAL TP ORDER PLACED] ID:{order_id} ClientID:{client_order_id} Sym:{symbol_ccxt} Side:{side} StopPx:{stop_price_str} Amt:{adjusted_amount_str}")
        trade_logger.info(f"REAL TP SET: {side.upper()} {symbol_ccxt}, Amt:{adjusted_amount_str}, StopPx:{stop_price_str}, OrdID:{order_id}, CliOrdID:{client_order_id}")
        return {'id': order_id, 'clientOrderId': client_order_id}
    except ccxt.ExchangeError as e: op_logger.error(f"[TP ORDER FAILED] Exch error {symbol_ccxt}: {e}"); return None
    except (RequestTimeout, ExchangeNotAvailable, OnMaintenance) as e: op_logger.warning(f"[TP ORDER FAILED] Network/Exch issue {symbol_ccxt}: {e}"); return None
    except Exception as e: op_logger.error(f"[TP ORDER FAILED] Unexp error {symbol_ccxt}: {e}", exc_info=True); return None

def cancel_order(symbol_ccxt, order_id=None, client_order_id=None):
    if not binance_rest: op_logger.error(f"[{symbol_ccxt}] CCXT needed for cancel."); return False
    if not order_id and not client_order_id: op_logger.debug(f"[{symbol_ccxt}] Cancel skipped, no ID."); return True
    target_id_str = f"orderId={order_id}" if order_id else f"clientOrderId={client_order_id}"
    op_logger.info(f"Attempting to cancel order {target_id_str} for {symbol_ccxt}...")
    try:
        if order_id: binance_rest.cancel_order(order_id, symbol_ccxt)
        else: binance_rest.cancel_order(client_order_id, symbol_ccxt, params={'origClientOrderId': client_order_id})
        op_logger.info(f"Successfully cancelled order {target_id_str} for {symbol_ccxt}.")
        return True
    except OrderNotFound: op_logger.warning(f"Order {target_id_str} not found for {symbol_ccxt}."); return True
    except (ExchangeNotAvailable, OnMaintenance, RequestTimeout) as e: op_logger.error(f"Cannot cancel {target_id_str}: {e}"); return False
    except RateLimitExceeded as e: op_logger.error(f"Rate limit cancelling {target_id_str}: {e}"); return False
    except ccxt.ExchangeError as e:
        if 'Order does not exist' in str(e) or 'Unknown order sent' in str(e) or '-2011' in str(e):
             op_logger.warning(f"Order {target_id_str} likely already closed/cancelled. Treating as success."); return True
        else: op_logger.error(f"Failed cancel {target_id_str}: {e}"); return False
    except Exception as e: op_logger.error(f"Unexp error cancelling {target_id_str}: {e}", exc_info=True); return False

def cancel_open_orders_for_symbol(symbol_ccxt):
    if not binance_rest: return False
    op_logger.warning(f"Attempting to cancel ALL open orders for {symbol_ccxt}...")
    cancelled_count = 0
    try:
        # fetch_open_orders 에러 핸들링 추가
        try:
             open_orders = binance_rest.fetch_open_orders(symbol_ccxt)
        except RequestTimeout:
             op_logger.error(f"Timeout fetching open orders for {symbol_ccxt}. Cannot cancel.")
             return False
        except (ExchangeNotAvailable, OnMaintenance) as e:
             op_logger.error(f"Exchange unavailable fetching open orders for {symbol_ccxt}: {e}. Cannot cancel.")
             return False
        except Exception as fetch_e:
             op_logger.error(f"Error fetching open orders for {symbol_ccxt}: {fetch_e}")
             return False # 조회 실패 시 취소 불가

        if not open_orders: op_logger.info(f"No open orders found for {symbol_ccxt}."); return True
        op_logger.info(f"Found {len(open_orders)} open orders for {symbol_ccxt}. Cancelling...")
        success = True
        for order in open_orders:
            order_id = order.get('id'); client_order_id = order.get('clientOrderId')
            if cancel_order(symbol_ccxt, order_id=order_id, client_order_id=client_order_id): cancelled_count += 1
            else: success = False # 하나라도 실패하면 False 반환 고려
            time.sleep(0.2)
        op_logger.info(f"Finished cancelling orders for {symbol_ccxt}. Cancelled {cancelled_count}/{len(open_orders)}.")
        return success # 모든 취소 시도가 성공했는지 여부 반환 가능
    except Exception as e: op_logger.error(f"Unexpected error cancelling orders for {symbol_ccxt}: {e}", exc_info=True); return False

def check_symbol_in_blacklist(symbol_ws):
    with blacklist_lock:
        expiry = blacklist.get(symbol_ws)
        if expiry and datetime.now(UTC) < expiry: return True
        elif expiry: op_logger.info(f"Blacklist expired for {symbol_ws}."); del blacklist[symbol_ws]; return False
        return False

def add_to_blacklist(symbol_ws, reason=""):
    symbol_clean = symbol_ws.split(':')[0] # :USDT 제거
    with blacklist_lock:
        expiry_time = datetime.now(UTC) + timedelta(hours=WHIPSAW_BLACKLIST_HOURS)
        blacklist[symbol_clean] = expiry_time
        op_logger.warning(f"Blacklisted {symbol_clean} until {expiry_time.astimezone(KST).strftime('%Y-%m-%d %H:%M:%S KST')}. Reason: {reason}")

def log_asset_status():
    global last_asset_log_time
    now = datetime.now(UTC)
    if now - last_asset_log_time >= timedelta(hours=1):
        try:
            balance = get_current_balance(); balance_str = f"{balance:.2f}" if balance is not None else "Error"
            with stats_lock: cur_trades, cur_wins = total_trades, winning_trades
            win_rate = (cur_wins / cur_trades * 100) if cur_trades > 0 else 0.0
            active_positions_summary = []; num_active = 0
            with real_positions_lock: active_positions_summary = list(real_positions.keys()); num_active = len(real_positions)
            asset_logger.info(f"Balance:{balance_str} {TARGET_ASSET}, Active Pos:{num_active} {active_positions_summary}, Trades:{cur_trades}, Wins:{cur_wins}(UDS), WinRate:{win_rate:.2f}%")
            last_asset_log_time = now
        except Exception as e: asset_logger.error(f"Error logging asset status: {e}", exc_info=True)

# ==============================================================================
# 상태 동기화 로직 (수정된 버전)
# ==============================================================================
def sync_positions_with_exchange():
    global real_positions
    op_logger.info("[SYNC_REST] Starting state synchronization via REST API (Fallback Check)...")
    if not binance_rest: op_logger.error("[SYNC_REST] CCXT instance not ready."); return
    try:
        exchange_positions_raw = binance_rest.fetch_positions(); time.sleep(0.5)
        exchange_pos_dict = {}
        for pos in exchange_positions_raw:
            try:
                amount = float(pos.get('info', {}).get('positionAmt', 0))
                if abs(amount) > 1e-9:
                    symbol_ccxt = pos.get('symbol')
                    if symbol_ccxt and symbol_ccxt.endswith(TARGET_ASSET):
                        symbol_ws = symbol_ccxt.replace('/USDT', 'USDT')
                        exchange_pos_dict[symbol_ws] = {'side': 'long' if amount > 0 else 'short', 'amount': abs(amount), 'entry_price': float(pos.get('entryPrice', 0)), 'symbol_ccxt': symbol_ccxt}
            except Exception as parse_err: op_logger.error(f"[SYNC_REST] Error parsing exch pos: {pos.get('info')}, Err: {parse_err}")

        with real_positions_lock: local_pos_dict = real_positions.copy()
        local_pos_symbols = set(local_pos_dict.keys()); exchange_pos_symbols = set(exchange_pos_dict.keys())
        L_only = local_pos_symbols - exchange_pos_symbols; E_only = exchange_pos_symbols - local_pos_symbols; Both = local_pos_symbols.intersection(exchange_pos_symbols)
        op_logger.info(f"[SYNC_REST] Check results: Local_Only={len(L_only)}, Exchange_Only={len(E_only)}, Both={len(Both)}")

        # L_only (로컬 O / 거래소 X): 경고만, 삭제 안함!
        if L_only:
            op_logger.warning(f"[SYNC_REST][WARN] Found {len(L_only)} local pos not on exch (via REST): {L_only}")
            for symbol_ws in L_only:
                op_logger.warning(f"[SYNC_REST][WARN] -> {symbol_ws}: UDS missed closure? Leaving local state intact.")
                pos_info = local_pos_dict.get(symbol_ws)
                if pos_info:
                    symbol_ccxt = symbol_ws.replace('USDT','/USDT')
                    sl_id, sl_coid = pos_info.get('sl_order_id'), pos_info.get('sl_client_order_id')
                    tp_id, tp_coid = pos_info.get('tp_order_id'), pos_info.get('tp_client_order_id')
                    op_logger.info(f"[{symbol_ws}] Attempting cancel potentially orphaned SL({sl_id}/{sl_coid}) / TP({tp_id}/{tp_coid})...")
                    if sl_id or sl_coid: Thread(target=cancel_order, args=(symbol_ccxt,), kwargs={'order_id': sl_id, 'client_order_id': sl_coid}, daemon=True).start(); time.sleep(0.1)
                    if tp_id or tp_coid: Thread(target=cancel_order, args=(symbol_ccxt,), kwargs={'order_id': tp_id, 'client_order_id': tp_coid}, daemon=True).start(); time.sleep(0.1)

        # E_only (거래소 O / 로컬 X): 추적 불가 -> 즉시 종료 시도 및 블랙리스트
        if E_only:
            op_logger.error(f"[SYNC_REST][ERROR] Found {len(E_only)} untracked positions on exchange: {E_only}")
            for symbol_ws in E_only:
                op_logger.error(f"[SYNC_REST][ERROR] -> {symbol_ws}: Closing immediately and blacklisting.")
                pos_info = exchange_pos_dict.get(symbol_ws); time.sleep(0.1)
                if not pos_info: continue
                symbol_ccxt = pos_info['symbol_ccxt']
                cancel_open_orders_for_symbol(symbol_ccxt); time.sleep(0.5) # 주문 먼저 취소
                try:
                    ticker = binance_rest.fetch_ticker(symbol_ccxt)
                    current_price = ticker['last'] if ticker else None
                    close_order_result = place_market_order_real(symbol_ccxt, 'sell' if pos_info['side'] == 'long' else 'buy', pos_info['amount'], current_price)
                    if not close_order_result or not close_order_result.get('id'): op_logger.error(f"[SYNC_REST] Failed to place market order for untracked {symbol_ws}.")
                except Exception as close_err: op_logger.error(f"[SYNC_REST] Error closing untracked {symbol_ws}: {close_err}")
                add_to_blacklist(symbol_ws, reason="Untracked position closed via REST Sync"); time.sleep(0.5)

        # Both (양쪽 존재): 불일치 시 경고 로그만
        if Both:
            op_logger.debug(f"[SYNC_REST] Checking {len(Both)} positions present in both states...")
            for symbol_ws in Both:
                local_info = local_pos_dict.get(symbol_ws); exchange_info = exchange_pos_dict.get(symbol_ws)
                if not local_info or not exchange_info: continue
                amount_diff = abs(local_info.get('amount', 0) - exchange_info.get('amount', 0)) > 1e-6
                side_mismatch = local_info.get('side') != exchange_info.get('side')
                if amount_diff or side_mismatch:
                    op_logger.warning(f"[SYNC_REST][WARN] Discrepancy for {symbol_ws}! Local:{local_info}, Exch:{exchange_info}. NOT auto-correcting.")

        op_logger.info("[SYNC_REST] REST state synchronization finished.")
    except RateLimitExceeded: op_logger.warning("[SYNC_REST] Rate limit exceeded."); time.sleep(60)
    except (RequestTimeout, ExchangeNotAvailable, OnMaintenance) as e: op_logger.warning(f"[SYNC_REST] Exchange/Network unavailable: {e}"); time.sleep(60)
    except AuthenticationError: op_logger.error("[SYNC_REST] Authentication error! Shutting down."); global shutdown_requested; shutdown_requested = True
    except Exception as e: op_logger.error(f"[SYNC_REST] Error during REST sync: {e}", exc_info=True); time.sleep(60)

def sync_state_periodically(interval_seconds):
    global shutdown_requested
    op_logger.info(f"REST Sync thread started. Interval: {interval_seconds}s.")
    while not shutdown_requested:
        try:
            for _ in range(interval_seconds):
                 if shutdown_requested: break
                 time.sleep(1)
            if not shutdown_requested: sync_positions_with_exchange()
        except Exception as e: op_logger.error(f"Error in REST sync loop: {e}", exc_info=True); time.sleep(60)
    op_logger.info("REST Sync thread finished.")

# ==============================================================================
# 심볼 목록 주기적 업데이트 로직
# ==============================================================================
def update_top_symbols_periodically(interval_seconds):
    global subscribed_symbols, historical_data, kline_websocket_running, kline_wsapp, shutdown_requested
    op_logger.info(f"Symbol Update thread started. Interval: {interval_seconds}s.")
    while not shutdown_requested:
        try:
            for _ in range(interval_seconds):
                 if shutdown_requested: break
                 time.sleep(1)
            if shutdown_requested: break

            if not kline_websocket_running or not kline_wsapp or not kline_wsapp.sock or not kline_wsapp.sock.connected:
                op_logger.warning("[Symbol Update] K-line WS not ready. Skipping.")
                continue

            op_logger.info("[Symbol Update] Starting periodic symbol list update...")
            new_top_symbols_ccxt = get_top_volume_symbols(TOP_N_SYMBOLS)
            if not new_top_symbols_ccxt: op_logger.warning("[Symbol Update] Failed to fetch new symbols."); continue
            new_top_symbols_ws = {s.replace('/', '') for s in new_top_symbols_ccxt}

            with subscribed_symbols_lock: current_subscribed = subscribed_symbols.copy()
            symbols_to_add = new_top_symbols_ws - current_subscribed
            symbols_to_remove = current_subscribed - new_top_symbols_ws

            # 제거
            if symbols_to_remove:
                op_logger.info(f"[Symbol Update] Removing: {symbols_to_remove}")
                unsubscribe_streams = [f"{s.lower()}@kline_{TIMEFRAME}" for s in symbols_to_remove]
                if unsubscribe_streams:
                    unsubscribe_message = {"method": "UNSUBSCRIBE", "params": unsubscribe_streams, "id": int(time.time())}
                    try: kline_wsapp.send(json.dumps(unsubscribe_message)); op_logger.info(f"[Symbol Update] Sent UNSUBSCRIBE.")
                    except Exception as e: op_logger.error(f"[Symbol Update] Failed send UNSUBSCRIBE: {e}")
                with subscribed_symbols_lock: subscribed_symbols -= symbols_to_remove
                with data_lock:
                    removed_count = sum(1 for symbol_ws in symbols_to_remove if historical_data.pop(symbol_ws, None))
                    op_logger.info(f"[Symbol Update] Removed historical data for {removed_count} symbols.")

            # 추가
            if symbols_to_add:
                op_logger.info(f"[Symbol Update] Adding: {symbols_to_add}")
                new_data_fetched_count = 0; errors_fetching = 0
                symbols_actually_added_to_data = set() # 실제 데이터 로드 성공한 심볼만 구독
                with data_lock:
                    for symbol_ws in symbols_to_add:
                        symbol_ccxt = symbol_ws.replace('USDT', '/USDT')
                        initial_df = fetch_initial_ohlcv(symbol_ccxt, TIMEFRAME, limit=max(INITIAL_CANDLE_FETCH_LIMIT, STOCH_RSI_PERIOD*2))
                        if initial_df is not None and not initial_df.empty:
                            historical_data[symbol_ws] = initial_df
                            new_data_fetched_count += 1
                            symbols_actually_added_to_data.add(symbol_ws)
                        else: errors_fetching += 1
                        if shutdown_requested: break # 중간에 종료 요청 시 중단
                        time.sleep(0.3)
                op_logger.info(f"[Symbol Update] Fetched initial data for {new_data_fetched_count} new symbols ({errors_fetching} errors).")

                if symbols_actually_added_to_data:
                    subscribe_streams = [f"{s.lower()}@kline_{TIMEFRAME}" for s in symbols_actually_added_to_data]
                    subscribe_message = {"method": "SUBSCRIBE", "params": subscribe_streams, "id": int(time.time())}
                    try:
                        if kline_wsapp and kline_wsapp.sock and kline_wsapp.sock.connected: # 보내기 직전 한번 더 확인
                            kline_wsapp.send(json.dumps(subscribe_message))
                            op_logger.info(f"[Symbol Update] Sent SUBSCRIBE for {len(symbols_actually_added_to_data)} symbols.")
                            with subscribed_symbols_lock: subscribed_symbols.update(symbols_actually_added_to_data)
                        else: op_logger.warning("[Symbol Update] K-line WS disconnected before sending SUBSCRIBE.")
                    except Exception as e: op_logger.error(f"[Symbol Update] Failed send SUBSCRIBE: {e}")

            with subscribed_symbols_lock: current_sub_count = len(subscribed_symbols)
            op_logger.info(f"[Symbol Update] Finished. Currently subscribed: {current_sub_count}")

        except Exception as e: op_logger.error(f"Error in symbol update loop: {e}", exc_info=True); time.sleep(60)
    op_logger.info("Symbol Update thread finished.")


# ==============================================================================
# 웹소켓 처리 로직 (K-line)
# ==============================================================================
def update_historical_data(symbol_ws, kline_data):
    global historical_data
    try:
        with data_lock:
            if symbol_ws not in historical_data: return False
            df = historical_data[symbol_ws]; kline_start_time = pd.to_datetime(kline_data['t'], unit='ms', utc=True)
            new_data = pd.DataFrame([{'timestamp': kline_start_time, 'open': float(kline_data['o']), 'high': float(kline_data['h']), 'low': float(kline_data['l']), 'close': float(kline_data['c']), 'volume': float(kline_data['v'])}]).set_index('timestamp')
            if kline_start_time in df.index: df.loc[kline_start_time] = new_data.iloc[0]
            else: df = pd.concat([df, new_data]); df = df.iloc[-MAX_CANDLE_HISTORY:]
            historical_data[symbol_ws] = df; return True
    except Exception as e: op_logger.error(f"[{symbol_ws}] Error updating historical data: {e}", exc_info=True); return False

def try_update_tp(open_symbol, open_symbol_ccxt, side, amount, tp_order_id, tp_client_order_id, current_tp_price, new_tp_target):
    if not current_tp_price or not new_tp_target or new_tp_target <= 0 or current_tp_price <= 0: return False
    price_diff_percent = abs(new_tp_target - current_tp_price) / current_tp_price * 100
    if price_diff_percent >= TP_UPDATE_THRESHOLD_PERCENT:
        op_logger.info(f"[{open_symbol}] TP target moved: {current_tp_price:.4f} -> {new_tp_target:.4f}. Updating...")
        if not cancel_order(open_symbol_ccxt, order_id=tp_order_id, client_order_id=tp_client_order_id): op_logger.error(f"[{open_symbol}] Failed cancel prev TP. Aborted."); return False
        time.sleep(0.2)
        new_tp_order_info = None
        try:
            adjusted_amount_str = binance_rest.amount_to_precision(open_symbol_ccxt, amount); new_tp_target_str = binance_rest.price_to_precision(open_symbol_ccxt, new_tp_target)
            new_tp_actual_price = float(new_tp_target_str)
            new_tp_order_info = place_take_profit_market_order(open_symbol_ccxt, 'sell' if side == 'long' else 'buy', new_tp_actual_price, float(adjusted_amount_str))
        except Exception as tp_e: op_logger.error(f"[{open_symbol}] Error placing new TP: {tp_e}")
        if new_tp_order_info and new_tp_order_info.get('id'):
            new_tp_id = new_tp_order_info['id']; new_tp_coid = new_tp_order_info.get('clientOrderId')
            with real_positions_lock:
                if open_symbol in real_positions:
                    real_positions[open_symbol]['tp_order_id'] = new_tp_id; real_positions[open_symbol]['tp_client_order_id'] = new_tp_coid
                    real_positions[open_symbol]['current_tp_price'] = new_tp_actual_price
                    op_logger.info(f"[{open_symbol}] TP updated to {new_tp_actual_price:.4f} (ID:{new_tp_id}).")
                    return True
                else: op_logger.warning(f"[{open_symbol}] Pos gone while updating TP."); cancel_order(open_symbol_ccxt, order_id=new_tp_id, client_order_id=new_tp_coid); return False
        else: op_logger.error(f"[{open_symbol}] Failed place new TP."); return False
    return False

def process_kline_message(symbol_ws, kline_data):
    global real_positions, entry_in_progress
    with subscribed_symbols_lock:
        if symbol_ws not in subscribed_symbols: return # 구독 안된 심볼 무시

    if not update_historical_data(symbol_ws, kline_data): return
    is_candle_closed = kline_data.get('x', False)
    with data_lock: current_df = historical_data.get(symbol_ws)
    if current_df is None: return
    indicator_df = calculate_indicators(current_df.copy())
    if indicator_df is None or indicator_df.empty: return

    try:
        last_candle = indicator_df.iloc[-1]; current_price = last_candle['close']
        stoch_k = last_candle.get('STOCHk', np.nan); stoch_d = last_candle.get('STOCHd', np.nan)
        bbl = last_candle.get('BBL', np.nan); bbu = last_candle.get('BBU', np.nan)
        if pd.isna(stoch_k) or pd.isna(stoch_d) or pd.isna(bbl) or pd.isna(bbu) or pd.isna(current_price): return
    except IndexError: return
    except Exception as e: op_logger.error(f"[{symbol_ws}] Indicator access error: {e}"); return

    now_utc = datetime.now(UTC); symbol_ccxt = symbol_ws.replace('USDT', '/USDT')
    positions_to_check = {};
    with real_positions_lock: positions_to_check = real_positions.copy()

    # TP 업데이트 (현재 K-line 심볼이 보유 중일 때만)
    if symbol_ws in positions_to_check:
        pos_info = positions_to_check[symbol_ws]
        side, entry_time, amount = pos_info['side'], pos_info['entry_time'], pos_info['amount']
        tp_order_id, tp_coid = pos_info.get('tp_order_id'), pos_info.get('tp_client_order_id')
        current_tp_price = pos_info.get('current_tp_price')
        if now_utc - entry_time >= timedelta(minutes=POSITION_MONITORING_DELAY_MINUTES):
            if tp_order_id and current_tp_price:
                 new_tp_target = bbu if side == 'long' else bbl
                 if not pd.isna(new_tp_target) and new_tp_target > 0:
                      try_update_tp(symbol_ws, symbol_ccxt, side, amount, tp_order_id, tp_coid, current_tp_price, new_tp_target)

    # 신규 진입 (캔들 마감 시)
    if is_candle_closed:
        with entry_lock: is_entry_attempted = entry_in_progress.get(symbol_ws, False)
        with real_positions_lock: position_exists = symbol_ws in real_positions
        if not is_entry_attempted and not position_exists and not check_symbol_in_blacklist(symbol_ws):
            with real_positions_lock: current_open_count = len(real_positions)
            if current_open_count >= MAX_OPEN_POSITIONS: return
            long_cond = stoch_k<=STOCH_RSI_LONG_ENTRY_THRESH and stoch_d<=STOCH_RSI_LONG_ENTRY_THRESH and stoch_k>stoch_d
            short_cond = stoch_k>=STOCH_RSI_SHORT_ENTRY_THRESH and stoch_d>=STOCH_RSI_SHORT_ENTRY_THRESH and stoch_k<stoch_d
            target_side = None; tp_target = None; entry_price = current_price
            if long_cond: target_side = 'buy'; tp_target = bbu if not pd.isna(bbu) and bbu > 0 else None
            elif short_cond: target_side = 'sell'; tp_target = bbl if not pd.isna(bbl) and bbl > 0 else None

            if target_side and tp_target is not None:
                with entry_lock: entry_in_progress[symbol_ws] = True
                try:
                    op_logger.info(f"[{symbol_ws}] Entry Cond MET: {target_side.upper()} @ {entry_price:.4f}, TP Target: {tp_target:.4f}")
                    balance = get_current_balance()
                    if balance <= 10.0: raise Exception(f"Insufficient balance ({balance:.2f})")
                    with real_positions_lock: open_count = len(real_positions)
                    if open_count >= MAX_OPEN_POSITIONS: raise Exception("Max pos reached")
                    portion = 1.0 / (MAX_OPEN_POSITIONS - open_count)
                    margin = balance * portion
                    # --- 최소 마진 체크 ---
                    # if margin < 10.0: raise Exception(f"Target margin {margin:.2f} too low.") # 필요 시 활성화
                    notional = margin * LEVERAGE; min_notional = 5.0
                    if notional < min_notional: raise Exception(f"Notional {notional:.2f} < min {min_notional}")
                    amount = notional / entry_price if entry_price > 0 else 0
                    if amount <= 0: raise Exception("Amount <= 0")
                    if not set_isolated_margin(symbol_ccxt, LEVERAGE): raise Exception("Set margin/lev failed")
                    entry_order = place_market_order_real(symbol_ccxt, target_side, amount, entry_price)
                    if not entry_order or not entry_order.get('id'): raise Exception("Entry order failed")
                    entry_oid, entry_coid = entry_order['id'], entry_order.get('clientOrderId')
                    op_logger.info(f"[{symbol_ws}] Entry order placed (ID:{entry_oid}). Awaiting UDS confirmation...")
                    sl_order, tp_order = None, None; final_sl, final_tp = None, None
                    try:
                        final_sl = entry_price*(LONG_STOP_LOSS_FACTOR if target_side=='buy' else SHORT_STOP_LOSS_FACTOR)
                        final_tp = tp_target
                        sl_side = 'sell' if target_side == 'buy' else 'buy'; tp_side = sl_side
                        sl_order = place_stop_market_order(symbol_ccxt, sl_side, final_sl, amount)
                        if not sl_order or not sl_order.get('id'): raise Exception("SL placement failed")
                        op_logger.info(f"[{symbol_ws}] SL placed (ID:{sl_order['id']})")
                        time.sleep(0.1)
                        tp_order = place_take_profit_market_order(symbol_ccxt, tp_side, final_tp, amount)
                        if not tp_order or not tp_order.get('id'): raise Exception("TP placement failed")
                        op_logger.info(f"[{symbol_ws}] TP placed (ID:{tp_order['id']})")
                        with real_positions_lock: # 임시 저장 (UDS에서 확정)
                            if len(real_positions) < MAX_OPEN_POSITIONS:
                                real_positions[symbol_ws] = {'side': 'long' if target_side=='buy' else 'short', 'entry_price': entry_price, 'amount': amount, 'entry_time': now_utc, 'entry_order_id': entry_oid, 'entry_client_order_id': entry_coid, 'sl_order_id': sl_order['id'], 'sl_client_order_id': sl_order.get('clientOrderId'), 'tp_order_id': tp_order['id'], 'tp_client_order_id': tp_order.get('clientOrderId'), 'current_tp_price': final_tp }
                                op_logger.info(f"[{symbol_ws}] Entry initiated. Active:{len(real_positions)}")
                            else: raise Exception("Max pos reached during SL/TP placement")
                    except Exception as sltp_err: # 롤백
                        op_logger.error(f"[{symbol_ws}] Error placing SL/TP: {sltp_err}. ROLLING BACK!")
                        if sl_order and sl_order.get('id'): cancel_order(symbol_ccxt, order_id=sl_order['id'], client_order_id=sl_order.get('clientOrderId'))
                        if tp_order and tp_order.get('id'): cancel_order(symbol_ccxt, order_id=tp_order['id'], client_order_id=tp_order.get('clientOrderId'))
                        cancel_order(symbol_ccxt, order_id=entry_oid, client_order_id=entry_coid) # 진입 주문 취소 시도
                        op_logger.warning(f"[{symbol_ws}] Rollback initiated. Manual check may be needed.")
                        with real_positions_lock: real_positions.pop(symbol_ws, None)
                        add_to_blacklist(symbol_ws, reason=f"Entry fail: {sltp_err}")
                except Exception as entry_err:
                    op_logger.error(f"[{symbol_ws}] Entry process failed: {entry_err}", exc_info=False) # exc_info=False 로 변경 (너무 길어짐 방지)
                    with real_positions_lock: real_positions.pop(symbol_ws, None) # 실패 시 임시 저장 제거
                finally:
                    with entry_lock: entry_in_progress.pop(symbol_ws, None)


# ==============================================================================
# 웹소켓 콜백 함수 (K-line) - 재연결 로직 포함
# ==============================================================================
def on_message_kline(wsapp, message):
    try:
        data = json.loads(message)
        if 'stream' in data and 'data' in data:
            stream_name = data['stream']; payload = data['data']
            if payload.get('e') == 'kline':
                symbol_lower = stream_name.split('@')[0]; symbol_upper = symbol_lower.upper()
                process_kline_message(symbol_upper, payload['k'])
        elif 'result' in data and data.get('id'): op_logger.info(f"K-line Subscription response: {data}")
        elif 'e' in data and data['e'] == 'error': op_logger.error(f"K-line WS API Error: {data}")
    except json.JSONDecodeError: op_logger.error(f"K-line JSON Decode Err: {message[:100]}")
    except Exception as e: op_logger.error(f"K-line Msg Proc Err: {e}", exc_info=True)

def on_error_kline(wsapp, error):
    op_logger.error(f"K-line WebSocket Error: {error}")
    if isinstance(error, ConnectionRefusedError): op_logger.error("Connection refused.")

def on_close_kline(wsapp, close_status_code, close_msg):
    global kline_websocket_running
    if not shutdown_requested:
        op_logger.warning(f"K-line WS closed unexpectedly! Code:{close_status_code}. Will reconnect.")
        kline_websocket_running = False # Trigger reconnect in main loop
    else:
        op_logger.info(f"K-line WS closed gracefully.")
        kline_websocket_running = False

def on_open_kline_initial(wsapp):
    global subscribed_symbols, historical_data, kline_websocket_running, shutdown_requested
    kline_websocket_running = True
    op_logger.info("K-line WebSocket initial connection opened.")
    op_logger.info("Fetching initial top symbols...")
    initial_top_symbols_ccxt = get_top_volume_symbols(TOP_N_SYMBOLS)
    if not initial_top_symbols_ccxt: op_logger.error("Could not fetch initial symbols."); shutdown_requested=True; wsapp.close(); return
    initial_symbols_ws = {s.replace('/', '') for s in initial_top_symbols_ccxt}
    op_logger.info(f"Subscribing to initial {len(initial_symbols_ws)} streams...")
    streams = [f"{s.lower()}@kline_{TIMEFRAME}" for s in initial_symbols_ws]
    if not streams: op_logger.error("No streams to subscribe."); wsapp.close(); return
    sub_id = 1; subscribe_message = {"method": "SUBSCRIBE", "params": streams, "id": sub_id}
    try: wsapp.send(json.dumps(subscribe_message)); time.sleep(1); op_logger.info(f"Initial K-line sub sent (ID:{sub_id}).")
    except Exception as e: op_logger.error(f"Failed initial K-line sub: {e}"); wsapp.close(); return
    with subscribed_symbols_lock: subscribed_symbols = initial_symbols_ws

    op_logger.info("Fetching initial historical data...")
    fetched_count, fetch_errors = 0, 0
    with data_lock: historical_data.clear()
    symbols_to_fetch = initial_symbols_ws.copy()
    for symbol_ws in symbols_to_fetch:
        if shutdown_requested: break
        symbol_ccxt = symbol_ws.replace('USDT','/USDT')
        df = fetch_initial_ohlcv(symbol_ccxt, TIMEFRAME, limit=max(INITIAL_CANDLE_FETCH_LIMIT, STOCH_RSI_PERIOD*2))

        if df is not None and not df.empty:
            # Lock을 사용하여 공유 데이터(historical_data)에 안전하게 접근
            with data_lock:
                historical_data[symbol_ws] = df
            # fetched_count는 지역 변수이므로 Lock 밖에서 증가시켜도 무방
            fetched_count += 1
        else:
            # 데이터 가져오기 실패 시 에러 카운트 증가
            fetch_errors += 1

        # Rate limit 방지용 sleep은 Lock 밖에서 수행
        time.sleep(0.3)
    op_logger.info(f"Initial data fetch complete ({fetched_count} OK, {fetch_errors} errors).")
    print("-" * 80 + "\nK-line WS connected. Listening...\n" + "-" * 80)

def on_open_kline_reconnect(wsapp):
    global kline_websocket_running, subscribed_symbols
    kline_websocket_running = True
    op_logger.info("K-line WebSocket RECONNECTED successfully.")
    with subscribed_symbols_lock: current_subs = subscribed_symbols.copy()
    if not current_subs: op_logger.warning("Sub list empty on reconnect."); return
    op_logger.info(f"Resubscribing to {len(current_subs)} streams...")
    streams = [f"{s.lower()}@kline_{TIMEFRAME}" for s in current_subs]
    if not streams: op_logger.warning("No streams to resub."); return
    resub_id = int(time.time()); subscribe_message = {"method": "SUBSCRIBE", "params": streams, "id": resub_id}
    try: wsapp.send(json.dumps(subscribe_message)); op_logger.info(f"Resub msg sent (ID:{resub_id}).")
    except Exception as e: op_logger.error(f"Failed resub msg: {e}"); wsapp.close()


# ==============================================================================
# 웹소켓 콜백 함수 (User Data Stream)
# ==============================================================================
def on_message_user_stream(wsapp, message):
    global real_positions, total_trades, winning_trades, blacklist
    try:
        data = json.loads(message)
        event_type = data.get('e')
        if event_type == 'ORDER_TRADE_UPDATE':
            order_data = data.get('o'); symbol_ws = order_data['s']; order_id = str(order_data['i'])
            client_order_id = order_data.get('c'); order_status = order_data['X']; order_side = order_data['S']
            trade_price = float(order_data['L']); trade_qty = float(order_data['l']); filled_qty = float(order_data['z'])
            total_qty = float(order_data['q']); avg_price = float(order_data['ap']) if order_data.get('ap') and float(order_data['ap']) > 0 else trade_price
            commission = float(order_data['n']) if order_data.get('n') else 0.0; commission_asset = order_data.get('N')
            trade_time_ms = order_data['T']; trade_time_dt = datetime.fromtimestamp(trade_time_ms / 1000, tz=UTC)
            is_reduce_only = order_data.get('R', False); order_type = order_data['o']
            op_logger.info(f"[UDS][ORDER] {symbol_ws} ID:{order_id}.. Status:{order_status} Side:{order_side} Type:{order_type} Fill:{filled_qty}/{total_qty} AvgPx:{avg_price:.4f}")

            if order_status in ['TRADE', 'PARTIALLY_FILLED'] and trade_qty > 0:
                trade_logger.info(f"[UDS][FILL] {symbol_ws} {order_side} {trade_qty} @ {trade_price:.4f} (Avg:{avg_price:.4f}) ID:{order_id}.. Status:{order_status}")
                with real_positions_lock, stats_lock:
                    pos_info = real_positions.get(symbol_ws); symbol_ccxt = symbol_ws.replace('USDT','/USDT')
                    # Entry fill
                    if not pos_info or (pos_info and str(pos_info.get('entry_order_id')) == order_id):
                        if not pos_info: # First entry fill
                             side = 'long' if order_side == 'BUY' else 'short'
                             # Preserve SL/TP info if it was temporarily stored during entry process
                             temp_sl_id = real_positions.get(symbol_ws,{}).get('sl_order_id')
                             temp_sl_coid = real_positions.get(symbol_ws,{}).get('sl_client_order_id')
                             temp_tp_id = real_positions.get(symbol_ws,{}).get('tp_order_id')
                             temp_tp_coid = real_positions.get(symbol_ws,{}).get('tp_client_order_id')
                             temp_tp_price = real_positions.get(symbol_ws,{}).get('current_tp_price')

                             real_positions[symbol_ws] = {'side': side, 'entry_price': avg_price, 'amount': filled_qty, 'entry_time': trade_time_dt, 'entry_order_id': order_id, 'entry_client_order_id': client_order_id, 'sl_order_id': temp_sl_id, 'sl_client_order_id': temp_sl_coid, 'tp_order_id': temp_tp_id, 'tp_client_order_id': temp_tp_coid, 'current_tp_price': temp_tp_price }
                             op_logger.info(f"[{symbol_ws}] Position OPENED via UDS Fill. Side:{side}, Qty:{filled_qty:.8f}, AvgPx:{avg_price:.5f}")
                        else: # Subsequent entry fills
                             pos_info['amount'] = filled_qty # Update total filled amount
                             if avg_price > 0: pos_info['entry_price'] = avg_price # Update average price
                             pos_info['entry_time'] = trade_time_dt # Update timestamp
                             op_logger.info(f"[{symbol_ws}] Position UPDATED via UDS Fill. Qty:{filled_qty:.8f}, AvgPx:{avg_price:.5f}")
                        if order_status == 'TRADE': op_logger.info(f"[{symbol_ws}] Entry order {order_id} fully filled.")
                    # Exit fill
                    elif pos_info and is_reduce_only:
                        closed_amount = trade_qty; remaining_amount = pos_info['amount'] - closed_amount
                        pnl = ((trade_price - pos_info['entry_price']) * closed_amount) if pos_info['side'] == 'long' else ((pos_info['entry_price'] - trade_price) * closed_amount)
                        pnl -= commission
                        trade_logger.info(f"[UDS][CLOSE] {symbol_ws} Closed {closed_amount:.8f} (Rem:{remaining_amount:.8f}) via {order_type} ID:{order_id}. PnL: {pnl:.5f}")
                        total_trades += 1;
                        if pnl > 0: winning_trades += 1
                        if order_status == 'TRADE' or abs(remaining_amount) < 1e-9: # Full close
                             op_logger.info(f"[{symbol_ws}] Position FULLY CLOSED via UDS Fill (ID:{order_id}). Removing state.")
                             if str(pos_info.get('sl_order_id')) == order_id: add_to_blacklist(symbol_ws, reason=f"SL Filled ({order_id}) via UDS")
                             opp_oid, opp_coid = (None, None)
                             if str(pos_info.get('sl_order_id')) == order_id: opp_oid, opp_coid = pos_info.get('tp_order_id'), pos_info.get('tp_client_order_id')
                             elif str(pos_info.get('tp_order_id')) == order_id: opp_oid, opp_coid = pos_info.get('sl_order_id'), pos_info.get('sl_client_order_id')
                             if opp_oid or opp_coid: op_logger.info(f"[{symbol_ws}] Cancelling remaining order ({opp_oid}/{opp_coid})..."); Thread(target=cancel_order, args=(symbol_ccxt,), kwargs={'order_id': opp_oid, 'client_order_id': opp_coid}, daemon=True).start()
                             if symbol_ws in real_positions: del real_positions[symbol_ws] # Delete local state
                        else: # Partial close
                             op_logger.info(f"[{symbol_ws}] Position PARTIALLY CLOSED via UDS Fill. Remaining: {remaining_amount:.8f}")
                             pos_info['amount'] = remaining_amount

            elif order_status in ['CANCELED', 'REJECTED', 'EXPIRED']:
                 op_logger.info(f"[UDS][ORDER_FINAL] {symbol_ws} Order {order_id} finalized: {order_status}")
                 with real_positions_lock: # Clear order ID from local state if it matches
                      pos_info = real_positions.get(symbol_ws)
                      if pos_info:
                          if str(pos_info.get('sl_order_id')) == order_id: pos_info['sl_order_id'] = None; pos_info['sl_client_order_id'] = None; op_logger.warning(f"[{symbol_ws}] SL order {order_id} {order_status}. SL removed locally.")
                          elif str(pos_info.get('tp_order_id')) == order_id: pos_info['tp_order_id'] = None; pos_info['tp_client_order_id'] = None; pos_info['current_tp_price'] = None; op_logger.warning(f"[{symbol_ws}] TP order {order_id} {order_status}. TP removed locally.")

        elif event_type == 'ACCOUNT_UPDATE':
            update_data = data.get('a', {}); reason = update_data.get('m'); op_logger.debug(f"[UDS][ACCOUNT] Reason: {reason}")
            positions_update = update_data.get('P', [])
            if positions_update:
                with real_positions_lock:
                    for pos_data in positions_update:
                        symbol_ws = pos_data['s']; amount = float(pos_data['pa']); entry_price = float(pos_data['ep'])
                        pos_info = real_positions.get(symbol_ws)
                        if pos_info and abs(amount) < 1e-9: # Position closed externally/liquidated
                            op_logger.warning(f"[UDS][ACCOUNT] Pos {symbol_ws} closed externally? (Amt=0, Reason:{reason}). Removing state.")
                            symbol_ccxt = symbol_ws.replace('USDT','/USDT')
                            Thread(target=cancel_order, args=(symbol_ccxt,), kwargs={'order_id': pos_info.get('sl_order_id'), 'client_order_id': pos_info.get('sl_client_order_id')}, daemon=True).start()
                            Thread(target=cancel_order, args=(symbol_ccxt,), kwargs={'order_id': pos_info.get('tp_order_id'), 'client_order_id': pos_info.get('tp_client_order_id')}, daemon=True).start()
                            if symbol_ws in real_positions: del real_positions[symbol_ws]
                            if reason != 'ORDER': add_to_blacklist(symbol_ws, reason=f"Pos closed externally? (Reason:{reason})")
                        elif pos_info and abs(amount) > 1e-9: # Update existing pos info
                             if abs(pos_info['amount'] - amount) > 1e-6: op_logger.warning(f"[{symbol_ws}] Amount mismatch ACCOUNT. Local:{pos_info['amount']}, Stream:{amount}. Updating."); pos_info['amount'] = amount
                             if abs(pos_info['entry_price'] - entry_price) > 1e-6: op_logger.warning(f"[{symbol_ws}] Entry mismatch ACCOUNT. Local:{pos_info['entry_price']:.5f}, Stream:{entry_price:.5f}. Updating."); pos_info['entry_price'] = entry_price
            balances_update = update_data.get('B', []) # Optional: Update balance info
            if balances_update:
                for bal_data in balances_update:
                     if bal_data['a'] == TARGET_ASSET: op_logger.debug(f"[UDS][BALANCE] {TARGET_ASSET} Bal: {bal_data['wb']}")

        elif event_type == 'listenKeyExpired':
             op_logger.warning("[UDS] Listen Key EXPIRED! Need to reconnect UDS.")
             global user_websocket_running; user_websocket_running = False # Trigger UDS reconnect/shutdown

    except json.JSONDecodeError: op_logger.error(f"UDS JSON Decode Err: {message[:100]}")
    except Exception as e: op_logger.error(f"UDS Msg Proc Err: {e}", exc_info=True)

def on_error_user_stream(wsapp, error):
    op_logger.error(f"User Stream WebSocket Error: {error}")
    global user_websocket_running
    if isinstance(error, websocket.WebSocketConnectionClosedException): user_websocket_running = False

def on_close_user_stream(wsapp, close_status_code, close_msg):
    global user_websocket_running
    op_logger.info(f"User Stream WebSocket closed. Code:{close_status_code}, Msg:{close_msg}")
    user_websocket_running = False # Indicate UDS is down

def on_open_user_stream(wsapp):
    global user_websocket_running
    user_websocket_running = True
    op_logger.info("User Data Stream WebSocket connection opened.")

# ==============================================================================
# User Data Stream 관리 함수
# ==============================================================================
def get_listen_key():
    global listen_key, binance_rest
    if not binance_rest: op_logger.error("CCXT REST not init to get listen key."); return None
    with listen_key_lock:
        op_logger.info("Requesting new User Data Stream listen key...")
        try:
            response = binance_rest.fapiPrivatePostListenKey()
            listen_key = response.get('listenKey')
            if listen_key: op_logger.info(f"Obtained listen key: {listen_key[:5]}..."); return listen_key
            else: op_logger.error(f"Failed get listen key: {response}"); return None
        except AuthenticationError: op_logger.error("Auth failed getting listen key."); return None
        except (RequestTimeout, ExchangeNotAvailable) as e: op_logger.warning(f"Network/Exch issue getting listen key: {e}"); return None
        except Exception as e: op_logger.error(f"Error getting listen key: {e}", exc_info=True); return None

def keep_listen_key_alive():
    global listen_key, binance_rest, user_websocket_running, shutdown_requested
    op_logger.info("Listen Key Keep-Alive thread started.")
    while not shutdown_requested:
        try:
            # Sleep in chunks to check shutdown flag more often
            for _ in range(LISTEN_KEY_REFRESH_INTERVAL_MINUTES * 60):
                 if shutdown_requested or not user_websocket_running: break # Exit if shutdown or UDS stopped
                 time.sleep(1)
            if shutdown_requested or not user_websocket_running: break

            with listen_key_lock:
                if not listen_key: op_logger.warning("No active listen key to keep alive."); continue
                if not binance_rest: op_logger.error("CCXT REST not available for keep-alive."); continue
                op_logger.info(f"Pinging listen key: {listen_key[:5]}...")
                try: binance_rest.fapiPrivatePutListenKey({'listenKey': listen_key}); op_logger.info("Listen key ping successful.")
                except AuthenticationError: op_logger.error("Auth failed pinging listen key! Shutting down."); shutdown_requested=True; break
                except ccxt.ExchangeError as e: op_logger.error(f"Exch error pinging listen key: {e}. Key might be invalid."); listen_key = None; user_websocket_running = False; break # Stop UDS if key invalid
                except (RequestTimeout, ExchangeNotAvailable) as e: op_logger.warning(f"Network/Exch issue pinging listen key: {e}")
                except Exception as e: op_logger.error(f"Error pinging listen key: {e}", exc_info=True)
        except Exception as e: op_logger.error(f"Error in listen key keep-alive loop: {e}", exc_info=True); time.sleep(60)
    op_logger.info("Listen Key Keep-Alive thread finished.")

def start_user_stream():
    global listen_key, user_wsapp, user_websocket_running
    if not get_listen_key(): op_logger.error("Failed get initial listen key. UDS cannot start."); user_websocket_running = False; return False
    keep_alive_thread = Thread(target=keep_listen_key_alive, daemon=True); keep_alive_thread.start()
    ws_url = f"wss://fstream.binance.com/ws/{listen_key}"
    op_logger.info(f"Connecting to User Data Stream: {ws_url[:35]}...")
    user_wsapp = websocket.WebSocketApp(ws_url, on_open=on_open_user_stream, on_message=on_message_user_stream, on_error=on_error_user_stream, on_close=on_close_user_stream)
    user_stream_thread = Thread(target=lambda: user_wsapp.run_forever(ping_interval=180, ping_timeout=10), daemon=True)
    user_stream_thread.start()
    op_logger.info("User Data Stream thread started.")
    # Check initial connection status
    time.sleep(3) # Give time for on_open to set the flag
    if not user_websocket_running:
         op_logger.error("User Data Stream failed to connect initially.")
         return False
    return True

# ==============================================================================
# 메인 실행 로직 (재연결 루프 포함)
# ==============================================================================
if __name__ == "__main__":
    start_time_str = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S %Z")
    op_logger.info(f"Bot starting at: {start_time_str}")

    if SIMULATION_MODE: op_logger.error("Set SIMULATION_MODE=False."); exit()
    if not API_KEY or API_KEY == "YOUR_BINANCE_API_KEY" or not API_SECRET: op_logger.error("API Key/Secret not set!"); exit()

    op_logger.warning("="*30 + f" REAL TRADING MODE - {log_prefix} " + "="*30)
    op_logger.warning("Strategy: Stoch Entry / SL-TP Exit (Dyn TP)")
    op_logger.warning(f"Settings: MaxPos:{MAX_OPEN_POSITIONS}, Lev:{LEVERAGE}x, TF:{TIMEFRAME}, SymUpdate:{SYMBOL_UPDATE_INTERVAL_HOURS}h, RESTSync:{REST_SYNC_INTERVAL_MINUTES}min")
    op_logger.warning("!!! USING REAL FUNDS - MONITOR CLOSELY !!!"); op_logger.warning("="*80)
    for i in range(3, 0, -1): print(f"Starting in {i}...", end='\r'); time.sleep(1)
    print("Starting now!      ")

    if not initialize_binance_rest(): op_logger.error("Exiting: CCXT REST init failure."); exit()
    op_logger.info("Running initial REST state sync..."); sync_positions_with_exchange(); op_logger.info("Initial REST sync complete."); log_asset_status()
    if not start_user_stream(): op_logger.error("Exiting: Failed start UDS."); exit()

    sync_thread = Thread(target=sync_state_periodically, args=(REST_SYNC_INTERVAL_MINUTES * 60,), daemon=True); sync_thread.start()

    ws_url_kline = f"wss://fstream.binance.com/stream"
    reconnect_delay = 5
    symbol_update_thread = None # Initialize symbol update thread variable

    try:
        while not shutdown_requested:
            if not kline_websocket_running:
                op_logger.info("Attempting K-line WebSocket connection/reconnection...")
                if kline_wsapp and kline_wsapp.sock:
                    try: kline_wsapp.close()
                    except Exception: pass
                    time.sleep(1)

                # Determine correct on_open callback
                current_on_open_kline = on_open_kline_initial if kline_thread is None else on_open_kline_reconnect

                kline_wsapp = websocket.WebSocketApp(ws_url_kline, on_open=current_on_open_kline, on_message=on_message_kline, on_error=on_error_kline, on_close=on_close_kline)
                kline_thread = Thread(target=lambda: kline_wsapp.run_forever(ping_interval=60, ping_timeout=10), daemon=True)
                kline_thread.start()
                op_logger.info("New K-line WS thread started. Waiting for connection...")

                # Start symbol update thread only if not already running
                if symbol_update_thread is None or not symbol_update_thread.is_alive():
                    symbol_update_thread = Thread(target=update_top_symbols_periodically, args=(SYMBOL_UPDATE_INTERVAL_HOURS * 60 * 60,), daemon=True)
                    symbol_update_thread.start()
                    op_logger.info("Symbol Update thread started/restarted.")

                connect_wait_start = time.time()
                while not kline_websocket_running and time.time() - connect_wait_start < 15:
                    if shutdown_requested: break
                    time.sleep(0.5)

                if kline_websocket_running: op_logger.info("K-line WS connected/reconnected."); reconnect_delay = 5
                else:
                    op_logger.error(f"K-line WS connection failed. Retrying in {reconnect_delay}s...")
                    if kline_wsapp and kline_wsapp.sock: kline_wsapp.close() # Close failed attempt
                    time.sleep(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 2, 60)

            elif not user_websocket_running and not shutdown_requested:
                op_logger.error("User Data Stream disconnected! Shutting down bot."); shutdown_requested = True
                if kline_wsapp and kline_wsapp.sock: kline_wsapp.close()

            else: # Both running or shutdown requested
                log_asset_status()
                time.sleep(1)

    except KeyboardInterrupt: op_logger.info("Keyboard interrupt. Shutting down..."); shutdown_requested = True
    except Exception as main_loop_err: op_logger.error(f"Critical error in main loop: {main_loop_err}", exc_info=True); shutdown_requested = True
    finally:
        op_logger.info("Initiating final shutdown sequence...")
        shutdown_requested = True # Ensure flag is set
        kline_websocket_running = False; user_websocket_running = False # Set flags for threads relying on them

        if kline_wsapp and kline_wsapp.sock and kline_wsapp.sock.connected: op_logger.info("Closing K-line WS..."); kline_wsapp.close()
        if user_wsapp and user_wsapp.sock and user_wsapp.sock.connected: op_logger.info("Closing User Data WS..."); user_wsapp.close()

        with listen_key_lock:
            if listen_key and binance_rest:
                op_logger.info(f"Deleting listen key...");
                try: binance_rest.fapiPrivateDeleteListenKey({'listenKey': listen_key})
                except Exception as del_key_err: op_logger.warning(f"Could not delete listen key: {del_key_err}")

        op_logger.info("Waiting for threads to finish (max 5s)...")
        # Wait for daemon threads implicitly or explicitly join if needed.
        time.sleep(5)

        op_logger.warning("Attempting to cancel all remaining open orders...")
        all_cancelled_final = True
        try:
            markets = binance_rest.fetch_markets()
            usdt_futures = [mkt['symbol'] for mkt in markets if mkt.get('type') == 'future' and mkt.get('quote') == 'USDT']
            op_logger.info(f"Checking orders for {len(usdt_futures)} USDT futures markets...")
            for symbol_ccxt in usdt_futures:
                if not cancel_open_orders_for_symbol(symbol_ccxt): op_logger.error(f"Failed cancel orders for {symbol_ccxt}.")
                time.sleep(0.3)
        except Exception as cancel_all_err: op_logger.error(f"Error final order cancellation: {cancel_all_err}"); all_cancelled_final = False

        if all_cancelled_final: op_logger.info("Finished attempting final order cancellation.")
        else: op_logger.error("Potential issues during final order cancellation. MANUAL CHECK REQUIRED.")

        op_logger.info("Fetching final balance...")
        final_balance = get_current_balance(); final_balance_str = f"{final_balance:.2f}" if final_balance is not None else "Error"
        with stats_lock: final_trades, final_wins = total_trades, winning_trades
        final_win_rate = (final_wins / final_trades * 100) if final_trades > 0 else 0.0
        final_msg = f"Final Balance:{final_balance_str} {TARGET_ASSET}, Trades:{final_trades}, Wins:{final_wins}(UDS), WinRate:{final_win_rate:.2f}%"
        op_logger.info(final_msg); asset_logger.info(final_msg)
        op_logger.info(f"{log_prefix} Bot shutdown complete.")