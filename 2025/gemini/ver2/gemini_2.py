# -*- coding: utf-8 -*-
# === 최종 버전 V2.3.1 (BBM 추세 기반 진입 로직 + 추세 강화 조건 + 586라인 문법 오류 수정) ===
# === 로직: BBM 2차 저항(낮은 고점)/지지(높은 저점) 시 진입 / 동적 BBands TP / 고정 SL / REST Sync / Auto K-line Reconnect ===

# Imports
import ccxt
from ccxt.base.errors import OrderNotFound, RateLimitExceeded, ExchangeNotAvailable, OnMaintenance, InvalidNonce, RequestTimeout, AuthenticationError, NetworkError, ExchangeError
import pandas as pd
import pandas_ta as ta # pandas_ta 라이브러리 사용
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
# 사용자 설정 값 (User Settings)
# ==============================================================================
API_KEY = "" # 실제 API 키로 변경하세요
API_SECRET = "" # 실제 API 시크릿으로 변경하세요
SIMULATION_MODE = False # 실제 거래 시 False로 설정
LEVERAGE = 10 # 레버리지 설정
MAX_OPEN_POSITIONS = 4 # 최대 동시 진입 포지션 수
TOP_N_SYMBOLS = 50 # 거래량 상위 N개 심볼 선택
TIMEFRAME = '15m' # 사용할 캔들 시간봉
TIMEFRAME_MINUTES = 15 # 시간봉 분 단위
TARGET_ASSET = 'USDT' # 타겟 자산 (테더)

# --- Stochastic RSI 설정 (현재 진입 로직에서 직접 사용 안 함, 지표 계산에는 포함) ---
STOCHRSI_LENGTH = 21
STOCHRSI_RSI_LENGTH = 21
STOCHRSI_K = 10
STOCHRSI_D = 10

# --- Bollinger Bands 설정 (동적 TP용 및 신규 진입 로직용) ---
BBANDS_PERIOD = 20 # 볼린저 밴드 기간
BBANDS_STDDEV = 2.0 # 볼린저 밴드 표준편차
BBM_REJECTION_PERCENT = 0.0002 # BBM 저항/지지 판단 시 허용 오차 (1%)

# --- SL 설정 ---
LONG_STOP_LOSS_FACTOR = 0.995 # 롱 포지션 손절 비율 (진입가 * 0.995)
SHORT_STOP_LOSS_FACTOR = 1.005 # 숏 포지션 손절 비율 (진입가 * 1.005)

# --- 기타 설정 ---
POSITION_MONITORING_DELAY_MINUTES = 5 # 포지션 진입 후 TP 업데이트 시작까지 대기 시간(분)
TP_UPDATE_THRESHOLD_PERCENT = 0.1 # TP 업데이트를 위한 최소 가격 변동률 (%)
REST_SYNC_INTERVAL_MINUTES = 5 # REST API 상태 동기화 주기(분)
SYMBOL_UPDATE_INTERVAL_HOURS = 2 # 거래 대상 심볼 목록 업데이트 주기(시간)
API_RETRY_COUNT = 3 # API 호출 실패 시 재시도 횟수
API_RETRY_DELAY_SECONDS = 2 # API 호출 재시도 간격(초)
FEE_RATE = 0.0005 # 예상 수수료율 (시장가 기준, 필요시 조정, 예: 0.05% -> 0.0005)
INITIAL_CANDLE_FETCH_LIMIT = 100 # 초기 캔들 데이터 로드 개수 (지표 계산 위해 충분히 확보)
MAX_CANDLE_HISTORY = 200 # 메모리에 유지할 최대 캔들 개수
KST = ZoneInfo("Asia/Seoul") # 한국 시간대
UTC = timezone.utc # UTC 시간대
pd.set_option('display.max_rows', None); pd.set_option('display.max_columns', None); pd.set_option('display.width', None) # Pandas 출력 옵션

# ==============================================================================
# 로깅 설정 (Logging Setup)
# ==============================================================================
log_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in locals() else os.getcwd()
log_filename_base = "bot_log"
log_prefix = "[BBM_TREND_V2.3.1_Enhanced]" # 로그 메시지 접두사 변경

op_logger = logging.getLogger('operation')
op_logger.setLevel(logging.INFO)
op_formatter = logging.Formatter(f'%(asctime)s - %(levelname)s - {log_prefix} - %(message)s')
op_handler = logging.FileHandler(os.path.join(log_dir, f'{log_filename_base}_operation.log'))
op_handler.setFormatter(op_formatter)
op_logger.addHandler(op_handler)
op_logger.addHandler(logging.StreamHandler())

trade_logger = logging.getLogger('trade'); trade_logger.setLevel(logging.INFO)
trade_formatter = logging.Formatter(f'%(asctime)s - {log_prefix} - %(message)s')
trade_handler = logging.FileHandler(os.path.join(log_dir, f'{log_filename_base}_trade.log'))
trade_handler.setFormatter(trade_formatter); trade_logger.addHandler(trade_handler)

asset_logger = logging.getLogger('asset'); asset_logger.setLevel(logging.INFO)
asset_formatter = logging.Formatter(f'%(asctime)s - {log_prefix} - %(message)s')
asset_handler = logging.FileHandler(os.path.join(log_dir, f'{log_filename_base}_asset.log'))
asset_handler.setFormatter(asset_formatter); asset_logger.addHandler(asset_handler)

# ==============================================================================
# 전역 변수 및 동기화 객체 (Global Variables & Synchronization Objects)
# ==============================================================================
real_positions = {}
real_positions_lock = Lock()
total_trades = 0
winning_trades = 0
stats_lock = Lock()
historical_data = {}
data_lock = Lock()
entry_in_progress = {}
entry_lock = Lock()
last_asset_log_time = datetime.now(UTC)
kline_websocket_running = False
kline_wsapp = None
subscribed_symbols = set()
subscribed_symbols_lock = Lock()
shutdown_requested = False
kline_thread = None
symbol_update_thread = None
sync_thread = None
binance_rest = None

# --- BBM 추세 진입 로직용 상태 변수 ---
first_resistance_details = {} # {symbol_ws: {'timestamp': pd.Timestamp | None, 'price': float | None}}
first_resistance_details_lock = Lock()
first_support_details = {}    # {symbol_ws: {'timestamp': pd.Timestamp | None, 'price': float | None}}
first_support_details_lock = Lock()

# ==============================================================================
# 심볼 형식 변환 유틸리티 (Symbol Format Conversion Utilities)
# ==============================================================================
def convert_symbol_to_ccxt(symbol_ws):
    if not symbol_ws.endswith(TARGET_ASSET):
        op_logger.warning(f"Cannot convert to CCXT: {symbol_ws} does not end with {TARGET_ASSET}")
        return symbol_ws
    return f"{symbol_ws[:-len(TARGET_ASSET)]}/{TARGET_ASSET}"

def convert_symbol_to_ws(symbol_ccxt):
    if f'/{TARGET_ASSET}' not in symbol_ccxt:
        op_logger.warning(f"Cannot convert to WS: {symbol_ccxt} does not contain /{TARGET_ASSET}")
        return symbol_ccxt
    return symbol_ccxt.replace(f'/{TARGET_ASSET}', TARGET_ASSET)

# ==============================================================================
# API 호출 재시도 헬퍼 함수 (API Call Retry Helper Function)
# ==============================================================================
def call_api_with_retry(api_call, max_retries=API_RETRY_COUNT, delay_seconds=API_RETRY_DELAY_SECONDS, error_message="API call failed"):
    retries = 0
    while retries < max_retries:
        try:
            return api_call()
        except (RequestTimeout, ExchangeNotAvailable, OnMaintenance, NetworkError, ExchangeError) as e:
            retries += 1
            op_logger.warning(f"{error_message}. Retry {retries}/{max_retries} after {delay_seconds}s. Error: {e}")
            if retries < max_retries:
                time.sleep(delay_seconds)
            else:
                op_logger.error(f"{error_message}. Max retries reached.")
                raise e
        except AuthenticationError as auth_e:
            op_logger.error(f"Auth Error during {error_message}: {auth_e}. Cannot retry.")
            raise auth_e
        except Exception as e:
            op_logger.error(f"Unexpected error during {error_message}: {e}", exc_info=True)
            raise e
    raise Exception(f"{error_message}. Unexpected exit from retry loop.")

# ==============================================================================
# API 및 데이터 처리 함수 (API & Data Processing Functions)
# ==============================================================================
def initialize_binance_rest():
    global binance_rest
    op_logger.info("Initializing CCXT REST...")
    if not API_KEY or API_KEY == "YOUR_BINANCE_API_KEY" or not API_SECRET or API_SECRET == "YOUR_BINANCE_API_SECRET":
        op_logger.error("API Key/Secret not configured or using placeholder values!")
        return False
    try:
        binance_rest = ccxt.binance({
            'apiKey': API_KEY,
            'secret': API_SECRET,
            'enableRateLimit': True,
            'options': { 'defaultType': 'future', 'adjustForTimeDifference': True }
        })
        binance_rest.load_markets()
        server_time = call_api_with_retry(lambda: binance_rest.fetch_time(), error_message="fetch_time")
        op_logger.info(f"Server time: {datetime.fromtimestamp(server_time / 1000, tz=UTC)}")
        op_logger.info("CCXT REST initialized.")
        return True
    except AuthenticationError:
        op_logger.error("REST API Auth Error! Check your API Key and Secret.")
        return False
    except Exception as e:
        op_logger.error(f"Failed to initialize CCXT REST: {e}", exc_info=True)
        return False

def get_current_balance(asset=TARGET_ASSET):
    if not binance_rest: return 0.0
    try:
        balance = call_api_with_retry(lambda: binance_rest.fetch_balance(params={'type': 'future'}), error_message="fetch_balance")
        return float(balance['free'].get(asset, 0.0))
    except Exception as e:
        op_logger.error(f"Unexpected error fetching balance for {asset} after retries: {e}")
        return 0.0

def get_top_volume_symbols(n=TOP_N_SYMBOLS):
    if not binance_rest: return []
    op_logger.info(f"Fetching top {n} symbols by volume...")
    try:
        tickers = call_api_with_retry(lambda: binance_rest.fetch_tickers(), error_message="fetch_tickers")
        futures_tickers = {
            s: t for s, t in tickers.items()
            if '/' in s and s.endswith(f"/{TARGET_ASSET}:{TARGET_ASSET}") and t.get('quoteVolume') is not None
        }
        if not futures_tickers:
            op_logger.warning(f"No {TARGET_ASSET} futures tickers found.")
            return []
        sorted_tickers = sorted(futures_tickers.values(), key=lambda x: x.get('quoteVolume', 0), reverse=True)
        top_symbols_ccxt = [t['symbol'].split(':')[0] for t in sorted_tickers[:n]]
        op_logger.info(f"Fetched top {len(top_symbols_ccxt)} symbols (CCXT format).")
        return top_symbols_ccxt
    except Exception as e:
        op_logger.error(f"Error fetching top symbols after retries: {e}")
        return []

def fetch_initial_ohlcv(symbol_ccxt, timeframe=TIMEFRAME, limit=INITIAL_CANDLE_FETCH_LIMIT):
    if not binance_rest: return None
    try:
        bbands_buffer = BBANDS_PERIOD + 50
        actual_limit = max(limit, bbands_buffer, MAX_CANDLE_HISTORY, STOCHRSI_LENGTH + STOCHRSI_RSI_LENGTH + 50)
        op_logger.debug(f"Fetching initial {actual_limit} candles for {symbol_ccxt} ({timeframe})...")
        ohlcv = call_api_with_retry(lambda: binance_rest.fetch_ohlcv(symbol_ccxt, timeframe=timeframe, limit=actual_limit),
                                    error_message=f"fetch_ohlcv for {symbol_ccxt}")
        if not ohlcv:
            op_logger.warning(f"No OHLCV data returned for {symbol_ccxt}.")
            return None
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
        df.set_index('timestamp', inplace=True)
        op_logger.debug(f"Fetched {len(df)} candles for {symbol_ccxt}.")
        return df
    except Exception as e:
        op_logger.error(f"Error fetching OHLCV for {symbol_ccxt} after retries: {e}")
        return None

def calculate_indicators(df):
    bbands_req_len = BBANDS_PERIOD
    stochrsi_req_len = STOCHRSI_LENGTH + STOCHRSI_RSI_LENGTH + 1
    required_len = max(bbands_req_len, stochrsi_req_len) + 50

    if df is None or len(df) < required_len:
        op_logger.debug(f"Not enough data for indicators: Have {len(df) if df is not None else 0}, Need >{required_len}")
        return None
    try:
        df_copy = df.copy()
        df_copy.ta.stochrsi(length=STOCHRSI_LENGTH, rsi_length=STOCHRSI_RSI_LENGTH, k=STOCHRSI_K, d=STOCHRSI_D, append=True)
        df_copy.ta.bbands(length=BBANDS_PERIOD, std=BBANDS_STDDEV, append=True)
        bbl_col = f'BBL_{BBANDS_PERIOD}_{float(BBANDS_STDDEV)}'
        bbm_col = f'BBM_{BBANDS_PERIOD}_{float(BBANDS_STDDEV)}'
        bbu_col = f'BBU_{BBANDS_PERIOD}_{float(BBANDS_STDDEV)}'
        rename_map = { bbl_col: 'BBL', bbm_col: 'BBM', bbu_col: 'BBU' }
        existing_rename_map = {k: v for k, v in rename_map.items() if k in df_copy.columns}

        if len(existing_rename_map) < 3:
             op_logger.warning(f"BBands columns not fully generated. Expected BBL, BBM, BBU. Available: {df_copy.columns.tolist()}")
             return None
        df_copy.rename(columns=existing_rename_map, inplace=True)

        required_indicator_cols = ['BBL', 'BBM', 'BBU']
        if not all(col in df_copy.columns for col in required_indicator_cols):
            op_logger.warning(f"Required BBands columns missing. Needed: {required_indicator_cols}, Have: {df_copy.columns.tolist()}")
            return None
        if len(df_copy) < 2 or df_copy[required_indicator_cols].iloc[-2:].isnull().any().any():
            op_logger.debug(f"Latest or previous BBands values (BBL, BBM, BBU) contain NaN.")
            return None
        return df_copy
    except Exception as e:
        op_logger.error(f"Indicator Calculation Error: {e}", exc_info=True)
        return None

def set_isolated_margin(symbol_ccxt, leverage):
    if not binance_rest: return False
    op_logger.info(f"Setting ISOLATED margin for {symbol_ccxt} / {leverage}x...")
    try:
        try:
            binance_rest.set_margin_mode('ISOLATED', symbol_ccxt, params={})
            op_logger.info(f"Margin mode successfully set to ISOLATED for {symbol_ccxt}.")
            time.sleep(0.2)
        except ccxt.ExchangeError as e:
            if 'No need to change margin type' in str(e) or 'already isolated' in str(e).lower():
                op_logger.warning(f"Margin mode for {symbol_ccxt} is already ISOLATED.")
            elif 'position exists' in str(e).lower():
                 op_logger.error(f"Cannot change margin mode for {symbol_ccxt}, an open position exists.")
                 return False
            else:
                op_logger.error(f"Failed to set margin mode for {symbol_ccxt}: {e}")
                return False
        try:
            binance_rest.set_leverage(leverage, symbol_ccxt, params={})
            op_logger.info(f"Leverage successfully set to {leverage}x for {symbol_ccxt}.")
            return True
        except ccxt.ExchangeError as e:
            if 'No need to change leverage' in str(e):
                op_logger.warning(f"Leverage for {symbol_ccxt} is already {leverage}x.")
                return True
            else:
                op_logger.error(f"Failed to set leverage for {symbol_ccxt}: {e}")
                return False
    except (RequestTimeout, ExchangeNotAvailable, OnMaintenance) as e:
        op_logger.warning(f"Temporary issue setting isolated margin for {symbol_ccxt}: {e}")
        return False
    except Exception as e:
        op_logger.error(f"Unexpected error setting isolated margin for {symbol_ccxt}: {e}", exc_info=True)
        return False

def place_market_order_real(symbol_ccxt, side, amount, current_price=None):
    if not binance_rest or amount <= 0:
        op_logger.error(f"[{symbol_ccxt}] Invalid arguments for market order: amount={amount}")
        return None
    try:
        mkt = binance_rest.market(symbol_ccxt)
        adj_amt_str = binance_rest.amount_to_precision(symbol_ccxt, amount)
        adj_amt = float(adj_amt_str)
        if adj_amt <= 0:
            op_logger.error(f"[{symbol_ccxt}] Adjusted amount '{adj_amt_str}' is <= 0 for market order.")
            return None
        min_notional = mkt.get('limits', {}).get('cost', {}).get('min', 5.0)
        if current_price and adj_amt * current_price < min_notional:
            op_logger.error(f"[{symbol_ccxt}] Estimated order value ({adj_amt * current_price:.2f}) is less than minimum required ({min_notional}). Amount: {adj_amt_str}, Price: {current_price}")
            return None
        op_logger.info(f"[REAL MARKET ORDER] Attempting {side.upper()} {adj_amt_str} {symbol_ccxt} @ MARKET")
        coid = f"bot_{uuid.uuid4().hex[:16]}"
        params = {'newClientOrderId': coid}
        order = binance_rest.create_market_order(symbol_ccxt, side, adj_amt, params=params)
        oid = order.get('id')
        op_logger.info(f"[REAL MARKET ORDER PLACED] ID:{oid} CliID:{coid} Sym:{symbol_ccxt} Side:{side} ReqAmt:{adj_amt_str}")
        trade_logger.info(f"REAL MARKET ORDER: {side.upper()} {symbol_ccxt}, ReqAmt:{adj_amt_str}, OrdID:{oid}, CliOrdID:{coid}")
        return {'id': oid, 'clientOrderId': coid, 'status': order.get('status', 'open')}
    except ccxt.InsufficientFunds as e:
        op_logger.error(f"[ORDER FAILED] Insufficient funds for {symbol_ccxt} market order: {e}")
        return None
    except ccxt.ExchangeError as e:
        op_logger.error(f"[ORDER FAILED] Exchange error placing market order for {symbol_ccxt}: {e}")
        return None
    except (RequestTimeout, ExchangeNotAvailable, OnMaintenance) as e:
        op_logger.warning(f"[ORDER FAILED] Network/Exchange issue placing market order for {symbol_ccxt}: {e}")
        return None
    except Exception as e:
        op_logger.error(f"[ORDER FAILED] Unexpected error placing market order for {symbol_ccxt}: {e}", exc_info=True)
        return None

def place_stop_market_order(symbol_ccxt, side, stop_price, amount):
    if not binance_rest or amount <= 0 or stop_price <= 0:
        op_logger.error(f"[{symbol_ccxt}] Invalid arguments for stop market order: amount={amount}, stop_price={stop_price}")
        return None
    try:
        amt_str = binance_rest.amount_to_precision(symbol_ccxt, amount)
        sp_str = binance_rest.price_to_precision(symbol_ccxt, stop_price)
        amt = float(amt_str)
        if amt <= 0:
            op_logger.error(f"[{symbol_ccxt}] SL Adjusted amount '{amt_str}' is <= 0.")
            return None
        op_logger.info(f"[REAL SL ORDER] Attempting {side.upper()} {amt_str} {symbol_ccxt} if price hits {sp_str}")
        coid = f"sl_{uuid.uuid4().hex[:16]}"
        params = {'stopPrice': sp_str, 'reduceOnly': True, 'newClientOrderId': coid}
        order = binance_rest.create_order(symbol_ccxt, 'STOP_MARKET', side, amt, None, params)
        oid = order.get('id')
        op_logger.info(f"[REAL SL PLACED] ID:{oid} CliID:{coid} Sym:{symbol_ccxt} Side:{side} StopPx:{sp_str} Amt:{amt_str}")
        trade_logger.info(f"REAL SL ORDER SET: {side.upper()} {symbol_ccxt}, Amt:{amt_str}, StopPx:{sp_str}, OrdID:{oid}, CliOrdID:{coid}")
        return {'id': oid, 'clientOrderId': coid}
    except ccxt.ExchangeError as e:
        op_logger.error(f"[SL FAILED] Exchange error placing stop market order for {symbol_ccxt}: {e}")
        return None
    except (RequestTimeout, ExchangeNotAvailable, OnMaintenance) as e:
        op_logger.warning(f"[SL FAILED] Network/Exchange issue placing stop market order for {symbol_ccxt}: {e}")
        return None
    except Exception as e:
        op_logger.error(f"[SL FAILED] Unexpected error placing stop market order for {symbol_ccxt}: {e}", exc_info=True)
        return None

def place_take_profit_market_order(symbol_ccxt, side, stop_price, amount):
    if not binance_rest or amount <= 0 or stop_price <= 0:
        op_logger.error(f"[{symbol_ccxt}] Invalid arguments for take profit market order: amount={amount}, stop_price={stop_price}")
        return None
    try:
        amt_str = binance_rest.amount_to_precision(symbol_ccxt, amount)
        sp_str = binance_rest.price_to_precision(symbol_ccxt, stop_price)
        amt = float(amt_str)
        if amt <= 0:
            op_logger.error(f"[{symbol_ccxt}] TP Adjusted amount '{amt_str}' is <= 0.")
            return None
        op_logger.info(f"[REAL TP ORDER] Attempting {side.upper()} {amt_str} {symbol_ccxt} if price hits {sp_str}")
        coid = f"tp_{uuid.uuid4().hex[:16]}"
        params = {'stopPrice': sp_str, 'reduceOnly': True, 'newClientOrderId': coid}
        order = binance_rest.create_order(symbol_ccxt, 'TAKE_PROFIT_MARKET', side, amt, None, params)
        oid = order.get('id')
        op_logger.info(f"[REAL TP PLACED] ID:{oid} CliID:{coid} Sym:{symbol_ccxt} Side:{side} StopPx:{sp_str} Amt:{amt_str}")
        trade_logger.info(f"REAL TP ORDER SET: {side.upper()} {symbol_ccxt}, Amt:{amt_str}, StopPx:{sp_str}, OrdID:{oid}, CliOrdID:{coid}")
        return {'id': oid, 'clientOrderId': coid}
    except ccxt.ExchangeError as e:
        op_logger.error(f"[TP FAILED] Exchange error placing take profit market order for {symbol_ccxt}: {e}")
        return None
    except (RequestTimeout, ExchangeNotAvailable, OnMaintenance) as e:
        op_logger.warning(f"[TP FAILED] Network/Exchange issue placing take profit market order for {symbol_ccxt}: {e}")
        return None
    except Exception as e:
        op_logger.error(f"[TP FAILED] Unexpected error placing take profit market order for {symbol_ccxt}: {e}", exc_info=True)
        return None

def cancel_order(symbol_ccxt, order_id=None, client_order_id=None):
    if not binance_rest or (not order_id and not client_order_id):
        op_logger.warning(f"[{symbol_ccxt}] Cancel order called with no ID.")
        return True
    target_id_str = f"ID={order_id}" if order_id else f"CliID={client_order_id}"
    op_logger.info(f"Attempting to cancel order {target_id_str} for {symbol_ccxt}...")
    try:
        if order_id:
            binance_rest.cancel_order(order_id, symbol_ccxt)
        else:
            binance_rest.cancel_order(client_order_id, symbol_ccxt, params={'origClientOrderId': client_order_id})
        op_logger.info(f"Successfully cancelled order {target_id_str} for {symbol_ccxt}.")
        return True
    except OrderNotFound:
        op_logger.warning(f"Order {target_id_str} for {symbol_ccxt} not found, likely already closed/cancelled.")
        return True
    except (ExchangeNotAvailable, OnMaintenance, RequestTimeout) as e:
        op_logger.error(f"Cannot cancel order {target_id_str} for {symbol_ccxt} due to temporary issue: {e}")
        return False
    except RateLimitExceeded as e:
        op_logger.error(f"Rate limit exceeded cancelling order {target_id_str} for {symbol_ccxt}: {e}")
        return False
    except ccxt.ExchangeError as e:
        if 'Order does not exist' in str(e) or '-2011' in str(e):
            op_logger.warning(f"Order {target_id_str} for {symbol_ccxt} likely already gone (ExchangeError).")
            return True
        else:
            op_logger.error(f"Failed to cancel order {target_id_str} for {symbol_ccxt}: {e}")
            return False
    except Exception as e:
        op_logger.error(f"Unexpected error cancelling order {target_id_str} for {symbol_ccxt}: {e}", exc_info=True)
        return False

def cancel_open_orders_for_symbol(symbol_ccxt):
    if not binance_rest: return False
    op_logger.warning(f"Attempting to cancel ALL open orders for {symbol_ccxt}...")
    cancelled_count, success = 0, True
    try:
        open_orders = call_api_with_retry(lambda: binance_rest.fetch_open_orders(symbol_ccxt),
                                          error_message=f"fetch_open_orders for {symbol_ccxt}")
    except Exception as fetch_e:
        op_logger.error(f"Error fetching open orders for {symbol_ccxt} to cancel: {fetch_e}")
        return False
    if not open_orders:
        op_logger.info(f"No open orders found for {symbol_ccxt}.")
        return True
    op_logger.info(f"Found {len(open_orders)} open orders for {symbol_ccxt}. Cancelling them...")
    for o in open_orders:
        if not cancel_order(symbol_ccxt, order_id=o.get('id'), client_order_id=o.get('clientOrderId')):
            success = False
        else:
            cancelled_count += 1
        time.sleep(0.2)
    op_logger.info(f"Finished cancellation attempt for {symbol_ccxt}. Cancelled {cancelled_count}/{len(open_orders)} orders.")
    return success

def log_asset_status():
    global last_asset_log_time
    now = datetime.now(UTC)
    if now - last_asset_log_time >= timedelta(hours=1):
        try:
            bal = get_current_balance()
            bal_str = f"{bal:.2f}" if bal is not None else "Error"
            with stats_lock:
                trades, wins = total_trades, winning_trades
            win_rate = (wins / trades * 100) if trades > 0 else 0.0
            active_pos_ws = []
            num_active = 0
            with real_positions_lock:
                active_pos_ws = list(real_positions.keys())
                num_active = len(real_positions)
            asset_logger.info(f"Balance:{bal_str} {TARGET_ASSET}, Active Positions:{num_active} {active_pos_ws}, Total Trades:{trades}(Delayed), Winning Trades:{wins}(Delayed), Win Rate:{win_rate:.2f}%")
            last_asset_log_time = now
        except Exception as e:
            asset_logger.error(f"Error logging asset status: {e}", exc_info=True)

# ==============================================================================
# 상태 동기화 로직 (State Synchronization Logic)
# ==============================================================================
def sync_positions_with_exchange():
    global real_positions, total_trades, first_resistance_details, first_support_details
    op_logger.info("[SYNC_REST] Starting state synchronization (REST API Based)...")
    if not binance_rest:
        op_logger.error("[SYNC_REST] CCXT REST instance not ready. Skipping sync.")
        return

    try:
        exchange_positions_raw = call_api_with_retry(lambda: binance_rest.fetch_positions(), error_message="fetch_positions")
        time.sleep(0.1)
        exchange_pos_dict = {}
        for pos in exchange_positions_raw:
            try:
                amount = float(pos.get('info', {}).get('positionAmt', 0))
                if abs(amount) < 1e-9: continue
                symbol_ccxt_raw = pos.get('symbol')
                if not symbol_ccxt_raw: continue
                symbol_ccxt_clean = symbol_ccxt_raw.split(':')[0]
                if f'/{TARGET_ASSET}' not in symbol_ccxt_clean: continue
                symbol_ws = convert_symbol_to_ws(symbol_ccxt_clean)
                exchange_pos_dict[symbol_ws] = {
                    'side': 'long' if amount > 0 else 'short',
                    'amount': abs(amount),
                    'entry_price': float(pos.get('entryPrice', 0)),
                    'symbol_ccxt': symbol_ccxt_clean,
                    'unrealized_pnl': float(pos.get('unrealizedPnl', 0))
                }
            except Exception as parse_err:
                op_logger.error(f"[SYNC_REST] Error parsing exchange position data: Raw Symbol='{pos.get('symbol')}', Info='{pos.get('info')}', Error: {parse_err}")

        with real_positions_lock: local_pos_dict = real_positions.copy()
        local_pos_symbols_ws = set(local_pos_dict.keys())
        exchange_pos_symbols_ws = set(exchange_pos_dict.keys())
        L_only_ws = local_pos_symbols_ws - exchange_pos_symbols_ws
        E_only_ws = exchange_pos_symbols_ws - local_pos_symbols_ws
        Both_ws = local_pos_symbols_ws.intersection(exchange_pos_symbols_ws)

        op_logger.info(f"[SYNC_REST] State Check: Local_Only={len(L_only_ws)}, Exchange_Only={len(E_only_ws)}, Both={len(Both_ws)}")
        if L_only_ws: op_logger.info(f"[SYNC_REST] Local Only Symbols (WS format): {L_only_ws}")
        if E_only_ws: op_logger.info(f"[SYNC_REST] Exchange Only Symbols (WS format): {E_only_ws}")

        if L_only_ws:
            op_logger.warning(f"[SYNC_REST][WARN] Local positions not found on exchange (Initial Check): {L_only_ws}")
            for symbol_ws in L_only_ws:
                symbol_ccxt = convert_symbol_to_ccxt(symbol_ws)
                remove_local_state = False
                try:
                    specific_pos_list = call_api_with_retry(
                        lambda: binance_rest.fetch_positions(symbols=[symbol_ccxt]),
                        error_message=f"fetch_positions(symbol) for {symbol_ws}"
                    )
                    position_confirmed_gone = False
                    if isinstance(specific_pos_list, list):
                        if not specific_pos_list: position_confirmed_gone = True
                        else:
                            try:
                                amount_recheck = float(specific_pos_list[0].get('info', {}).get('positionAmt', 0))
                                if abs(amount_recheck) < 1e-9: position_confirmed_gone = True
                                else: op_logger.error(f"[SYNC_REST] Discrepancy! Position for {symbol_ws} exists on re-check. Amount: {amount_recheck}.")
                            except Exception as parse_e:
                                op_logger.error(f"[SYNC_REST] Error parsing re-checked position for {symbol_ws}: {parse_e}"); position_confirmed_gone = True
                    else: op_logger.error(f"[SYNC_REST] Unexpected response type for {symbol_ws}: {type(specific_pos_list)}"); position_confirmed_gone = True
                    if position_confirmed_gone: op_logger.warning(f"[SYNC_REST] Confirmed no active position for {symbol_ws}. Removing local state."); remove_local_state = True
                except Exception as recheck_e: op_logger.error(f"[SYNC_REST] Error re-checking {symbol_ws}: {recheck_e}"); remove_local_state = True

                if remove_local_state:
                    removed_info = None
                    with real_positions_lock:
                        if symbol_ws in real_positions: removed_info = real_positions.pop(symbol_ws, None)
                    if removed_info:
                        op_logger.info(f"[{symbol_ws}] Local position state removed.")
                        with stats_lock: total_trades += 1
                        with first_resistance_details_lock: first_resistance_details.pop(symbol_ws, None)
                        with first_support_details_lock: first_support_details.pop(symbol_ws, None)
                        op_logger.info(f"[{symbol_ws}] Cleared BBM confirmation details for removed position.")
                        op_logger.info(f"[{symbol_ws}] Cancelling orphaned orders for {symbol_ccxt}...")
                        cancel_open_orders_for_symbol(symbol_ccxt)

        if E_only_ws:
            op_logger.error(f"[SYNC_REST][CRITICAL] Untracked positions on exchange: {E_only_ws}. Attempting to close.")
            for symbol_ws in E_only_ws:
                pos_info = exchange_pos_dict.get(symbol_ws)
                time.sleep(0.1)
                if not pos_info: op_logger.error(f"[SYNC_REST] No info for untracked {symbol_ws}."); continue
                symbol_ccxt = pos_info.get('symbol_ccxt')
                if not symbol_ccxt: op_logger.error(f"[SYNC_REST] No symbol_ccxt for {symbol_ws}."); continue
                cancel_open_orders_for_symbol(symbol_ccxt); time.sleep(0.5)
                try:
                    ticker = call_api_with_retry(lambda: binance_rest.fetch_ticker(symbol_ccxt), error_message=f"fetch_ticker for closing {symbol_ccxt}")
                    current_price = ticker['last'] if ticker and 'last' in ticker else None
                    close_order_result = place_market_order_real(symbol_ccxt, 'sell' if pos_info['side'] == 'long' else 'buy', pos_info['amount'], current_price)
                    if not close_order_result or not close_order_result.get('id'):
                        op_logger.error(f"[SYNC_REST] Failed to close untracked {symbol_ccxt}. MANUAL INTERVENTION.")
                    else:
                        op_logger.info(f"[SYNC_REST] Closed untracked {symbol_ccxt}. Order ID: {close_order_result.get('id')}")
                        with stats_lock: # Corrected indentation for with statement
                            total_trades += 1
                except Exception as close_err: op_logger.error(f"[SYNC_REST] Error closing untracked {symbol_ccxt}: {close_err}. MANUAL INTERVENTION.")
                time.sleep(0.5)

        if Both_ws:
            for symbol_ws in Both_ws:
                local_info = local_pos_dict.get(symbol_ws); exchange_info = exchange_pos_dict.get(symbol_ws)
                if not local_info or not exchange_info: op_logger.error(f"[SYNC_REST] Inconsistency for {symbol_ws} in Both_ws."); continue
                symbol_ccxt = exchange_info.get('symbol_ccxt')
                if not symbol_ccxt: op_logger.error(f"[SYNC_REST] No symbol_ccxt for {symbol_ws} in Both_ws."); continue
                amount_diff = abs(local_info.get('amount', 0) - exchange_info.get('amount', 0)) > 1e-6
                side_mismatch = local_info.get('side') != exchange_info.get('side')
                if amount_diff or side_mismatch:
                    op_logger.warning(f"[SYNC_REST][DISCREPANCY] {symbol_ccxt}! Local: {local_info.get('side')} {local_info.get('amount',0):.8f}, Exch: {exchange_info.get('side')} {exchange_info.get('amount',0):.8f}. Manual check advised.")
                try:
                    open_orders = call_api_with_retry(lambda: binance_rest.fetch_open_orders(symbol_ccxt), error_message=f"fetch_open_orders for sync {symbol_ccxt}")
                    open_order_ids = {str(o['id']) for o in open_orders}
                    sl_id = str(local_info.get('sl_order_id')) if local_info.get('sl_order_id') else None
                    if sl_id and sl_id not in open_order_ids:
                        op_logger.warning(f"[SYNC_REST] Local SL ID {sl_id} for {symbol_ccxt} not on exchange. Clearing local SL.")
                        with real_positions_lock:
                            if symbol_ws in real_positions and str(real_positions[symbol_ws].get('sl_order_id')) == sl_id:
                                real_positions[symbol_ws]['sl_order_id'] = None; real_positions[symbol_ws]['sl_client_order_id'] = None
                    tp_id = str(local_info.get('tp_order_id')) if local_info.get('tp_order_id') else None
                    if tp_id and tp_id not in open_order_ids:
                        op_logger.warning(f"[SYNC_REST] Local TP ID {tp_id} for {symbol_ccxt} not on exchange. Clearing local TP.")
                        with real_positions_lock:
                            if symbol_ws in real_positions and str(real_positions[symbol_ws].get('tp_order_id')) == tp_id:
                                real_positions[symbol_ws]['tp_order_id'] = None; real_positions[symbol_ws]['tp_client_order_id'] = None; real_positions[symbol_ws]['current_tp_price'] = None
                except Exception as order_check_e: op_logger.error(f"[SYNC_REST] Error checking orders for {symbol_ccxt}: {order_check_e}")
        op_logger.info("[SYNC_REST] REST state synchronization finished.")
    except AuthenticationError: op_logger.error("[SYNC_REST] Auth error! Shutting down."); global shutdown_requested; shutdown_requested = True
    except Exception as e: op_logger.error(f"[SYNC_REST] Critical error: {e}", exc_info=True); time.sleep(60)

def sync_state_periodically(interval_seconds):
    global shutdown_requested
    op_logger.info(f"REST Sync thread started. Interval: {interval_seconds}s.")
    while not shutdown_requested:
        try:
            wait_until = time.time() + interval_seconds
            while time.time() < wait_until and not shutdown_requested: time.sleep(1)
            if shutdown_requested: break
            sync_positions_with_exchange()
        except Exception as e: op_logger.error(f"Error in REST sync loop: {e}", exc_info=True); time.sleep(60)
    op_logger.info("REST Sync thread finished.")

# ==============================================================================
# 심볼 목록 주기적 업데이트 로직 (Symbol List Periodic Update Logic)
# ==============================================================================
def update_top_symbols_periodically(interval_seconds):
    global subscribed_symbols, historical_data, kline_websocket_running, kline_wsapp, shutdown_requested, first_resistance_details, first_support_details
    op_logger.info(f"Symbol Update thread started. Interval: {interval_seconds}s.")
    while not shutdown_requested:
        try:
            wait_until = time.time() + interval_seconds
            while time.time() < wait_until and not shutdown_requested: time.sleep(1)
            if shutdown_requested: break
            if not kline_websocket_running or not kline_wsapp or not kline_wsapp.sock or not kline_wsapp.sock.connected:
                op_logger.warning("[Symbol Update] K-line WS not ready. Skipping."); continue

            op_logger.info("[Symbol Update] Starting symbol update...")
            new_sym_ccxt = get_top_volume_symbols(TOP_N_SYMBOLS)
            if not new_sym_ccxt: op_logger.warning("[Symbol Update] Failed to fetch new symbols."); continue
            new_sym_ws = {convert_symbol_to_ws(s) for s in new_sym_ccxt}

            with subscribed_symbols_lock: current_subs_ws = subscribed_symbols.copy()
            to_add_ws = new_sym_ws - current_subs_ws
            to_remove_ws = current_subs_ws - new_sym_ws

            if to_remove_ws:
                op_logger.info(f"[Symbol Update] Removing (WS): {to_remove_ws}")
                streams_to_unsub = [f"{s.lower()}@kline_{TIMEFRAME}" for s in to_remove_ws]
                if streams_to_unsub:
                    msg = {"method": "UNSUBSCRIBE", "params": streams_to_unsub, "id": int(time.time())}
                    try:
                        if kline_wsapp and kline_wsapp.sock and kline_wsapp.sock.connected:
                            kline_wsapp.send(json.dumps(msg)); op_logger.info(f"[Symbol Update] UNSUBSCRIBE sent for {len(streams_to_unsub)}.")
                        else: op_logger.warning("[Symbol Update] K-line WS disconnected before UNSUBSCRIBE.")
                    except Exception as e: op_logger.error(f"[Symbol Update] Failed to send UNSUBSCRIBE: {e}")
                with subscribed_symbols_lock: subscribed_symbols -= to_remove_ws
                removed_count = 0
                with data_lock:
                    for s_ws in to_remove_ws:
                        if historical_data.pop(s_ws, None) is not None: removed_count += 1
                        with first_resistance_details_lock: first_resistance_details.pop(s_ws, None)
                        with first_support_details_lock: first_support_details.pop(s_ws, None)
                op_logger.info(f"[Symbol Update] Removed hist_data & BBM details for {removed_count} symbols.")

            if to_add_ws:
                op_logger.info(f"[Symbol Update] Adding (WS): {to_add_ws}")
                fetched_count, error_count = 0, 0; added_to_data_ws = set()
                for symbol_ws in to_add_ws:
                    if shutdown_requested: break
                    symbol_ccxt_to_fetch = convert_symbol_to_ccxt(symbol_ws)
                    df = fetch_initial_ohlcv(symbol_ccxt_to_fetch, TIMEFRAME)
                    if df is not None and not df.empty:
                        with data_lock: historical_data[symbol_ws] = df
                        with first_resistance_details_lock: first_resistance_details[symbol_ws] = {'timestamp': None, 'price': None}
                        with first_support_details_lock: first_support_details[symbol_ws] = {'timestamp': None, 'price': None}
                        fetched_count += 1; added_to_data_ws.add(symbol_ws)
                    else: error_count += 1; op_logger.warning(f"[Symbol Update] Failed to fetch data for new {symbol_ccxt_to_fetch}.")
                    time.sleep(0.3)
                op_logger.info(f"[Symbol Update] Fetched data for {fetched_count} new symbols ({error_count} errors).")
                if added_to_data_ws:
                    streams_to_sub = [f"{s.lower()}@kline_{TIMEFRAME}" for s in added_to_data_ws]
                    msg = {"method": "SUBSCRIBE", "params": streams_to_sub, "id": int(time.time())}
                    try:
                        if kline_wsapp and kline_wsapp.sock and kline_wsapp.sock.connected:
                            kline_wsapp.send(json.dumps(msg)); op_logger.info(f"[Symbol Update] SUBSCRIBE sent for {len(added_to_data_ws)}.")
                            with subscribed_symbols_lock: subscribed_symbols.update(added_to_data_ws)
                        else:
                            op_logger.warning("[Symbol Update] K-line WS disconnected before SUBSCRIBE new.");
                            with data_lock:
                                for s_ws in added_to_data_ws:
                                    historical_data.pop(s_ws, None)
                                    with first_resistance_details_lock: first_resistance_details.pop(s_ws, None)
                                    with first_support_details_lock: first_support_details.pop(s_ws, None)
                    except Exception as e:
                        op_logger.error(f"[Symbol Update] Failed to send SUBSCRIBE new: {e}");
                        with data_lock:
                            for s_ws in added_to_data_ws:
                                historical_data.pop(s_ws, None)
                                with first_resistance_details_lock: first_resistance_details.pop(s_ws, None)
                                with first_support_details_lock: first_support_details.pop(s_ws, None)
            with subscribed_symbols_lock: current_count = len(subscribed_symbols)
            op_logger.info(f"[Symbol Update] Finished. Subscribed to {current_count} symbols.")
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
            df = historical_data[symbol_ws]
            k_time = pd.to_datetime(kline_data['t'], unit='ms', utc=True)
            new_data_row = pd.DataFrame([{'open': float(kline_data['o']), 'high': float(kline_data['h']),
                                          'low': float(kline_data['l']), 'close': float(kline_data['c']),
                                          'volume': float(kline_data['v'])}], index=[k_time])
            if k_time in df.index: df.loc[k_time] = new_data_row.iloc[0]
            else: df = pd.concat([df, new_data_row]); df = df.iloc[-MAX_CANDLE_HISTORY:]
            historical_data[symbol_ws] = df
            return True
    except Exception as e: op_logger.error(f"[{symbol_ws}] Error updating historical data: {e}"); return False

def try_update_tp(symbol_ws, side, amount, tp_order_id, tp_client_order_id, current_tp_price, new_tp_target_price):
    symbol_ccxt = convert_symbol_to_ccxt(symbol_ws)
    with real_positions_lock:
        if symbol_ws not in real_positions:
            op_logger.warning(f"[{symbol_ws}] TP update skipped: Position no longer exists."); return False
    if not current_tp_price or not new_tp_target_price or new_tp_target_price <= 0 or current_tp_price <= 0: return False
    diff_percent = abs(new_tp_target_price - current_tp_price) / current_tp_price * 100
    if diff_percent >= TP_UPDATE_THRESHOLD_PERCENT:
        op_logger.info(f"[{symbol_ws}] TP target moved: {current_tp_price:.5f} -> {new_tp_target_price:.5f}. Updating TP for {symbol_ccxt}...")
        if not cancel_order(symbol_ccxt, order_id=tp_order_id, client_order_id=tp_client_order_id):
            op_logger.error(f"[{symbol_ws}] Failed to cancel prev TP for {symbol_ccxt}. Aborting TP update."); return False
        time.sleep(0.2)
        new_tp_info = None
        try:
            amt_str = binance_rest.amount_to_precision(symbol_ccxt, amount)
            tp_price_str = binance_rest.price_to_precision(symbol_ccxt, new_tp_target_price)
            final_tp_price = float(tp_price_str)
            new_tp_info = place_take_profit_market_order(symbol_ccxt, 'sell' if side == 'long' else 'buy', final_tp_price, float(amt_str))
        except Exception as e: op_logger.error(f"[{symbol_ws}] Error placing new TP for {symbol_ccxt}: {e}"); return False

        if new_tp_info and new_tp_info.get('id'):
            new_id, new_coid = new_tp_info['id'], new_tp_info.get('clientOrderId')
            with real_positions_lock:
                if symbol_ws in real_positions:
                    real_positions[symbol_ws]['tp_order_id'] = new_id
                    real_positions[symbol_ws]['tp_client_order_id'] = new_coid
                    real_positions[symbol_ws]['current_tp_price'] = final_tp_price
                    op_logger.info(f"[{symbol_ws}] Updated TP for {symbol_ccxt} to {final_tp_price:.5f} (New ID:{new_id}).")
                    return True
                else:
                    op_logger.warning(f"[{symbol_ws}] Position gone while updating TP for {symbol_ccxt}. Cancelling new TP (ID:{new_id}).")
                    Thread(target=cancel_order, args=(symbol_ccxt,), kwargs={'order_id': new_id, 'client_order_id': new_coid}, daemon=True).start()
                    return False
        else: op_logger.error(f"[{symbol_ws}] Failed to place new TP for {symbol_ccxt}. TP NOT SET!"); return False
    return False

def process_kline_message(symbol_ws, kline_data):
    global real_positions, entry_in_progress, first_resistance_details, first_support_details

    with subscribed_symbols_lock:
        if symbol_ws not in subscribed_symbols: return

    if not update_historical_data(symbol_ws, kline_data): return

    is_closed = kline_data.get('x', False)

    with data_lock: df = historical_data.get(symbol_ws)
    if df is None: return

    idf = calculate_indicators(df.copy())
    if idf is None or idf.empty or len(idf) < 2:
        return

    try:
        current_candle = idf.iloc[-1]
        prev_candle = idf.iloc[-2]
        current_price = current_candle['close']
        bbl, bbm, bbu = current_candle.get('BBL'), current_candle.get('BBM'), current_candle.get('BBU')
        prev_bbm_value = prev_candle.get('BBM')
        if any(pd.isna(v) for v in [current_price, bbl, bbm, bbu, prev_bbm_value,
                                     prev_candle['high'], prev_candle['low'], prev_candle['close'], prev_candle['open'],
                                     current_candle['high'], current_candle['low'], current_candle['open']]):
            return
    except IndexError: return
    except Exception as e: op_logger.error(f"[{symbol_ws}] Error accessing indicator data: {e}"); return

    now = datetime.now(UTC)
    symbol_ccxt = convert_symbol_to_ccxt(symbol_ws)
    pos_copy = {}
    with real_positions_lock: pos_copy = real_positions.copy()

    if symbol_ws in pos_copy:
        pinfo = pos_copy[symbol_ws]
        side, entry_time, amount = pinfo['side'], pinfo['entry_time'], pinfo['amount']
        tp_id, tp_coid, current_tp_val = pinfo.get('tp_order_id'), pinfo.get('tp_client_order_id'), pinfo.get('current_tp_price')
        if now - entry_time >= timedelta(minutes=POSITION_MONITORING_DELAY_MINUTES) and tp_id and current_tp_val:
            new_tp_target = bbu if side == 'long' else bbl
            if not pd.isna(new_tp_target) and new_tp_target > 0:
                try_update_tp(symbol_ws, side, amount, tp_id, tp_coid, current_tp_val, new_tp_target)

    if is_closed:
        with entry_lock: is_entry_attempted = entry_in_progress.get(symbol_ws, False)
        with real_positions_lock: position_exists = symbol_ws in real_positions

        if not is_entry_attempted and not position_exists:
            with real_positions_lock: open_position_count = len(real_positions)
            if open_position_count >= MAX_OPEN_POSITIONS: return

            with first_resistance_details_lock: resistance_info = first_resistance_details.get(symbol_ws, {'timestamp': None, 'price': None})
            with first_support_details_lock: support_info = first_support_details.get(symbol_ws, {'timestamp': None, 'price': None})

            tgt_side, tp_tgt, entry_px = None, None, current_price
            entry_condition_met = False

            prev_high_near_prev_bbm = (prev_candle['high'] >= prev_bbm_value * (1 - BBM_REJECTION_PERCENT)) and \
                                      (prev_candle['high'] <= prev_bbm_value * (1 + BBM_REJECTION_PERCENT))
            current_candle_confirms_rejection = current_candle['close'] < prev_candle['high'] and current_candle['close'] < current_candle['open']

            if prev_high_near_prev_bbm and current_candle_confirms_rejection:
                if resistance_info['price'] is not None:
                    first_recorded_resistance_high = resistance_info['price']
                    current_attempt_high = prev_candle['high']
                    if current_attempt_high < first_recorded_resistance_high:
                        op_logger.info(f"[{symbol_ws}] Second BBM resistance confirmed. PrevHigh:{current_attempt_high:.5f} (vs First:{first_recorded_resistance_high:.5f}) near PrevBBM:{prev_bbm_value:.5f}. CurrentClose:{current_price:.5f}. Attempting SHORT.")
                        tgt_side = 'sell'
                        tp_tgt = bbl if not pd.isna(bbl) and bbl > 0 else None
                        entry_condition_met = True
                    else:
                        op_logger.info(f"[{symbol_ws}] Second BBM resistance attempt at {current_attempt_high:.5f} NOT lower than first at {first_recorded_resistance_high:.5f}. Updating first resistance to current point.")
                        with first_resistance_details_lock: first_resistance_details[symbol_ws] = {'timestamp': prev_candle.name, 'price': current_attempt_high}
                        with first_support_details_lock: first_support_details[symbol_ws] = {'timestamp': None, 'price': None}
                else:
                    op_logger.info(f"[{symbol_ws}] First BBM resistance confirmed. PrevHigh:{prev_candle['high']:.5f} near PrevBBM:{prev_bbm_value:.5f}. CurrentClose:{current_price:.5f}. Waiting for second.")
                    with first_resistance_details_lock: first_resistance_details[symbol_ws] = {'timestamp': prev_candle.name, 'price': prev_candle['high']}
                    with first_support_details_lock: first_support_details[symbol_ws] = {'timestamp': None, 'price': None}
            elif resistance_info['price'] is not None and current_price > prev_bbm_value * (1 + BBM_REJECTION_PERCENT * 1.5):
                 op_logger.info(f"[{symbol_ws}] Price broke above BBM after 1st resistance. Resetting resistance state. Close:{current_price:.5f}, PrevBBM:{prev_bbm_value:.5f}")
                 with first_resistance_details_lock: first_resistance_details[symbol_ws] = {'timestamp': None, 'price': None}

            if not entry_condition_met:
                prev_low_near_prev_bbm = (prev_candle['low'] <= prev_bbm_value * (1 + BBM_REJECTION_PERCENT)) and \
                                         (prev_candle['low'] >= prev_bbm_value * (1 - BBM_REJECTION_PERCENT))
                current_candle_confirms_support = current_candle['close'] > prev_candle['low'] and current_candle['close'] > current_candle['open']

                if prev_low_near_prev_bbm and current_candle_confirms_support:
                    if support_info['price'] is not None:
                        first_recorded_support_low = support_info['price']
                        current_attempt_low = prev_candle['low']
                        if current_attempt_low > first_recorded_support_low:
                            op_logger.info(f"[{symbol_ws}] Second BBM support confirmed. PrevLow:{current_attempt_low:.5f} (vs First:{first_recorded_support_low:.5f}) near PrevBBM:{prev_bbm_value:.5f}. CurrentClose:{current_price:.5f}. Attempting LONG.")
                            tgt_side = 'buy'
                            tp_tgt = bbu if not pd.isna(bbu) and bbu > 0 else None
                            entry_condition_met = True
                        else:
                            op_logger.info(f"[{symbol_ws}] Second BBM support attempt at {current_attempt_low:.5f} NOT higher than first at {first_recorded_support_low:.5f}. Updating first support to current point.")
                            with first_support_details_lock: first_support_details[symbol_ws] = {'timestamp': prev_candle.name, 'price': current_attempt_low}
                            with first_resistance_details_lock: first_resistance_details[symbol_ws] = {'timestamp': None, 'price': None}
                    else:
                        op_logger.info(f"[{symbol_ws}] First BBM support confirmed. PrevLow:{prev_candle['low']:.5f} near PrevBBM:{prev_bbm_value:.5f}. CurrentClose:{current_price:.5f}. Waiting for second.")
                        with first_support_details_lock: first_support_details[symbol_ws] = {'timestamp': prev_candle.name, 'price': prev_candle['low']}
                        with first_resistance_details_lock: first_resistance_details[symbol_ws] = {'timestamp': None, 'price': None}
                elif support_info['price'] is not None and current_price < prev_bbm_value * (1 - BBM_REJECTION_PERCENT * 1.5):
                    op_logger.info(f"[{symbol_ws}] Price broke below BBM after 1st support. Resetting support state. Close:{current_price:.5f}, PrevBBM:{prev_bbm_value:.5f}")
                    with first_support_details_lock: first_support_details[symbol_ws] = {'timestamp': None, 'price': None}

            if entry_condition_met and tgt_side and tp_tgt is not None and tp_tgt > 0 and entry_px > 0:
                expected_profit_ratio = abs(tp_tgt - entry_px) / entry_px
                total_fee_ratio = 2 * FEE_RATE
                if expected_profit_ratio <= total_fee_ratio:
                    op_logger.info(f"[{symbol_ws}] {tgt_side.upper()} entry for {symbol_ccxt} SKIPPED: Profit ratio ({expected_profit_ratio:.4f}) vs Fee ({total_fee_ratio:.4f}). TP:{tp_tgt:.5f}, Entry:{entry_px:.5f}")
                else:
                    with entry_lock: entry_in_progress[symbol_ws] = True
                    try:
                        op_logger.info(f"[{symbol_ws}] ===> Starting Entry: {symbol_ccxt} {tgt_side.upper()} @ {entry_px:.5f}, Initial TP (BB): {tp_tgt:.5f}")
                        current_balance = get_current_balance()
                        if current_balance <= 10.0: raise Exception(f"Low balance ({current_balance:.2f})")
                        with real_positions_lock: oc = len(real_positions)
                        if oc >= MAX_OPEN_POSITIONS: raise Exception("Max positions reached")
                        portion = 1.0 / (MAX_OPEN_POSITIONS - oc) if MAX_OPEN_POSITIONS > oc else 1.0
                        margin_to_use = current_balance * portion
                        notional_value = margin_to_use * LEVERAGE
                        min_notional = binance_rest.market(symbol_ccxt).get('limits',{}).get('cost',{}).get('min',5.0)
                        if notional_value < min_notional: raise Exception(f"Notional ({notional_value:.2f}) < min ({min_notional}) for {symbol_ccxt}")
                        entry_amount = notional_value / entry_px if entry_px > 0 else 0
                        if entry_amount <= 0: raise Exception("Entry amount is zero/negative")
                        if not set_isolated_margin(symbol_ccxt, LEVERAGE): raise Exception("Failed to set margin/leverage")

                        entry_order = place_market_order_real(symbol_ccxt, tgt_side, entry_amount, entry_px)
                        if not entry_order or not entry_order.get('id'): raise Exception("Market entry order failed")
                        entry_oid, entry_coid = entry_order['id'], entry_order.get('clientOrderId')
                        op_logger.info(f"[{symbol_ws}] Market entry for {symbol_ccxt} placed (ID:{entry_oid}). Assuming filled.")

                        sl_order, tp_order = None, None; final_sl_price, final_tp_price = None, None
                        try:
                            final_sl_price = entry_px * (LONG_STOP_LOSS_FACTOR if tgt_side == 'buy' else SHORT_STOP_LOSS_FACTOR)
                            final_tp_price = tp_tgt
                            sl_tp_side = 'sell' if tgt_side == 'buy' else 'buy'
                            sl_order = place_stop_market_order(symbol_ccxt, sl_tp_side, final_sl_price, entry_amount)
                            if not sl_order or not sl_order.get('id'): raise Exception("SL order failed")
                            op_logger.info(f"[{symbol_ws}] SL for {symbol_ccxt} placed (ID:{sl_order['id']}) @ {final_sl_price:.5f}")
                            time.sleep(0.1)
                            tp_order = place_take_profit_market_order(symbol_ccxt, sl_tp_side, final_tp_price, entry_amount)
                            if not tp_order or not tp_order.get('id'): raise Exception("TP order failed")
                            op_logger.info(f"[{symbol_ws}] TP for {symbol_ccxt} placed (ID:{tp_order['id']}) @ {final_tp_price:.5f}")

                            with real_positions_lock:
                                if len(real_positions) < MAX_OPEN_POSITIONS:
                                    real_positions[symbol_ws] = {
                                        'side': 'long' if tgt_side == 'buy' else 'short', 'entry_price': entry_px,
                                        'amount': entry_amount, 'entry_time': now,
                                        'entry_order_id': entry_oid, 'entry_client_order_id': entry_coid,
                                        'sl_order_id': sl_order['id'], 'sl_client_order_id': sl_order.get('clientOrderId'),
                                        'tp_order_id': tp_order['id'], 'tp_client_order_id': tp_order.get('clientOrderId'),
                                        'current_tp_price': final_tp_price
                                    }
                                    op_logger.info(f"[{symbol_ws}] <<< Entry for {symbol_ccxt} SUCCESSFUL. Active Pos: {len(real_positions)} >>>")
                                    with first_resistance_details_lock: first_resistance_details[symbol_ws] = {'timestamp': None, 'price': None}
                                    with first_support_details_lock: first_support_details[symbol_ws] = {'timestamp': None, 'price': None}
                                else: raise Exception("Max positions during final storage")
                        except Exception as sltp_e:
                            op_logger.error(f"[{symbol_ws}] SL/TP error for {symbol_ccxt}: {sltp_e}. ROLLBACK!")
                            if sl_order and sl_order.get('id'): Thread(target=cancel_order, args=(symbol_ccxt,), kwargs={'order_id': sl_order['id']}, daemon=True).start()
                            if tp_order and tp_order.get('id'): Thread(target=cancel_order, args=(symbol_ccxt,), kwargs={'order_id': tp_order['id']}, daemon=True).start()
                            Thread(target=cancel_order, args=(symbol_ccxt,), kwargs={'order_id': entry_oid}, daemon=True).start()
                            with real_positions_lock: real_positions.pop(symbol_ws, None)
                            op_logger.warning(f"[{symbol_ws}] Entry SL/TP failed for {symbol_ccxt}. Reason: {sltp_e}")
                    except Exception as entry_e:
                        op_logger.error(f"[{symbol_ws}] Entry process for {symbol_ccxt} failed: {entry_e}", exc_info=False)
                        with real_positions_lock: real_positions.pop(symbol_ws, None)
                        op_logger.warning(f"[{symbol_ws}] Entry process failed for {symbol_ccxt}. Reason: {entry_e}")
                    finally:
                        with entry_lock: entry_in_progress.pop(symbol_ws, None)
            elif entry_condition_met and (tp_tgt is None or tp_tgt <= 0 or entry_px <= 0) :
                 op_logger.warning(f"[{symbol_ws}] {tgt_side.upper() if tgt_side else 'N/A'} entry for {symbol_ccxt} SKIPPED due to invalid TP ({tp_tgt}) or EntryPx ({entry_px}). First confirmation state retained.")

# ==============================================================================
# 웹소켓 콜백 함수 (K-line)
# ==============================================================================
def on_message_kline(wsapp, message):
    try:
        data = json.loads(message)
        if 'stream' in data and 'data' in data:
            stream_name = data['stream']; payload = data['data']
            if payload.get('e') == 'kline':
                symbol_upper_ws = stream_name.split('@')[0].upper()
                process_kline_message(symbol_upper_ws, payload['k'])
        elif 'result' in data and data.get('id'): op_logger.info(f"K-line WS sub response: {data}")
        elif 'e' in data and data['e'] == 'error': op_logger.error(f"K-line WS API Error: {data}")
    except json.JSONDecodeError: op_logger.error(f"K-line WS JSON Decode Error: {message[:100]}")
    except Exception as e: op_logger.error(f"Error processing K-line WS message: {e}", exc_info=True)

def on_error_kline(wsapp, error):
    op_logger.error(f"K-line WS Error: {error}")
    if isinstance(error, ConnectionRefusedError): op_logger.error("Connection refused. Check network/Binance status.")

def on_close_kline(wsapp, close_status_code, close_msg):
    global kline_websocket_running
    if not shutdown_requested:
        op_logger.warning(f"K-line WS closed unexpectedly! Code:{close_status_code}, Msg:{close_msg}. Reconnecting...")
        kline_websocket_running = False
    else: op_logger.info(f"K-line WS closed gracefully."); kline_websocket_running = False

def on_open_kline_initial(wsapp):
    global subscribed_symbols, historical_data, kline_websocket_running, shutdown_requested, first_resistance_details, first_support_details
    kline_websocket_running = True
    op_logger.info("K-line WS initial connection opened.")
    op_logger.info("Fetching initial top symbols...")
    initial_sym_ccxt = get_top_volume_symbols(TOP_N_SYMBOLS)
    if not initial_sym_ccxt: op_logger.error("Could not fetch initial symbols. Shutting down."); shutdown_requested=True; wsapp.close(); return
    initial_sym_ws = {convert_symbol_to_ws(s) for s in initial_sym_ccxt}
    op_logger.info(f"Subscribing to initial {len(initial_sym_ws)} K-line streams (WS)...")
    streams = [f"{s.lower()}@kline_{TIMEFRAME}" for s in initial_sym_ws]
    if not streams: op_logger.error("No streams to subscribe. Shutting down."); shutdown_requested=True; wsapp.close(); return
    sub_id = 1; msg = {"method": "SUBSCRIBE", "params": streams, "id": sub_id}
    try: wsapp.send(json.dumps(msg)); time.sleep(1); op_logger.info(f"Initial K-line sub request sent (ID:{sub_id}).")
    except Exception as e: op_logger.error(f"Failed to send initial K-line sub: {e}. Shutting down."); shutdown_requested=True; wsapp.close(); return

    with subscribed_symbols_lock: subscribed_symbols = initial_sym_ws
    op_logger.info("Fetching initial historical data...")
    fetched_count, error_count = 0, 0
    with data_lock: historical_data.clear()
    with first_resistance_details_lock: first_resistance_details.clear()
    with first_support_details_lock: first_support_details.clear()

    symbols_to_fetch_ws = initial_sym_ws.copy()
    for symbol_ws in symbols_to_fetch_ws:
        if shutdown_requested: break
        sym_ccxt_fetch = convert_symbol_to_ccxt(symbol_ws)
        df = fetch_initial_ohlcv(sym_ccxt_fetch, TIMEFRAME)
        if df is not None and not df.empty:
            with data_lock: historical_data[symbol_ws] = df
            with first_resistance_details_lock: first_resistance_details[symbol_ws] = {'timestamp': None, 'price': None}
            with first_support_details_lock: first_support_details[symbol_ws] = {'timestamp': None, 'price': None}
            fetched_count += 1
        else: error_count += 1; op_logger.warning(f"Failed to fetch initial data for {sym_ccxt_fetch}.")
        time.sleep(0.3)
    op_logger.info(f"Initial hist_data fetch complete ({fetched_count} OK, {error_count} errors). BBM details initialized.")
    print("-" * 80 + f"\nK-line WebSocket connected. Bot listening ({log_prefix})...\n" + "-" * 80)

def on_open_kline_reconnect(wsapp):
    global kline_websocket_running, subscribed_symbols
    kline_websocket_running = True
    op_logger.info("K-line WS RECONNECTED.")
    with subscribed_symbols_lock: current_subs_ws = subscribed_symbols.copy()
    if not current_subs_ws: op_logger.warning("Sub list empty on reconnect."); return
    op_logger.info(f"Resubscribing to {len(current_subs_ws)} K-line streams (WS)...")
    streams = [f"{s.lower()}@kline_{TIMEFRAME}" for s in current_subs_ws]
    if not streams: op_logger.warning("No streams to resubscribe."); return
    resub_id = int(time.time()); msg = {"method": "SUBSCRIBE", "params": streams, "id": resub_id}
    try: wsapp.send(json.dumps(msg)); op_logger.info(f"Resubscription request sent (ID:{resub_id}).")
    except Exception as e: op_logger.error(f"Failed to send resub request: {e}"); wsapp.close()

# ==============================================================================
# 메인 실행 로직 (Main Execution Logic)
# ==============================================================================
if __name__ == "__main__":
    start_time_str = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S %Z")
    op_logger.info(f"Bot starting at: {start_time_str}")

    if SIMULATION_MODE: op_logger.error("SIMULATION_MODE is True. Set to False for real trading. Exiting."); exit()
    if not API_KEY or API_KEY == "YOUR_BINANCE_API_KEY" or not API_SECRET or API_SECRET == "YOUR_BINANCE_API_SECRET":
        op_logger.error("API Key/Secret not set or placeholder. Configure them. Exiting."); exit()

    op_logger.warning("="*30 + f" REAL TRADING MODE - {log_prefix} " + "="*30)
    op_logger.warning("Strategy: BBM 2nd Rejection(Lower High)/Support(Higher Low) Entry / Dynamic BBands TP / Fixed SL")
    op_logger.warning(f"Key Settings: BBands({BBANDS_PERIOD},{BBANDS_STDDEV}), BBM_Rejection_Range={BBM_REJECTION_PERCENT*100:.1f}%")
    op_logger.warning(f"             TP Update Delay={POSITION_MONITORING_DELAY_MINUTES}min, Threshold={TP_UPDATE_THRESHOLD_PERCENT}%")
    op_logger.warning(f"             MaxPos={MAX_OPEN_POSITIONS}, Leverage={LEVERAGE}x, Timeframe={TIMEFRAME}, SL={1-LONG_STOP_LOSS_FACTOR:.3%}/{SHORT_STOP_LOSS_FACTOR-1:.3%}")
    op_logger.warning(f"             SymbolUpdateInterval={SYMBOL_UPDATE_INTERVAL_HOURS}h, RESTSyncInterval={REST_SYNC_INTERVAL_MINUTES}min, FeeRate={FEE_RATE}")
    op_logger.warning("!!! THIS BOT WILL USE REAL FUNDS - MONITOR CLOSELY !!!"); op_logger.warning("="*80)
    for i in range(3, 0, -1): print(f"Starting in {i}...", end='\r'); time.sleep(1)
    print("Starting now!          ")

    if not initialize_binance_rest(): op_logger.error("Exiting: CCXT REST init failure."); exit()
    op_logger.info("Initial REST state synchronization..."); sync_positions_with_exchange()
    op_logger.info("Initial REST sync complete."); log_asset_status()

    sync_thread = Thread(target=sync_state_periodically, args=(REST_SYNC_INTERVAL_MINUTES * 60,), daemon=True); sync_thread.start()
    ws_url_kline = f"wss://fstream.binance.com/stream"; reconnect_delay = 5

    try:
        while not shutdown_requested:
            if not kline_websocket_running:
                op_logger.info("Attempting K-line WS connection/reconnection...")
                if kline_wsapp and kline_wsapp.sock:
                    try: kline_wsapp.close(); time.sleep(1)
                    except Exception as close_e: op_logger.warning(f"Error closing previous WS: {close_e}")
                current_on_open = on_open_kline_initial if kline_thread is None else on_open_kline_reconnect
                kline_wsapp = websocket.WebSocketApp(ws_url_kline, on_open=current_on_open, on_message=on_message_kline, on_error=on_error_kline, on_close=on_close_kline)

                if kline_thread is None or not kline_thread.is_alive():
                    kline_thread = Thread(target=lambda: kline_wsapp.run_forever(ping_interval=60, ping_timeout=10), daemon=True)
                    kline_thread.start(); op_logger.info("New K-line WS thread started. Waiting for connection...")

                if symbol_update_thread is None or not symbol_update_thread.is_alive():
                    symbol_update_thread = Thread(target=update_top_symbols_periodically, args=(SYMBOL_UPDATE_INTERVAL_HOURS * 60 * 60,), daemon=True)
                    symbol_update_thread.start(); op_logger.info("Symbol Update thread started/restarted.")

                connect_wait_start = time.time()
                while not kline_websocket_running and time.time() - connect_wait_start < 15:
                    if shutdown_requested: break
                    time.sleep(0.5)

                if kline_websocket_running:
                    op_logger.info("K-line WS connection established/re-established.")
                    reconnect_delay = 5
                else:
                    op_logger.error(f"K-line WS connection failed. Retrying in {reconnect_delay}s...")
                    if kline_wsapp and kline_wsapp.sock: kline_wsapp.close()
                    time.sleep(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 2, 60)
            else:
                log_asset_status()
                time.sleep(1)
    except KeyboardInterrupt:
        op_logger.info("Keyboard interrupt. Initiating graceful shutdown...")
        shutdown_requested = True
    except Exception as main_loop_err:
        op_logger.error(f"Critical error in main loop: {main_loop_err}", exc_info=True)
        shutdown_requested = True
    finally:
        op_logger.info("Initiating final shutdown sequence...")
        shutdown_requested = True
        kline_websocket_running = False

        if kline_wsapp and kline_wsapp.sock:
            op_logger.info("Closing K-line WebSocket connection...")
            kline_wsapp.close()
        if kline_thread and kline_thread.is_alive():
            op_logger.info("Waiting for K-line thread to finish...")
            kline_thread.join(timeout=5)

        if sync_thread and sync_thread.is_alive():
            op_logger.info("Waiting for Sync thread to finish...")
            sync_thread.join(timeout=5)
        if symbol_update_thread and symbol_update_thread.is_alive():
            op_logger.info("Waiting for Symbol Update thread to finish...")
            symbol_update_thread.join(timeout=5)

        op_logger.warning("Attempting to cancel ALL remaining open orders...")
        all_cancelled_final = True
        try:
            if binance_rest:
                markets = binance_rest.load_markets()
                usdt_futures_symbols_ccxt = [mkt['symbol'] for mkt in markets.values() if mkt.get('type') == 'future' and mkt.get('quote') == TARGET_ASSET and mkt.get('active')]
                op_logger.info(f"Checking orders in {len(usdt_futures_symbols_ccxt)} active USDT futures markets...")
                for symbol_ccxt_cancel in usdt_futures_symbols_ccxt:
                    if not cancel_open_orders_for_symbol(symbol_ccxt_cancel):
                        op_logger.error(f"Failed to cancel orders for {symbol_ccxt_cancel} during final cleanup.")
                        all_cancelled_final = False
                    time.sleep(0.3)
            else:
                 op_logger.error("CCXT REST not available for final order cancellation.")
                 all_cancelled_final = False
        except Exception as cancel_all_err:
            op_logger.error(f"Error during final order cancellation: {cancel_all_err}")
            all_cancelled_final = False

        if all_cancelled_final: op_logger.info("Finished final order cancellation attempt.")
        else: op_logger.error("Potential issues during final order cancellation. MANUAL CHECK OF OPEN ORDERS STRONGLY ADVISED.")

        final_balance = get_current_balance()
        bal_str = f"{final_balance:.2f}" if final_balance is not None else "Error"
        with stats_lock: trades, wins = total_trades, winning_trades
        wr = (wins / trades * 100) if trades > 0 else 0.0
        final_msg = f"Final Balance:{bal_str} {TARGET_ASSET}, Total Trades:{trades}(Delayed), Winning Trades:{wins}(Delayed), Win Rate:{wr:.2f}%"
        op_logger.info(final_msg); asset_logger.info(final_msg)
        op_logger.info(f"{log_prefix} Bot shutdown complete.")
        print(f"{log_prefix} Bot shutdown complete.")

