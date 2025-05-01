# -*- coding: utf-8 -*-
# Imports
import ccxt
from ccxt.base.errors import OrderNotFound, RateLimitExceeded, ExchangeNotAvailable, OnMaintenance, InvalidNonce, RequestTimeout, AuthenticationError, NetworkError, ExchangeError
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
API_KEY = "YOUR_BINANCE_API_KEY"
API_SECRET = "YOUR_BINANCE_API_SECRET"
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
POSITION_MONITORING_DELAY_MINUTES = 5 # TP 업데이트 시작 딜레이
WHIPSAW_BLACKLIST_HOURS = 2
TP_UPDATE_THRESHOLD_PERCENT = 0.1
REST_SYNC_INTERVAL_MINUTES = 5       # <<<--- REST 동기화 주기 (UDS 없으므로 중요!)
SYMBOL_UPDATE_INTERVAL_HOURS = 2      # <<<--- 심볼 업데이트 주기
API_RETRY_COUNT = 3                   # <<<--- API 호출 재시도 횟수
API_RETRY_DELAY_SECONDS = 2           # <<<--- API 재시도 간격
FEE_RATE = 0.0005
INITIAL_CANDLE_FETCH_LIMIT = 100
MAX_CANDLE_HISTORY = 200
KST = ZoneInfo("Asia/Seoul")
UTC = timezone.utc
pd.set_option('display.max_rows', None); pd.set_option('display.max_columns', None); pd.set_option('display.width', None)

# ==============================================================================
# 로깅 설정
# ==============================================================================
log_dir = os.path.dirname(os.path.abspath(__file__))
log_prefix = "[REST_AUTO_SYM_V1]" # 버전 명시 (REST 기반 + 자동 심볼)
# 운영 로그
op_logger = logging.getLogger('operation')
op_logger.setLevel(logging.INFO)
op_formatter = logging.Formatter(f'%(asctime)s - %(levelname)s - {log_prefix} - %(message)s')
op_handler = logging.FileHandler(os.path.join(log_dir, 'operation_rest_auto_sym_v1.log')) # 파일명 변경
op_handler.setFormatter(op_formatter); op_logger.addHandler(op_handler); op_logger.addHandler(logging.StreamHandler())
# 매매 로그, 자산 로그
trade_logger = logging.getLogger('trade'); trade_logger.setLevel(logging.INFO)
trade_formatter = logging.Formatter(f'%(asctime)s - {log_prefix} - %(message)s')
trade_handler = logging.FileHandler(os.path.join(log_dir, 'trade_rest_auto_sym_v1.log'))
trade_handler.setFormatter(trade_formatter); trade_logger.addHandler(trade_handler)
asset_logger = logging.getLogger('asset'); asset_logger.setLevel(logging.INFO)
asset_formatter = logging.Formatter(f'%(asctime)s - {log_prefix} - %(message)s')
asset_handler = logging.FileHandler(os.path.join(log_dir, 'asset_rest_auto_sym_v1.log'))
asset_handler.setFormatter(asset_formatter); asset_logger.addHandler(asset_handler)

# ==============================================================================
# 전역 변수 및 동기화 객체 (UDS 관련 변수 제거)
# ==============================================================================
real_positions = {}; real_positions_lock = Lock()
total_trades = 0; winning_trades = 0; stats_lock = Lock() # 통계 정확도 낮아짐
historical_data = {}; data_lock = Lock()
blacklist = {}; blacklist_lock = Lock()
entry_in_progress = {}; entry_lock = Lock()
last_asset_log_time = datetime.now(UTC)
kline_websocket_running = False
kline_wsapp = None
subscribed_symbols = set(); subscribed_symbols_lock = Lock()
shutdown_requested = False
kline_thread = None
symbol_update_thread = None
sync_thread = None # REST 동기화 스레드 추적용
binance_rest = None

# ==============================================================================
# API 호출 재시도 헬퍼 함수 (*** 신규 추가 ***)
# ==============================================================================
def call_api_with_retry(api_call, max_retries=API_RETRY_COUNT, delay_seconds=API_RETRY_DELAY_SECONDS, error_message="API call failed"):
    """ 지정된 API 호출을 재시도 로직과 함께 실행 """
    retries = 0
    while retries < max_retries:
        try:
            return api_call() # API 호출 실행
        except (RequestTimeout, ExchangeNotAvailable, OnMaintenance, NetworkError, ExchangeError) as e: # 재시도 대상 에러
            retries += 1
            op_logger.warning(f"{error_message}. Retry {retries}/{max_retries} after {delay_seconds}s. Error: {e}")
            if retries < max_retries:
                time.sleep(delay_seconds)
            else:
                op_logger.error(f"{error_message}. Max retries reached. Giving up.")
                raise e # 마지막 시도 실패 시 에러 다시 발생시킴
        except AuthenticationError as auth_e: # 인증 에러는 재시도 불가
            op_logger.error(f"Authentication Error during {error_message}: {auth_e}. Cannot retry.")
            raise auth_e # 인증 에러는 즉시 전파
        except Exception as e: # 그 외 예상치 못한 에러
            op_logger.error(f"Unexpected error during {error_message}: {e}", exc_info=True)
            raise e # 예상 못한 에러는 즉시 전파
    # 루프가 정상 종료되면 (이론상 여기에 도달하면 안 됨)
    raise Exception(f"{error_message}. Failed after {max_retries} retries.")

# ==============================================================================
# API 및 데이터 처리 함수 (*** 재시도 로직 적용 필요 부분 확인 ***)
# ==============================================================================
def initialize_binance_rest():
    global binance_rest; op_logger.info("Initializing CCXT REST...")
    if not API_KEY or API_KEY == "YOUR_BINANCE_API_KEY" or not API_SECRET: op_logger.error("API Key/Secret not configured!"); return False
    try:
        binance_rest = ccxt.binance({'apiKey': API_KEY, 'secret': API_SECRET, 'enableRateLimit': True, 'options': { 'defaultType': 'future', 'adjustForTimeDifference': True }})
        # load_markets 및 fetch_time 에도 재시도 적용 고려 가능
        binance_rest.load_markets()
        server_time = call_api_with_retry(lambda: binance_rest.fetch_time(), error_message="fetch_time")
        op_logger.info(f"Server time: {datetime.fromtimestamp(server_time / 1000, tz=UTC)}"); op_logger.info("CCXT REST initialized."); return True
    except AuthenticationError: op_logger.error("REST API Auth Error!"); return False
    except Exception as e: op_logger.error(f"Failed init CCXT REST: {e}", exc_info=True); return False

def get_current_balance(asset=TARGET_ASSET):
    if not binance_rest: return 0.0
    try:
        # 잔고 조회에 재시도 적용
        balance = call_api_with_retry(lambda: binance_rest.fetch_balance(params={'type': 'future'}), error_message="fetch_balance")
        return float(balance['free'].get(asset, 0.0))
    except Exception as e: op_logger.error(f"Unexpected error fetching balance after retries: {e}"); return 0.0

def get_top_volume_symbols(n=TOP_N_SYMBOLS):
    if not binance_rest: return []
    op_logger.info(f"Fetching top {n} symbols by volume...")
    try:
        # 티커 조회에 재시도 적용
        tickers = call_api_with_retry(lambda: binance_rest.fetch_tickers(), error_message="fetch_tickers")
        futures_tickers = {s: t for s, t in tickers.items() if '/' in s and s.endswith(f"/{TARGET_ASSET}:{TARGET_ASSET}") and t.get('quoteVolume') is not None}
        if not futures_tickers: op_logger.warning("No USDT futures found."); return []
        sorted_tickers = sorted(futures_tickers.values(), key=lambda x: x.get('quoteVolume', 0), reverse=True)
        top_symbols_ccxt = [t['symbol'].split(':')[0] for t in sorted_tickers[:n]]; op_logger.info(f"Fetched top {len(top_symbols_ccxt)} symbols."); return top_symbols_ccxt
    except Exception as e: op_logger.error(f"Error fetching top symbols after retries: {e}"); return []

def fetch_initial_ohlcv(symbol_ccxt, timeframe=TIMEFRAME, limit=INITIAL_CANDLE_FETCH_LIMIT):
     if not binance_rest: return None
     try:
         actual_limit = max(limit, STOCH_RSI_PERIOD * 2 + 50); op_logger.debug(f"Fetching {actual_limit} candles for {symbol_ccxt}...")
         # OHLCV 조회에 재시도 적용
         ohlcv = call_api_with_retry(lambda: binance_rest.fetch_ohlcv(symbol_ccxt, timeframe=timeframe, limit=actual_limit), error_message=f"fetch_ohlcv for {symbol_ccxt}")
         if not ohlcv: op_logger.warning(f"No OHLCV for {symbol_ccxt}."); return None
         df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
         df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True); df.set_index('timestamp', inplace=True); op_logger.debug(f"Fetched {len(df)} candles for {symbol_ccxt}."); return df
     except Exception as e: op_logger.error(f"Error fetching OHLCV for {symbol_ccxt} after retries: {e}"); return None

# --- calculate_indicators, set_isolated_margin (API 호출 없으므로 재시도 불필요) ---
def calculate_indicators(df):
    req_len = max(BBANDS_PERIOD, STOCH_RSI_PERIOD*2); ok = (df is not None and len(df) >= req_len)
    if not ok: return None
    try: # ... (기존 로직 동일) ...
        dfc = df.copy(); dfc.ta.bbands(length=BBANDS_PERIOD, std=BBANDS_STDDEV, append=True); dfc.ta.stochrsi(length=STOCH_RSI_PERIOD, rsi_length=STOCH_RSI_PERIOD, k=STOCH_RSI_K, d=STOCH_RSI_D, append=True)
        bbl, bbm, bbu = f'BBL_{BBANDS_PERIOD}_{float(BBANDS_STDDEV)}', f'BBM_{BBANDS_PERIOD}_{float(BBANDS_STDDEV)}', f'BBU_{BBANDS_PERIOD}_{float(BBANDS_STDDEV)}'
        sk, sd = f'STOCHRSIk_{STOCH_RSI_PERIOD}_{STOCH_RSI_PERIOD}_{STOCH_RSI_K}_{STOCH_RSI_D}', f'STOCHRSId_{STOCH_RSI_PERIOD}_{STOCH_RSI_PERIOD}_{STOCH_RSI_K}_{STOCH_RSI_D}'
        rmap = {bbl: 'BBL', bbm: 'BBM', bbu: 'BBU', sk: 'STOCHk', sd: 'STOCHd'}; eramp = {k: v for k, v in rmap.items() if k in dfc.columns}
        if len(eramp)<5: return None
        dfc.rename(columns=eramp, inplace=True); req_cols = ['BBL', 'BBU', 'STOCHk', 'STOCHd']
        if not all(c in dfc.columns for c in req_cols): return None
        if dfc[req_cols].iloc[-1].isnull().any(): return None
        return dfc
    except Exception as e: op_logger.error(f"Indicator Calc Error: {e}", exc_info=True); return None

def set_isolated_margin(symbol_ccxt, leverage):
    # 이 함수 내부의 set_margin_mode, set_leverage 호출에도 재시도 적용 가능하나,
    # 보통 빠르게 실패하거나 성공하므로 일단 제외. 필요시 추가.
    if not binance_rest: return False
    op_logger.info(f"Setting ISO margin for {symbol_ccxt} / {leverage}x...")
    try: # ... (기존 로직 동일) ...
        try: binance_rest.set_margin_mode('ISOLATED', symbol_ccxt, params={}); op_logger.info(f"Margin mode set ISO for {symbol_ccxt}."); time.sleep(0.2)
        except ccxt.ExchangeError as e:
            if 'No need to change' in str(e) or 'already set' in str(e): op_logger.warning(f"{symbol_ccxt} already ISO.")
            elif 'position exists' in str(e): op_logger.error(f"Cannot change margin mode for {symbol_ccxt}, pos exists."); return False
            else: op_logger.error(f"Failed set margin mode for {symbol_ccxt}: {e}"); return False
        try: binance_rest.set_leverage(leverage, symbol_ccxt, params={}); op_logger.info(f"Leverage set {leverage}x for {symbol_ccxt}."); return True
        except ccxt.ExchangeError as e:
             if 'No need to change leverage' in str(e): op_logger.warning(f"Leverage for {symbol_ccxt} already {leverage}x."); return True
             else: op_logger.error(f"Failed set leverage for {symbol_ccxt}: {e}"); return False
    except (RequestTimeout, ExchangeNotAvailable, OnMaintenance) as e: op_logger.warning(f"Failed set iso margin: {e}"); return False
    except Exception as e: op_logger.error(f"Unexpected error setting iso margin: {e}", exc_info=True); return False

# --- 주문 함수들 (create_order 등)은 ccxt 내부에서 재시도 할 수도 있지만, 명시적 재시도 추가 가능 ---
# 일단 현재는 주문 함수 자체에는 재시도 로직 미적용. 실패 시 None 반환 유지.
def place_market_order_real(symbol_ccxt, side, amount, current_price=None):
    if not binance_rest or amount <= 0: op_logger.error(f"[{symbol_ccxt}] Invalid args market order."); return None
    try: # ... (기존 로직 동일, 내부 API 호출은 create_market_order) ...
        mkt = binance_rest.market(symbol_ccxt); adj_amt_str = binance_rest.amount_to_precision(symbol_ccxt, amount); adj_amt = float(adj_amt_str)
        if adj_amt <= 0: op_logger.error(f"[{symbol_ccxt}] Adjusted amount {adj_amt_str} <= 0."); return None
        min_notnl = mkt.get('limits', {}).get('cost', {}).get('min', 5.0)
        if current_price and adj_amt * current_price < min_notnl: op_logger.error(f"[{symbol_ccxt}] Order value < min {min_notnl}. Amt:{adj_amt_str}"); return None
        op_logger.info(f"[REAL ORDER] Attempt {side.upper()} {adj_amt_str} {symbol_ccxt} @ MARKET")
        coid = f"bot_{uuid.uuid4().hex[:16]}"; params = {'newClientOrderId': coid}
        # 여기에 call_api_with_retry 적용 가능:
        # order = call_api_with_retry(lambda: binance_rest.create_market_order(symbol_ccxt, side, adj_amt, params=params), error_message=f"create_market_order for {symbol_ccxt}")
        order = binance_rest.create_market_order(symbol_ccxt, side, adj_amt, params=params) # 일단 그대로 둠
        oid, fill_amt, avg_px, ts = order.get('id'), order.get('filled'), order.get('average'), order.get('timestamp')
        op_logger.info(f"[REAL ORDER PLACED] ID:{oid} CliID:{coid} Sym:{symbol_ccxt} Side:{side} ReqAmt:{adj_amt_str}")
        trade_logger.info(f"REAL MARKET: {side.upper()} {symbol_ccxt}, ReqAmt:{adj_amt_str}, OrdID:{oid}, CliOrdID:{coid}")
        # 반환값은 단순화 (체결 정보는 동기화 시 확인)
        return {'id': oid, 'clientOrderId': coid, 'status': order.get('status', 'open')} # status 반환
    except ccxt.InsufficientFunds as e: op_logger.error(f"[ORDER FAILED] Insuf funds {symbol_ccxt}: {e}"); return None
    except ccxt.ExchangeError as e: op_logger.error(f"[ORDER FAILED] Exch error {symbol_ccxt}: {e}"); return None
    except (RequestTimeout, ExchangeNotAvailable, OnMaintenance) as e: op_logger.warning(f"[ORDER FAILED] Network/Exch issue {symbol_ccxt}: {e}"); return None
    except Exception as e: op_logger.error(f"[ORDER FAILED] Unexp error {symbol_ccxt}: {e}", exc_info=True); return None

def place_stop_market_order(symbol_ccxt, side, stop_price, amount):
    # create_order 호출에 재시도 적용 가능
    if not binance_rest or amount <= 0 or stop_price <= 0: op_logger.error(f"[{symbol_ccxt}] Invalid args SL."); return None
    try: # ... (기존 로직 동일) ...
        amt_str = binance_rest.amount_to_precision(symbol_ccxt, amount); sp_str = binance_rest.price_to_precision(symbol_ccxt, stop_price); amt = float(amt_str)
        if amt <= 0: op_logger.error(f"[{symbol_ccxt}] SL Adjusted amount <= 0."); return None
        op_logger.info(f"[REAL SL ORDER] Attempt {side.upper()} {amt_str} {symbol_ccxt} if price hits {sp_str}")
        coid = f"sl_{uuid.uuid4().hex[:16]}"; params = {'stopPrice': sp_str, 'reduceOnly': True, 'newClientOrderId': coid}
        # 여기에 call_api_with_retry 적용 가능
        order = binance_rest.create_order(symbol_ccxt, 'STOP_MARKET', side, amt, None, params); oid = order.get('id')
        op_logger.info(f"[REAL SL PLACED] ID:{oid} CliID:{coid} Sym:{symbol_ccxt} Side:{side} StopPx:{sp_str} Amt:{amt_str}")
        trade_logger.info(f"REAL SL SET: {side.upper()} {symbol_ccxt}, Amt:{amt_str}, StopPx:{sp_str}, OrdID:{oid}, CliOrdID:{coid}")
        return {'id': oid, 'clientOrderId': coid}
    except ccxt.ExchangeError as e: op_logger.error(f"[SL FAILED] Exch error {symbol_ccxt}: {e}"); return None
    except (RequestTimeout, ExchangeNotAvailable, OnMaintenance) as e: op_logger.warning(f"[SL FAILED] Network/Exch issue {symbol_ccxt}: {e}"); return None
    except Exception as e: op_logger.error(f"[SL FAILED] Unexp error {symbol_ccxt}: {e}", exc_info=True); return None


def place_take_profit_market_order(symbol_ccxt, side, stop_price, amount):
    # create_order 호출에 재시도 적용 가능
    if not binance_rest or amount <= 0 or stop_price <= 0: op_logger.error(f"[{symbol_ccxt}] Invalid args TP."); return None
    try: # ... (기존 로직 동일) ...
        amt_str = binance_rest.amount_to_precision(symbol_ccxt, amount); sp_str = binance_rest.price_to_precision(symbol_ccxt, stop_price); amt = float(amt_str)
        if amt <= 0: op_logger.error(f"[{symbol_ccxt}] TP Adjusted amount <= 0."); return None
        op_logger.info(f"[REAL TP ORDER] Attempt {side.upper()} {amt_str} {symbol_ccxt} if price hits {sp_str}")
        coid = f"tp_{uuid.uuid4().hex[:16]}"; params = {'stopPrice': sp_str, 'reduceOnly': True, 'newClientOrderId': coid}
        # 여기에 call_api_with_retry 적용 가능
        order = binance_rest.create_order(symbol_ccxt, 'TAKE_PROFIT_MARKET', side, amt, None, params); oid = order.get('id')
        op_logger.info(f"[REAL TP PLACED] ID:{oid} CliID:{coid} Sym:{symbol_ccxt} Side:{side} StopPx:{sp_str} Amt:{amt_str}")
        trade_logger.info(f"REAL TP SET: {side.upper()} {symbol_ccxt}, Amt:{amt_str}, StopPx:{sp_str}, OrdID:{oid}, CliOrdID:{coid}")
        return {'id': oid, 'clientOrderId': coid}
    except ccxt.ExchangeError as e: op_logger.error(f"[TP FAILED] Exch error {symbol_ccxt}: {e}"); return None
    except (RequestTimeout, ExchangeNotAvailable, OnMaintenance) as e: op_logger.warning(f"[TP FAILED] Network/Exch issue {symbol_ccxt}: {e}"); return None
    except Exception as e: op_logger.error(f"[TP FAILED] Unexp error {symbol_ccxt}: {e}", exc_info=True); return None

def cancel_order(symbol_ccxt, order_id=None, client_order_id=None):
    # cancel_order 호출에 재시도 적용 가능
    if not binance_rest or (not order_id and not client_order_id): return True
    target_id_str = f"ID={order_id}" if order_id else f"CliID={client_order_id}"
    op_logger.info(f"Attempt cancel {target_id_str} for {symbol_ccxt}...")
    try:
        # 여기에 call_api_with_retry 적용 가능
        if order_id: binance_rest.cancel_order(order_id, symbol_ccxt)
        else: binance_rest.cancel_order(client_order_id, symbol_ccxt, params={'origClientOrderId': client_order_id})
        op_logger.info(f"Success cancel {target_id_str}."); return True
    except OrderNotFound: op_logger.warning(f"Order {target_id_str} not found."); return True
    except (ExchangeNotAvailable, OnMaintenance, RequestTimeout) as e: op_logger.error(f"Cannot cancel {target_id_str}: {e}"); return False
    except RateLimitExceeded as e: op_logger.error(f"Rate limit cancel {target_id_str}: {e}"); return False
    except ccxt.ExchangeError as e:
        if 'Order does not exist' in str(e) or '-2011' in str(e): op_logger.warning(f"Order {target_id_str} likely already gone."); return True
        else: op_logger.error(f"Failed cancel {target_id_str}: {e}"); return False
    except Exception as e: op_logger.error(f"Unexp error cancelling {target_id_str}: {e}", exc_info=True); return False

def cancel_open_orders_for_symbol(symbol_ccxt):
    if not binance_rest: return False
    op_logger.warning(f"Attempting cancel ALL open orders for {symbol_ccxt}...")
    cancelled_count, success = 0, True
    try:
        # fetch_open_orders 호출에 재시도 적용
        open_orders = call_api_with_retry(lambda: binance_rest.fetch_open_orders(symbol_ccxt), error_message=f"fetch_open_orders for {symbol_ccxt}")
        if not open_orders: op_logger.info(f"No open orders found for {symbol_ccxt}."); return True
        op_logger.info(f"Found {len(open_orders)} orders for {symbol_ccxt}. Cancelling...")
        for o in open_orders:
            # 개별 cancel_order 호출에도 재시도 적용 가능하나, 일단 위 함수 결과만 사용
            if not cancel_order(symbol_ccxt, order_id=o.get('id'), client_order_id=o.get('clientOrderId')): success = False
            else: cancelled_count += 1
            time.sleep(0.2)
        op_logger.info(f"Finished cancel for {symbol_ccxt}. Cancelled {cancelled_count}/{len(open_orders)}."); return success
    except Exception as e: op_logger.error(f"Unexpected error cancelling orders for {symbol_ccxt}: {e}", exc_info=True); return False

# --- 블랙리스트, 자산 로깅 함수 (기존과 동일) ---
def check_symbol_in_blacklist(symbol_ws):
    # ... (기존 로직) ...
    with blacklist_lock: expiry = blacklist.get(symbol_ws)
    if expiry and datetime.now(UTC) < expiry: return True
    elif expiry: op_logger.info(f"Blacklist expired for {symbol_ws}."); del blacklist[symbol_ws]; return False
    return False

def add_to_blacklist(symbol_ws, reason=""):
    # ... (기존 로직) ...
    symbol_clean = symbol_ws.split(':')[0]; expiry = datetime.now(UTC) + timedelta(hours=WHIPSAW_BLACKLIST_HOURS)
    with blacklist_lock: blacklist[symbol_clean] = expiry
    op_logger.warning(f"Blacklisted {symbol_clean} until {expiry.astimezone(KST):%Y-%m-%d %H:%M:%S KST}. Reason: {reason}")

def log_asset_status():
    # ... (기존 로직) ...
    global last_asset_log_time
    now = datetime.now(UTC)
    if now - last_asset_log_time >= timedelta(hours=1):
        try:
            bal = get_current_balance(); bal_str = f"{bal:.2f}" if bal is not None else "Error"
            with stats_lock: trades, wins = total_trades, winning_trades # 통계 정확도 낮음!
            win_rate = (wins / trades * 100) if trades > 0 else 0.0; active_pos = []; num_active = 0
            with real_positions_lock: active_pos = list(real_positions.keys()); num_active = len(real_positions)
            asset_logger.info(f"Balance:{bal_str} {TARGET_ASSET}, Active:{num_active} {active_pos}, Trades:{trades}(Delayed), Wins:{wins}(Delayed), WR:{win_rate:.2f}%") # 통계 지연 명시
            last_asset_log_time = now
        except Exception as e: asset_logger.error(f"Error logging asset status: {e}", exc_info=True)

# ==============================================================================
# 상태 동기화 로직 (*** 재시도 및 강화된 로직 적용 ***)
# ==============================================================================
def sync_positions_with_exchange():
    global real_positions, total_trades, winning_trades # 통계 업데이트 로직 추가

    op_logger.info("[SYNC_REST] Starting state synchronization (REST API Based)...")
    if not binance_rest: op_logger.error("[SYNC_REST] CCXT instance not ready."); return

    try:
        # 1. 거래소 실제 포지션 조회 (재시도 적용)
        op_logger.debug("[SYNC_REST] Fetching positions from exchange...")
        exchange_positions_raw = call_api_with_retry(lambda: binance_rest.fetch_positions(), error_message="fetch_positions")
        time.sleep(0.1)

        exchange_pos_dict = {}
        for pos in exchange_positions_raw:
            try:
                amount = float(pos.get('info', {}).get('positionAmt', 0))
                if abs(amount) > 1e-9:
                    symbol_ccxt = pos.get('symbol')
                    if symbol_ccxt and symbol_ccxt.endswith(TARGET_ASSET):
                        symbol_ws = symbol_ccxt.replace('/USDT', 'USDT')
                        exchange_pos_dict[symbol_ws] = {
                            'side': 'long' if amount > 0 else 'short', 'amount': abs(amount),
                            'entry_price': float(pos.get('entryPrice', 0)), 'symbol_ccxt': symbol_ccxt,
                            'unrealized_pnl': float(pos.get('unrealizedPnl', 0)) # 미실현 손익 추가
                        }
            except Exception as parse_err: op_logger.error(f"[SYNC_REST] Error parsing exch pos: {pos.get('info')}, Err: {parse_err}")

        # 2. 로컬 포지션 상태 복사
        with real_positions_lock: local_pos_dict = real_positions.copy()

        # 3. 상태 비교 및 그룹 식별
        local_pos_symbols = set(local_pos_dict.keys()); exchange_pos_symbols = set(exchange_pos_dict.keys())
        L_only = local_pos_symbols - exchange_pos_symbols; E_only = exchange_pos_symbols - local_pos_symbols; Both = local_pos_symbols.intersection(exchange_pos_symbols)
        op_logger.info(f"[SYNC_REST] Check: Local_Only={len(L_only)}, Exchange_Only={len(E_only)}, Both={len(Both)}")

        # --- 4. 각 그룹 처리 ---

        # 4.1 L_only (로컬 O / 거래소 X): 재확인 후 로컬 상태 제거 및 주문 취소
        if L_only:
            op_logger.warning(f"[SYNC_REST][WARN] Local pos not on exch (Initial Check): {L_only}")
            for symbol_ws in L_only:
                symbol_ccxt = symbol_ws.replace('USDT','/USDT')
                op_logger.warning(f"[SYNC_REST][WARN] -> Re-checking position for {symbol_ws} specifically...")
                try:
                    # 특정 심볼 포지션 재조회 (재시도 포함)
                    specific_pos = call_api_with_retry(lambda: binance_rest.fetch_position(symbol_ccxt), error_message=f"fetch_position for {symbol_ws}")
                    # 재조회 결과도 포지션 없음(수량 0) 확인
                    if specific_pos and abs(float(specific_pos.get('info', {}).get('positionAmt', 0))) < 1e-9 :
                        op_logger.warning(f"[SYNC_REST] Confirmed no position for {symbol_ws} on exchange after recheck. Removing local state.")
                        with real_positions_lock:
                            removed_info = real_positions.pop(symbol_ws, None) # Lock 안에서 제거
                        if removed_info:
                            # 통계 업데이트 (정확도 낮음: PNL 알 수 없음)
                            with stats_lock: total_trades += 1 # 승패는 기록 불가
                            # 관련 주문 취소 시도
                            op_logger.info(f"[{symbol_ws}] Attempting cancel orders for removed local pos...")
                            cancel_open_orders_for_symbol(symbol_ccxt) # 해당 심볼 모든 주문 취소
                            # 블랙리스트 추가는 하지 않음 (SL 인지 TP 인지 알 수 없음)
                    else:
                        op_logger.error(f"[SYNC_REST] Discrepancy persist for {symbol_ws}. fetch_positions said no pos, but fetch_position returned: {specific_pos}. Manual check advised.")

                except Exception as recheck_e:
                     op_logger.error(f"[SYNC_REST] Error re-checking position for {symbol_ws}: {recheck_e}. Cannot resolve discrepancy.")


        # 4.2 E_only (거래소 O / 로컬 X): 추적 불가 -> 즉시 종료 시도 및 블랙리스트
        if E_only:
            op_logger.error(f"[SYNC_REST][ERROR] Untracked positions on exchange: {E_only}")
            for symbol_ws in E_only:
                op_logger.error(f"[SYNC_REST][ERROR] -> {symbol_ws}: Closing immediately & blacklisting.")
                pos_info = exchange_pos_dict.get(symbol_ws); time.sleep(0.1);
                if not pos_info: continue
                symbol_ccxt = pos_info['symbol_ccxt']
                # 주문 취소 먼저 (재시도 적용)
                cancel_open_orders_for_symbol(symbol_ccxt); time.sleep(0.5)
                try:
                    # 현재가 조회 (재시도 적용)
                    ticker = call_api_with_retry(lambda: binance_rest.fetch_ticker(symbol_ccxt), error_message=f"fetch_ticker for {symbol_ws}")
                    current_price = ticker['last'] if ticker else None
                    # 시장가 주문 (재시도 미적용 - 주문 자체는 한번만 시도)
                    close_order_result = place_market_order_real(symbol_ccxt, 'sell' if pos_info['side']=='long' else 'buy', pos_info['amount'], current_price)
                    if not close_order_result or not close_order_result.get('id'): op_logger.error(f"[SYNC_REST] Failed placing close order for untracked {symbol_ws}.")
                    else: # 종료 주문 성공 시 통계 업데이트 (손실로 간주?)
                         with stats_lock: total_trades += 1 # 손실로 간주하여 wins는 불증가
                except Exception as close_err: op_logger.error(f"[SYNC_REST] Error closing untracked {symbol_ws}: {close_err}")
                # 블랙리스트 추가
                add_to_blacklist(symbol_ws, reason="Untracked position closed via REST Sync"); time.sleep(0.5)

        # 4.3 Both (양쪽 존재): 상세 비교 및 SL/TP 주문 상태 확인
        if Both:
            op_logger.info(f"[SYNC_REST] Checking {len(Both)} positions present in both states...")
            for symbol_ws in Both:
                local_info = local_pos_dict.get(symbol_ws)
                exchange_info = exchange_pos_dict.get(symbol_ws)
                if not local_info or not exchange_info: continue
                symbol_ccxt = exchange_info['symbol_ccxt']

                # 상태 비교 (수량, 방향 등)
                amount_diff = abs(local_info.get('amount', 0) - exchange_info.get('amount', 0)) > 1e-6
                side_mismatch = local_info.get('side') != exchange_info.get('side')
                if amount_diff or side_mismatch:
                    op_logger.warning(f"[SYNC_REST][WARN] Discrepancy {symbol_ws}! Local:{local_info}, Exch:{exchange_info}. NOT auto-correcting.")
                    # 필요시 로컬 상태 강제 업데이트 고려

                # SL/TP 주문 상태 확인 (해당 심볼의 open orders 조회 - 재시도 적용)
                try:
                    op_logger.debug(f"[SYNC_REST] Fetching open orders for {symbol_ws}...")
                    open_orders = call_api_with_retry(lambda: binance_rest.fetch_open_orders(symbol_ccxt), error_message=f"fetch_open_orders for {symbol_ws}")
                    open_order_ids = {str(o['id']) for o in open_orders} # 문자열 ID 셋

                    # 로컬 SL 주문 ID 확인
                    sl_id = str(local_info.get('sl_order_id')) if local_info.get('sl_order_id') else None
                    if sl_id and sl_id not in open_order_ids:
                         op_logger.warning(f"[SYNC_REST] Local SL order {sl_id} for {symbol_ws} not found open on exchange. Clearing ID locally.")
                         with real_positions_lock:
                              if symbol_ws in real_positions: # 다시 확인
                                   real_positions[symbol_ws]['sl_order_id'] = None
                                   real_positions[symbol_ws]['sl_client_order_id'] = None

                    # 로컬 TP 주문 ID 확인
                    tp_id = str(local_info.get('tp_order_id')) if local_info.get('tp_order_id') else None
                    if tp_id and tp_id not in open_order_ids:
                         op_logger.warning(f"[SYNC_REST] Local TP order {tp_id} for {symbol_ws} not found open on exchange. Clearing ID locally.")
                         with real_positions_lock:
                              if symbol_ws in real_positions:
                                   real_positions[symbol_ws]['tp_order_id'] = None
                                   real_positions[symbol_ws]['tp_client_order_id'] = None
                                   real_positions[symbol_ws]['current_tp_price'] = None

                except Exception as order_check_e:
                     op_logger.error(f"[SYNC_REST] Error checking open orders for {symbol_ws}: {order_check_e}")


        op_logger.info("[SYNC_REST] REST state synchronization finished.")

    except AuthenticationError: op_logger.error("[SYNC_REST] Auth error! Shutting down."); global shutdown_requested; shutdown_requested=True
    except Exception as e: op_logger.error(f"[SYNC_REST] Error during REST sync: {e}", exc_info=True); time.sleep(60) # 에러 시 잠시 대기

def sync_state_periodically(interval_seconds):
    global shutdown_requested
    op_logger.info(f"REST Sync thread started. Interval: {interval_seconds}s.")
    while not shutdown_requested:
        try:
            for _ in range(interval_seconds):
                 if shutdown_requested: break; time.sleep(1)
            if not shutdown_requested: sync_positions_with_exchange()
        except Exception as e: op_logger.error(f"Error in REST sync loop: {e}", exc_info=True); time.sleep(60)
    op_logger.info("REST Sync thread finished.")

# ==============================================================================
# 심볼 목록 주기적 업데이트 로직 (*** REST 기반 환경용 ***)
# ==============================================================================
def update_top_symbols_periodically(interval_seconds):
    global subscribed_symbols, historical_data, kline_websocket_running, kline_wsapp, shutdown_requested
    op_logger.info(f"Symbol Update thread started. Interval: {interval_seconds}s.")
    while not shutdown_requested:
        try:
            for _ in range(interval_seconds):
                 if shutdown_requested: break; time.sleep(1)
            if shutdown_requested: break
            if not kline_websocket_running or not kline_wsapp or not kline_wsapp.sock or not kline_wsapp.sock.connected: op_logger.warning("[Symbol Update] K-line WS not ready."); continue

            op_logger.info("[Symbol Update] Starting symbol update...")
            new_sym_ccxt = get_top_volume_symbols(TOP_N_SYMBOLS) # 재시도 내장됨
            if not new_sym_ccxt: op_logger.warning("[Symbol Update] Failed fetch new symbols."); continue
            new_sym_ws = {s.replace('/','') for s in new_sym_ccxt}
            with subscribed_symbols_lock: current_subs = subscribed_symbols.copy()
            to_add = new_sym_ws - current_subs; to_remove = current_subs - new_sym_ws

            # 제거
            if to_remove:
                op_logger.info(f"[Symbol Update] Removing: {to_remove}")
                streams = [f"{s.lower()}@kline_{TIMEFRAME}" for s in to_remove]
                if streams:
                    msg = {"method": "UNSUBSCRIBE", "params": streams, "id": int(time.time())}
                    try: kline_wsapp.send(json.dumps(msg)); op_logger.info(f"[Symbol Update] Sent UNSUB.")
                    except Exception as e: op_logger.error(f"[Symbol Update] Failed send UNSUB: {e}")
                with subscribed_symbols_lock: subscribed_symbols -= to_remove
                with data_lock: removed = sum(1 for s in to_remove if historical_data.pop(s, None))
                op_logger.info(f"[Symbol Update] Removed data for {removed} symbols.")
                # 제거된 심볼 포지션 강제 종료 안 함 (동기화 로직에 맡김)

            # 추가
            if to_add:
                op_logger.info(f"[Symbol Update] Adding: {to_add}")
                fetched, errors, added_to_data = 0, 0, set()
                for symbol_ws in to_add:
                    ccxt_s = symbol_ws.replace('USDT','/USDT')
                    df = fetch_initial_ohlcv(ccxt_s, TIMEFRAME, limit=max(INITIAL_CANDLE_FETCH_LIMIT, STOCH_RSI_PERIOD*2)) # 재시도 내장됨
                    if df is not None and not df.empty:
                         with data_lock: historical_data[symbol_ws] = df
                         fetched += 1; added_to_data.add(symbol_ws)
                    else: errors += 1
                    if shutdown_requested: break
                    time.sleep(0.3)
                op_logger.info(f"[Symbol Update] Fetched data for {fetched} new symbols ({errors} err).")
                if added_to_data:
                    streams = [f"{s.lower()}@kline_{TIMEFRAME}" for s in added_to_data]
                    msg = {"method": "SUBSCRIBE", "params": streams, "id": int(time.time())}
                    try:
                        if kline_wsapp and kline_wsapp.sock and kline_wsapp.sock.connected:
                             kline_wsapp.send(json.dumps(msg)); op_logger.info(f"[Symbol Update] Sent SUB for {len(added_to_data)}.")
                             with subscribed_symbols_lock: subscribed_symbols.update(added_to_data)
                        else: op_logger.warning("[Symbol Update] K-line WS disconnected before SUB.")
                    except Exception as e: op_logger.error(f"[Symbol Update] Failed send SUB: {e}")

            with subscribed_symbols_lock: current_count = len(subscribed_symbols)
            op_logger.info(f"[Symbol Update] Finished. Subscribed: {current_count}")
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
            df = historical_data[symbol_ws]; t = pd.to_datetime(kline_data['t'],unit='ms',utc=True)
            nd = pd.DataFrame([{'open':float(kline_data['o']),'high':float(kline_data['h']),'low':float(kline_data['l']),'close':float(kline_data['c']),'volume':float(kline_data['v'])}], index=[t])
            if t in df.index: df.loc[t] = nd.iloc[0]
            else: df = pd.concat([df, nd]); df=df.iloc[-MAX_CANDLE_HISTORY:]
            historical_data[symbol_ws]=df; return True
    except Exception as e: op_logger.error(f"[{symbol_ws}] Err update history: {e}"); return False

def try_update_tp(sym_ws, sym_ccxt, side, amt, tp_id, tp_coid, cur_tp, new_tp):
    if not cur_tp or not new_tp or new_tp<=0 or cur_tp<=0: return False
    diff = abs(new_tp - cur_tp) / cur_tp * 100
    if diff >= TP_UPDATE_THRESHOLD_PERCENT:
        op_logger.info(f"[{sym_ws}] TP target moved: {cur_tp:.5f}->{new_tp:.5f}. Updating...")
        # TP 업데이트 시 기존 주문 취소 먼저 (재시도 적용 가능)
        if not cancel_order(sym_ccxt, order_id=tp_id, client_order_id=tp_coid): op_logger.error(f"[{sym_ws}] Failed cancel prev TP."); return False
        time.sleep(0.2); new_tp_info = None
        try:
            amt_str=binance_rest.amount_to_precision(sym_ccxt, amt); tp_str=binance_rest.price_to_precision(sym_ccxt, new_tp); tp_px=float(tp_str)
            # 새 TP 주문 생성 (재시도 미적용)
            new_tp_info = place_take_profit_market_order(sym_ccxt, 'sell' if side=='long' else 'buy', tp_px, float(amt_str))
        except Exception as e: op_logger.error(f"[{sym_ws}] Err placing new TP: {e}")
        if new_tp_info and new_tp_info.get('id'):
            nid, ncoid = new_tp_info['id'], new_tp_info.get('clientOrderId')
            with real_positions_lock: # Lock 사용
                if sym_ws in real_positions:
                    real_positions[sym_ws]['tp_order_id'] = nid; real_positions[sym_ws]['tp_client_order_id'] = ncoid; real_positions[sym_ws]['current_tp_price'] = tp_px
                    op_logger.info(f"[{sym_ws}] TP updated to {tp_px:.5f} (ID:{nid})."); return True
                else: op_logger.warning(f"[{sym_ws}] Pos gone on TP update."); cancel_order(sym_ccxt, order_id=nid, client_order_id=ncoid); return False
        else: op_logger.error(f"[{sym_ws}] Failed place new TP."); return False
    return False

def process_kline_message(symbol_ws, kline_data):
    global real_positions, entry_in_progress # UDS 관련 통계 변수 제거
    with subscribed_symbols_lock:
        if symbol_ws not in subscribed_symbols: return
    if not update_historical_data(symbol_ws, kline_data): return
    is_closed = kline_data.get('x', False)
    with data_lock: df = historical_data.get(symbol_ws)
    if df is None: return
    idf = calculate_indicators(df.copy())
    if idf is None or idf.empty: return
    try:
        last = idf.iloc[-1]; price = last['close']
        sk, sd = last.get('STOCHk',np.nan), last.get('STOCHd',np.nan)
        bbl, bbu = last.get('BBL',np.nan), last.get('BBU',np.nan)
        if any(pd.isna(v) for v in [sk,sd,bbl,bbu,price]): return
    except Exception as e: op_logger.error(f"[{symbol_ws}] Indicator access error: {e}"); return
    now = datetime.now(UTC); sym_ccxt = symbol_ws.replace('USDT','/USDT')
    pos_copy = {};
    with real_positions_lock: pos_copy = real_positions.copy()

    # TP Update Check
    if symbol_ws in pos_copy:
        pinfo = pos_copy[symbol_ws]; side, etime, amt = pinfo['side'], pinfo['entry_time'], pinfo['amount']
        tp_id, tp_coid, cur_tp = pinfo.get('tp_order_id'), pinfo.get('tp_client_order_id'), pinfo.get('current_tp_price')
        if now-etime >= timedelta(minutes=POSITION_MONITORING_DELAY_MINUTES) and tp_id and cur_tp:
            new_tp = bbu if side == 'long' else bbl
            if not pd.isna(new_tp) and new_tp > 0: try_update_tp(symbol_ws, sym_ccxt, side, amt, tp_id, tp_coid, cur_tp, new_tp)

    # Entry Check (캔들 마감 시)
    if is_closed:
        with entry_lock: attempted = entry_in_progress.get(symbol_ws, False)
        with real_positions_lock: exists = symbol_ws in real_positions
        if not attempted and not exists and not check_symbol_in_blacklist(symbol_ws):
            with real_positions_lock: open_count = len(real_positions)
            if open_count >= MAX_OPEN_POSITIONS: return # 최대 포지션 도달 시 진입 불가
            l_cond=sk<=STOCH_RSI_LONG_ENTRY_THRESH and sd<=STOCH_RSI_LONG_ENTRY_THRESH and sk>sd
            s_cond=sk>=STOCH_RSI_SHORT_ENTRY_THRESH and sd>=STOCH_RSI_SHORT_ENTRY_THRESH and sk<sd
            tgt_side, tp_tgt, entry_px = None, None, price
            if l_cond: tgt_side='buy'; tp_tgt=bbu if not pd.isna(bbu) and bbu>0 else None
            elif s_cond: tgt_side='sell'; tp_tgt=bbl if not pd.isna(bbl) and bbl>0 else None

            if tgt_side and tp_tgt is not None:
                with entry_lock: entry_in_progress[symbol_ws] = True
                try:
                    op_logger.info(f"[{symbol_ws}] Entry MET: {tgt_side.upper()} @ {entry_px:.5f}, TP:{tp_tgt:.5f}")
                    bal=get_current_balance(); # 재시도 내장됨
                    if bal<=10.0: raise Exception(f"Insuf balance ({bal:.2f})")
                    with real_positions_lock: oc = len(real_positions)
                    if oc >= MAX_OPEN_POSITIONS: raise Exception("Max pos reached")
                    portion=1.0/(MAX_OPEN_POSITIONS-oc); margin=bal*portion
                    notional=margin*LEVERAGE; min_notional=5.0
                    if notional<min_notional: raise Exception(f"Notional {notional:.2f}<min {min_notional}")
                    entry_amt = notional/entry_px if entry_px>0 else 0
                    if entry_amt<=0: raise Exception("Amount <= 0")
                    # 마진/레버리지 설정 (재시도 없음)
                    if not set_isolated_margin(sym_ccxt, LEVERAGE): raise Exception("Set margin fail")
                    # 시장가 진입 (재시도 없음)
                    entry_ord = place_market_order_real(sym_ccxt, tgt_side, entry_amt, entry_px)
                    if not entry_ord or not entry_ord.get('id'): raise Exception("Entry order fail")
                    entry_oid, entry_coid = entry_ord['id'], entry_ord.get('clientOrderId')
                    op_logger.info(f"[{symbol_ws}] Entry order placed (ID:{entry_oid}).") # UDS 대기 문구 삭제

                    # SL/TP 주문 설정 (재시도 없음)
                    sl_ord, tp_ord, f_sl, f_tp = None, None, None, None
                    try:
                        f_sl=entry_px*(LONG_STOP_LOSS_FACTOR if tgt_side=='buy' else SHORT_STOP_LOSS_FACTOR); f_tp=tp_tgt
                        sl_side, tp_side = ('sell' if tgt_side=='buy' else 'buy'), ('sell' if tgt_side=='buy' else 'buy')
                        sl_ord = place_stop_market_order(sym_ccxt, sl_side, f_sl, entry_amt)
                        if not sl_ord or not sl_ord.get('id'): raise Exception("SL place fail")
                        op_logger.info(f"[{symbol_ws}] SL placed (ID:{sl_ord['id']})"); time.sleep(0.1)
                        tp_ord = place_take_profit_market_order(sym_ccxt, tp_side, f_tp, entry_amt)
                        if not tp_ord or not tp_ord.get('id'): raise Exception("TP place fail")
                        op_logger.info(f"[{symbol_ws}] TP placed (ID:{tp_ord['id']})")

                        # 포지션 정보 즉시 저장 (REST 기반이므로 UDS 확인 불가)
                        with real_positions_lock:
                            if len(real_positions) < MAX_OPEN_POSITIONS:
                                real_positions[symbol_ws] = {'side':('long' if tgt_side=='buy' else 'short'), 'entry_price':entry_px, 'amount':entry_amt, 'entry_time':now, 'entry_order_id':entry_oid, 'entry_client_order_id':entry_coid, 'sl_order_id':sl_ord['id'], 'sl_client_order_id':sl_ord.get('clientOrderId'), 'tp_order_id':tp_ord['id'], 'tp_client_order_id':tp_ord.get('clientOrderId'), 'current_tp_price':f_tp }
                                op_logger.info(f"[{symbol_ws}] Entry successful & Stored. Active:{len(real_positions)}")
                            else: # 동시 진입 등으로 최대치 초과 시 롤백
                                op_logger.error(f"[{symbol_ws}] Max positions reached during SL/TP placement! Rolling back...")
                                raise Exception("Max pos reached during SL/TP place")
                    except Exception as sltp_e: # 롤백 로직
                        op_logger.error(f"[{symbol_ws}] Err place SL/TP: {sltp_e}. ROLLBACK!");
                        # 롤백 시 주문 취소는 백그라운드 스레드로 실행 (종료 방지)
                        if sl_ord and sl_ord.get('id'): Thread(target=cancel_order, args=(sym_ccxt,), kwargs={'order_id':sl_ord['id'], 'client_order_id':sl_ord.get('clientOrderId')}, daemon=True).start()
                        if tp_ord and tp_ord.get('id'): Thread(target=cancel_order, args=(sym_ccxt,), kwargs={'order_id':tp_ord['id'], 'client_order_id':tp_ord.get('clientOrderId')}, daemon=True).start()
                        Thread(target=cancel_order, args=(sym_ccxt,), kwargs={'order_id':entry_oid, 'client_order_id':entry_coid}, daemon=True).start()
                        op_logger.warning(f"[{symbol_ws}] Rollback initiated.")
                        with real_positions_lock: real_positions.pop(symbol_ws, None) # 임시 저장된 상태 제거
                        add_to_blacklist(symbol_ws, reason=f"Entry fail:{sltp_e}") # 실패 시 블랙리스트

                except Exception as entry_e:
                    op_logger.error(f"[{symbol_ws}] Entry process failed: {entry_e}", exc_info=False)
                    # 실패 시에도 임시 상태 제거 (혹시 모르니)
                    with real_positions_lock: real_positions.pop(symbol_ws, None)
                finally:
                    # 진입 시도 플래그 해제
                    with entry_lock: entry_in_progress.pop(symbol_ws, None)


# ==============================================================================
# 웹소켓 콜백 함수 (K-line) - 재연결 로직 포함 (*** UDS 관련 로직 없음 ***)
# ==============================================================================
def on_message_kline(wsapp, message):
    # UDS 메시지 처리 로직 제거됨
    try:
        data = json.loads(message)
        if 'stream' in data and 'data' in data:
            stream_name = data['stream']; payload = data['data']
            if payload.get('e') == 'kline': symbol_upper = stream_name.split('@')[0].upper(); process_kline_message(symbol_upper, payload['k'])
        elif 'result' in data and data.get('id'): op_logger.info(f"K-line Sub response: {data}")
        elif 'e' in data and data['e'] == 'error': op_logger.error(f"K-line WS API Error: {data}")
    except json.JSONDecodeError: op_logger.error(f"K-line JSON Decode Err: {message[:100]}")
    except Exception as e: op_logger.error(f"K-line Msg Proc Err: {e}", exc_info=True)

def on_error_kline(wsapp, error):
    op_logger.error(f"K-line WebSocket Error: {error}")
    if isinstance(error, ConnectionRefusedError): op_logger.error("Connection refused.")

def on_close_kline(wsapp, close_status_code, close_msg):
    global kline_websocket_running
    if not shutdown_requested: op_logger.warning(f"K-line WS closed unexpectedly! Code:{close_status_code}. Will reconnect."); kline_websocket_running = False
    else: op_logger.info(f"K-line WS closed gracefully."); kline_websocket_running = False

def on_open_kline_initial(wsapp):
    global subscribed_symbols, historical_data, kline_websocket_running, shutdown_requested
    kline_websocket_running = True; op_logger.info("K-line WS initial connection opened.")
    op_logger.info("Fetching initial top symbols..."); initial_sym_ccxt = get_top_volume_symbols(TOP_N_SYMBOLS) # 재시도 내장
    if not initial_sym_ccxt: op_logger.error("Could not fetch initial symbols."); shutdown_requested=True; wsapp.close(); return
    initial_sym_ws = {s.replace('/','') for s in initial_sym_ccxt}; op_logger.info(f"Subscribing to initial {len(initial_sym_ws)} streams...")
    streams = [f"{s.lower()}@kline_{TIMEFRAME}" for s in initial_sym_ws]
    if not streams: op_logger.error("No streams to subscribe."); wsapp.close(); return
    sub_id=1; msg={"method":"SUBSCRIBE", "params":streams, "id":sub_id}
    try: wsapp.send(json.dumps(msg)); time.sleep(1); op_logger.info(f"Initial K-line sub sent (ID:{sub_id}).")
    except Exception as e: op_logger.error(f"Failed initial K-line sub: {e}"); wsapp.close(); return
    with subscribed_symbols_lock: subscribed_symbols = initial_sym_ws

    op_logger.info("Fetching initial historical data...")
    fetched, errors = 0, 0
    with data_lock: historical_data.clear() # Lock 사용
    symbols_to_fetch = initial_sym_ws.copy()
    for symbol_ws in symbols_to_fetch:
        if shutdown_requested: break
        sym_ccxt = symbol_ws.replace('USDT','/USDT')
        df = fetch_initial_ohlcv(sym_ccxt, TIMEFRAME, limit=max(INITIAL_CANDLE_FETCH_LIMIT, STOCH_RSI_PERIOD*2)) # 재시도 내장
        if df is not None and not df.empty:
            with data_lock: historical_data[symbol_ws]=df # Lock 사용
            fetched+=1
        else: errors+=1
        time.sleep(0.3)
    op_logger.info(f"Initial data fetch complete ({fetched} OK, {errors} errors).")
    print("-" * 80 + "\nK-line WS connected. Listening (REST Sync Mode)...\n" + "-" * 80) # 문구 수정

def on_open_kline_reconnect(wsapp):
    global kline_websocket_running, subscribed_symbols
    kline_websocket_running = True; op_logger.info("K-line WebSocket RECONNECTED.")
    with subscribed_symbols_lock: current_subs = subscribed_symbols.copy()
    if not current_subs: op_logger.warning("Sub list empty on reconnect."); return
    op_logger.info(f"Resubscribing to {len(current_subs)} streams..."); streams = [f"{s.lower()}@kline_{TIMEFRAME}" for s in current_subs]
    if not streams: op_logger.warning("No streams to resub."); return
    resub_id = int(time.time()); msg = {"method": "SUBSCRIBE", "params": streams, "id": resub_id}
    try: wsapp.send(json.dumps(msg)); op_logger.info(f"Resub msg sent (ID:{resub_id}).")
    except Exception as e: op_logger.error(f"Failed resub msg: {e}"); wsapp.close()


# ==============================================================================
# 메인 실행 로직 (*** UDS 관련 로직 제거, K-line 재연결만 유지 ***)
# ==============================================================================
if __name__ == "__main__":
    start_time_str = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S %Z")
    op_logger.info(f"Bot starting at: {start_time_str}")

    if SIMULATION_MODE: op_logger.error("Set SIMULATION_MODE=False."); exit()
    if not API_KEY or API_KEY == "YOUR_BINANCE_API_KEY" or not API_SECRET: op_logger.error("API Key/Secret not set!"); exit()

    op_logger.warning("="*30 + f" REAL TRADING MODE - {log_prefix} " + "="*30)
    op_logger.warning("Strategy: Stoch Entry / SL-TP Exit (Dyn TP) / REST Sync / Auto K-line Reconnect") # 설명 변경
    op_logger.warning(f"Settings: MaxPos:{MAX_OPEN_POSITIONS}, Lev:{LEVERAGE}x, TF:{TIMEFRAME}, SymUpd:{SYMBOL_UPDATE_INTERVAL_HOURS}h, RESTSync:{REST_SYNC_INTERVAL_MINUTES}min")
    op_logger.warning("!!! USING REAL FUNDS - MONITOR CLOSELY !!!"); op_logger.warning("="*80)
    for i in range(3, 0, -1): print(f"Starting in {i}...", end='\r'); time.sleep(1)
    print("Starting now!      ")

    if not initialize_binance_rest(): op_logger.error("Exiting: CCXT REST init failure."); exit()
    op_logger.info("Running initial REST state sync..."); sync_positions_with_exchange(); op_logger.info("Initial REST sync complete."); log_asset_status()

    # 주기적 REST 동기화 스레드 시작
    sync_thread = Thread(target=sync_state_periodically, args=(REST_SYNC_INTERVAL_MINUTES * 60,), daemon=True); sync_thread.start()

    # --- K-line 웹소켓 연결 및 재연결 루프 ---
    ws_url_kline = f"wss://fstream.binance.com/stream"
    reconnect_delay = 5

    try:
        while not shutdown_requested:
            # K-line 웹소켓 상태 확인 및 재연결 시도
            if not kline_websocket_running:
                op_logger.info("Attempting K-line WS connection/reconnection...")
                if kline_wsapp and kline_wsapp.sock:
                    try: kline_wsapp.close()
                    except Exception: pass
                    time.sleep(1)

                current_on_open = on_open_kline_initial if kline_thread is None else on_open_kline_reconnect
                kline_wsapp = websocket.WebSocketApp(ws_url_kline, on_open=current_on_open, on_message=on_message_kline, on_error=on_error_kline, on_close=on_close_kline)

                # K-line 스레드 시작/재시작
                if kline_thread is None or not kline_thread.is_alive():
                     kline_thread = Thread(target=lambda: kline_wsapp.run_forever(ping_interval=60, ping_timeout=10), daemon=True)
                     kline_thread.start()
                     op_logger.info("New K-line WS thread started. Waiting...")
                else: op_logger.warning("K-line thread still alive during reconnect attempt?") # 이전 스레드가 아직 살아있는 경우?

                # 심볼 업데이트 스레드 시작 (필요시)
                if symbol_update_thread is None or not symbol_update_thread.is_alive():
                    symbol_update_thread = Thread(target=update_top_symbols_periodically, args=(SYMBOL_UPDATE_INTERVAL_HOURS * 60 * 60,), daemon=True)
                    symbol_update_thread.start(); op_logger.info("Symbol Update thread started/restarted.")

                # 연결 대기
                connect_wait_start = time.time()
                while not kline_websocket_running and time.time() - connect_wait_start < 15:
                    if shutdown_requested: break; time.sleep(0.5)

                if kline_websocket_running: op_logger.info("K-line WS connected/reconnected."); reconnect_delay = 5
                else:
                    op_logger.error(f"K-line WS connection failed. Retrying in {reconnect_delay}s...")
                    if kline_wsapp and kline_wsapp.sock: kline_wsapp.close()
                    time.sleep(reconnect_delay); reconnect_delay = min(reconnect_delay * 2, 60)

            # K-line WS 정상 실행 중일 때
            else:
                log_asset_status() # 자산 로깅
                time.sleep(1) # 메인 스레드 휴식

    except KeyboardInterrupt: op_logger.info("Keyboard interrupt. Shutting down..."); shutdown_requested = True
    except Exception as main_loop_err: op_logger.error(f"Critical error in main loop: {main_loop_err}", exc_info=True); shutdown_requested = True
    finally:
        # --- 최종 종료 처리 ---
        op_logger.info("Initiating final shutdown sequence...")
        shutdown_requested = True
        kline_websocket_running = False # 스레드 종료 유도

        if kline_wsapp and kline_wsapp.sock: op_logger.info("Closing K-line WS..."); kline_wsapp.close()
        # UDS 관련 종료 로직 제거됨

        op_logger.info("Waiting for threads to finish (max 5s)...")
        time.sleep(5)

        op_logger.warning("Attempting to cancel all remaining open orders...")
        # ... (미체결 주문 취소 로직 - 이전 코드와 동일) ...
        all_cancelled_final = True
        try:
            markets = binance_rest.fetch_markets(); usdt_futures = [mkt['symbol'] for mkt in markets if mkt.get('type')=='future' and mkt.get('quote')=='USDT']
            op_logger.info(f"Checking orders for {len(usdt_futures)} USDT futures markets...")
            for symbol_ccxt in usdt_futures:
                if not cancel_open_orders_for_symbol(symbol_ccxt): op_logger.error(f"Failed cancel orders for {symbol_ccxt}.")
                time.sleep(0.3)
        except Exception as cancel_all_err: op_logger.error(f"Error final order cancellation: {cancel_all_err}"); all_cancelled_final = False
        if all_cancelled_final: op_logger.info("Finished attempting final order cancellation.")
        else: op_logger.error("Potential issues during final order cancellation. MANUAL CHECK REQUIRED.")

        op_logger.info("Fetching final balance...");
        # ... (최종 잔고/통계 로깅 - 통계는 지연된 정보임을 인지) ...
        final_balance=get_current_balance(); bal_str=f"{final_balance:.2f}" if final_balance is not None else "Error"
        with stats_lock: trades,wins=total_trades,winning_trades; wr=(wins/trades*100) if trades>0 else 0.0
        final_msg = f"Final Balance:{bal_str} {TARGET_ASSET}, Trades:{trades}(Delayed), Wins:{wins}(Delayed), WR:{wr:.2f}%" # 통계 지연 명시
        op_logger.info(final_msg); asset_logger.info(final_msg)
        op_logger.info(f"{log_prefix} Bot shutdown complete.")