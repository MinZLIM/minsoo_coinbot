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
log_prefix = "[REAL_UDS_AUTO_SYM_RECON_V3]" # 버전 명시
# 운영 로그
op_logger = logging.getLogger('operation')
op_logger.setLevel(logging.INFO)
op_formatter = logging.Formatter(f'%(asctime)s - %(levelname)s - {log_prefix} - %(message)s')
op_handler = logging.FileHandler(os.path.join(log_dir, 'operation_real_uds_auto_sym_recon_v3.log'))
op_handler.setFormatter(op_formatter)
op_logger.addHandler(op_handler)
op_logger.addHandler(logging.StreamHandler())
# 매매 로그, 자산 로그
trade_logger = logging.getLogger('trade')
trade_logger.setLevel(logging.INFO)
trade_formatter = logging.Formatter(f'%(asctime)s - {log_prefix} - %(message)s')
trade_handler = logging.FileHandler(os.path.join(log_dir, 'trade_real_uds_auto_sym_recon_v3.log'))
trade_handler.setFormatter(trade_formatter)
trade_logger.addHandler(trade_handler)
asset_logger = logging.getLogger('asset')
asset_logger.setLevel(logging.INFO)
asset_formatter = logging.Formatter(f'%(asctime)s - {log_prefix} - %(message)s')
asset_handler = logging.FileHandler(os.path.join(log_dir, 'asset_real_uds_auto_sym_recon_v3.log'))
asset_handler.setFormatter(asset_formatter)
asset_logger.addHandler(asset_handler)

# ==============================================================================
# 전역 변수 및 동기화 객체
# ==============================================================================
real_positions = {}; real_positions_lock = Lock()
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
subscribed_symbols = set(); subscribed_symbols_lock = Lock()
shutdown_requested = False
kline_thread = None
user_stream_thread = None
keep_alive_thread = None
symbol_update_thread = None
binance_rest = None

# ==============================================================================
# API 및 데이터 처리 함수
# ==============================================================================
def initialize_binance_rest():
    global binance_rest; op_logger.info("Initializing CCXT REST...")
    if not API_KEY or API_KEY == "YOUR_BINANCE_API_KEY" or not API_SECRET: op_logger.error("API Key/Secret not configured!"); return False
    try:
        binance_rest = ccxt.binance({'apiKey': API_KEY, 'secret': API_SECRET, 'enableRateLimit': True, 'options': { 'defaultType': 'future', 'adjustForTimeDifference': True }})
        binance_rest.load_markets(); server_time = binance_rest.fetch_time()
        op_logger.info(f"Server time: {datetime.fromtimestamp(server_time / 1000, tz=UTC)}"); op_logger.info("CCXT REST initialized."); return True
    except AuthenticationError: op_logger.error("REST API Auth Error!"); return False
    except RequestTimeout: op_logger.error("REST API timeout."); return False
    except ExchangeNotAvailable: op_logger.error("Exchange unavailable."); return False
    except Exception as e: op_logger.error(f"Failed init CCXT REST: {e}", exc_info=True); return False

def get_current_balance(asset=TARGET_ASSET):
    if not binance_rest: return 0.0
    try: balance = binance_rest.fetch_balance(params={'type': 'future'}); return float(balance['free'].get(asset, 0.0))
    except (RequestTimeout, ExchangeNotAvailable, OnMaintenance) as e: op_logger.warning(f"Error fetching balance: {e}"); return 0.0 # None 대신 0.0 반환
    except Exception as e: op_logger.error(f"Unexpected error fetching balance: {e}"); return 0.0

def get_top_volume_symbols(n=TOP_N_SYMBOLS):
    if not binance_rest: return []
    op_logger.info(f"Fetching top {n} symbols by volume...")
    try:
        tickers = binance_rest.fetch_tickers(); futures_tickers = {s: t for s, t in tickers.items() if '/' in s and s.endswith(f"/{TARGET_ASSET}:{TARGET_ASSET}") and t.get('quoteVolume') is not None}
        if not futures_tickers: op_logger.warning("No USDT futures found."); return []
        sorted_tickers = sorted(futures_tickers.values(), key=lambda x: x.get('quoteVolume', 0), reverse=True)
        top_symbols_ccxt = [t['symbol'].split(':')[0] for t in sorted_tickers[:n]]; op_logger.info(f"Fetched top {len(top_symbols_ccxt)} symbols."); return top_symbols_ccxt
    except (RequestTimeout, ExchangeNotAvailable, OnMaintenance) as e: op_logger.warning(f"Error fetching top symbols: {e}"); return []
    except Exception as e: op_logger.error(f"Error fetching top symbols: {e}"); return []

def fetch_initial_ohlcv(symbol_ccxt, timeframe=TIMEFRAME, limit=INITIAL_CANDLE_FETCH_LIMIT):
     if not binance_rest: return None
     try:
         actual_limit = max(limit, STOCH_RSI_PERIOD * 2 + 50); op_logger.debug(f"Fetching {actual_limit} candles for {symbol_ccxt}...")
         ohlcv = binance_rest.fetch_ohlcv(symbol_ccxt, timeframe=timeframe, limit=actual_limit)
         if not ohlcv: op_logger.warning(f"No OHLCV for {symbol_ccxt}."); return None
         df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
         df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True); df.set_index('timestamp', inplace=True); op_logger.debug(f"Fetched {len(df)} candles for {symbol_ccxt}."); return df
     except (RequestTimeout, ExchangeNotAvailable, OnMaintenance) as e: op_logger.warning(f"Error fetching OHLCV for {symbol_ccxt}: {e}"); return None
     except Exception as e: op_logger.error(f"Error fetching OHLCV for {symbol_ccxt}: {e}"); return None

def calculate_indicators(df):
    req_len = max(BBANDS_PERIOD, STOCH_RSI_PERIOD*2); ok = (df is not None and len(df) >= req_len)
    if not ok: return None
    try:
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
    if not binance_rest: return False
    op_logger.info(f"Setting ISO margin for {symbol_ccxt} / {leverage}x...")
    try:
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

def place_market_order_real(symbol_ccxt, side, amount, current_price=None):
    if not binance_rest or amount <= 0: op_logger.error(f"[{symbol_ccxt}] Invalid args for market order."); return None
    try:
        mkt = binance_rest.market(symbol_ccxt); adj_amt_str = binance_rest.amount_to_precision(symbol_ccxt, amount); adj_amt = float(adj_amt_str)
        if adj_amt <= 0: op_logger.error(f"[{symbol_ccxt}] Adjusted amount {adj_amt_str} <= 0."); return None
        min_notnl = mkt.get('limits', {}).get('cost', {}).get('min', 5.0)
        if current_price and adj_amt * current_price < min_notnl: op_logger.error(f"[{symbol_ccxt}] Order value < min {min_notnl}. Amt:{adj_amt_str}"); return None
        op_logger.info(f"[REAL ORDER] Attempt {side.upper()} {adj_amt_str} {symbol_ccxt} @ MARKET")
        coid = f"bot_{uuid.uuid4().hex[:16]}"; params = {'newClientOrderId': coid}
        order = binance_rest.create_market_order(symbol_ccxt, side, adj_amt, params=params)
        oid, fill_amt, avg_px, ts = order.get('id'), order.get('filled'), order.get('average'), order.get('timestamp')
        op_logger.info(f"[REAL ORDER PLACED] ID:{oid} CliID:{coid} Sym:{symbol_ccxt} Side:{side} ReqAmt:{adj_amt_str}")
        trade_logger.info(f"REAL MARKET: {side.upper()} {symbol_ccxt}, ReqAmt:{adj_amt_str}, OrdID:{oid}, CliOrdID:{coid}")
        if fill_amt is not None and avg_px is not None: op_logger.info(f"[REAL ORDER FILLED IMMED] FillAmt:{fill_amt}, AvgPx:{avg_px}"); return {'id': oid, 'clientOrderId': coid, 'average': avg_px, 'filled': fill_amt, 'timestamp': ts, 'status': 'closed'}
        else: op_logger.warning(f"[REAL ORDER] Placed (ID:{oid}) but fill info missing."); return {'id': oid, 'clientOrderId': coid, 'status': 'open'}
    except ccxt.InsufficientFunds as e: op_logger.error(f"[ORDER FAILED] Insuf funds {symbol_ccxt}: {e}"); return None
    except ccxt.ExchangeError as e: op_logger.error(f"[ORDER FAILED] Exch error {symbol_ccxt}: {e}"); return None
    except (RequestTimeout, ExchangeNotAvailable, OnMaintenance) as e: op_logger.warning(f"[ORDER FAILED] Network/Exch issue {symbol_ccxt}: {e}"); return None
    except Exception as e: op_logger.error(f"[ORDER FAILED] Unexp error {symbol_ccxt}: {e}", exc_info=True); return None

def place_stop_market_order(symbol_ccxt, side, stop_price, amount):
    if not binance_rest or amount <= 0 or stop_price <= 0: op_logger.error(f"[{symbol_ccxt}] Invalid args for SL."); return None
    try:
        amt_str = binance_rest.amount_to_precision(symbol_ccxt, amount); sp_str = binance_rest.price_to_precision(symbol_ccxt, stop_price); amt = float(amt_str)
        if amt <= 0: op_logger.error(f"[{symbol_ccxt}] SL Adjusted amount <= 0."); return None
        op_logger.info(f"[REAL SL ORDER] Attempt {side.upper()} {amt_str} {symbol_ccxt} if price hits {sp_str}")
        coid = f"sl_{uuid.uuid4().hex[:16]}"; params = {'stopPrice': sp_str, 'reduceOnly': True, 'newClientOrderId': coid}
        order = binance_rest.create_order(symbol_ccxt, 'STOP_MARKET', side, amt, None, params); oid = order.get('id')
        op_logger.info(f"[REAL SL PLACED] ID:{oid} CliID:{coid} Sym:{symbol_ccxt} Side:{side} StopPx:{sp_str} Amt:{amt_str}")
        trade_logger.info(f"REAL SL SET: {side.upper()} {symbol_ccxt}, Amt:{amt_str}, StopPx:{sp_str}, OrdID:{oid}, CliOrdID:{coid}")
        return {'id': oid, 'clientOrderId': coid}
    except ccxt.ExchangeError as e: op_logger.error(f"[SL FAILED] Exch error {symbol_ccxt}: {e}"); return None
    except (RequestTimeout, ExchangeNotAvailable, OnMaintenance) as e: op_logger.warning(f"[SL FAILED] Network/Exch issue {symbol_ccxt}: {e}"); return None
    except Exception as e: op_logger.error(f"[SL FAILED] Unexp error {symbol_ccxt}: {e}", exc_info=True); return None

def place_take_profit_market_order(symbol_ccxt, side, stop_price, amount):
    if not binance_rest or amount <= 0 or stop_price <= 0: op_logger.error(f"[{symbol_ccxt}] Invalid args for TP."); return None
    try:
        amt_str = binance_rest.amount_to_precision(symbol_ccxt, amount); sp_str = binance_rest.price_to_precision(symbol_ccxt, stop_price); amt = float(amt_str)
        if amt <= 0: op_logger.error(f"[{symbol_ccxt}] TP Adjusted amount <= 0."); return None
        op_logger.info(f"[REAL TP ORDER] Attempt {side.upper()} {amt_str} {symbol_ccxt} if price hits {sp_str}")
        coid = f"tp_{uuid.uuid4().hex[:16]}"; params = {'stopPrice': sp_str, 'reduceOnly': True, 'newClientOrderId': coid}
        order = binance_rest.create_order(symbol_ccxt, 'TAKE_PROFIT_MARKET', side, amt, None, params); oid = order.get('id')
        op_logger.info(f"[REAL TP PLACED] ID:{oid} CliID:{coid} Sym:{symbol_ccxt} Side:{side} StopPx:{sp_str} Amt:{amt_str}")
        trade_logger.info(f"REAL TP SET: {side.upper()} {symbol_ccxt}, Amt:{amt_str}, StopPx:{sp_str}, OrdID:{oid}, CliOrdID:{coid}")
        return {'id': oid, 'clientOrderId': coid}
    except ccxt.ExchangeError as e: op_logger.error(f"[TP FAILED] Exch error {symbol_ccxt}: {e}"); return None
    except (RequestTimeout, ExchangeNotAvailable, OnMaintenance) as e: op_logger.warning(f"[TP FAILED] Network/Exch issue {symbol_ccxt}: {e}"); return None
    except Exception as e: op_logger.error(f"[TP FAILED] Unexp error {symbol_ccxt}: {e}", exc_info=True); return None

def cancel_order(symbol_ccxt, order_id=None, client_order_id=None):
    if not binance_rest or (not order_id and not client_order_id): return True
    target_id_str = f"ID={order_id}" if order_id else f"CliID={client_order_id}"
    op_logger.info(f"Attempt cancel {target_id_str} for {symbol_ccxt}...")
    try:
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
    try: open_orders = binance_rest.fetch_open_orders(symbol_ccxt)
    except Exception as fetch_e: op_logger.error(f"Error fetching open orders for {symbol_ccxt}: {fetch_e}"); return False
    if not open_orders: op_logger.info(f"No open orders found for {symbol_ccxt}."); return True
    op_logger.info(f"Found {len(open_orders)} orders for {symbol_ccxt}. Cancelling...")
    for o in open_orders:
        if not cancel_order(symbol_ccxt, order_id=o.get('id'), client_order_id=o.get('clientOrderId')): success = False
        else: cancelled_count += 1
        time.sleep(0.2) # Add delay between cancels
    op_logger.info(f"Finished cancel for {symbol_ccxt}. Cancelled {cancelled_count}/{len(open_orders)}."); return success

def check_symbol_in_blacklist(symbol_ws):
    with blacklist_lock: expiry = blacklist.get(symbol_ws)
    if expiry and datetime.now(UTC) < expiry: return True
    elif expiry: op_logger.info(f"Blacklist expired for {symbol_ws}."); del blacklist[symbol_ws]; return False
    return False

def add_to_blacklist(symbol_ws, reason=""):
    symbol_clean = symbol_ws.split(':')[0]; expiry = datetime.now(UTC) + timedelta(hours=WHIPSAW_BLACKLIST_HOURS)
    with blacklist_lock: blacklist[symbol_clean] = expiry
    op_logger.warning(f"Blacklisted {symbol_clean} until {expiry.astimezone(KST):%Y-%m-%d %H:%M:%S KST}. Reason: {reason}")

def log_asset_status():
    global last_asset_log_time
    now = datetime.now(UTC)
    if now - last_asset_log_time >= timedelta(hours=1):
        try:
            bal = get_current_balance(); bal_str = f"{bal:.2f}" if bal is not None else "Error"
            with stats_lock: trades, wins = total_trades, winning_trades
            win_rate = (wins / trades * 100) if trades > 0 else 0.0; active_pos = []; num_active = 0
            with real_positions_lock: active_pos = list(real_positions.keys()); num_active = len(real_positions)
            asset_logger.info(f"Balance:{bal_str} {TARGET_ASSET}, Active:{num_active} {active_pos}, Trades:{trades}, Wins:{wins}, WR:{win_rate:.2f}%")
            last_asset_log_time = now
        except Exception as e: asset_logger.error(f"Error logging asset status: {e}", exc_info=True)

# ==============================================================================
# 상태 동기화 로직 (수정된 버전 유지)
# ==============================================================================
def sync_positions_with_exchange():
    global real_positions
    op_logger.info("[SYNC_REST] Starting state sync (Fallback Check)...")
    if not binance_rest: op_logger.error("[SYNC_REST] CCXT not ready."); return
    try:
        exch_pos_raw = binance_rest.fetch_positions(); time.sleep(0.5)
        exch_pos = {};
        for pos in exch_pos_raw:
            try:
                amt = float(pos.get('info',{}).get('positionAmt',0))
                if abs(amt)>1e-9:
                    sym_ccxt = pos.get('symbol');
                    if sym_ccxt and sym_ccxt.endswith(TARGET_ASSET):
                        sym_ws = sym_ccxt.replace('/USDT','USDT')
                        exch_pos[sym_ws] = {'side': 'long' if amt > 0 else 'short', 'amount': abs(amt), 'entry_price': float(pos.get('entryPrice', 0)), 'symbol_ccxt': sym_ccxt }
            except Exception as pe: op_logger.error(f"[SYNC_REST] Error parsing exch pos: {pos.get('info')}, Err: {pe}")

        with real_positions_lock: local_pos = real_positions.copy()
        local_sym = set(local_pos.keys()); exch_sym = set(exch_pos.keys())
        L_only = local_sym - exch_sym; E_only = exch_sym - local_sym; Both = local_sym.intersection(exch_sym)
        op_logger.info(f"[SYNC_REST] Check: L_only={len(L_only)}, E_only={len(E_only)}, Both={len(Both)}")

        # L_only (로컬 O / 거래소 X): 경고만, 삭제 안함!
        if L_only:
            op_logger.warning(f"[SYNC_REST][WARN] Local pos not on exch (via REST): {L_only}")
            for sym_ws in L_only:
                op_logger.warning(f"[SYNC_REST][WARN] -> {sym_ws}: UDS missed closure? Leaving local state intact.")
                pos_info = local_pos.get(sym_ws)
                if pos_info: # Optionally try cancelling orders
                    sym_ccxt = sym_ws.replace('USDT','/USDT')
                    sl_id, sl_coid = pos_info.get('sl_order_id'), pos_info.get('sl_client_order_id')
                    tp_id, tp_coid = pos_info.get('tp_order_id'), pos_info.get('tp_client_order_id')
                    op_logger.info(f"[{sym_ws}] Attempting cancel potentially orphaned SL({sl_id}/{sl_coid}) / TP({tp_id}/{tp_coid})...")
                    # Run cancels in background threads to avoid blocking sync too long
                    if sl_id or sl_coid: Thread(target=cancel_order, args=(sym_ccxt,), kwargs={'order_id': sl_id, 'client_order_id': sl_coid}, daemon=True).start(); time.sleep(0.1)
                    if tp_id or tp_coid: Thread(target=cancel_order, args=(sym_ccxt,), kwargs={'order_id': tp_id, 'client_order_id': tp_coid}, daemon=True).start(); time.sleep(0.1)

        # E_only (거래소 O / 로컬 X): 추적 불가 -> 즉시 종료 시도 및 블랙리스트
        if E_only:
            op_logger.error(f"[SYNC_REST][ERROR] Untracked positions on exchange: {E_only}")
            for sym_ws in E_only:
                op_logger.error(f"[SYNC_REST][ERROR] -> {sym_ws}: Closing immediately & blacklisting.")
                pos_info = exch_pos.get(sym_ws); time.sleep(0.1);
                if not pos_info: continue
                sym_ccxt = pos_info['symbol_ccxt']
                cancel_open_orders_for_symbol(sym_ccxt); time.sleep(0.5) # Cancel orders first
                try:
                    ticker = binance_rest.fetch_ticker(sym_ccxt); price = ticker['last'] if ticker else None
                    res = place_market_order_real(sym_ccxt, 'sell' if pos_info['side']=='long' else 'buy', pos_info['amount'], price)
                    if not res or not res.get('id'): op_logger.error(f"[SYNC_REST] Failed placing close order for {sym_ws}.")
                except Exception as ce: op_logger.error(f"[SYNC_REST] Error closing untracked {sym_ws}: {ce}")
                add_to_blacklist(sym_ws, reason="Untracked position closed via REST Sync"); time.sleep(0.5)

        # Both (양쪽 존재): 불일치 시 경고 로그
        if Both:
            op_logger.debug(f"[SYNC_REST] Checking {len(Both)} positions in both states...")
            for sym_ws in Both:
                l_info, e_info = local_pos.get(sym_ws), exch_pos.get(sym_ws);
                if not l_info or not e_info: continue
                amt_diff = abs(l_info.get('amount',0) - e_info.get('amount',0)) > 1e-6; side_mm = l_info.get('side') != e_info.get('side')
                if amt_diff or side_mm: op_logger.warning(f"[SYNC_REST][WARN] Discrepancy {sym_ws}! Local:{l_info}, Exch:{e_info}. NOT auto-correcting.")

        op_logger.info("[SYNC_REST] REST state synchronization finished.")
    except RateLimitExceeded: op_logger.warning("[SYNC_REST] Rate limit exceeded."); time.sleep(60)
    except (RequestTimeout, ExchangeNotAvailable, OnMaintenance) as e: op_logger.warning(f"[SYNC_REST] Network/Exch unavailable: {e}"); time.sleep(60)
    except AuthenticationError: op_logger.error("[SYNC_REST] Auth error! Shutting down."); global shutdown_requested; shutdown_requested=True
    except Exception as e: op_logger.error(f"[SYNC_REST] Error: {e}", exc_info=True); time.sleep(60)

def sync_state_periodically(interval_seconds):
    global shutdown_requested
    op_logger.info(f"REST Sync thread started. Interval: {interval_seconds}s.")
    while not shutdown_requested:
        try:
            # Sleep in chunks to check shutdown flag
            for _ in range(interval_seconds):
                 if shutdown_requested: break
                 time.sleep(1)
            if not shutdown_requested: sync_positions_with_exchange()
        except Exception as e: op_logger.error(f"Error in REST sync loop: {e}", exc_info=True); time.sleep(60)
    op_logger.info("REST Sync thread finished.")

# ==============================================================================
# 심볼 목록 주기적 업데이트 로직 (수정된 with 구문 적용)
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
            if not kline_websocket_running or not kline_wsapp or not kline_wsapp.sock or not kline_wsapp.sock.connected: op_logger.warning("[Symbol Update] K-line WS not ready."); continue

            op_logger.info("[Symbol Update] Starting symbol update...")
            new_sym_ccxt = get_top_volume_symbols(TOP_N_SYMBOLS)
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
                # 구독 목록 및 데이터 제거
                with subscribed_symbols_lock: subscribed_symbols -= to_remove
                with data_lock: removed = sum(1 for s in to_remove if historical_data.pop(s, None))
                op_logger.info(f"[Symbol Update] Removed data for {removed} symbols.")

            # 추가
            if to_add:
                op_logger.info(f"[Symbol Update] Adding: {to_add}")
                fetched, errors, added_to_data = 0, 0, set()
                # 데이터 로드는 Lock 밖에서
                for symbol_ws in to_add: # Use 'symbol_ws' consistently
                    ccxt_s = symbol_ws.replace('USDT','/USDT')
                    df = fetch_initial_ohlcv(ccxt_s, TIMEFRAME, limit=max(INITIAL_CANDLE_FETCH_LIMIT, STOCH_RSI_PERIOD*2))
                    if df is not None and not df.empty:
                         # 데이터 저장은 Lock 안에서
                         with data_lock: historical_data[symbol_ws] = df
                         fetched += 1
                         added_to_data.add(symbol_ws)
                    else: errors += 1
                    if shutdown_requested: break
                    time.sleep(0.3)
                op_logger.info(f"[Symbol Update] Fetched data for {fetched} new symbols ({errors} err).")

                # 구독 추가
                if added_to_data:
                    streams = [f"{s.lower()}@kline_{TIMEFRAME}" for s in added_to_data]
                    msg = {"method": "SUBSCRIBE", "params": streams, "id": int(time.time())}
                    try:
                        # 구독 메시지 전송과 구독 목록 업데이트
                        if kline_wsapp and kline_wsapp.sock and kline_wsapp.sock.connected:
                             kline_wsapp.send(json.dumps(msg)) # Send first
                             op_logger.info(f"[Symbol Update] Sent SUB for {len(added_to_data)}.")
                             # Then update shared state under lock
                             with subscribed_symbols_lock: # <<<--- Corrected position of lock
                                 subscribed_symbols.update(added_to_data)
                        else: op_logger.warning("[Symbol Update] K-line WS disconnected before SUB.")
                    except Exception as e: op_logger.error(f"[Symbol Update] Failed send SUB: {e}")

            with subscribed_symbols_lock: current_count = len(subscribed_symbols)
            op_logger.info(f"[Symbol Update] Finished. Subscribed: {current_count}")
        except Exception as e: op_logger.error(f"Error in symbol update loop: {e}", exc_info=True); time.sleep(60)
    op_logger.info("Symbol Update thread finished.")


# ==============================================================================
# 웹소켓 처리 로직 (K-line) (수정된 with 구문 적용)
# ==============================================================================
def update_historical_data(symbol_ws, kline_data):
    global historical_data
    try:
        with data_lock: # Lock for both read and write access
            if symbol_ws not in historical_data: return False
            df = historical_data[symbol_ws]; t = pd.to_datetime(kline_data['t'],unit='ms',utc=True)
            nd = pd.DataFrame([{'open':float(kline_data['o']),'high':float(kline_data['h']),'low':float(kline_data['l']),'close':float(kline_data['c']),'volume':float(kline_data['v'])}], index=[t])
            if t in df.index: df.loc[t] = nd.iloc[0]
            else: df = pd.concat([df, nd]); df=df.iloc[-MAX_CANDLE_HISTORY:] # Limit history size
            historical_data[symbol_ws]=df # Update in place
            return True
    except Exception as e: op_logger.error(f"[{symbol_ws}] Err update history: {e}"); return False

def try_update_tp(sym_ws, sym_ccxt, side, amt, tp_id, tp_coid, cur_tp, new_tp):
    if not cur_tp or not new_tp or new_tp<=0 or cur_tp<=0: return False
    diff = abs(new_tp - cur_tp) / cur_tp * 100
    if diff >= TP_UPDATE_THRESHOLD_PERCENT:
        op_logger.info(f"[{sym_ws}] TP target moved: {cur_tp:.5f}->{new_tp:.5f}. Updating...")
        if not cancel_order(sym_ccxt, order_id=tp_id, client_order_id=tp_coid): op_logger.error(f"[{sym_ws}] Failed cancel prev TP."); return False
        time.sleep(0.2); new_tp_info = None
        try:
            amt_str=binance_rest.amount_to_precision(sym_ccxt, amt); tp_str=binance_rest.price_to_precision(sym_ccxt, new_tp); tp_px=float(tp_str)
            new_tp_info = place_take_profit_market_order(sym_ccxt, 'sell' if side=='long' else 'buy', tp_px, float(amt_str))
        except Exception as e: op_logger.error(f"[{sym_ws}] Err placing new TP: {e}")
        if new_tp_info and new_tp_info.get('id'):
            nid, ncoid = new_tp_info['id'], new_tp_info.get('clientOrderId')
            # Update real_positions under lock
            with real_positions_lock:
                if sym_ws in real_positions:
                    real_positions[sym_ws]['tp_order_id'] = nid
                    real_positions[sym_ws]['tp_client_order_id'] = ncoid
                    real_positions[sym_ws]['current_tp_price'] = tp_px
                    op_logger.info(f"[{sym_ws}] TP updated to {tp_px:.5f} (ID:{nid}).")
                    return True
                else: # Position closed during update
                    op_logger.warning(f"[{sym_ws}] Pos gone on TP update.");
                    # Attempt to cancel the newly placed TP
                    Thread(target=cancel_order, args=(sym_ccxt,), kwargs={'order_id':nid, 'client_order_id':ncoid}, daemon=True).start()
                    return False
        else: op_logger.error(f"[{sym_ws}] Failed place new TP."); return False
    return False

def process_kline_message(symbol_ws, kline_data):
    global real_positions, entry_in_progress
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

    # Entry Check
    if is_closed:
        with entry_lock: attempted = entry_in_progress.get(symbol_ws, False)
        with real_positions_lock: exists = symbol_ws in real_positions
        if not attempted and not exists and not check_symbol_in_blacklist(symbol_ws):
            with real_positions_lock: open_count = len(real_positions)
            if open_count >= MAX_OPEN_POSITIONS: return
            l_cond=sk<=STOCH_RSI_LONG_ENTRY_THRESH and sd<=STOCH_RSI_LONG_ENTRY_THRESH and sk>sd
            s_cond=sk>=STOCH_RSI_SHORT_ENTRY_THRESH and sd>=STOCH_RSI_SHORT_ENTRY_THRESH and sk<sd
            tgt_side, tp_tgt, entry_px = None, None, price
            if l_cond: tgt_side='buy'; tp_tgt=bbu if not pd.isna(bbu) and bbu>0 else None
            elif s_cond: tgt_side='sell'; tp_tgt=bbl if not pd.isna(bbl) and bbl>0 else None

            if tgt_side and tp_tgt is not None:
                with entry_lock: entry_in_progress[symbol_ws] = True # <<< Corrected indentation
                try:
                    op_logger.info(f"[{symbol_ws}] Entry MET: {tgt_side.upper()} @ {entry_px:.5f}, TP:{tp_tgt:.5f}")
                    bal=get_current_balance();
                    if bal<=10.0: raise Exception(f"Insuf balance ({bal:.2f})")
                    with real_positions_lock: oc = len(real_positions)
                    if oc >= MAX_OPEN_POSITIONS: raise Exception("Max pos reached")
                    portion=1.0/(MAX_OPEN_POSITIONS-oc); margin=bal*portion
                    notional=margin*LEVERAGE; min_notional=5.0
                    if notional<min_notional: raise Exception(f"Notional {notional:.2f}<min {min_notional}")
                    entry_amt = notional/entry_px if entry_px>0 else 0
                    if entry_amt<=0: raise Exception("Amount <= 0")
                    if not set_isolated_margin(sym_ccxt, LEVERAGE): raise Exception("Set margin fail")
                    entry_ord = place_market_order_real(sym_ccxt, tgt_side, entry_amt, entry_px)
                    if not entry_ord or not entry_ord.get('id'): raise Exception("Entry order fail")
                    entry_oid, entry_coid = entry_ord['id'], entry_ord.get('clientOrderId')
                    op_logger.info(f"[{symbol_ws}] Entry order placed (ID:{entry_oid}). Awaiting UDS...")
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
                        # Temporary store under lock (confirmed/updated by UDS later)
                        with real_positions_lock:
                            if len(real_positions) < MAX_OPEN_POSITIONS:
                                real_positions[symbol_ws] = {
                                    'side':('long' if tgt_side=='buy' else 'short'),
                                    'entry_price':entry_px, 'amount':entry_amt, 'entry_time':now,
                                    'entry_order_id':entry_oid, 'entry_client_order_id':entry_coid,
                                    'sl_order_id':sl_ord['id'], 'sl_client_order_id':sl_ord.get('clientOrderId'),
                                    'tp_order_id':tp_ord['id'], 'tp_client_order_id':tp_ord.get('clientOrderId'),
                                    'current_tp_price':f_tp }
                                op_logger.info(f"[{symbol_ws}] Entry initiated. Active:{len(real_positions)}")
                            else: raise Exception("Max pos reached during SL/TP place")
                    except Exception as sltp_e: # Rollback logic
                        op_logger.error(f"[{symbol_ws}] Err place SL/TP: {sltp_e}. ROLLBACK!");
                        # Attempt to cancel orders (use threads?)
                        if sl_ord and sl_ord.get('id'): Thread(target=cancel_order, args=(sym_ccxt,), kwargs={'order_id':sl_ord['id'], 'client_order_id':sl_ord.get('clientOrderId')}, daemon=True).start()
                        if tp_ord and tp_ord.get('id'): Thread(target=cancel_order, args=(sym_ccxt,), kwargs={'order_id':tp_ord['id'], 'client_order_id':tp_ord.get('clientOrderId')}, daemon=True).start()
                        Thread(target=cancel_order, args=(sym_ccxt,), kwargs={'order_id':entry_oid, 'client_order_id':entry_coid}, daemon=True).start()
                        op_logger.warning(f"[{symbol_ws}] Rollback initiated.")
                        # Remove temporary state under lock
                        with real_positions_lock: real_positions.pop(symbol_ws, None)
                        add_to_blacklist(symbol_ws, reason=f"Entry fail:{sltp_e}")
                # --- Corrected except/finally block ---
                except Exception as entry_e:
                    op_logger.error(f"[{symbol_ws}] Entry process failed: {entry_e}", exc_info=False)
                    with real_positions_lock: # Lock needed for pop
                         real_positions.pop(symbol_ws, None)
                finally:
                    with entry_lock: # Lock needed for pop
                        entry_in_progress.pop(symbol_ws, None)


# ==============================================================================
# 웹소켓 콜백 함수 (K-line) - 재연결 로직 포함 (수정된 with 구문 적용)
# ==============================================================================
def on_message_kline(wsapp, message):
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
    op_logger.info("Fetching initial top symbols..."); initial_sym_ccxt = get_top_volume_symbols(TOP_N_SYMBOLS)
    if not initial_sym_ccxt: op_logger.error("Could not fetch initial symbols."); shutdown_requested=True; wsapp.close(); return
    initial_sym_ws = {s.replace('/','') for s in initial_sym_ccxt}; op_logger.info(f"Subscribing to initial {len(initial_sym_ws)} streams...")
    streams = [f"{s.lower()}@kline_{TIMEFRAME}" for s in initial_sym_ws]
    if not streams: op_logger.error("No streams to subscribe."); wsapp.close(); return
    sub_id=1; msg={"method":"SUBSCRIBE", "params":streams, "id":sub_id}
    try: wsapp.send(json.dumps(msg)); time.sleep(1); op_logger.info(f"Initial K-line sub sent (ID:{sub_id}).")
    except Exception as e: op_logger.error(f"Failed initial K-line sub: {e}"); wsapp.close(); return
    # --- Corrected historical data fetching section ---
    with subscribed_symbols_lock: subscribed_symbols = initial_sym_ws
    op_logger.info("Fetching initial historical data...")
    fetched, errors = 0, 0
    # Clear data under lock
    with data_lock:
        historical_data.clear()
    symbols_to_fetch = initial_sym_ws.copy() # Use copy for iteration
    for symbol_ws in symbols_to_fetch: # Correct loop variable name
        if shutdown_requested: break
        sym_ccxt = symbol_ws.replace('USDT','/USDT')
        # Fetch outside lock
        df = fetch_initial_ohlcv(sym_ccxt, TIMEFRAME, limit=max(INITIAL_CANDLE_FETCH_LIMIT, STOCH_RSI_PERIOD*2))
        # Write under lock
        if df is not None and not df.empty:
            with data_lock: # Lock only for dictionary write
                historical_data[symbol_ws]=df
            fetched+=1 # Increment outside lock
        else:
            errors+=1
        time.sleep(0.3) # Delay outside lock
    # --- End of correction ---
    op_logger.info(f"Initial data fetch complete ({fetched} OK, {errors} errors).")
    print("-" * 80 + "\nK-line WS connected. Listening...\n" + "-" * 80)

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
# 웹소켓 콜백 함수 (User Data Stream)
# ==============================================================================
def on_message_user_stream(wsapp, message):
    global real_positions, total_trades, winning_trades, blacklist
    try:
        data = json.loads(message); event_type = data.get('e')
        if event_type == 'ORDER_TRADE_UPDATE':
            o = data.get('o'); sym_ws = o['s']; oid = str(o['i']); coid = o.get('c'); status = o['X']; side = o['S']
            lpx, lqty, fqty, tqty = float(o['L']), float(o['l']), float(o['z']), float(o['q'])
            ap = float(o['ap']) if o.get('ap') and float(o['ap'])>0 else lpx; comm = float(o.get('n',0.0)); comm_a = o.get('N')
            tms = o['T']; tdt = datetime.fromtimestamp(tms/1000, tz=UTC); reduce = o.get('R',False); otype = o['o']
            op_logger.info(f"[UDS][ORDER] {sym_ws} ID:{oid}.. Stat:{status} Side:{side} Type:{otype} Fill:{fqty}/{tqty} AvgPx:{ap:.5f}")

            if status in ['TRADE','PARTIALLY_FILLED'] and lqty > 0:
                trade_logger.info(f"[UDS][FILL] {sym_ws} {side} {lqty:.8f} @ {lpx:.5f} (Avg:{ap:.5f}) ID:{oid}.. Stat:{status}")
                # Use combined lock for consistency
                with real_positions_lock, stats_lock:
                    pinfo = real_positions.get(sym_ws); sym_ccxt = sym_ws.replace('USDT','/USDT')
                    # Entry Fill
                    if not pinfo or (pinfo and str(pinfo.get('entry_order_id')) == oid):
                        if not pinfo: # First fill
                             pos_side = 'long' if side=='BUY' else 'short'
                             temp_slid,temp_slcoid = real_positions.get(sym_ws,{}).get('sl_order_id'), real_positions.get(sym_ws,{}).get('sl_client_order_id')
                             temp_tpid,temp_tpcoid = real_positions.get(sym_ws,{}).get('tp_order_id'), real_positions.get(sym_ws,{}).get('tp_client_order_id')
                             temp_tppx = real_positions.get(sym_ws,{}).get('current_tp_price')
                             real_positions[sym_ws] = {'side':pos_side, 'entry_price':ap, 'amount':fqty, 'entry_time':tdt, 'entry_order_id':oid, 'entry_client_order_id':coid, 'sl_order_id':temp_slid, 'sl_client_order_id':temp_slcoid, 'tp_order_id':temp_tpid, 'tp_client_order_id':temp_tpcoid, 'current_tp_price':temp_tppx}
                             op_logger.info(f"[{sym_ws}] Pos OPENED via UDS. Side:{pos_side}, Qty:{fqty:.8f}, AvgPx:{ap:.5f}")
                        else: # Subsequent fills
                             pinfo['amount'] = fqty; pinfo['entry_price'] = ap if ap > 0 else pinfo['entry_price']; pinfo['entry_time'] = tdt
                             op_logger.info(f"[{sym_ws}] Pos UPDATED via UDS. Qty:{fqty:.8f}, AvgPx:{pinfo['entry_price']:.5f}")
                        if status=='TRADE': op_logger.info(f"[{sym_ws}] Entry order {oid} fully filled.")
                    # Exit Fill
                    elif pinfo and reduce:
                        closed=lqty; remain=pinfo['amount']-closed; pnl=((lpx-pinfo['entry_price'])*closed if pinfo['side']=='long' else (pinfo['entry_price']-lpx)*closed) - comm
                        trade_logger.info(f"[UDS][CLOSE] {sym_ws} Closed {closed:.8f} (Rem:{remain:.8f}) via {otype} ID:{oid}. PnL:{pnl:.5f}")
                        total_trades+=1;
                        if pnl>0: winning_trades+=1
                        if status=='TRADE' or abs(remain)<1e-9: # Full close
                            op_logger.info(f"[{sym_ws}] Pos FULLY CLOSED via UDS (ID:{oid}). Removing state.")
                            if str(pinfo.get('sl_order_id'))==oid: add_to_blacklist(sym_ws, reason=f"SL Filled ({oid}) via UDS")
                            opp_oid, opp_coid = (None,None)
                            if str(pinfo.get('sl_order_id'))==oid: opp_oid, opp_coid = pinfo.get('tp_order_id'), pinfo.get('tp_client_order_id')
                            elif str(pinfo.get('tp_order_id'))==oid: opp_oid, opp_coid = pinfo.get('sl_order_id'), pinfo.get('sl_client_order_id')
                            if opp_oid or opp_coid: op_logger.info(f"[{sym_ws}] Cancelling remaining order ({opp_oid}/{opp_coid})..."); Thread(target=cancel_order, args=(sym_ccxt,), kwargs={'order_id':opp_oid, 'client_order_id':opp_coid}, daemon=True).start()
                            # Delete local state safely
                            if sym_ws in real_positions: del real_positions[sym_ws]
                        else: # Partial close
                             op_logger.info(f"[{sym_ws}] Pos PARTIALLY CLOSED via UDS. Rem:{remain:.8f}"); pinfo['amount']=remain

            elif status in ['CANCELED','REJECTED','EXPIRED']:
                 op_logger.info(f"[UDS][ORDER_FINAL] {sym_ws} Order {oid} finalized: {status}")
                 with real_positions_lock: # Clear ID from local state
                      pinfo = real_positions.get(sym_ws)
                      if pinfo:
                          if str(pinfo.get('sl_order_id'))==oid: pinfo['sl_order_id']=None; pinfo['sl_client_order_id']=None; op_logger.warning(f"[{sym_ws}] SL {oid} {status}. Removed locally.")
                          elif str(pinfo.get('tp_order_id'))==oid: pinfo['tp_order_id']=None; pinfo['tp_client_order_id']=None; pinfo['current_tp_price']=None; op_logger.warning(f"[{sym_ws}] TP {oid} {status}. Removed locally.")

        elif event_type == 'ACCOUNT_UPDATE':
            udata = data.get('a',{}); reason = udata.get('m'); op_logger.debug(f"[UDS][ACCOUNT] Reason:{reason}")
            pos_upd = udata.get('P',[])
            if pos_upd:
                with real_positions_lock:
                    for pdata in pos_upd:
                        sym_ws=pdata['s']; amt=float(pdata['pa']); ep=float(pdata['ep']); pinfo=real_positions.get(sym_ws)
                        if pinfo and abs(amt)<1e-9: # Pos closed externally
                            op_logger.warning(f"[UDS][ACCOUNT] Pos {sym_ws} closed externally? (Amt=0, Reason:{reason}). Removing.")
                            sym_ccxt=sym_ws.replace('USDT','/USDT')
                            Thread(target=cancel_order, args=(sym_ccxt,), kwargs={'order_id':pinfo.get('sl_order_id'), 'client_order_id':pinfo.get('sl_client_order_id')}, daemon=True).start()
                            Thread(target=cancel_order, args=(sym_ccxt,), kwargs={'order_id':pinfo.get('tp_order_id'), 'client_order_id':pinfo.get('tp_client_order_id')}, daemon=True).start()
                            if sym_ws in real_positions: del real_positions[sym_ws]
                            if reason!='ORDER': add_to_blacklist(sym_ws, reason=f"Pos closed externally?(Reason:{reason})")
                        elif pinfo and abs(amt)>1e-9: # Update existing pos
                             if abs(pinfo['amount']-amt)>1e-6: op_logger.warning(f"[{sym_ws}] Amount mismatch ACC. Local:{pinfo['amount']}, Stream:{amt}. Upd."); pinfo['amount']=amt
                             if abs(pinfo['entry_price']-ep)>1e-6: op_logger.warning(f"[{sym_ws}] Entry mismatch ACC. Local:{pinfo['entry_price']:.5f}, Stream:{ep:.5f}. Upd."); pinfo['entry_price']=ep
            bal_upd = udata.get('B',[])
            # if bal_upd: for bdata in bal_upd: if bdata['a']==TARGET_ASSET: op_logger.debug(f"[UDS][BALANCE] {TARGET_ASSET} Bal:{bdata['wb']}")

        elif event_type == 'listenKeyExpired':
             op_logger.warning("[UDS] Listen Key EXPIRED! UDS needs reconnect."); global user_websocket_running; user_websocket_running=False

    except json.JSONDecodeError: op_logger.error(f"UDS JSON Decode Err: {message[:100]}")
    except Exception as e: op_logger.error(f"UDS Msg Proc Err: {e}", exc_info=True)

def on_error_user_stream(wsapp, error):
    global user_websocket_running # Add this global declaration
    op_logger.error(f"User Stream WebSocket Error: {error}")
    # Let on_close handle the state change primarily

def on_close_user_stream(wsapp, close_status_code, close_msg):
    global user_websocket_running
    if not shutdown_requested: op_logger.warning(f"User Stream WS closed unexpectedly! Code:{close_status_code}. Will attempt reconnect."); user_websocket_running = False
    else: op_logger.info(f"User Stream WS closed gracefully."); user_websocket_running = False

def on_open_user_stream(wsapp):
    global user_websocket_running
    user_websocket_running = True
    op_logger.info("User Data Stream WebSocket connection opened/reopened.")

# ==============================================================================
# User Data Stream 관리 함수
# ==============================================================================
def get_listen_key():
    global listen_key, binance_rest
    if not binance_rest: op_logger.error("CCXT REST not init."); return None
    with listen_key_lock:
        op_logger.info("Requesting new UDS listen key...")
        try:
            response = binance_rest.fapiPrivatePostListenKey(); listen_key = response.get('listenKey')
            if listen_key: op_logger.info(f"Obtained listen key: {listen_key[:5]}..."); return listen_key
            else: op_logger.error(f"Failed get listen key: {response}"); return None
        except AuthenticationError: op_logger.error("Auth failed getting listen key."); return None
        except (RequestTimeout, ExchangeNotAvailable) as e: op_logger.warning(f"Network/Exch issue get listen key: {e}"); return None
        except Exception as e: op_logger.error(f"Error getting listen key: {e}", exc_info=True); return None

def keep_listen_key_alive():
    global listen_key, binance_rest, user_websocket_running, shutdown_requested, keep_alive_thread
    op_logger.info("Listen Key Keep-Alive thread started.")
    while not shutdown_requested: # Use combined condition at start
        try:
            # Check running state more frequently within sleep
            is_running = True
            for _ in range(LISTEN_KEY_REFRESH_INTERVAL_MINUTES * 60):
                 if shutdown_requested or not user_websocket_running: is_running=False; break
                 time.sleep(1)
            if not is_running: break # Exit loop if flags changed

            with listen_key_lock:
                current_key = listen_key
                if not current_key: op_logger.warning("No active listen key for keep-alive."); continue
                if not binance_rest: op_logger.error("CCXT REST not available."); continue

                op_logger.info(f"Pinging listen key: {current_key[:5]}...")
                try: binance_rest.fapiPrivatePutListenKey({'listenKey': current_key}); op_logger.info("Listen key ping successful.")
                except AuthenticationError: op_logger.error("Auth failed pinging! Shutting down."); shutdown_requested=True; break
                except ccxt.ExchangeError as e: op_logger.error(f"Exch error pinging: {e}. Key invalid?"); listen_key=None; user_websocket_running=False; break # Stop UDS
                except (RequestTimeout, ExchangeNotAvailable) as e: op_logger.warning(f"Network/Exch issue pinging: {e}")
                except Exception as e: op_logger.error(f"Error pinging listen key: {e}", exc_info=True)
        except Exception as e: op_logger.error(f"Error in keep-alive loop: {e}", exc_info=True); time.sleep(60)
    keep_alive_thread = None # Clear reference
    op_logger.info("Listen Key Keep-Alive thread finished.")

def start_user_stream_initial():
    global listen_key, user_wsapp, user_websocket_running, user_stream_thread, keep_alive_thread
    if not get_listen_key(): op_logger.error("Failed get initial listen key."); user_websocket_running = False; return False
    # Start Keep Alive thread
    if keep_alive_thread is None or not keep_alive_thread.is_alive():
         keep_alive_thread = Thread(target=keep_listen_key_alive, daemon=True); keep_alive_thread.start()
    ws_url = f"wss://fstream.binance.com/ws/{listen_key}"
    op_logger.info(f"Connecting to User Data Stream: {ws_url[:35]}...")
    user_wsapp = websocket.WebSocketApp(ws_url, on_open=on_open_user_stream, on_message=on_message_user_stream, on_error=on_error_user_stream, on_close=on_close_user_stream)
    # Start User Stream thread
    if user_stream_thread is None or not user_stream_thread.is_alive():
         user_stream_thread = Thread(target=lambda: user_wsapp.run_forever(ping_interval=180, ping_timeout=10), daemon=True)
         user_stream_thread.start()
         op_logger.info("User Data Stream thread started.")
    time.sleep(3)
    if not user_websocket_running: op_logger.error("User Data Stream failed to connect initially."); return False
    return True


# ==============================================================================
# 메인 실행 로직 (UDS 재연결 및 즉시 동기화)
# ==============================================================================
if __name__ == "__main__":
    start_time_str = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S %Z")
    op_logger.info(f"Bot starting at: {start_time_str}")

    if SIMULATION_MODE: op_logger.error("Set SIMULATION_MODE=False."); exit()
    if not API_KEY or API_KEY == "YOUR_BINANCE_API_KEY" or not API_SECRET: op_logger.error("API Key/Secret not set!"); exit()

    op_logger.warning("="*30 + f" REAL TRADING MODE - {log_prefix} " + "="*30)
    op_logger.warning("Strategy: Stoch Entry / SL-TP Exit (Dyn TP) / Auto Reconnect")
    op_logger.warning(f"Settings: MaxPos:{MAX_OPEN_POSITIONS}, Lev:{LEVERAGE}x, TF:{TIMEFRAME}, SymUpd:{SYMBOL_UPDATE_INTERVAL_HOURS}h, RESTSync:{REST_SYNC_INTERVAL_MINUTES}min")
    op_logger.warning("!!! USING REAL FUNDS - MONITOR CLOSELY !!!"); op_logger.warning("="*80)
    for i in range(3, 0, -1): print(f"Starting in {i}...", end='\r'); time.sleep(1)
    print("Starting now!      ")

    if not initialize_binance_rest(): op_logger.error("Exiting: CCXT REST init failure."); exit()
    op_logger.info("Running initial REST state sync..."); sync_positions_with_exchange(); op_logger.info("Initial REST sync complete."); log_asset_status()
    if not start_user_stream_initial(): op_logger.error("Exiting: Failed initial UDS start."); exit()

    sync_thread = Thread(target=sync_state_periodically, args=(REST_SYNC_INTERVAL_MINUTES * 60,), daemon=True); sync_thread.start()

    ws_url_kline = f"wss://fstream.binance.com/stream"
    kline_reconnect_delay = 5
    uds_reconnect_delay = 5

    try:
        while not shutdown_requested:
            # 1. K-line WS Check & Reconnect
            if not kline_websocket_running and not shutdown_requested:
                op_logger.info("Attempting K-line WS connection/reconnection...")
                if kline_wsapp and kline_wsapp.sock:
                    try: kline_wsapp.close()
                    except Exception: pass
                    time.sleep(1)

                current_on_open = on_open_kline_initial if kline_thread is None else on_open_kline_reconnect
                kline_wsapp = websocket.WebSocketApp(ws_url_kline, on_open=current_on_open, on_message=on_message_kline, on_error=on_error_kline, on_close=on_close_kline)
                # Start new thread only if previous one has finished or doesn't exist
                if kline_thread is None or not kline_thread.is_alive():
                     kline_thread = Thread(target=lambda: kline_wsapp.run_forever(ping_interval=60, ping_timeout=10), daemon=True)
                     kline_thread.start()
                     op_logger.info("New K-line WS thread started. Waiting...")
                else:
                     op_logger.warning("K-line thread still alive during reconnect attempt?")

                # Start symbol update thread if needed
                if symbol_update_thread is None or not symbol_update_thread.is_alive():
                    symbol_update_thread = Thread(target=update_top_symbols_periodically, args=(SYMBOL_UPDATE_INTERVAL_HOURS * 60 * 60,), daemon=True)
                    symbol_update_thread.start(); op_logger.info("Symbol Update thread started/restarted.")

                connect_wait_start = time.time()
                while not kline_websocket_running and time.time() - connect_wait_start < 15:
                    if shutdown_requested: break; time.sleep(0.5)

                if kline_websocket_running: op_logger.info("K-line WS connected/reconnected."); kline_reconnect_delay = 5
                else:
                    op_logger.error(f"K-line WS connection failed. Retrying in {kline_reconnect_delay}s...")
                    if kline_wsapp and kline_wsapp.sock: kline_wsapp.close()
                    time.sleep(kline_reconnect_delay); kline_reconnect_delay = min(kline_reconnect_delay * 2, 60)

            # 2. UDS Check & Reconnect
            elif not user_websocket_running and not shutdown_requested:
                op_logger.warning(f"User Data Stream disconnected! Attempting reconnect in {uds_reconnect_delay}s...")
                if user_wsapp and user_wsapp.sock:
                    try: user_wsapp.close(); op_logger.info("Closed previous UDS WS.")
                    except Exception: pass
                # Wait for keep-alive thread to potentially finish (it checks user_websocket_running)
                if keep_alive_thread and keep_alive_thread.is_alive(): keep_alive_thread.join(timeout=1)

                time.sleep(uds_reconnect_delay)
                uds_reconnect_delay = min(uds_reconnect_delay * 2, 60)

                op_logger.info("Attempting to re-establish User Data Stream...")
                if get_listen_key(): # Try to get a new key
                    # Start new Keep Alive thread
                    if keep_alive_thread is None or not keep_alive_thread.is_alive():
                         keep_alive_thread = Thread(target=keep_listen_key_alive, daemon=True); keep_alive_thread.start()

                    # Start new UDS WebSocket
                    uds_ws_url = f"wss://fstream.binance.com/ws/{listen_key}"
                    user_wsapp = websocket.WebSocketApp(uds_ws_url, on_open=on_open_user_stream, on_message=on_message_user_stream, on_error=on_error_user_stream, on_close=on_close_user_stream)
                    if user_stream_thread is None or not user_stream_thread.is_alive():
                         user_stream_thread = Thread(target=lambda: user_wsapp.run_forever(ping_interval=180, ping_timeout=10), daemon=True)
                         user_stream_thread.start(); op_logger.info("New UDS thread started.")

                    # Wait for connection and trigger sync
                    uds_connect_wait_start = time.time()
                    while not user_websocket_running and time.time() - uds_connect_wait_start < 15:
                         if shutdown_requested: break; time.sleep(0.5)

                    if user_websocket_running:
                        op_logger.info("User Data Stream RECONNECTED successfully!")
                        uds_reconnect_delay = 5
                        op_logger.info("Triggering immediate REST Sync after UDS reconnect...")
                        try: sync_positions_with_exchange() # Immediate Sync!
                        except Exception as sync_e: op_logger.error(f"Error during immediate sync: {sync_e}")
                    else:
                        op_logger.error("User Data Stream reconnection attempt failed.")
                        if user_wsapp and user_wsapp.sock: user_wsapp.close()
                else:
                    op_logger.error("Failed to get new listen key for UDS reconnect. Retrying later...")

            # 3. Both WS Running: Normal Operation
            else:
                log_asset_status()
                time.sleep(1)

    except KeyboardInterrupt: op_logger.info("Keyboard interrupt. Shutting down..."); shutdown_requested = True
    except Exception as main_loop_err: op_logger.error(f"Critical error in main loop: {main_loop_err}", exc_info=True); shutdown_requested = True
    finally:
        # --- Final Shutdown ---
        op_logger.info("Initiating final shutdown sequence...")
        shutdown_requested = True; kline_websocket_running = False; user_websocket_running = False
        if kline_wsapp and kline_wsapp.sock: op_logger.info("Closing K-line WS..."); kline_wsapp.close()
        if user_wsapp and user_wsapp.sock: op_logger.info("Closing User Data WS..."); user_wsapp.close()
        with listen_key_lock:
            if listen_key and binance_rest:
                # 종료 시 Listen Key 삭제 시도 (서버 자원 정리)
                op_logger.info(f"Attempting to delete listen key: {listen_key[:5]}...") # 로깅 분리
                try:
                    # CCXT의 Binance 선물 Listen Key 삭제 메서드 호출
                    binance_rest.fapiPrivateDeleteListenKey({'listenKey': listen_key})
                    op_logger.info("Listen key deletion request successful (or key was already invalid).")
                except Exception as dk_e:
                    # 삭제 실패는 종료 프로세스를 막지 않음 (경고만 기록)
                    op_logger.warning(f"Could not delete listen key (might be expired/invalid or network issue): {dk_e}")
        op_logger.info("Waiting for threads to finish (max 5s)..."); time.sleep(5)
        op_logger.warning("Attempting to cancel all remaining open orders...")
        # ... (Cancel order logic) ...
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

        op_logger.info("Fetching final balance...");
        final_balance=get_current_balance(); bal_str=f"{final_balance:.2f}" if final_balance is not None else "Error"
        with stats_lock: trades,wins=total_trades,winning_trades; wr=(wins/trades*100) if trades>0 else 0.0
        final_msg = f"Final Balance:{bal_str} {TARGET_ASSET}, Trades:{trades}, Wins:{wins}(UDS), WR:{wr:.2f}%"
        op_logger.info(final_msg); asset_logger.info(final_msg)
        op_logger.info(f"{log_prefix} Bot shutdown complete.")