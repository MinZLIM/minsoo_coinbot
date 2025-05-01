# -*- coding: utf-8 -*-
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
import uuid # 고유 ID 생성을 위해 추가
from functools import partial # for callback arguments
import numpy as np # NaN 처리를 위해 추가

# zoneinfo는 Python 3.9 이상, 하위 버전은 pytz 필요
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from pytz import timezone as ZoneInfo # Fallback for older Python

# ==============================================================================
# 사용자 설정 값 (*** 블랙리스트 시간, 동기화 주기, 심볼 업데이트 주기 변경 ***)
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

# 포지션 조건 설정 (*** 블랙리스트 시간, 동기화/업데이트 주기 변경 ***)
STOCH_RSI_LONG_ENTRY_THRESH = 15
STOCH_RSI_SHORT_ENTRY_THRESH = 85
LONG_STOP_LOSS_FACTOR = 0.99
SHORT_STOP_LOSS_FACTOR = 1.01
POSITION_MONITORING_DELAY_MINUTES = 5 # TP 업데이트 시작 딜레이
WHIPSAW_BLACKLIST_HOURS = 2          # <<< --- 블랙리스트 2시간 유지
TP_UPDATE_THRESHOLD_PERCENT = 0.1

# 동기화 및 업데이트 설정
REST_SYNC_INTERVAL_MINUTES = 30       # <<< --- REST API 동기화 주기 30분으로 변경
SYMBOL_UPDATE_INTERVAL_HOURS = 2      # <<< --- 심볼 업데이트 주기 2시간으로 설정
LISTEN_KEY_REFRESH_INTERVAL_MINUTES = 30 # <<<--- Listen Key 갱신 주기

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
# 로깅 설정 (운영 로그 INFO, 프리픽스/파일명 수정)
# ==============================================================================
log_dir = os.path.dirname(os.path.abspath(__file__))
log_prefix = "[REAL_UDS_AUTO_SYM]" # 로그 프리픽스 변경 (User Data Stream + Auto Symbol Update)

# 운영 로그
op_logger = logging.getLogger('operation')
op_logger.setLevel(logging.INFO)
op_formatter = logging.Formatter(f'%(asctime)s - %(levelname)s - {log_prefix} - %(message)s')
op_handler = logging.FileHandler(os.path.join(log_dir, 'operation_real_uds_auto_sym.log')) # 파일명 변경
op_handler.setFormatter(op_formatter)
op_logger.addHandler(op_handler)
op_logger.addHandler(logging.StreamHandler()) # 콘솔 출력 유지

# 매매 로그
trade_logger = logging.getLogger('trade')
trade_logger.setLevel(logging.INFO)
trade_formatter = logging.Formatter(f'%(asctime)s - {log_prefix} - %(message)s')
trade_handler = logging.FileHandler(os.path.join(log_dir, 'trade_real_uds_auto_sym.log')) # 파일명 변경
trade_handler.setFormatter(trade_formatter)
trade_logger.addHandler(trade_handler)

# 자산 로그
asset_logger = logging.getLogger('asset')
asset_logger.setLevel(logging.INFO)
asset_formatter = logging.Formatter(f'%(asctime)s - {log_prefix} - %(message)s')
asset_handler = logging.FileHandler(os.path.join(log_dir, 'asset_real_uds_auto_sym.log')) # 파일명 변경
asset_handler.setFormatter(asset_formatter)
asset_logger.addHandler(asset_handler)

# ==============================================================================
# 전역 변수 및 동기화 객체 (*** listen_key, wsapp 참조, Lock 추가 ***)
# ==============================================================================
real_positions = {}
real_positions_lock = Lock()
total_trades = 0; winning_trades = 0; stats_lock = Lock()
historical_data = {}; data_lock = Lock()
blacklist = {}; blacklist_lock = Lock()
entry_in_progress = {}; entry_lock = Lock()
last_asset_log_time = datetime.now(UTC)

# 웹소켓 관련
kline_websocket_running = True # K-line 웹소켓 실행 상태
user_websocket_running = True  # User Data 웹소켓 실행 상태
kline_wsapp = None             # K-line 웹소켓 앱 인스턴스 참조
user_wsapp = None              # User Data 웹소켓 앱 인스턴스 참조
listen_key = None              # User Data Stream Listen Key
listen_key_lock = Lock()       # Listen Key 접근/갱신 Lock

subscribed_symbols = set()     # 현재 구독 중인 심볼 (WS 기준)
subscribed_symbols_lock = Lock() # 구독 심볼 셋 접근 Lock

# CCXT 인스턴스
binance_rest = None

# ==============================================================================
# API 및 데이터 처리 함수 (이전과 유사, 일부 수정/추가)
# ==============================================================================
def initialize_binance_rest():
    global binance_rest; op_logger.info("Initializing CCXT REST...")
    if not API_KEY or API_KEY == "YOUR_BINANCE_API_KEY" or not API_SECRET:
        op_logger.error("API Key/Secret not configured properly!"); return False
    try:
        binance_rest = ccxt.binance({
            'apiKey': API_KEY,
            'secret': API_SECRET,
            'enableRateLimit': True,
            'options': {
                'defaultType': 'future',
                'adjustForTimeDifference': True,
                # 'warnOnFetchOpenOrdersWithoutSymbol': False # 경고 무시 옵션 (필요시)
            }
        })
        binance_rest.load_markets()
        # 시간 동기화 시도
        try:
            server_time = binance_rest.fetch_time()
            op_logger.info(f"Server time: {datetime.fromtimestamp(server_time / 1000, tz=UTC)}")
        except Exception as time_err:
            op_logger.warning(f"Failed to fetch server time, time difference adjustment might be needed: {time_err}")
        op_logger.info("CCXT REST initialized successfully."); return True
    except AuthenticationError: op_logger.error("REST API Authentication Error! Check your API keys."); return False
    except RequestTimeout: op_logger.error("REST API connection timed out."); return False
    except ExchangeNotAvailable: op_logger.error("Exchange is currently unavailable."); return False
    except Exception as e: op_logger.error(f"Failed to initialize CCXT REST: {e}", exc_info=True); return False

def get_current_balance(asset=TARGET_ASSET):
    if not binance_rest: return 0.0
    try:
        balance = binance_rest.fetch_balance(params={'type': 'future'})
        # 사용 가능 잔고 (free) 또는 전체 잔고 (total) 중 선택
        available = balance['free'].get(asset, 0.0)
        # total = balance['total'].get(asset, 0.0) # 필요시 전체 잔고 사용
        return float(available) if available else 0.0
    except (RequestTimeout, ExchangeNotAvailable, OnMaintenance) as e:
        op_logger.warning(f"Error fetching balance (Network/Exchange Issue): {e}"); return 0.0
    except Exception as e: op_logger.error(f"Unexpected error fetching balance: {e}"); return 0.0

def get_top_volume_symbols(n=TOP_N_SYMBOLS): # n=30
    if not binance_rest: return []
    op_logger.info(f"Fetching top {n} symbols by quote volume...")
    try:
        tickers = binance_rest.fetch_tickers()
        # USDT 무기한 선물만 필터링 (symbol format: BASE/QUOTE:QUOTE)
        futures_tickers = {s: t for s, t in tickers.items()
                           if '/' in s and s.endswith(f"/{TARGET_ASSET}:{TARGET_ASSET}") and t.get('quoteVolume') is not None}

        if not futures_tickers:
             op_logger.warning("No USDT perpetual futures tickers found.")
             return []

        # quoteVolume 기준으로 정렬 (내림차순)
        sorted_tickers = sorted(futures_tickers.values(), key=lambda x: x.get('quoteVolume', 0), reverse=True)

        # CCXT 심볼 형식 (BASE/QUOTE)만 추출
        top_symbols_ccxt = [t['symbol'].split(':')[0] for t in sorted_tickers[:n]]
        op_logger.info(f"Fetched top {len(top_symbols_ccxt)} symbols: {top_symbols_ccxt[:5]}...")
        return top_symbols_ccxt
    except (RequestTimeout, ExchangeNotAvailable, OnMaintenance) as e:
        op_logger.warning(f"Error fetching top symbols (Network/Exchange Issue): {e}"); return []
    except Exception as e: op_logger.error(f"Error fetching top symbols: {e}"); return []

def fetch_initial_ohlcv(symbol_ccxt, timeframe=TIMEFRAME, limit=INITIAL_CANDLE_FETCH_LIMIT):
     if not binance_rest: return None
     try:
         # 필요한 최소 캔들 수 보장 (Stoch RSI 계산 위함)
         actual_limit = max(limit, STOCH_RSI_PERIOD * 2 + 50)
         op_logger.debug(f"Fetching initial {actual_limit} candles for {symbol_ccxt} ({timeframe})...")
         ohlcv = binance_rest.fetch_ohlcv(symbol_ccxt, timeframe=timeframe, limit=actual_limit)
         if not ohlcv:
             op_logger.warning(f"No OHLCV data returned for {symbol_ccxt}.")
             return None
         df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
         df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
         df.set_index('timestamp', inplace=True)
         op_logger.debug(f"Successfully fetched {len(df)} candles for {symbol_ccxt}.")
         return df
     except (RequestTimeout, ExchangeNotAvailable, OnMaintenance) as e:
        op_logger.warning(f"Error fetching initial OHLCV for {symbol_ccxt} (Network/Exchange Issue): {e}"); return None
     except Exception as e: op_logger.error(f"Error fetching initial OHLCV for {symbol_ccxt}: {e}"); return None

def calculate_indicators(df): # BBands, Stoch RSI 만 계산
    # 지표 계산에 필요한 최소 데이터 길이 확인
    required_len = max(BBANDS_PERIOD, STOCH_RSI_PERIOD * 2) # Stoch RSI가 더 많은 데이터 요구 가능
    if df is None or len(df) < required_len:
        # op_logger.debug(f"Not enough data to calculate indicators (Need {required_len}, Have {len(df) if df is not None else 0}).")
        return None
    try:
        df_copy = df.copy()
        # pandas_ta 라이브러리 사용하여 지표 계산 및 추가
        df_copy.ta.bbands(length=BBANDS_PERIOD, std=BBANDS_STDDEV, append=True)
        df_copy.ta.stochrsi(length=STOCH_RSI_PERIOD, rsi_length=STOCH_RSI_PERIOD, k=STOCH_RSI_K, d=STOCH_RSI_D, append=True)

        # 생성된 컬럼 이름 확인 및 리네임 (pandas_ta 버전에 따라 이름이 달라질 수 있음)
        # 컬럼 이름 예시: 'BBL_20_2.0', 'BBM_20_2.0', 'BBU_20_2.0', 'STOCHRSIk_14_14_3_3', 'STOCHRSId_14_14_3_3'
        # 설정값에 맞춰 동적으로 컬럼 이름 생성
        bbl_col = f'BBL_{BBANDS_PERIOD}_{float(BBANDS_STDDEV)}'
        bbm_col = f'BBM_{BBANDS_PERIOD}_{float(BBANDS_STDDEV)}'
        bbu_col = f'BBU_{BBANDS_PERIOD}_{float(BBANDS_STDDEV)}'
        stochk_col = f'STOCHRSIk_{STOCH_RSI_PERIOD}_{STOCH_RSI_PERIOD}_{STOCH_RSI_K}_{STOCH_RSI_D}'
        stochd_col = f'STOCHRSId_{STOCH_RSI_PERIOD}_{STOCH_RSI_PERIOD}_{STOCH_RSI_K}_{STOCH_RSI_D}'

        rename_map = {
            bbl_col: 'BBL', bbm_col: 'BBM', bbu_col: 'BBU',
            stochk_col: 'STOCHk', stochd_col: 'STOCHd'
        }

        # 실제 존재하는 컬럼만 리네임
        existing_rename_map = {k: v for k, v in rename_map.items() if k in df_copy.columns}
        if len(existing_rename_map) < 5: # 5개 지표(BBL,BBM,BBU,STOCHk,STOCHd)가 다 생성 안됐을 경우
             # op_logger.warning(f"Indicator columns not fully generated. Available: {df_copy.columns.tolist()}")
             # 생성된 컬럼들 확인 (디버깅용)
             # print(df_copy.columns)
             return None

        df_copy.rename(columns=existing_rename_map, inplace=True)

        # 필요한 최종 컬럼 존재 및 마지막 행 값 유효성(NaN) 확인
        required_cols = ['BBL', 'BBU', 'STOCHk', 'STOCHd']
        if not all(col in df_copy.columns for col in required_cols):
            # op_logger.warning(f"Required indicator columns missing after rename. Needed: {required_cols}, Have: {df_copy.columns.tolist()}")
            return None
        if df_copy[required_cols].iloc[-1].isnull().any():
            # op_logger.debug("Latest indicator values contain NaN.")
            return None

        return df_copy
    except Exception as e: op_logger.error(f"Indicator Calculation Error: {e}", exc_info=True); return None


def set_isolated_margin(symbol_ccxt, leverage):
    if not binance_rest: return False
    op_logger.info(f"Setting ISOLATED margin for {symbol_ccxt} with {leverage}x leverage...")
    try:
        # 1. 마진 모드 설정 (ISOLATED)
        try:
            binance_rest.set_margin_mode('ISOLATED', symbol_ccxt, params={})
            op_logger.info(f"Margin mode set to ISOLATED for {symbol_ccxt}.")
            time.sleep(0.2) # 잠시 대기
        except ccxt.ExchangeError as e:
            # 이미 ISOLATED 모드인 경우 무시하고 진행
            if 'No need to change margin type' in str(e) or 'Margin type already set to ISOLATED' in str(e):
                op_logger.warning(f"{symbol_ccxt} is already in ISOLATED margin mode.")
            # 포지션이 존재하여 변경 불가한 경우 에러 처리
            elif 'position exists' in str(e):
                 op_logger.error(f"Cannot change margin mode for {symbol_ccxt}, position exists.")
                 return False
            else:
                op_logger.error(f"Failed to set margin mode for {symbol_ccxt}: {e}")
                return False

        # 2. 레버리지 설정
        try:
            binance_rest.set_leverage(leverage, symbol_ccxt, params={})
            op_logger.info(f"Leverage set to {leverage}x for {symbol_ccxt}.")
            return True
        except ccxt.ExchangeError as e:
             # 레버리지 변경 불필요 메시지는 성공으로 간주 가능
             if 'No need to change leverage' in str(e):
                 op_logger.warning(f"Leverage for {symbol_ccxt} is already {leverage}x.")
                 return True
             else:
                op_logger.error(f"Failed to set leverage for {symbol_ccxt}: {e}")
                return False

    except (RequestTimeout, ExchangeNotAvailable, OnMaintenance) as e:
        op_logger.warning(f"Failed to set isolated margin for {symbol_ccxt} (Network/Exchange Issue): {e}"); return False
    except Exception as e:
        op_logger.error(f"Unexpected error setting isolated margin for {symbol_ccxt}: {e}", exc_info=True)
        return False

# --- 주문 관련 함수들 ---
def place_market_order_real(symbol_ccxt, side, amount, current_price=None):
    if not binance_rest: op_logger.error("CCXT instance not ready for placing market order."); return None
    if amount <= 0: op_logger.error(f"[{symbol_ccxt}] Invalid order amount: {amount}"); return None

    try:
        market = binance_rest.market(symbol_ccxt)
        # 주문 수량 정밀도 조절
        adjusted_amount_str = binance_rest.amount_to_precision(symbol_ccxt, amount)
        adjusted_amount = float(adjusted_amount_str)
        if adjusted_amount <= 0:
             op_logger.error(f"[{symbol_ccxt}] Adjusted amount {adjusted_amount_str} is zero or negative.")
             return None

        # 최소 주문 금액 확인 (예: 5 USDT) - 바이낸스 규정 확인 필요
        min_notional = market.get('limits', {}).get('cost', {}).get('min', 5.0)
        if current_price and adjusted_amount * current_price < min_notional:
             op_logger.error(f"[{symbol_ccxt}] Order value ({adjusted_amount * current_price:.2f}) is below minimum notional ({min_notional}). Amount: {adjusted_amount_str}")
             # 강제로 최소 금액 맞추기 (주의: 의도와 다를 수 있음)
             # adjusted_amount = float(binance_rest.amount_to_precision(symbol_ccxt, min_notional / current_price * 1.01))
             # op_logger.warning(f"[{symbol_ccxt}] Adjusting amount to meet minimum notional: {adjusted_amount}")
             # if adjusted_amount <= 0: return None
             return None # 최소 주문 금액 미달 시 주문 거부

        op_logger.info(f"[REAL ORDER] Attempting {side.upper()} {adjusted_amount_str} {symbol_ccxt} @ MARKET")
        # 고유 clientOrderId 생성 (선택 사항, 중복 주문 방지 및 추적 용이)
        client_order_id = f"bot_{uuid.uuid4().hex[:16]}"
        params = {'newClientOrderId': client_order_id}

        order = binance_rest.create_market_order(symbol_ccxt, side, adjusted_amount, params=params)

        op_logger.info(f"[REAL ORDER PLACED] ID:{order.get('id')} ClientID:{client_order_id} Sym:{symbol_ccxt} Side:{side} ReqAmt:{adjusted_amount_str}")
        trade_logger.info(f"REAL MARKET EXECUTE: {side.upper()} {symbol_ccxt}, ReqAmt:{adjusted_amount_str}, OrdID:{order.get('id')}, CliOrdID:{client_order_id}")

        # 결과 확인 (API 응답에 따라 다름)
        filled_amount = order.get('filled')
        avg_price = order.get('average')
        ts_ms = order.get('timestamp')
        order_id = order.get('id')

        # 즉시 체결 정보 반환되는 경우
        if filled_amount is not None and avg_price is not None:
            op_logger.info(f"[REAL ORDER FILLED IMMED] {symbol_ccxt} FillAmt:{filled_amount}, AvgPx:{avg_price}, OrdID:{order_id}")
            return {'symbol': symbol_ccxt, 'average': avg_price, 'filled': filled_amount, 'timestamp': ts_ms, 'id': order_id, 'clientOrderId': client_order_id, 'status': 'closed'} # 'status' 추가

        # 체결 정보가 즉시 안 올 경우 (거의 없지만 대비) -> User Data Stream에서 확인해야 함
        else:
            op_logger.warning(f"[REAL ORDER] Placed {symbol_ccxt} (ID:{order_id}) but immediate fill info missing. Relying on User Stream.")
            # User Stream에서 처리될 때까지 임시 정보 반환 또는 None 반환 결정 필요
            # 여기서는 주문 성공 자체만 알리고, 상세 정보는 User Stream 처리 기다림
            return {'symbol': symbol_ccxt, 'id': order_id, 'clientOrderId': client_order_id, 'status': 'open'} # 'status' open으로 반환

    except ccxt.InsufficientFunds as e: op_logger.error(f"[ORDER FAILED] Insufficient funds for {symbol_ccxt}: {e}"); log_asset_status(); return None
    except ccxt.ExchangeError as e: op_logger.error(f"[ORDER FAILED] Exchange error placing market order for {symbol_ccxt}: {e}"); return None
    except (RequestTimeout, ExchangeNotAvailable, OnMaintenance) as e: op_logger.warning(f"[ORDER FAILED] Network/Exchange issue placing market order for {symbol_ccxt}: {e}"); return None
    except Exception as e: op_logger.error(f"[ORDER FAILED] Unexpected error placing market order for {symbol_ccxt}: {e}", exc_info=True); return None

def place_stop_market_order(symbol_ccxt, side, stop_price, amount):
    if not binance_rest: op_logger.error(f"[{symbol_ccxt}] CCXT instance needed for SL order."); return None
    if amount <= 0 or stop_price <= 0: op_logger.error(f"[{symbol_ccxt}] Invalid SL amount({amount}) or stop_price({stop_price})."); return None

    try:
        adjusted_amount_str = binance_rest.amount_to_precision(symbol_ccxt, amount)
        stop_price_str = binance_rest.price_to_precision(symbol_ccxt, stop_price)
        adjusted_amount = float(adjusted_amount_str)

        if adjusted_amount <= 0: op_logger.error(f"[{symbol_ccxt}] SL Adjusted amount {adjusted_amount_str} is zero."); return None

        op_logger.info(f"[REAL SL ORDER] Attempting {side.upper()} {adjusted_amount_str} {symbol_ccxt} if price hits {stop_price_str} (STOP_MARKET)")

        # reduceOnly: true (포지션 종료용 주문)
        # closePosition: true (격리 모드에서 전체 포지션 종료 시 사용 가능, 주의 필요)
        params = {'stopPrice': stop_price_str, 'reduceOnly': True}
        client_order_id = f"sl_{uuid.uuid4().hex[:16]}"
        params['newClientOrderId'] = client_order_id

        order = binance_rest.create_order(symbol_ccxt, 'STOP_MARKET', side, adjusted_amount, None, params)
        order_id = order.get('id')

        op_logger.info(f"[REAL SL ORDER PLACED] ID:{order_id} ClientID:{client_order_id} Sym:{symbol_ccxt} Side:{side} StopPx:{stop_price_str} Amt:{adjusted_amount_str}")
        trade_logger.info(f"REAL SL SET: {side.upper()} {symbol_ccxt}, Amt:{adjusted_amount_str}, StopPx:{stop_price_str}, OrdID:{order_id}, CliOrdID:{client_order_id}")
        # 주문 ID와 clientOrderId 둘 다 반환
        return {'id': order_id, 'clientOrderId': client_order_id}

    except ccxt.ExchangeError as e: op_logger.error(f"[SL ORDER FAILED] Exchange error placing SL for {symbol_ccxt}: {e}"); return None
    except (RequestTimeout, ExchangeNotAvailable, OnMaintenance) as e: op_logger.warning(f"[SL ORDER FAILED] Network/Exchange issue placing SL for {symbol_ccxt}: {e}"); return None
    except Exception as e: op_logger.error(f"[SL ORDER FAILED] Unexpected error placing SL for {symbol_ccxt}: {e}", exc_info=True); return None

def place_take_profit_market_order(symbol_ccxt, side, stop_price, amount):
    if not binance_rest: op_logger.error(f"[{symbol_ccxt}] CCXT instance needed for TP order."); return None
    if amount <= 0 or stop_price <= 0: op_logger.error(f"[{symbol_ccxt}] Invalid TP amount({amount}) or stop_price({stop_price})."); return None

    try:
        adjusted_amount_str = binance_rest.amount_to_precision(symbol_ccxt, amount)
        stop_price_str = binance_rest.price_to_precision(symbol_ccxt, stop_price)
        adjusted_amount = float(adjusted_amount_str)

        if adjusted_amount <= 0: op_logger.error(f"[{symbol_ccxt}] TP Adjusted amount {adjusted_amount_str} is zero."); return None

        op_logger.info(f"[REAL TP ORDER] Attempting {side.upper()} {adjusted_amount_str} {symbol_ccxt} if price hits {stop_price_str} (TAKE_PROFIT_MARKET)")
        params = {'stopPrice': stop_price_str, 'reduceOnly': True}
        client_order_id = f"tp_{uuid.uuid4().hex[:16]}"
        params['newClientOrderId'] = client_order_id

        order = binance_rest.create_order(symbol_ccxt, 'TAKE_PROFIT_MARKET', side, adjusted_amount, None, params)
        order_id = order.get('id')

        op_logger.info(f"[REAL TP ORDER PLACED] ID:{order_id} ClientID:{client_order_id} Sym:{symbol_ccxt} Side:{side} StopPx:{stop_price_str} Amt:{adjusted_amount_str}")
        trade_logger.info(f"REAL TP SET: {side.upper()} {symbol_ccxt}, Amt:{adjusted_amount_str}, StopPx:{stop_price_str}, OrdID:{order_id}, CliOrdID:{client_order_id}")
        return {'id': order_id, 'clientOrderId': client_order_id}

    except ccxt.ExchangeError as e: op_logger.error(f"[TP ORDER FAILED] Exchange error placing TP for {symbol_ccxt}: {e}"); return None
    except (RequestTimeout, ExchangeNotAvailable, OnMaintenance) as e: op_logger.warning(f"[TP ORDER FAILED] Network/Exchange issue placing TP for {symbol_ccxt}: {e}"); return None
    except Exception as e: op_logger.error(f"[TP ORDER FAILED] Unexpected error placing TP for {symbol_ccxt}: {e}", exc_info=True); return None

def cancel_order(symbol_ccxt, order_id=None, client_order_id=None):
    if not binance_rest: op_logger.error(f"[{symbol_ccxt}] CCXT needed for cancel."); return False
    if not order_id and not client_order_id: op_logger.debug(f"[{symbol_ccxt}] Cancel skipped, no order_id or client_order_id provided."); return True # 취소할 ID 없으면 성공 간주

    target_id_str = f"orderId={order_id}" if order_id else f"clientOrderId={client_order_id}"
    op_logger.info(f"Attempting to cancel order {target_id_str} for {symbol_ccxt}...")

    try:
        if order_id:
            binance_rest.cancel_order(order_id, symbol_ccxt)
        else:
            # clientOrderId로 취소 (CCXT는 기본적으로 ID 사용, params 필요)
            binance_rest.cancel_order(client_order_id, symbol_ccxt, params={'origClientOrderId': client_order_id})

        op_logger.info(f"Successfully cancelled order {target_id_str} for {symbol_ccxt}.")
        return True
    except OrderNotFound:
        op_logger.warning(f"Order {target_id_str} not found for {symbol_ccxt} (already executed, cancelled, or invalid ID?)."); return True # 못 찾으면 이미 처리된 것으로 간주
    except (ExchangeNotAvailable, OnMaintenance, RequestTimeout) as e:
        op_logger.error(f"Cannot cancel {target_id_str} for {symbol_ccxt} due to exchange/network issue: {e}"); return False
    except RateLimitExceeded as e: op_logger.error(f"Rate limit hit cancelling {target_id_str} for {symbol_ccxt}: {e}"); return False
    except ccxt.ExchangeError as e:
        # 이미 취소/완료된 주문 취소 시 발생하는 특정 에러 코드는 성공으로 간주 가능 (예: 바이낸스 -2011)
        if 'Order does not exist' in str(e) or 'Unknown order sent' in str(e) or '-2011' in str(e):
             op_logger.warning(f"Order {target_id_str} likely already closed/cancelled (Exchange Msg: {e}). Treating as success.")
             return True
        else:
            op_logger.error(f"Failed to cancel {target_id_str} for {symbol_ccxt}. ExchangeError: {e}"); return False
    except Exception as e: op_logger.error(f"Unexpected error cancelling {target_id_str} for {symbol_ccxt}: {e}", exc_info=True); return False

def cancel_open_orders_for_symbol(symbol_ccxt):
    if not binance_rest: return False
    op_logger.warning(f"Attempting to cancel ALL open orders for {symbol_ccxt}...")
    cancelled_count = 0
    try:
        open_orders = binance_rest.fetch_open_orders(symbol_ccxt)
        if not open_orders:
            op_logger.info(f"No open orders found for {symbol_ccxt} to cancel.")
            return True
        op_logger.info(f"Found {len(open_orders)} open orders for {symbol_ccxt}. Cancelling...")
        for order in open_orders:
            order_id = order.get('id')
            client_order_id = order.get('clientOrderId')
            if cancel_order(symbol_ccxt, order_id=order_id, client_order_id=client_order_id):
                cancelled_count += 1
            time.sleep(0.2) # Rate limit 방지
        op_logger.info(f"Finished cancelling orders for {symbol_ccxt}. Cancelled {cancelled_count}/{len(open_orders)}.")
        return True
    except (RequestTimeout, ExchangeNotAvailable, OnMaintenance) as e:
        op_logger.error(f"Failed to cancel orders for {symbol_ccxt} (Network/Exchange Issue): {e}"); return False
    except Exception as e:
        op_logger.error(f"Unexpected error cancelling orders for {symbol_ccxt}: {e}", exc_info=True)
        return False

# --- 블랙리스트 관련 ---
def check_symbol_in_blacklist(symbol_ws):
    with blacklist_lock:
        expiry = blacklist.get(symbol_ws)
        if expiry and datetime.now(UTC) < expiry:
            # op_logger.debug(f"Symbol {symbol_ws} is currently blacklisted until {expiry.astimezone(KST).strftime('%H:%M:%S')}.")
            return True
        elif expiry:
            # 만료된 경우 블랙리스트에서 제거
            op_logger.info(f"Blacklist expired for {symbol_ws}. Removing.")
            del blacklist[symbol_ws]
            return False
        return False

def add_to_blacklist(symbol_ws, reason=""):
    with blacklist_lock:
        expiry_time = datetime.now(UTC) + timedelta(hours=WHIPSAW_BLACKLIST_HOURS)
        blacklist[symbol_ws] = expiry_time
        op_logger.warning(f"Blacklisted {symbol_ws} until {expiry_time.astimezone(KST).strftime('%Y-%m-%d %H:%M:%S KST')}. Reason: {reason}")

# --- 자산 로깅 ---
def log_asset_status():
    global last_asset_log_time
    now = datetime.now(UTC)
    # 1시간마다 자산 로그 기록
    if now - last_asset_log_time >= timedelta(hours=1):
        try:
            balance = get_current_balance()
            if balance is None: balance = "Error" # 잔고 조회 실패 시
            else: balance = f"{balance:.2f}"

            with stats_lock: cur_trades, cur_wins = total_trades, winning_trades
            win_rate = (cur_wins / cur_trades * 100) if cur_trades > 0 else 0.0

            # 현재 보유 포지션 요약 추가
            active_positions_summary = []
            with real_positions_lock:
                 active_positions_summary = list(real_positions.keys())
                 num_active = len(real_positions)

            asset_logger.info(f"Balance:{balance} {TARGET_ASSET}, Active Pos:{num_active} {active_positions_summary}, Trades:{cur_trades}, Wins:{cur_wins}(UDS Based), WinRate:{win_rate:.2f}%")
            last_asset_log_time = now
        except Exception as e:
            asset_logger.error(f"Error logging asset status: {e}", exc_info=True)


# ==============================================================================
# 상태 동기화 로직 (*** 역할 축소: 보조적 확인 및 정리 ***)
# ==============================================================================
def sync_positions_with_exchange():
    global real_positions
    op_logger.info("[SYNC_REST] Starting state synchronization via REST API (Fallback Check)...")
    if not binance_rest: op_logger.error("[SYNC_REST] CCXT instance not ready."); return

    try:
        # 1. 거래소 실제 포지션 조회
        exchange_pos = binance_rest.fetch_positions()
        time.sleep(0.5) # Rate limit

        # 현재 거래소에 열려있는 포지션 정보 정리 (symbol_ws -> pos_info)
        current_exchange_pos_dict = {}
        for pos in exchange_pos:
            try:
                amount = float(pos.get('info', {}).get('positionAmt', 0))
                # 매우 작은 수량은 무시 (오차 가능성)
                if abs(amount) > 1e-9:
                    symbol_ccxt = pos.get('symbol')
                    if symbol_ccxt and symbol_ccxt.endswith(TARGET_ASSET): # USDT 페어만 확인
                        symbol_ws = symbol_ccxt.replace('/USDT', 'USDT')
                        current_exchange_pos_dict[symbol_ws] = {
                            'side': 'long' if amount > 0 else 'short',
                            'amount': abs(amount),
                            'entry_price': float(pos.get('entryPrice', 0)),
                            'symbol_ccxt': symbol_ccxt
                        }
            except Exception as parse_err:
                op_logger.error(f"[SYNC_REST] Error parsing exchange position data: {pos.get('info')}, Err: {parse_err}")

        # 2. 로컬 포지션과 비교
        with real_positions_lock:
            local_pos_symbols = set(real_positions.keys())
            exchange_pos_symbols = set(current_exchange_pos_dict.keys())

            # Case 1: 로컬 O / 거래소 X -> User Stream 누락 가능성? 로컬 제거 및 로그 남기기 (블랙리스트는 UDS에서 처리)
            symbols_to_remove_local = local_pos_symbols - exchange_pos_symbols
            for symbol_ws in symbols_to_remove_local:
                op_logger.warning(f"[SYNC_REST] Local pos {symbol_ws} not found on exchange (closed via UDS or external?). Removing from local state.")
                if symbol_ws in real_positions:
                    # 관련 SL/TP 주문 취소 시도 (이미 처리되었을 가능성 높음)
                    pos_info = real_positions[symbol_ws]
                    symbol_ccxt = symbol_ws.replace('USDT','/USDT')
                    cancel_order(symbol_ccxt, order_id=pos_info.get('sl_order_id'), client_order_id=pos_info.get('sl_client_order_id'))
                    time.sleep(0.1)
                    cancel_order(symbol_ccxt, order_id=pos_info.get('tp_order_id'), client_order_id=pos_info.get('tp_client_order_id'))
                    del real_positions[symbol_ws] # Lock 안에서 삭제

            # Case 2: 거래소 O / 로컬 X -> 비정상 상태! 즉시 종료 시도 및 관련 주문 취소
            symbols_to_close_untracked = exchange_pos_symbols - local_pos_symbols
            for symbol_ws in symbols_to_close_untracked:
                op_logger.error(f"[SYNC_REST] Untracked position {symbol_ws} found on exchange! Attempting to close immediately.")
                pos_info = current_exchange_pos_dict[symbol_ws]
                symbol_ccxt = pos_info['symbol_ccxt']
                side_to_close = 'sell' if pos_info['side'] == 'long' else 'buy'
                amount_to_close = pos_info['amount']

                # 모든 관련 주문 취소 시도 먼저
                cancel_open_orders_for_symbol(symbol_ccxt)
                time.sleep(0.5)

                # 시장가 종료 시도
                op_logger.info(f"[SYNC_REST] Closing untracked {symbol_ccxt} ({side_to_close} {amount_to_close}) via market order.")
                try:
                    ticker = binance_rest.fetch_ticker(symbol_ccxt)
                    current_price = ticker['last'] if ticker else None
                    place_market_order_real(symbol_ccxt, side_to_close, amount_to_close, current_price)
                except Exception as close_err:
                    op_logger.error(f"[SYNC_REST] Failed to place market order to close untracked {symbol_ccxt}: {close_err}")
                # 블랙리스트 추가 고려 (비정상 종료)
                add_to_blacklist(symbol_ws, reason="Untracked position closed via REST Sync")


            # Case 3: 로컬 O / 거래소 O -> User Stream이 잘 동작했다면 큰 차이 없어야 함. 간단한 상태 확인 및 로그.
            symbols_to_check = local_pos_symbols.intersection(exchange_pos_symbols)
            for symbol_ws in symbols_to_check:
                 local_info = real_positions.get(symbol_ws)
                 exchange_info = current_exchange_pos_dict.get(symbol_ws)
                 if local_info and exchange_info:
                     # 수량이나 방향이 크게 다르면 로그 남기기 (UDS 처리 지연/오류 가능성)
                     amount_diff = abs(local_info.get('amount', 0) - exchange_info.get('amount', 0))
                     if local_info.get('side') != exchange_info.get('side') or amount_diff > 1e-6: # 아주 작은 차이는 무시
                         op_logger.warning(f"[SYNC_REST] Potential discrepancy for {symbol_ws}. Local: {local_info}, Exchange: {exchange_info}. User Stream might be delayed or missed events.")
                         # 필요시 추가 조치 (예: 로컬 정보를 거래소 정보로 강제 업데이트)
                         # real_positions[symbol_ws]['amount'] = exchange_info['amount']
                         # real_positions[symbol_ws]['entry_price'] = exchange_info['entry_price']


        # 3. 미체결 주문 확인 (선택적 강화)
        # fetch_open_orders()는 부하가 클 수 있으므로, 필요시에만 사용하거나 User Stream 신뢰도 높으면 생략 가능
        # op_logger.info("[SYNC_REST] Checking for orphaned open orders...")
        # try:
        #     all_open_orders = binance_rest.fetch_open_orders() # 모든 심볼 조회 (부하 주의!)
        #     relevant_open_orders = [o for o in all_open_orders if o['symbol'].endswith(TARGET_ASSET)]
        #     op_logger.info(f"[SYNC_REST] Found {len(relevant_open_orders)} open orders for {TARGET_ASSET} pairs.")
        #     # ... 로컬 포지션과 연관 없는 주문 찾아 취소하는 로직 ...
        # except Exception as order_err:
        #     op_logger.error(f"[SYNC_REST] Error fetching/checking open orders: {order_err}")


        op_logger.info("[SYNC_REST] REST state synchronization finished.")

    except RateLimitExceeded: op_logger.warning("[SYNC_REST] Rate limit exceeded during REST sync."); time.sleep(60)
    except (RequestTimeout, ExchangeNotAvailable, OnMaintenance) as e: op_logger.warning(f"[SYNC_REST] Exchange/Network unavailable during REST sync: {e}"); time.sleep(60)
    except AuthenticationError: op_logger.error("[SYNC_REST] Authentication error during REST sync! Check API keys."); global kline_websocket_running, user_websocket_running; kline_websocket_running = False; user_websocket_running = False # 봇 중지
    except Exception as e: op_logger.error(f"[SYNC_REST] Error during REST sync: {e}", exc_info=True); time.sleep(60)

def sync_state_periodically(interval_seconds):
    global kline_websocket_running # kline 웹소켓 상태 따라 실행 여부 결정
    op_logger.info(f"REST Sync thread started. Interval: {interval_seconds}s.")
    while kline_websocket_running: # 메인 kline WS가 실행 중일 때만 동작
        try:
            time.sleep(interval_seconds)
            if kline_websocket_running: # 종료 상태 체크 한번 더
                sync_positions_with_exchange()
        except Exception as e: op_logger.error(f"Error in REST sync loop: {e}", exc_info=True); time.sleep(60) # 루프 에러 시 잠시 대기

# ==============================================================================
# 심볼 목록 주기적 업데이트 로직 (*** 신규 추가 ***)
# ==============================================================================
def update_top_symbols_periodically(interval_seconds):
    global subscribed_symbols, historical_data, kline_websocket_running, kline_wsapp
    op_logger.info(f"Symbol Update thread started. Interval: {interval_seconds}s.")

    while kline_websocket_running:
        try:
            time.sleep(interval_seconds)
            if not kline_websocket_running: break

            # K-line 웹소켓 연결 상태 확인
            if not kline_wsapp or not kline_wsapp.sock or not kline_wsapp.sock.connected:
                op_logger.warning("[Symbol Update] K-line WebSocket not connected. Skipping symbol update.")
                continue

            op_logger.info("[Symbol Update] Starting periodic symbol list update...")

            # 1. 최신 상위 심볼 목록 가져오기
            new_top_symbols_ccxt = get_top_volume_symbols(TOP_N_SYMBOLS)
            if not new_top_symbols_ccxt:
                op_logger.warning("[Symbol Update] Failed to fetch new top symbols. Skipping update.")
                continue
            # WS용 심볼 형식으로 변환 (e.g., BTCUSDT) 및 Set으로 만들기
            new_top_symbols_ws = {s.replace('/', '') for s in new_top_symbols_ccxt}

            # 2. 현재 구독 목록과 비교
            with subscribed_symbols_lock:
                current_subscribed = subscribed_symbols.copy()

            symbols_to_add = new_top_symbols_ws - current_subscribed
            symbols_to_remove = current_subscribed - new_top_symbols_ws

            # 3. 제거할 심볼 처리 (UNSUBSCRIBE 및 데이터 정리)
            if symbols_to_remove:
                op_logger.info(f"[Symbol Update] Symbols to remove: {symbols_to_remove}")
                unsubscribe_streams = [f"{s.lower()}@kline_{TIMEFRAME}" for s in symbols_to_remove]
                if unsubscribe_streams:
                    unsubscribe_message = {"method": "UNSUBSCRIBE", "params": unsubscribe_streams, "id": int(time.time())}
                    try:
                        kline_wsapp.send(json.dumps(unsubscribe_message))
                        op_logger.info(f"[Symbol Update] Sent UNSUBSCRIBE for {len(symbols_to_remove)} symbols.")
                    except Exception as e:
                        op_logger.error(f"[Symbol Update] Failed to send UNSUBSCRIBE message: {e}")

                # 구독 목록 및 히스토리 데이터에서 제거
                with subscribed_symbols_lock:
                    subscribed_symbols -= symbols_to_remove
                with data_lock:
                    removed_count = 0
                    for symbol_ws in symbols_to_remove:
                        if historical_data.pop(symbol_ws, None):
                             removed_count +=1
                    op_logger.info(f"[Symbol Update] Removed historical data for {removed_count}/{len(symbols_to_remove)} symbols.")

                # --- 중요: 제거된 심볼의 기존 포지션 처리 정책 ---
                # 여기서는 별도 처리 안함. 기존 SL/TP 또는 동기화 로직에 의해 청산되도록 함.
                # 필요 시, 여기서 해당 심볼 포지션 강제 종료 로직 추가 가능 (real_positions_lock 필요)
                # for symbol_ws in symbols_to_remove: close_position_if_exists(symbol_ws)

            # 4. 추가할 심볼 처리 (데이터 Fetch 및 SUBSCRIBE)
            if symbols_to_add:
                op_logger.info(f"[Symbol Update] Symbols to add: {symbols_to_add}")
                new_data_fetched_count = 0
                errors_fetching = 0

                # 새 심볼 데이터 가져오기
                with data_lock:
                    for symbol_ws in symbols_to_add:
                        symbol_ccxt = symbol_ws.replace('USDT', '/USDT')
                        initial_df = fetch_initial_ohlcv(symbol_ccxt, TIMEFRAME, limit=max(INITIAL_CANDLE_FETCH_LIMIT, STOCH_RSI_PERIOD*2))
                        if initial_df is not None and not initial_df.empty:
                            historical_data[symbol_ws] = initial_df
                            new_data_fetched_count += 1
                        else:
                            op_logger.warning(f"[Symbol Update] Failed to fetch initial data for new symbol {symbol_ws}.")
                            errors_fetching += 1
                        time.sleep(0.3) # Rate limit 방지

                op_logger.info(f"[Symbol Update] Fetched initial data for {new_data_fetched_count}/{len(symbols_to_add)} new symbols ({errors_fetching} fetch errors).")

                # 웹소켓 구독
                subscribe_streams = [f"{s.lower()}@kline_{TIMEFRAME}" for s in symbols_to_add]
                if subscribe_streams:
                    subscribe_message = {"method": "SUBSCRIBE", "params": subscribe_streams, "id": int(time.time())}
                    try:
                        kline_wsapp.send(json.dumps(subscribe_message))
                        op_logger.info(f"[Symbol Update] Sent SUBSCRIBE for {len(symbols_to_add)} symbols.")
                        # 구독 성공 시 구독 목록 업데이트
                        with subscribed_symbols_lock:
                            subscribed_symbols.update(symbols_to_add)
                    except Exception as e:
                        op_logger.error(f"[Symbol Update] Failed to send SUBSCRIBE message: {e}")

            op_logger.info(f"[Symbol Update] Update finished. Currently subscribed to {len(subscribed_symbols)} symbols.")

        except Exception as e:
            op_logger.error(f"Error in symbol update loop: {e}", exc_info=True)
            time.sleep(60) # 에러 발생 시 잠시 대기 후 재시도


# ==============================================================================
# 웹소켓 처리 로직 (K-line) (*** 구독 목록 확인 로직 추가 ***)
# ==============================================================================

def update_historical_data(symbol_ws, kline_data):
    global historical_data
    try:
        with data_lock: # Lock 추가
            if symbol_ws not in historical_data:
                # op_logger.debug(f"[{symbol_ws}] Received kline but no historical data record. Possibly unsubscribed or initial fetch failed.")
                return False # 데이터 없으면 업데이트 불가

            df = historical_data[symbol_ws]
            kline_start_time = pd.to_datetime(kline_data['t'], unit='ms', utc=True)

            # 새 캔들 데이터 생성
            new_data = pd.DataFrame([{
                'timestamp': kline_start_time,
                'open': float(kline_data['o']),
                'high': float(kline_data['h']),
                'low': float(kline_data['l']),
                'close': float(kline_data['c']),
                'volume': float(kline_data['v'])
            }]).set_index('timestamp')

            # 기존 캔들 업데이트 또는 새 캔들 추가
            if kline_start_time in df.index:
                df.loc[kline_start_time] = new_data.iloc[0]
            else:
                df = pd.concat([df, new_data])
                # 데이터 길이 제한
                if len(df) > MAX_CANDLE_HISTORY:
                    df = df.iloc[-MAX_CANDLE_HISTORY:]

            historical_data[symbol_ws] = df # 업데이트된 DataFrame 저장
            return True
    except Exception as e:
        op_logger.error(f"[{symbol_ws}] Error updating historical data: {e}", exc_info=True)
        return False

# TP 업데이트 로직 (변경 없음)
def try_update_tp(open_symbol, open_symbol_ccxt, side, amount, tp_order_id, tp_client_order_id, current_tp_price, new_tp_target):
    if not current_tp_price or not new_tp_target or new_tp_target <= 0 or current_tp_price <= 0: return False

    price_diff_percent = abs(new_tp_target - current_tp_price) / current_tp_price * 100

    if price_diff_percent >= TP_UPDATE_THRESHOLD_PERCENT:
        op_logger.info(f"[{open_symbol}] TP target moved significantly: {current_tp_price:.4f} -> {new_tp_target:.4f}. Updating TP order...")

        # 1. 기존 TP 주문 취소
        # order_id 또는 client_order_id 둘 중 하나로 시도
        if not cancel_order(open_symbol_ccxt, order_id=tp_order_id, client_order_id=tp_client_order_id):
             op_logger.error(f"[{open_symbol}] Failed to cancel previous TP order ({tp_order_id}/{tp_client_order_id}). Update aborted.")
             return False # 취소 실패 시 업데이트 중단

        time.sleep(0.2) # 취소 처리 시간 확보

        # 2. 새 TP 주문 생성
        tp_side = 'sell' if side == 'long' else 'buy'
        new_tp_order_info = None
        try:
            # 수량/가격 정밀도 다시 확인
            adjusted_amount_str = binance_rest.amount_to_precision(open_symbol_ccxt, amount)
            new_tp_target_str = binance_rest.price_to_precision(open_symbol_ccxt, new_tp_target)
            new_tp_actual_price = float(new_tp_target_str)

            new_tp_order_info = place_take_profit_market_order(open_symbol_ccxt, tp_side, new_tp_actual_price, float(adjusted_amount_str))

        except Exception as tp_e: op_logger.error(f"[{open_symbol}] Error placing new TP order: {tp_e}")

        # 3. 새 TP 주문 정보로 로컬 상태 업데이트
        if new_tp_order_info and new_tp_order_info.get('id'):
            new_tp_id = new_tp_order_info['id']
            new_tp_coid = new_tp_order_info.get('clientOrderId')
            with real_positions_lock: # Lock 잡고 업데이트
                if open_symbol in real_positions:
                    real_positions[open_symbol]['tp_order_id'] = new_tp_id
                    real_positions[open_symbol]['tp_client_order_id'] = new_tp_coid # clientOrderId도 저장
                    real_positions[open_symbol]['current_tp_price'] = new_tp_actual_price
                    op_logger.info(f"[{open_symbol}] TP order successfully updated to {new_tp_actual_price:.4f} (ID: {new_tp_id}, COID: {new_tp_coid}).")
                    return True # 업데이트 성공
                else:
                    # 포지션이 그 사이에 사라진 경우 (거의 없지만 방어 코드)
                    op_logger.warning(f"[{open_symbol}] Position disappeared while updating TP. Attempting to cancel the newly placed TP {new_tp_id}.")
                    cancel_order(open_symbol_ccxt, order_id=new_tp_id, client_order_id=new_tp_coid)
                    return False
        else:
             op_logger.error(f"[{open_symbol}] Failed to place new TP order or get order ID.")
             # 롤백 시도? (이미 이전 주문 취소됨) - 일단 실패로 처리
             return False

    # TP 업데이트 필요 없음 (변동폭 작음)
    return False

# K-line 메시지 처리 함수
def process_kline_message(symbol_ws, kline_data):
    global total_trades, winning_trades, real_positions, entry_in_progress

    # 0. 구독 중인 심볼인지 확인 (주기적 업데이트 반영)
    with subscribed_symbols_lock:
        if symbol_ws not in subscribed_symbols:
            # op_logger.debug(f"Ignoring kline for unsubscribed symbol: {symbol_ws}")
            return

    # 1. 히스토리 데이터 업데이트
    if not update_historical_data(symbol_ws, kline_data): return

    is_candle_closed = kline_data.get('x', False) # 캔들 마감 여부

    # 2. 지표 계산
    with data_lock: current_df = historical_data.get(symbol_ws)
    if current_df is None: return # 데이터 없으면 종료
    indicator_df = calculate_indicators(current_df.copy()) # 복사본 전달
    if indicator_df is None or indicator_df.empty: return

    # 3. 지표 값 추출
    try:
        last_candle = indicator_df.iloc[-1]
        current_price = last_candle['close']
        stoch_k = last_candle.get('STOCHk', np.nan); stoch_d = last_candle.get('STOCHd', np.nan)
        bbl = last_candle.get('BBL', np.nan); bbu = last_candle.get('BBU', np.nan)
        # 필수 지표 값이 NaN이면 진행 불가
        if pd.isna(stoch_k) or pd.isna(stoch_d) or pd.isna(bbl) or pd.isna(bbu) or pd.isna(current_price):
             # op_logger.debug(f"[{symbol_ws}] Indicator values contain NaN at latest candle.")
             return
    except IndexError: op_logger.warning(f"[{symbol_ws}] IndexError accessing indicators (dataframe too short?)."); return
    except Exception as e: op_logger.error(f"[{symbol_ws}] Error accessing indicator values: {e}"); return

    now_utc = datetime.now(UTC)
    symbol_ccxt = symbol_ws.replace('USDT', '/USDT') # CCXT용 심볼 형식

    # 4. 현재 보유 포지션 확인 (읽기 전용 복사)
    positions_to_check = {}
    with real_positions_lock: positions_to_check = real_positions.copy()

    # --- 4.1. 열린 포지션 관리 (TP 업데이트만 수행) ---
    if symbol_ws in positions_to_check: # 현재 kline 메시지의 심볼이 보유 중인 경우만 TP 업데이트 시도
        pos_info = positions_to_check[symbol_ws]
        side, entry_time, amount = pos_info['side'], pos_info['entry_time'], pos_info['amount']
        tp_order_id = pos_info.get('tp_order_id')
        tp_client_order_id = pos_info.get('tp_client_order_id') # Client Order ID 사용
        current_tp_price = pos_info.get('current_tp_price')

        # 포지션 진입 후 일정 시간(딜레이) 경과 후 TP 업데이트 시작
        time_since_entry = now_utc - entry_time
        if time_since_entry >= timedelta(minutes=POSITION_MONITORING_DELAY_MINUTES):
            # TP 주문이 있고, 현재 TP 가격 정보가 있을 때만 업데이트 시도
            if tp_order_id and current_tp_price:
                 # 새 TP 목표 가격 (볼린저 밴드 상단 또는 하단)
                 new_tp_target = bbu if side == 'long' else bbl
                 if not pd.isna(new_tp_target) and new_tp_target > 0:
                      # try_update_tp 함수 호출 (실패해도 여기서 추가 처리 없음)
                      try_update_tp(symbol_ws, symbol_ccxt, side, amount, tp_order_id, tp_client_order_id, current_tp_price, new_tp_target)
                 else:
                      op_logger.warning(f"[{symbol_ws}] Cannot update TP, new target BB value is invalid: {new_tp_target}")

    # --- 4.2. 신규 진입 조건 확인 (캔들 마감 시에만) ---
    if is_candle_closed:
        # 진입 시도 중이 아니고, 현재 해당 심볼 포지션이 없고(UDS 업데이트 반영), 블랙리스트가 아닐 때
        with entry_lock: is_entry_attempted = entry_in_progress.get(symbol_ws, False)
        with real_positions_lock: position_exists = symbol_ws in real_positions # 실시간 상태 확인

        if not is_entry_attempted and not position_exists and not check_symbol_in_blacklist(symbol_ws):
            # 최대 포지션 수 확인
            with real_positions_lock: current_open_count = len(real_positions)
            if current_open_count >= MAX_OPEN_POSITIONS:
                # op_logger.debug(f"Max positions ({MAX_OPEN_POSITIONS}) reached. Cannot enter {symbol_ws}.")
                return # 최대치 도달 시 진입 불가

            # 스토캐스틱 RSI 진입 조건 확인
            long_stoch_cond = stoch_k <= STOCH_RSI_LONG_ENTRY_THRESH and stoch_d <= STOCH_RSI_LONG_ENTRY_THRESH and stoch_k > stoch_d
            short_stoch_cond = stoch_k >= STOCH_RSI_SHORT_ENTRY_THRESH and stoch_d >= STOCH_RSI_SHORT_ENTRY_THRESH and stoch_k < stoch_d

            target_side = None
            take_profit_price_target = None
            entry_trigger_price = current_price # 마감 가격 기준으로 진입 시도

            if long_stoch_cond:
                target_side = 'buy'
                take_profit_price_target = bbu if not pd.isna(bbu) and bbu > 0 else None
                # op_logger.debug(f"[{symbol_ws}] Long condition met. StochK:{stoch_k:.2f}, StochD:{stoch_d:.2f}")
            elif short_stoch_cond:
                target_side = 'sell'
                take_profit_price_target = bbl if not pd.isna(bbl) and bbl > 0 else None
                # op_logger.debug(f"[{symbol_ws}] Short condition met. StochK:{stoch_k:.2f}, StochD:{stoch_d:.2f}")

            # 진입 조건 만족 + TP 타겟 유효할 때 진입 프로세스 시작
            if target_side and take_profit_price_target is not None:
                with entry_lock: entry_in_progress[symbol_ws] = True # 진입 시도 플래그 설정
                try:
                    op_logger.info(f"[{symbol_ws}] Entry condition MET for {target_side.upper()} at candle close. Price: {entry_trigger_price:.4f}, TP Target: {take_profit_price_target:.4f}. Starting entry process...")

                    # 1. 잔고 확인 및 주문량 계산
                    available_balance = get_current_balance()
                    if available_balance <= 10.0: # 최소 잔고 (버퍼 포함)
                        raise Exception(f"Insufficient available balance ({available_balance:.2f})")

                    # 동적 마진 할당 (현재 열린 포지션 수 기준)
                    with real_positions_lock: current_open_count_before_entry = len(real_positions) # 진입 직전 개수 다시 확인
                    if current_open_count_before_entry >= MAX_OPEN_POSITIONS:
                        raise Exception(f"Max positions ({MAX_OPEN_POSITIONS}) reached just before entry.")

                    # 남은 슬롯에 따라 사용 가능한 잔액 비율 결정
                    open_slots = MAX_OPEN_POSITIONS - current_open_count_before_entry
                    if open_slots <= 0: raise Exception("No open slots available.") # 이론상 여기 오면 안됨
                    # 간단하게 남은 슬롯으로 1/N 분할 (또는 다른 전략 적용 가능)
                    portion_to_use = 1.0 / open_slots
                    target_margin = available_balance * portion_to_use

                    # 최소 마진 금액 (예: 10 USDT)
                    if target_margin < 10.0:
                         raise Exception(f"Calculated target margin {target_margin:.2f} is too low.")

                    target_notional_value = target_margin * LEVERAGE
                    # 최소 주문 금액 (Notional) 확인
                    # min_notional = binance_rest.market(symbol_ccxt).get('limits', {}).get('cost', {}).get('min', 5.0)
                    min_notional = 5.0 # 하드코딩 (API 조회 실패 대비)
                    if target_notional_value < min_notional:
                         raise Exception(f"Target Notional Value {target_notional_value:.2f} is below minimum {min_notional}.")

                    order_amount = target_notional_value / entry_trigger_price if entry_trigger_price > 0 else 0
                    if order_amount <= 0: raise Exception("Calculated order amount is zero or negative.")

                    # 2. 최소 수익성 검증 (간단 버전)
                    temp_sl_price = entry_trigger_price * LONG_STOP_LOSS_FACTOR if target_side == 'buy' else entry_trigger_price * SHORT_STOP_LOSS_FACTOR
                    potential_profit_points = abs(take_profit_price_target - entry_trigger_price)
                    potential_loss_points = abs(entry_trigger_price - temp_sl_price)
                    # 예상 R:R (Risk-Reward Ratio) - 예: 1.0 이상 요구
                    # if potential_loss_points > 0 and potential_profit_points / potential_loss_points < 1.0:
                    #      raise Exception(f"Profitability check failed: Estimated R:R < 1.0 (P:{potential_profit_points:.4f}, L:{potential_loss_points:.4f})")

                    # 3. 격리 마진 및 레버리지 설정
                    if not set_isolated_margin(symbol_ccxt, LEVERAGE):
                         raise Exception("Failed to set isolated margin or leverage.")

                    # 4. 시장가 진입 주문
                    entry_order_result = place_market_order_real(symbol_ccxt, target_side, order_amount, entry_trigger_price)
                    if not entry_order_result or not entry_order_result.get('id'): # 주문 실패 또는 ID 못받음
                         raise Exception("Entry market order placement failed or no order ID received.")

                    entry_order_id = entry_order_result['id']
                    entry_client_order_id = entry_order_result.get('clientOrderId')
                    op_logger.info(f"[{symbol_ws}] Entry market order placed (ID: {entry_order_id}, COID: {entry_client_order_id}). Waiting for fill confirmation via User Stream...")

                    # --- 중요: 실제 포지션 정보 저장은 User Data Stream의 Fill 이벤트에서 수행 ---
                    # 여기서는 진입 주문이 성공적으로 제출되었음만 기록하고, SL/TP 주문 설정으로 넘어감.
                    # 단, SL/TP 설정에 필요한 임시 정보는 저장할 수 있음.

                    # 5. SL / TP 주문 설정 시도
                    sl_order_info, tp_order_info = None, None
                    final_sl_price, final_tp_price = None, None # 실제 설정될 가격
                    try:
                        # User Stream에서 체결 정보 받아 실제 진입가 확정 후 SL/TP 계산이 더 정확하나,
                        # 여기서는 우선 entry_trigger_price 기준으로 계산하고 주문.
                        # 또는, 주문 후 잠시 대기하며 User Stream 체결 이벤트를 기다리는 방법도 있음 (복잡도 증가)

                        # 여기서는 트리거 가격 기준으로 SL/TP 설정 시도
                        final_sl_price = entry_trigger_price * LONG_STOP_LOSS_FACTOR if target_side == 'buy' else entry_trigger_price * SHORT_STOP_LOSS_FACTOR
                        final_tp_price = take_profit_price_target # 초기 TP 타겟 사용

                        # 주문 수량은 실제 체결량(User Stream에서 받아옴)을 사용하는 것이 이상적.
                        # 여기서는 요청 수량(order_amount)으로 우선 설정 시도.
                        # User Stream에서 부분 체결 시 SL/TP 수량 조정 필요할 수 있음 (추가 구현 사항)
                        sl_side = 'sell' if target_side == 'buy' else 'buy'
                        tp_side = sl_side

                        # SL 주문
                        sl_order_info = place_stop_market_order(symbol_ccxt, sl_side, final_sl_price, order_amount)
                        if not sl_order_info or not sl_order_info.get('id'):
                             raise Exception("Stop Loss order placement failed.")
                        op_logger.info(f"[{symbol_ws}] SL order placed (ID: {sl_order_info['id']}, COID: {sl_order_info.get('clientOrderId')})")
                        time.sleep(0.1) # 잠시 틈

                        # TP 주문
                        tp_order_info = place_take_profit_market_order(symbol_ccxt, tp_side, final_tp_price, order_amount)
                        if not tp_order_info or not tp_order_info.get('id'):
                             raise Exception("Take Profit order placement failed.")
                        op_logger.info(f"[{symbol_ws}] TP order placed (ID: {tp_order_info['id']}, COID: {tp_order_info.get('clientOrderId')})")

                        # --- 임시 포지션 정보 저장 (User Stream 처리 전까지) ---
                        # 실제 fill 정보(가격, 수량, 시간)는 User Stream에서 업데이트해야 함!
                        # SL/TP 주문 ID와 가격은 여기서 설정된 값을 우선 저장.
                        with real_positions_lock:
                             # 최종 포지션 수 확인
                             current_open_count_final = len(real_positions)
                             if current_open_count_final < MAX_OPEN_POSITIONS:
                                 # 임시 저장 (나중에 UDS Fill 이벤트에서 덮어써야 함)
                                 real_positions[symbol_ws] = {
                                     'side': 'long' if target_side == 'buy' else 'short',
                                     'entry_price': entry_trigger_price, # 임시 값
                                     'amount': order_amount,          # 임시 값
                                     'entry_time': now_utc,           # 임시 값
                                     'entry_order_id': entry_order_id,
                                     'entry_client_order_id': entry_client_order_id,
                                     'sl_order_id': sl_order_info['id'],
                                     'sl_client_order_id': sl_order_info.get('clientOrderId'),
                                     'tp_order_id': tp_order_info['id'],
                                     'tp_client_order_id': tp_order_info.get('clientOrderId'),
                                     'current_tp_price': final_tp_price # 초기 TP 가격 저장
                                 }
                                 op_logger.info(f"[{symbol_ws}] Entry process initiated. Placed Entry/SL/TP orders. Awaiting User Stream confirmation. Active Pos (inc. this): {current_open_count_final + 1}")
                             else:
                                 # 이론상 여기 오면 안되지만, 동시성 문제 발생 시 대비
                                 op_logger.error(f"[{symbol_ws}] Max positions reached unexpectedly during SL/TP placement! Rolling back entry...")
                                 raise Exception("Max positions reached during SL/TP placement")

                    except Exception as sltp_err:
                        # SL 또는 TP 주문 실패 시 롤백 시도
                        op_logger.error(f"[{symbol_ws}] Error placing SL/TP after entry order: {sltp_err}. ROLLING BACK ENTRY!")
                        # 1. 생성된 SL/TP 주문 취소 시도
                        if sl_order_info and sl_order_info.get('id'): cancel_order(symbol_ccxt, order_id=sl_order_info['id'], client_order_id=sl_order_info.get('clientOrderId'))
                        if tp_order_info and tp_order_info.get('id'): cancel_order(symbol_ccxt, order_id=tp_order_info['id'], client_order_id=tp_order_info.get('clientOrderId'))
                        # 2. 진입 주문 취소 시도 (이미 체결되었을 가능성 높음)
                        cancel_order(symbol_ccxt, order_id=entry_order_id, client_order_id=entry_client_order_id)
                        # 3. 만약 진입 주문이 체결되었다면, 반대 포지션으로 청산 시도 (최후의 수단)
                        #    (User Stream에서 실제 체결 여부 확인 후 청산하는 것이 더 안전함)
                        #    여기서는 일단 로그만 남기고 User Stream 처리 또는 REST 동기화에 맡김.
                        op_logger.warning(f"[{symbol_ws}] Rollback initiated. Manual check might be required if entry order ({entry_order_id}) was filled.")
                        # 롤백 시 임시 저장된 포지션 정보 제거
                        with real_positions_lock: real_positions.pop(symbol_ws, None)
                        # 블랙리스트 추가 고려
                        add_to_blacklist(symbol_ws, reason=f"Entry failed during SL/TP placement: {sltp_err}")
                        # raise sltp_err # 에러 다시 발생시켜 진입 시도 중단 명확화

                except Exception as entry_err:
                    op_logger.error(f"[{symbol_ws}] Entry process failed: {entry_err}", exc_info=True)
                    # 실패 시 임시 저장된 포지션 정보 제거
                    with real_positions_lock: real_positions.pop(symbol_ws, None)

                finally:
                    with entry_lock: entry_in_progress.pop(symbol_ws, None) # 진입 시도 플래그 해제

# ==============================================================================
# 웹소켓 콜백 함수 (K-line)
# ==============================================================================
def on_message_kline(wsapp, message):
    # op_logger.debug(f"K-line Raw Message: {message[:200]}") # 디버깅용 Raw 메시지
    try:
        data = json.loads(message)
        # 스트림 데이터 형식 확인 (e.g., {"stream":"btcusdt@kline_15m","data":{...}})
        if 'stream' in data and 'data' in data:
            stream_name = data['stream']
            payload = data['data']
            # K-line 이벤트인지 확인 ('e' 필드)
            if payload.get('e') == 'kline':
                symbol_lower = stream_name.split('@')[0]
                symbol_upper = symbol_lower.upper() # WS용 심볼 (e.g., BTCUSDT)
                # op_logger.debug(f"Processing kline for {symbol_upper}")
                process_kline_message(symbol_upper, payload['k']) # 'k' 객체가 kline 데이터
        # 구독 성공/실패 메시지 등 처리 (id 확인)
        elif 'result' in data and data.get('id'):
            op_logger.info(f"Subscription response: {data}")
        # 에러 메시지 처리
        elif 'e' in data and data['e'] == 'error':
            op_logger.error(f"K-line WS API Error: Code={data.get('c')} Msg={data.get('m')}")
        # PING 메시지 응답 처리 (Binance는 PONG 자동 전송 불필요)
        elif 'ping' in message:
             wsapp.send(json.dumps({'pong': message['ping']}))
             op_logger.debug("Sent PONG")

    except json.JSONDecodeError: op_logger.error(f"K-line WS JSON Decode Error: {message[:100]}")
    except Exception as e: op_logger.error(f"K-line WS Message Proc Error: {e}", exc_info=True)

def on_error_kline(wsapp, error):
    # ConnectionClosedException 등 특정 에러 처리 가능
    op_logger.error(f"K-line WebSocket Error: {error}")
    # 여기서 재연결 로직 추가 가능

def on_close_kline(wsapp, close_status_code, close_msg):
    global kline_websocket_running
    op_logger.info(f"K-line WebSocket closed. Code:{close_status_code}, Msg:{close_msg}")
    kline_websocket_running = False # 실행 상태 변경

def on_open_kline(wsapp):
    global subscribed_symbols, historical_data, kline_websocket_running
    kline_websocket_running = True
    op_logger.info("K-line WebSocket connection opened.")

    # --- 초기 심볼 목록 가져오기 및 구독 ---
    op_logger.info("Fetching initial top symbols for K-line subscription...")
    initial_top_symbols_ccxt = get_top_volume_symbols(TOP_N_SYMBOLS)
    if not initial_top_symbols_ccxt:
        op_logger.error("Could not fetch initial top symbols. Shutting down K-line WS.")
        wsapp.close()
        return

    initial_symbols_ws = {s.replace('/', '') for s in initial_top_symbols_ccxt}
    op_logger.info(f"Subscribing to initial {len(initial_symbols_ws)} K-line streams ({TIMEFRAME})...")

    streams_to_subscribe = [f"{s.lower()}@kline_{TIMEFRAME}" for s in initial_symbols_ws]
    if not streams_to_subscribe:
         op_logger.error("No symbols to subscribe to. Closing WS.")
         wsapp.close(); return

    subscribe_message = {"method": "SUBSCRIBE", "params": streams_to_subscribe, "id": 1}
    try:
        wsapp.send(json.dumps(subscribe_message))
        time.sleep(1) # 메시지 전송 후 잠시 대기
        with subscribed_symbols_lock:
            subscribed_symbols = initial_symbols_ws # 초기 구독 목록 설정
        op_logger.info(f"Initial K-line subscription message sent for {len(initial_symbols_ws)} symbols.")
    except Exception as e:
        op_logger.error(f"Failed to send initial K-line subscription: {e}")
        wsapp.close(); return

    # --- 초기 히스토리 데이터 로딩 ---
    op_logger.info("Fetching initial historical data for subscribed symbols...")
    if not binance_rest:
        op_logger.error("REST instance not available for initial data fetch. Closing WS.")
        wsapp.close(); return

    fetched_count = 0
    fetch_errors = 0
    with data_lock:
        historical_data.clear() # 기존 데이터 초기화
        for symbol_upper in initial_symbols_ws:
             # 구독 목록에 실제 추가되었는지 한번 더 확인 (이론상 필요 없지만 안전 장치)
             with subscribed_symbols_lock: needs_data = symbol_upper in subscribed_symbols

             if needs_data:
                 symbol_ccxt = symbol_upper.replace('USDT', '/USDT')
                 initial_df = fetch_initial_ohlcv(symbol_ccxt, TIMEFRAME, limit=max(INITIAL_CANDLE_FETCH_LIMIT, STOCH_RSI_PERIOD*2))
                 if initial_df is not None and not initial_df.empty:
                     historical_data[symbol_upper] = initial_df
                     fetched_count += 1
                 else:
                     op_logger.warning(f"No initial data fetched for {symbol_upper}.")
                     fetch_errors += 1
                 time.sleep(0.3) # Rate limit 방지

    op_logger.info(f"Initial data fetch complete ({fetched_count} symbols OK, {fetch_errors} errors).")
    print("-" * 80 + "\nK-line WebSocket connected. Listening for REAL TRADING signals (UDS Sync Mode)...\n" + "-" * 80)

# ==============================================================================
# 웹소켓 콜백 함수 (User Data Stream) (*** 신규 추가 ***)
# ==============================================================================

def on_message_user_stream(wsapp, message):
    # op_logger.debug(f"User Stream Raw: {message[:300]}") # 디버깅용
    global real_positions, total_trades, winning_trades, blacklist

    try:
        data = json.loads(message)
        event_type = data.get('e')

        # --- 주문 업데이트 이벤트 처리 ---
        if event_type == 'ORDER_TRADE_UPDATE':
            order_data = data.get('o')
            if not order_data: return

            symbol_ws = order_data['s'] # e.g., BTCUSDT
            symbol_ccxt = symbol_ws.replace('USDT','/USDT')
            order_status = order_data['X'] # NEW, CANCELED, EXPIRED, TRADE (Filled), PARTIALLY_FILLED, REJECTED
            order_id = str(order_data['i']) # 주문 ID (문자열로 변환)
            client_order_id = order_data.get('c') # Client Order ID
            order_side = order_data['S'] # BUY or SELL
            position_side = order_data.get('ps') # LONG or SHORT (헷지 모드용, 단방향에서는 side와 유사)
            trade_price = float(order_data['L']) # 마지막 체결 가격
            trade_qty = float(order_data['l']) # 마지막 체결 수량
            filled_qty = float(order_data['z']) # 누적 체결 수량
            total_qty = float(order_data['q']) # 주문 총 수량
            avg_price = float(order_data['ap']) if order_data.get('ap') and float(order_data['ap']) > 0 else trade_price # 평균 체결 가격
            commission = float(order_data['n']) if order_data.get('n') else 0.0 # 수수료
            commission_asset = order_data.get('N') # 수수료 자산
            trade_time_ms = order_data['T'] # 거래 시간 (ms)
            trade_time_dt = datetime.fromtimestamp(trade_time_ms / 1000, tz=UTC)
            is_reduce_only = order_data.get('R', False) # Reduce Only 주문 여부
            stop_price = float(order_data.get('sp', 0.0)) # TP/SL 주문의 발동 가격
            order_type = order_data['o'] # LIMIT, MARKET, STOP_MARKET, TAKE_PROFIT_MARKET 등

            op_logger.info(f"[UDS][ORDER] {symbol_ws} ID:{order_id} ClientID:{client_order_id} Status:{order_status} Side:{order_side} Type:{order_type} FillQty:{filled_qty}/{total_qty} AvgPx:{avg_price:.4f} LastPx:{trade_price:.4f}")

            # --- 체결 이벤트 처리 (Filled / Partially Filled) ---
            if order_status in ['TRADE', 'PARTIALLY_FILLED'] and trade_qty > 0:
                trade_logger.info(f"[UDS][FILL] {symbol_ws} {order_side} {trade_qty} @ {trade_price:.4f} (Avg:{avg_price:.4f}) OrdID:{order_id} Status:{order_status} Comm:{commission}{commission_asset}")

                with real_positions_lock, stats_lock: # 포지션과 통계 동시 접근
                    pos_info = real_positions.get(symbol_ws)

                    # --- 진입 주문 체결 처리 ---
                    # 포지션이 없거나, 해당 주문 ID가 진입 주문 ID와 일치할 때
                    if not pos_info or (pos_info and str(pos_info.get('entry_order_id')) == order_id):
                        if not pos_info: # 첫 진입 체결
                            side = 'long' if order_side == 'BUY' else 'short'
                            real_positions[symbol_ws] = {
                                'side': side,
                                'entry_price': avg_price, # 평균가로 업데이트
                                'amount': filled_qty, # 누적 체결량
                                'entry_time': trade_time_dt, # 첫 체결 시간 기준? 마지막 체결 시간 기준? -> 마지막 체결 시간으로
                                'entry_order_id': order_id,
                                'entry_client_order_id': client_order_id,
                                # SL/TP 정보는 임시 저장된 것 유지 (아래에서 재확인 가능)
                                'sl_order_id': real_positions.get(symbol_ws,{}).get('sl_order_id'),
                                'sl_client_order_id': real_positions.get(symbol_ws,{}).get('sl_client_order_id'),
                                'tp_order_id': real_positions.get(symbol_ws,{}).get('tp_order_id'),
                                'tp_client_order_id': real_positions.get(symbol_ws,{}).get('tp_client_order_id'),
                                'current_tp_price': real_positions.get(symbol_ws,{}).get('current_tp_price')
                            }
                            op_logger.info(f"[{symbol_ws}] Position opened via UDS Fill. Side:{side}, Qty:{filled_qty}, AvgPx:{avg_price:.4f}")
                        else: # 추가 진입 또는 부분 체결 업데이트
                            # 평균 단가 및 수량 업데이트 (가중 평균)
                            current_amount = pos_info.get('amount', 0)
                            current_entry_price = pos_info.get('entry_price', 0)
                            new_total_amount = filled_qty # 누적 체결량이 총량
                            # 새 평균 단가 계산 (주의: 바이낸스 API의 avg_price('ap')가 이미 누적 평균일 수 있음)
                            # API 'ap' 필드 신뢰
                            if avg_price > 0:
                                 pos_info['entry_price'] = avg_price
                            else: # 'ap'가 0이면 직접 계산 (오차 가능성)
                                 if new_total_amount > 0:
                                      pos_info['entry_price'] = ((current_entry_price * current_amount) + (trade_price * trade_qty)) / new_total_amount
                            pos_info['amount'] = new_total_amount
                            pos_info['entry_time'] = trade_time_dt # 마지막 체결 시간으로 업데이트
                            op_logger.info(f"[{symbol_ws}] Position updated via UDS Fill. New Qty:{new_total_amount}, New AvgPx:{pos_info['entry_price']:.4f}")

                        # 진입 주문 완전 체결 시 로그
                        if order_status == 'TRADE':
                            op_logger.info(f"[{symbol_ws}] Entry order {order_id} fully filled.")
                            # 필요시 SL/TP 주문 수량 업데이트 (부분 체결 대응) - 복잡성 증가로 일단 생략

                    # --- 종료 주문 (SL/TP) 체결 처리 ---
                    elif pos_info and is_reduce_only: # 포지션이 있고 ReduceOnly 주문일 때
                         closed_amount = trade_qty
                         remaining_amount = pos_info['amount'] - closed_amount
                         pnl = 0 # 실현 손익 계산

                         if pos_info['side'] == 'long': # 롱 포지션 종료 (매도 체결)
                             pnl = (trade_price - pos_info['entry_price']) * closed_amount
                         else: # 숏 포지션 종료 (매수 체결)
                              pnl = (pos_info['entry_price'] - trade_price) * closed_amount

                         pnl -= commission # 수수료 차감

                         trade_logger.info(f"[UDS][CLOSE] {symbol_ws} Closed {closed_amount} (Rem:{remaining_amount:.8f}) via {order_type} OrdID:{order_id}. PnL: {pnl:.4f} {TARGET_ASSET}")
                         total_trades += 1
                         if pnl > 0: winning_trades += 1

                         # 포지션 완전 종료 시
                         if order_status == 'TRADE' or abs(remaining_amount) < 1e-9: # 완전 체결 또는 남은 수량 매우 작을 때
                             op_logger.info(f"[{symbol_ws}] Position fully closed via UDS Fill (Order ID: {order_id}). Removing from local state.")
                             # --- 블랙리스트 로직 ---
                             # SL 주문이 체결된 경우 블랙리스트 추가
                             if str(pos_info.get('sl_order_id')) == order_id:
                                  add_to_blacklist(symbol_ws, reason=f"SL Filled ({order_id}) via User Stream")

                             # 남은 반대 주문 (TP 또는 SL) 취소 시도
                             opposite_order_id, opposite_coid = (None, None)
                             if str(pos_info.get('sl_order_id')) == order_id: # SL이 체결됨 -> TP 취소
                                 opposite_order_id = pos_info.get('tp_order_id')
                                 opposite_coid = pos_info.get('tp_client_order_id')
                             elif str(pos_info.get('tp_order_id')) == order_id: # TP가 체결됨 -> SL 취소
                                 opposite_order_id = pos_info.get('sl_order_id')
                                 opposite_coid = pos_info.get('sl_client_order_id')

                             if opposite_order_id or opposite_coid:
                                 op_logger.info(f"[{symbol_ws}] Position closed. Cancelling remaining order ({opposite_order_id}/{opposite_coid})...")
                                 cancel_order(symbol_ccxt, order_id=opposite_order_id, client_order_id=opposite_coid)

                             # 로컬 상태에서 포지션 제거
                             del real_positions[symbol_ws]

                         else: # 부분 종료 시
                             op_logger.info(f"[{symbol_ws}] Position partially closed via UDS Fill. Remaining: {remaining_amount}")
                             pos_info['amount'] = remaining_amount
                             # 부분 종료 시 SL/TP 수량도 조정해야 하지만, 복잡하므로 일단 유지
                             # (전체 종료 주문이므로 남은 수량만큼 다시 SL/TP 걸어야 할 수도 있음)


            # --- 주문 취소 / 거절 등 기타 상태 처리 ---
            elif order_status in ['CANCELED', 'REJECTED', 'EXPIRED']:
                 op_logger.info(f"[UDS][ORDER_FINAL] {symbol_ws} Order {order_id} finalized with status: {order_status}")
                 # 로컬 포지션 정보에서 해당 주문 ID 제거 (만약 SL/TP 였다면)
                 with real_positions_lock:
                      pos_info = real_positions.get(symbol_ws)
                      if pos_info:
                          if str(pos_info.get('sl_order_id')) == order_id:
                              pos_info['sl_order_id'] = None
                              pos_info['sl_client_order_id'] = None
                              op_logger.warning(f"[{symbol_ws}] SL order {order_id} {order_status}. SL removed from local state.")
                              # SL 취소 시 어떻게 할지? (예: 즉시 시장가 종료 또는 다른 SL 재설정) - 현재는 제거만
                          elif str(pos_info.get('tp_order_id')) == order_id:
                              pos_info['tp_order_id'] = None
                              pos_info['tp_client_order_id'] = None
                              pos_info['current_tp_price'] = None # TP 가격 정보도 제거
                              op_logger.warning(f"[{symbol_ws}] TP order {order_id} {order_status}. TP removed from local state.")
                              # TP 취소 시 어떻게 할지? (예: 다른 TP 재설정) - 현재는 제거만

        # --- 계정/포지션 업데이트 이벤트 처리 ---
        elif event_type == 'ACCOUNT_UPDATE':
            update_data = data.get('a', {}) # 업데이트 상세 정보
            reason = update_data.get('m') # 이벤트 발생 사유 (e.g., ORDER, FUNDING_FEE)
            op_logger.debug(f"[UDS][ACCOUNT] Reason: {reason}")

            # 포지션 정보 업데이트 처리
            positions_update = update_data.get('P', [])
            if positions_update:
                with real_positions_lock:
                    for pos_data in positions_update:
                        symbol_ws = pos_data['s']
                        amount = float(pos_data['pa']) # 포지션 수량
                        entry_price = float(pos_data['ep']) # 진입 가격
                        unrealized_pnl = float(pos_data['up']) # 미실현 손익
                        margin_type = pos_data['mt'] # isolated or cross
                        position_side = pos_data.get('ps') # LONG/SHORT/BOTH (헷지모드)

                        op_logger.debug(f"[UDS][POS_UPDATE] {symbol_ws} Amt:{amount} Entry:{entry_price} uPnL:{unrealized_pnl} Side:{position_side}")

                        pos_info = real_positions.get(symbol_ws)
                        # 로컬에 포지션 정보가 있는데, 업데이트된 수량이 0이면 외부 종료 간주
                        if pos_info and abs(amount) < 1e-9:
                            op_logger.warning(f"[UDS][ACCOUNT] Position {symbol_ws} closed based on ACCOUNT_UPDATE (Amount is zero). Reason: {reason}. Removing from local state.")
                            # 관련 주문 취소 및 로컬 상태 제거 (FILL 이벤트 놓쳤을 경우 대비)
                            symbol_ccxt = symbol_ws.replace('USDT','/USDT')
                            cancel_order(symbol_ccxt, order_id=pos_info.get('sl_order_id'), client_order_id=pos_info.get('sl_client_order_id'))
                            time.sleep(0.1)
                            cancel_order(symbol_ccxt, order_id=pos_info.get('tp_order_id'), client_order_id=pos_info.get('tp_client_order_id'))
                            del real_positions[symbol_ws]
                            # 외부 종료 시 블랙리스트 추가 고려
                            if reason != 'ORDER': # ORDER 사유가 아닌데 닫혔다면
                                add_to_blacklist(symbol_ws, reason=f"Position closed externally? (ACCOUNT_UPDATE, Reason:{reason})")

                        # 로컬에 정보가 있고, 수량이 0이 아니면 정보 업데이트 (펀딩피 등으로 미세 조정될 수 있음)
                        elif pos_info and abs(amount) > 1e-9:
                             if abs(pos_info['amount'] - amount) > 1e-6: # 수량 차이 크면 로그
                                 op_logger.warning(f"[{symbol_ws}] Amount mismatch in ACCOUNT_UPDATE. Local:{pos_info['amount']}, Stream:{amount}. Updating local.")
                                 pos_info['amount'] = amount
                             if abs(pos_info['entry_price'] - entry_price) > 1e-6: # 진입가 차이 크면 로그
                                  op_logger.warning(f"[{symbol_ws}] Entry price mismatch in ACCOUNT_UPDATE. Local:{pos_info['entry_price']:.4f}, Stream:{entry_price:.4f}. Updating local.")
                                  pos_info['entry_price'] = entry_price

            # 잔고 정보 업데이트 처리 (필요시)
            balances_update = update_data.get('B', [])
            if balances_update:
                 for bal_data in balances_update:
                     asset = bal_data['a']
                     wallet_balance = float(bal_data['wb']) # 지갑 잔고
                     cross_unrealized_pnl = float(bal_data['cw']) # 교차 미실현 손익
                     if asset == TARGET_ASSET:
                          op_logger.debug(f"[UDS][BALANCE] {asset} Wallet Balance: {wallet_balance}, Cross uPnL: {cross_unrealized_pnl}")
                          # 필요시 get_current_balance 대신 이 정보 활용

        # --- Listen Key 만료 이벤트 ---
        elif event_type == 'listenKeyExpired':
             op_logger.warning("[UDS] Listen Key EXPIRED! Need to refresh and reconnect.")
             # TODO: 여기서 Listen Key 갱신 및 재연결 로직 호출 필요
             # 예: schedule_uds_reconnect()

    except json.JSONDecodeError: op_logger.error(f"User Stream WS JSON Decode Error: {message[:100]}")
    except Exception as e: op_logger.error(f"User Stream WS Message Proc Error: {e}", exc_info=True)


def on_error_user_stream(wsapp, error):
    op_logger.error(f"User Stream WebSocket Error: {error}")
    global user_websocket_running
    if isinstance(error, websocket.WebSocketConnectionClosedException):
         op_logger.warning("User Stream WebSocket connection closed unexpectedly.")
         user_websocket_running = False # 스트림 종료 플래그
         # 재연결 로직 호출 고려
    elif "timed out" in str(error):
         op_logger.warning("User Stream connection timed out.")
         # PING/PONG 문제일 수 있음
    # 기타 에러 처리

def on_close_user_stream(wsapp, close_status_code, close_msg):
    global user_websocket_running
    op_logger.info(f"User Stream WebSocket closed. Code:{close_status_code}, Msg:{close_msg}")
    user_websocket_running = False # 스트림 종료 플래그

def on_open_user_stream(wsapp):
    global user_websocket_running
    user_websocket_running = True
    op_logger.info("User Data Stream WebSocket connection opened.")
    # 연결 성공 시 별도 작업 필요 없음

# ==============================================================================
# User Data Stream 관리 함수 (*** 신규 추가 ***)
# ==============================================================================

def get_listen_key():
    global listen_key, binance_rest
    if not binance_rest: op_logger.error("CCXT REST not initialized to get listen key."); return None
    with listen_key_lock: # Lock 추가
        op_logger.info("Requesting new User Data Stream listen key...")
        try:
            response = binance_rest.fapiPrivatePostListenKey() # 선물 API용 listen key 요청
            listen_key = response.get('listenKey')
            if listen_key:
                op_logger.info(f"Obtained new listen key: {listen_key[:5]}...")
                return listen_key
            else:
                op_logger.error(f"Failed to obtain listen key from response: {response}")
                return None
        except AuthenticationError: op_logger.error("Authentication failed requesting listen key. Check API keys."); return None
        except (RequestTimeout, ExchangeNotAvailable) as e: op_logger.warning(f"Network/Exchange issue requesting listen key: {e}"); return None
        except Exception as e: op_logger.error(f"Error requesting listen key: {e}", exc_info=True); return None

def keep_listen_key_alive():
    global listen_key, binance_rest, user_websocket_running
    op_logger.info("Listen Key Keep-Alive thread started.")
    while user_websocket_running: # User Stream 실행 중에만 동작
        try:
            time.sleep(LISTEN_KEY_REFRESH_INTERVAL_MINUTES * 60) # 설정된 주기마다 실행
            if not user_websocket_running: break # 종료 체크

            with listen_key_lock: # Lock 추가
                if not listen_key:
                    op_logger.warning("No active listen key to keep alive. Skipping ping.")
                    continue
                if not binance_rest:
                     op_logger.error("CCXT REST not available to keep listen key alive.")
                     continue

                op_logger.info(f"Pinging listen key: {listen_key[:5]}...")
                try:
                    binance_rest.fapiPrivatePutListenKey({'listenKey': listen_key}) # 선물 API용 ping
                    op_logger.info("Listen key ping successful.")
                except AuthenticationError: op_logger.error("Authentication failed pinging listen key."); user_websocket_running = False; break # 인증 실패 시 종료
                except ccxt.ExchangeError as e:
                     # 키가 만료되거나 유효하지 않은 경우 에러 발생 가능
                     op_logger.error(f"Exchange error pinging listen key: {e}. Key might be invalid.")
                     # 여기서 새 키 요청 및 재연결 시도 가능
                     listen_key = None # 기존 키 무효화
                     # schedule_uds_reconnect() # 재연결 요청
                     break # 일단 루프 종료
                except (RequestTimeout, ExchangeNotAvailable) as e: op_logger.warning(f"Network/Exchange issue pinging listen key: {e}")
                except Exception as e: op_logger.error(f"Error pinging listen key: {e}", exc_info=True)

        except Exception as e:
            op_logger.error(f"Error in listen key keep-alive loop: {e}", exc_info=True)
            time.sleep(60) # 루프 에러 시 잠시 대기

def start_user_stream():
    global listen_key, user_wsapp, user_websocket_running

    # 1. Listen Key 가져오기
    if not get_listen_key():
        op_logger.error("Failed to get initial listen key. User Data Stream cannot start.")
        user_websocket_running = False
        return False

    # 2. Listen Key 갱신 스레드 시작
    keep_alive_thread = Thread(target=keep_listen_key_alive, daemon=True)
    keep_alive_thread.start()

    # 3. User Data Stream 웹소켓 연결 시작
    ws_url = f"wss://fstream.binance.com/ws/{listen_key}"
    op_logger.info(f"Connecting to User Data Stream: {ws_url[:35]}...")

    user_wsapp = websocket.WebSocketApp(ws_url,
                                      on_open=on_open_user_stream,
                                      on_message=on_message_user_stream,
                                      on_error=on_error_user_stream,
                                      on_close=on_close_user_stream)

    # 별도 스레드에서 User Stream 실행
    user_stream_thread = Thread(target=lambda: user_wsapp.run_forever(), daemon=True)
    user_stream_thread.start()
    op_logger.info("User Data Stream thread started.")
    return True


# ==============================================================================
# 메인 실행 로직 (*** 스레드 시작 순서, 종료 로직 수정 ***)
# ==============================================================================
if __name__ == "__main__":
    start_time_str = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S %Z")
    op_logger.info(f"Bot starting at: {start_time_str}")

    if SIMULATION_MODE: op_logger.error("Set SIMULATION_MODE to False for real trading."); exit()

    op_logger.warning("="*30 + f" REAL TRADING MODE ACTIVE - {log_prefix} " + "="*30)
    op_logger.warning("Strategy: Stoch(21,10,10) Entry on Close / SL(Fixed %)/TP(Dynamic BBands) Exit")
    op_logger.warning(f"Settings: MaxPos:{MAX_OPEN_POSITIONS}, Leverage:{LEVERAGE}x, Timeframe:{TIMEFRAME}, SymbolUpdate:{SYMBOL_UPDATE_INTERVAL_HOURS}h, RESTSync:{REST_SYNC_INTERVAL_MINUTES}min")
    op_logger.warning("!!! USING REAL FUNDS - MONITOR CLOSELY AND USE EXTREME CAUTION !!!")
    op_logger.warning("="*80)
    for i in range(5, 0, -1): print(f"Starting in {i} seconds...", end='\r'); time.sleep(1)
    print("Starting now!                  ")

    # 1. CCXT REST API 초기화
    if not initialize_binance_rest():
        op_logger.error("Exiting due to CCXT REST API initialization failure.")
        exit()

    # 2. 초기 상태 동기화 (봇 시작 시 한번 실행)
    op_logger.info("Running initial state synchronization via REST API...")
    sync_positions_with_exchange()
    op_logger.info("Initial REST sync complete.")
    log_asset_status() # 초기 자산 상태 로깅

    # 3. User Data Stream 시작 (Listen Key 포함) - K-line WS보다 먼저 시작
    if not start_user_stream():
         op_logger.error("Failed to start User Data Stream. Exiting.")
         exit()
    time.sleep(3) # User Stream 연결 안정화 시간

    # 4. 주기적 REST 동기화 스레드 시작 (보조 역할)
    sync_thread = Thread(target=sync_state_periodically, args=(REST_SYNC_INTERVAL_MINUTES * 60,), daemon=True)
    sync_thread.start()

    # 5. K-line 웹소켓 시작 (메인 루프)
    #    - on_open_kline 내부에서 초기 심볼 가져오기, 구독, 데이터 로딩 수행
    ws_url_kline = f"wss://fstream.binance.com/stream" # 기본 URL (스트림은 SUBSCRIBE 명령으로 지정)
    op_logger.info(f"Preparing K-line WebSocket connection...")
    kline_wsapp = websocket.WebSocketApp(ws_url_kline,
                                        on_open=on_open_kline,
                                        on_message=on_message_kline,
                                        on_error=on_error_kline,
                                        on_close=on_close_kline)

    # 6. 주기적 심볼 업데이트 스레드 시작 (K-line WS 앱 인스턴스 필요)
    symbol_update_thread = Thread(target=update_top_symbols_periodically, args=(SYMBOL_UPDATE_INTERVAL_HOURS * 60 * 60,), daemon=True)
    symbol_update_thread.start()

    # 7. K-line 웹소켓 실행 (이 함수는 블로킹됨)
    kline_thread = Thread(target=lambda: kline_wsapp.run_forever(ping_interval=60, ping_timeout=10), daemon=True) # PING 설정 추가
    kline_thread.start()
    op_logger.info("K-line WebSocket thread started. Bot is running...")

    # --- 메인 스레드는 대기 및 종료 처리 ---
    try:
        while kline_websocket_running and user_websocket_running: # 두 WS 중 하나라도 꺼지면 종료 준비
            time.sleep(1)
            log_asset_status() # 주기적으로 자산 상태 로깅 (log_asset_status 함수 내부에서 시간 체크)

            # 비정상 종료 감지 (예: user stream이 먼저 닫힌 경우)
            if not user_websocket_running and kline_websocket_running:
                 op_logger.warning("User data stream seems down. Stopping K-line stream as well.")
                 kline_wsapp.close()
                 kline_websocket_running = False

    except KeyboardInterrupt:
        op_logger.info("Keyboard interrupt received. Shutting down...")
    except Exception as main_loop_err:
        op_logger.error(f"Error in main loop: {main_loop_err}", exc_info=True)
    finally:
        op_logger.info("Initiating shutdown sequence...")
        kline_websocket_running = False
        user_websocket_running = False

        # 웹소켓 종료 시도
        if kline_wsapp and kline_wsapp.sock and kline_wsapp.sock.connected:
            op_logger.info("Closing K-line WebSocket...")
            kline_wsapp.close()
        if user_wsapp and user_wsapp.sock and user_wsapp.sock.connected:
            op_logger.info("Closing User Data Stream WebSocket...")
            user_wsapp.close()

        # Listen Key 삭제 시도 (선택 사항)
        with listen_key_lock:
            if listen_key and binance_rest:
                op_logger.info(f"Deleting listen key: {listen_key[:5]}...")
                try:
                    binance_rest.fapiPrivateDeleteListenKey({'listenKey': listen_key})
                except Exception as del_key_err:
                    op_logger.warning(f"Could not delete listen key: {del_key_err}")

        op_logger.info("Waiting for threads to finish...")
        time.sleep(5) # 스레드 정리 시간

        # --- 최종 정리 작업 ---
        # TODO: 종료 시 모든 미체결 주문 취소 로직 추가 (필수)
        op_logger.warning("Attempting to cancel all remaining open orders...")
        all_cancelled = True
        with subscribed_symbols_lock: symbols_to_check_orders = subscribed_symbols.copy()
        # 또는 모든 USDT 페어 조회
        # all_markets = binance_rest.load_markets()
        # usdt_futures = [mkt['id'] for mkt in all_markets.values() if mkt['type'] == 'future' and mkt['quote'] == 'USDT']
        for symbol_ws in symbols_to_check_orders:
            symbol_ccxt = symbol_ws.replace('USDT','/USDT')
            if not cancel_open_orders_for_symbol(symbol_ccxt):
                 all_cancelled = False
            time.sleep(0.3)
        if all_cancelled: op_logger.info("Attempted to cancel all open orders for subscribed symbols.")
        else: op_logger.error("Failed to cancel some open orders during shutdown. MANUAL CHECK REQUIRED.")

        # 최종 잔고 및 통계 로깅
        op_logger.info("Fetching final balance...")
        final_balance = get_current_balance()
        if final_balance is None: final_balance = "Error"
        else: final_balance = f"{final_balance:.2f}"

        with stats_lock: final_trades, final_wins = total_trades, winning_trades
        final_win_rate = (final_wins / final_trades * 100) if final_trades > 0 else 0.0

        final_msg = f"Final Balance:{final_balance} {TARGET_ASSET}, Total Trades:{final_trades}, Winning Trades:{final_wins}(UDS Based), Win Rate:{final_win_rate:.2f}%"
        op_logger.info(final_msg)
        asset_logger.info(final_msg)
        op_logger.info("Real trading bot shutdown complete.")