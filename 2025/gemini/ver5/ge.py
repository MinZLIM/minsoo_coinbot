# -*- coding: utf-8 -*-
# === 최종 버전 V2.6 (15m MACD/BBM 진입 + 3m MACD 히스토그램 필터 / 고정 TP 0.8% / 3m MACD 반대 신호 종료) ===
# === 수정: 3분봉 진입 필터를 MACD 시그널 기울기에서 MACD 히스토그램 조건으로 변경 ===
# === 수정: calculate_indicators 함수에 MACD 히스토그램 계산 추가 ===
# === 수정: 1시간 타임아웃 로직 제거 ===
# === 수정: 포지션 진입 후 3분봉 모니터링 및 반대 MACD 신호 시 포지션 종료 로직 추가 ===
# === 수정: 포지션 진입 조건에 3분봉 MACD 히스토그램 필터 추가 ===
# === 수정: 고정 TP를 0.8%로 변경 (로그 접두사 반영) ===
# === 수정: 동적 TP 업데이트 로직 제거 유지 ===
# === 수정: 8h 추세 필터, 최소 잔고 체크, 블랙리스트 로직 제거 유지 ===
# === 수정: 동기화 로직 심볼 형식 불일치 문제 수정 유지 ===
# === 수정: calculate_indicators 함수 CCI 컬럼 이름 문제 수정 유지 (CCI는 현재 사용 안 함) ===


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
TOP_N_SYMBOLS = 100 # 거래량 상위 N개 심볼 선택
TIMEFRAME = '15m' # 사용할 메인 캔들 시간봉
TIMEFRAME_MINUTES = 15 # 메인 시간봉 분 단위

# --- Stochastic RSI 설정 (지표 계산에는 포함, 진입/종료 조건에 직접 사용 안 함) ---
STOCHRSI_LENGTH = 14
STOCHRSI_RSI_LENGTH = 14
STOCHRSI_K = 3
STOCHRSI_D = 3
STOCHRSI_OVERSOLD = 10
STOCHRSI_OVERBOUGHT = 90

# --- MACD 설정 ---
MACD_FAST_PERIOD = 12
MACD_SLOW_PERIOD = 26
MACD_SIGNAL_PERIOD = 9

# --- Bollinger Bands 설정 (지표 계산에는 포함, 진입 조건에 사용) ---
BBANDS_PERIOD = 20 # 볼린저 밴드 기간
BBANDS_STDDEV = 2.0 # 볼린저 밴드 표준편차

# --- SL 설정 ---
LONG_STOP_LOSS_FACTOR = 0.99 # 롱 포지션 손절 비율 (진입가 * 0.99)
SHORT_STOP_LOSS_FACTOR = 1.01 # 숏 포지션 손절 비율 (진입가 * 1.01)

# --- 기타 설정 ---
REST_SYNC_INTERVAL_MINUTES = 5 # REST API 상태 동기화 주기(분)
SYMBOL_UPDATE_INTERVAL_HOURS = 2 # 거래 대상 심볼 목록 업데이트 주기(시간)
API_RETRY_COUNT = 3 # API 호출 실패 시 재시도 횟수
API_RETRY_DELAY_SECONDS = 2 # API 호출 재시도 간격(초)
TARGET_ASSET = 'USDT' # 타겟 자산 (테더)
TIMEFRAME_3M = '3m' # 포지션 모니터링용 3분봉
TIMEFRAME_3M_MINUTES = 3 # 3분봉 분 단위
INITIAL_CANDLE_FETCH_LIMIT = 100 # 초기 캔들 데이터 로드 개수 (지표 계산 위해 충분히 확보)
MAX_CANDLE_HISTORY = 200 # 메모리에 유지할 최대 캔들 개수 (메인/3분봉 공용)
KST = ZoneInfo("Asia/Seoul") # 한국 시간대
UTC = timezone.utc # UTC 시간대
pd.set_option('display.max_rows', None); pd.set_option('display.max_columns', None); pd.set_option('display.width', None) # Pandas 출력 옵션

# ==============================================================================
# 로깅 설정 (Logging Setup)
# ==============================================================================
log_dir = os.path.dirname(os.path.abspath(__file__)) # 로그 파일 저장 디렉토리
log_filename_base = "bot_log" # 로그 파일 기본 이름
log_prefix = "[15m_MACD_BBM_TP0.8_3mHistoEntry_3mMACDExit_V2.6]" # 로그 메시지 접두사 (전략 및 버전 명시)

# 운영 로그 (Operation Log)
op_logger = logging.getLogger('operation')
op_logger.setLevel(logging.INFO) # 로그 레벨 설정 (INFO 이상 기록)
op_formatter = logging.Formatter(f'%(asctime)s - %(levelname)s - {log_prefix} - %(message)s') # 로그 포맷
op_handler = logging.FileHandler(os.path.join(log_dir, f'{log_filename_base}_operation.log')) # 파일 핸들러
op_handler.setFormatter(op_formatter)
op_logger.addHandler(op_handler)
op_logger.addHandler(logging.StreamHandler()) # 콘솔 출력 핸들러 추가

# 매매 로그 (Trade Log), 자산 로그 (Asset Log)
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
historical_data_3m = {} 
data_3m_lock = Lock() 
entry_in_progress = {} 
entry_lock = Lock() 
last_asset_log_time = datetime.now(UTC) 
kline_websocket_running = False 
kline_wsapp = None 
subscribed_symbols = set() 
subscribed_symbols_lock = Lock() 
subscribed_symbols_3m = set() 
subscribed_symbols_3m_lock = Lock() 
shutdown_requested = False 
kline_thread = None 
symbol_update_thread = None 
sync_thread = None 
binance_rest = None 

# ==============================================================================
# 심볼 형식 변환 유틸리티 (Symbol Format Conversion Utilities)
# ==============================================================================
def convert_symbol_to_ccxt(symbol_ws):
    """웹소켓 형식 심볼(BTCUSDT)을 CCXT 형식 심볼(BTC/USDT)로 변환"""
    if not symbol_ws.endswith(TARGET_ASSET):
        op_logger.warning(f"Cannot convert {symbol_ws} to CCXT format: Does not end with {TARGET_ASSET}")
        return symbol_ws
    return f"{symbol_ws[:-len(TARGET_ASSET)]}/{TARGET_ASSET}"

def convert_symbol_to_ws(symbol_ccxt):
    """CCXT 형식 심볼(BTC/USDT)을 웹소켓 형식 심볼(BTCUSDT)로 변환"""
    return symbol_ccxt.replace(f'/{TARGET_ASSET}', TARGET_ASSET)

# ==============================================================================
# API 호출 재시도 헬퍼 함수 (API Call Retry Helper Function)
# ==============================================================================
def call_api_with_retry(api_call, max_retries=API_RETRY_COUNT, delay_seconds=API_RETRY_DELAY_SECONDS, error_message="API call failed"):
    """지정된 횟수만큼 API 호출을 재시도하는 함수"""
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
    """CCXT REST API 객체를 초기화하고 서버 시간 확인"""
    global binance_rest
    op_logger.info("Initializing CCXT REST...")
    if not API_KEY or API_KEY == "YOUR_BINANCE_API_KEY" or not API_SECRET:
        op_logger.error("API Key/Secret not configured!")
        return False
    try:
        binance_rest = ccxt.binance({
            'apiKey': API_KEY,
            'secret': API_SECRET,
            'enableRateLimit': True, 
            'options': {
                'defaultType': 'future', 
                'adjustForTimeDifference': True 
            }
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
    """지정된 자산의 사용 가능 잔고를 조회 (선물 지갑)"""
    if not binance_rest: return 0.0
    try:
        balance = call_api_with_retry(lambda: binance_rest.fetch_balance(params={'type': 'future'}), error_message="fetch_balance")
        return float(balance['free'].get(asset, 0.0))
    except Exception as e:
        op_logger.error(f"Unexpected error fetching balance after retries: {e}")
        return 0.0

def get_top_volume_symbols(n=TOP_N_SYMBOLS):
    """거래량 상위 N개의 USDT 선물 심볼 목록을 반환 (CCXT 형식)"""
    if not binance_rest: return []
    op_logger.info(f"Fetching top {n} symbols by volume...")
    try:
        tickers = call_api_with_retry(lambda: binance_rest.fetch_tickers(), error_message="fetch_tickers")
        futures_tickers = {
            s: t for s, t in tickers.items()
            if '/' in s and s.endswith(f"/{TARGET_ASSET}:{TARGET_ASSET}") and t.get('quoteVolume') is not None
        }
        if not futures_tickers:
            op_logger.warning("No USDT futures tickers found.")
            return []
        sorted_tickers = sorted(futures_tickers.values(), key=lambda x: x.get('quoteVolume', 0), reverse=True)
        top_symbols_ccxt = [t['symbol'].split(':')[0] for t in sorted_tickers[:n]]
        op_logger.info(f"Fetched top {len(top_symbols_ccxt)} symbols.")
        return top_symbols_ccxt
    except Exception as e:
        op_logger.error(f"Error fetching top symbols after retries: {e}")
        return []

def fetch_ohlcv_data(symbol_ccxt, timeframe, limit):
    """지정된 심볼, 타임프레임, 개수의 OHLCV 데이터를 조회 (API 호출 래퍼)"""
    if not binance_rest: return None
    try:
        op_logger.debug(f"Fetching {limit} candles for {symbol_ccxt} ({timeframe})...")
        ohlcv = call_api_with_retry(
            lambda: binance_rest.fetch_ohlcv(symbol_ccxt, timeframe=timeframe, limit=limit),
            error_message=f"fetch_ohlcv for {symbol_ccxt} ({timeframe})"
        )
        if not ohlcv:
            op_logger.warning(f"No OHLCV data returned for {symbol_ccxt} ({timeframe}).")
            return None
        return ohlcv
    except Exception as e:
        op_logger.error(f"Error fetching OHLCV for {symbol_ccxt} ({timeframe}): {e}")
        return None

def fetch_initial_ohlcv(symbol_ccxt, timeframe=TIMEFRAME, limit=INITIAL_CANDLE_FETCH_LIMIT, for_3m_chart=False):
    """지정된 심볼의 초기 OHLCV 데이터를 조회하여 DataFrame으로 반환"""
    stochrsi_buffer = STOCHRSI_LENGTH + STOCHRSI_RSI_LENGTH + 50
    macd_buffer = MACD_SLOW_PERIOD + MACD_SIGNAL_PERIOD + 50 
    bbands_buffer = BBANDS_PERIOD + 50
    actual_limit = max(limit, macd_buffer, (bbands_buffer if not for_3m_chart else 0), (stochrsi_buffer if not for_3m_chart else 0))

    ohlcv = fetch_ohlcv_data(symbol_ccxt, timeframe, actual_limit)
    if not ohlcv: return None

    try:
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True) 
        df.set_index('timestamp', inplace=True) 
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = pd.to_numeric(df[col])
        op_logger.debug(f"Successfully processed {len(df)} initial candles for {symbol_ccxt} ({timeframe}).")
        return df
    except Exception as e:
        op_logger.error(f"Error processing initial OHLCV data for {symbol_ccxt}: {e}")
        return None


def calculate_indicators(df):
    """주어진 DataFrame에 기술적 지표(Stochastic RSI, Bollinger Bands, MACD, MACD Histogram)를 계산하여 추가"""
    min_len_macd = MACD_SLOW_PERIOD + MACD_SIGNAL_PERIOD + 10 
    min_len_stochrsi = STOCHRSI_LENGTH + STOCHRSI_RSI_LENGTH + STOCHRSI_D + 10 
    min_len_bbands = BBANDS_PERIOD + 10 
    required_len = max(min_len_macd, min_len_stochrsi, min_len_bbands)

    if df is None or len(df) < required_len:
        op_logger.debug(f"Not enough data for indicators: Have {len(df) if df is not None else 0}, Need >{required_len}")
        return None
    try:
        df_copy = df.copy() 

        # Stochastic RSI 계산
        df_copy.ta.stochrsi(length=STOCHRSI_LENGTH, rsi_length=STOCHRSI_RSI_LENGTH, k=STOCHRSI_K, d=STOCHRSI_D, append=True)
        stoch_k_col_orig = f'STOCHRSIk_{STOCHRSI_LENGTH}_{STOCHRSI_RSI_LENGTH}_{STOCHRSI_K}_{STOCHRSI_D}'
        stoch_d_col_orig = f'STOCHRSId_{STOCHRSI_LENGTH}_{STOCHRSI_RSI_LENGTH}_{STOCHRSI_K}_{STOCHRSI_D}'

        # Bollinger Bands 계산
        df_copy.ta.bbands(length=BBANDS_PERIOD, std=BBANDS_STDDEV, append=True)
        bbl_col_orig = f'BBL_{BBANDS_PERIOD}_{float(BBANDS_STDDEV)}'
        bbm_col_orig = f'BBM_{BBANDS_PERIOD}_{float(BBANDS_STDDEV)}'
        bbu_col_orig = f'BBU_{BBANDS_PERIOD}_{float(BBANDS_STDDEV)}'

        # MACD 계산 (히스토그램 포함)
        df_copy.ta.macd(fast=MACD_FAST_PERIOD, slow=MACD_SLOW_PERIOD, signal=MACD_SIGNAL_PERIOD, append=True)
        macd_line_col_orig = f'MACD_{MACD_FAST_PERIOD}_{MACD_SLOW_PERIOD}_{MACD_SIGNAL_PERIOD}'
        macd_signal_col_orig = f'MACDs_{MACD_FAST_PERIOD}_{MACD_SLOW_PERIOD}_{MACD_SIGNAL_PERIOD}'
        macd_hist_col_orig = f'MACDh_{MACD_FAST_PERIOD}_{MACD_SLOW_PERIOD}_{MACD_SIGNAL_PERIOD}' # MACD 히스토그램 컬럼

        # 컬럼 이름 변경
        rename_map = {
            stoch_k_col_orig: 'STOCHk', stoch_d_col_orig: 'STOCHd',
            bbl_col_orig: 'BBL', bbm_col_orig: 'BBM', bbu_col_orig: 'BBU',
            macd_line_col_orig: 'MACD_line', macd_signal_col_orig: 'MACD_signal',
            macd_hist_col_orig: 'MACD_hist' # 히스토그램 컬럼 이름 변경
        }
        existing_rename_map = {k: v for k, v in rename_map.items() if k in df_copy.columns}
        
        required_orig_cols = [bbl_col_orig, bbm_col_orig, bbu_col_orig, macd_line_col_orig, macd_signal_col_orig, macd_hist_col_orig]
        if not all(col in df_copy.columns for col in required_orig_cols):
             op_logger.warning(f"Not all required original indicator columns were generated. Expected: {required_orig_cols}, Available: {df_copy.columns.tolist()}")
             return None

        df_copy.rename(columns=existing_rename_map, inplace=True)

        required_final_cols = ['BBL', 'BBM', 'BBU', 'MACD_line', 'MACD_signal', 'MACD_hist']
        if not all(col in df_copy.columns for col in required_final_cols):
            op_logger.warning(f"Required final indicator columns missing after rename. Needed: {required_final_cols}, Have: {df_copy.columns.tolist()}")
            return None

        # 최근 2개 캔들의 MACD 값 (라인, 시그널, 히스토그램)과 최근 1개 캔들의 BBands 값에 NaN이 있는지 확인
        if len(df_copy) < 2 or \
           df_copy[['MACD_line', 'MACD_signal', 'MACD_hist']].iloc[-2:].isnull().any().any() or \
           df_copy[['BBL', 'BBM', 'BBU']].iloc[-1].isnull().any():
            op_logger.debug(f"Latest or previous indicator values (MACD, BBands, MACD_hist) contain NaN.")
            return None

        return df_copy
    except Exception as e:
        op_logger.error(f"Indicator Calculation Error: {e}", exc_info=True)
        return None

def set_isolated_margin(symbol_ccxt, leverage):
    """지정된 심볼의 마진 모드를 격리(ISOLATED)로 설정하고 레버리지를 조절"""
    if not binance_rest: return False
    op_logger.info(f"Setting ISOLATED margin for {symbol_ccxt} / {leverage}x...")
    try:
        try:
            binance_rest.set_margin_mode('ISOLATED', symbol_ccxt, params={})
            op_logger.info(f"Margin mode successfully set to ISOLATED for {symbol_ccxt}.")
            time.sleep(0.2)
        except ccxt.ExchangeError as e:
            if 'No need to change margin type' in str(e) or 'already isolated' in str(e):
                op_logger.warning(f"Margin mode for {symbol_ccxt} is already ISOLATED.")
            elif 'position exists' in str(e):
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
    """실제 시장가 주문을 실행 (진입 또는 종료)"""
    if not binance_rest or amount <= 0:
        op_logger.error(f"[{symbol_ccxt}] Invalid arguments for market order: amount={amount}")
        return None
    try:
        mkt = binance_rest.market(symbol_ccxt)
        adj_amt_str = binance_rest.amount_to_precision(symbol_ccxt, amount)
        adj_amt = float(adj_amt_str)
        if adj_amt <= 0:
            op_logger.error(f"[{symbol_ccxt}] Adjusted amount '{adj_amt_str}' is <= 0.")
            return None

        min_notional = mkt.get('limits', {}).get('cost', {}).get('min', 5.0)
        if current_price and adj_amt * current_price < min_notional:
            op_logger.error(f"[{symbol_ccxt}] Estimated order value ({adj_amt * current_price:.2f}) is less than minimum required ({min_notional}). Amount: {adj_amt_str}")
            return None

        op_logger.info(f"[REAL ORDER] Attempting {side.upper()} {adj_amt_str} {symbol_ccxt} @ MARKET")
        coid = f"bot_{uuid.uuid4().hex[:16]}"
        params = {'newClientOrderId': coid}
        order = binance_rest.create_market_order(symbol_ccxt, side, adj_amt, params=params)
        oid = order.get('id')
        op_logger.info(f"[REAL ORDER PLACED] ID:{oid} CliID:{coid} Sym:{symbol_ccxt} Side:{side} ReqAmt:{adj_amt_str}")
        trade_logger.info(f"REAL MARKET ORDER: {side.upper()} {symbol_ccxt}, ReqAmt:{adj_amt_str}, OrdID:{oid}, CliOrdID:{coid}")
        return {'id': oid, 'clientOrderId': coid, 'status': order.get('status', 'open')}
    except ccxt.InsufficientFunds as e:
        op_logger.error(f"[ORDER FAILED] Insufficient funds for {symbol_ccxt}: {e}")
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

def _create_conditional_order(symbol_ccxt, side, stop_price, amount, order_type, client_order_id_prefix, log_prefix_msg):
    """STOP_MARKET 또는 TAKE_PROFIT_MARKET 주문을 생성하는 내부 함수"""
    if not binance_rest or amount <= 0 or stop_price <= 0:
        op_logger.error(f"[{symbol_ccxt}] Invalid arguments for {order_type} order: amount={amount}, stop_price={stop_price}")
        return None
    try:
        amt_str = binance_rest.amount_to_precision(symbol_ccxt, amount)
        sp_str = binance_rest.price_to_precision(symbol_ccxt, stop_price)
        amt = float(amt_str)
        if amt <= 0:
            op_logger.error(f"[{symbol_ccxt}] {log_prefix_msg} Adjusted amount '{amt_str}' is <= 0.")
            return None

        op_logger.info(f"[REAL {log_prefix_msg} ORDER] Attempting {side.upper()} {amt_str} {symbol_ccxt} ({order_type}) if price hits {sp_str}")
        coid = f"{client_order_id_prefix}_{uuid.uuid4().hex[:16]}"
        params = {'stopPrice': sp_str, 'reduceOnly': True, 'newClientOrderId': coid}
        order = binance_rest.create_order(symbol_ccxt, order_type, side, amt, None, params)
        oid = order.get('id')
        op_logger.info(f"[REAL {log_prefix_msg} PLACED] ID:{oid} CliID:{coid} Sym:{symbol_ccxt} Side:{side} StopPx:{sp_str} Amt:{amt_str} Type:{order_type}")
        trade_logger.info(f"REAL {log_prefix_msg} ORDER SET: {side.upper()} {symbol_ccxt}, Type:{order_type}, Amt:{amt_str}, StopPx:{sp_str}, OrdID:{oid}, CliOrdID:{coid}")
        return {'id': oid, 'clientOrderId': coid}
    except ccxt.ExchangeError as e:
        if order_type == 'TAKE_PROFIT_MARKET' and e.code == -2021:
             op_logger.warning(f"[{symbol_ccxt}] {log_prefix_msg} WARNING: Could not place {order_type} for {symbol_ccxt}, order would immediately trigger: {e}")
             return None
        else:
            op_logger.error(f"[{symbol_ccxt}] {log_prefix_msg} FAILED: Exchange error placing {order_type} order for {symbol_ccxt}: {e}")
            return None
    except (RequestTimeout, ExchangeNotAvailable, OnMaintenance) as e:
        op_logger.warning(f"[{symbol_ccxt}] {log_prefix_msg} FAILED: Network/Exchange issue placing {order_type} order for {symbol_ccxt}: {e}")
        return None
    except Exception as e:
        op_logger.error(f"[{symbol_ccxt}] {log_prefix_msg} FAILED: Unexpected error placing {order_type} order for {symbol_ccxt}: {e}", exc_info=True)
        return None

def place_stop_market_order(symbol_ccxt, side, stop_price, amount):
    """실제 STOP_MARKET 주문을 실행 (손절 주문)"""
    return _create_conditional_order(symbol_ccxt, side, stop_price, amount, 'STOP_MARKET', 'sl', 'SL')

def place_take_profit_market_order(symbol_ccxt, side, stop_price, amount):
    """실제 TAKE_PROFIT_MARKET 주문을 실행 (익절 주문)"""
    return _create_conditional_order(symbol_ccxt, side, stop_price, amount, 'TAKE_PROFIT_MARKET', 'tp', 'TP')

def cancel_order(symbol_ccxt, order_id=None, client_order_id=None):
    """지정된 주문 ID 또는 Client Order ID를 사용하여 주문을 취소"""
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
        op_logger.info(f"Successfully cancelled order {target_id_str}.")
        return True
    except OrderNotFound:
        op_logger.warning(f"Order {target_id_str} not found, likely already closed/cancelled.")
        return True
    except (ExchangeNotAvailable, OnMaintenance, RequestTimeout) as e:
        op_logger.error(f"Cannot cancel order {target_id_str} due to temporary issue: {e}")
        return False
    except RateLimitExceeded as e:
        op_logger.error(f"Rate limit exceeded cancelling order {target_id_str}: {e}")
        return False
    except ccxt.ExchangeError as e:
        if 'Order does not exist' in str(e) or '-2011' in str(e):
            op_logger.warning(f"Order {target_id_str} likely already gone (ExchangeError).")
            return True
        else:
            op_logger.error(f"Failed to cancel order {target_id_str}: {e}")
            return False
    except Exception as e:
        op_logger.error(f"Unexpected error cancelling order {target_id_str}: {e}", exc_info=True)
        return False

def cancel_open_orders_for_symbol(symbol_ccxt):
    """지정된 심볼의 모든 미체결 주문을 취소"""
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
    """현재 자산 상태 (잔고, 활성 포지션, 통계)를 주기적으로 로깅"""
    global last_asset_log_time
    now = datetime.now(UTC)
    if now - last_asset_log_time >= timedelta(hours=1):
        try:
            bal = get_current_balance() 
            bal_str = f"{bal:.2f}" if bal is not None else "Error"
            with stats_lock: 
                trades, wins = total_trades, winning_trades
            win_rate = (wins / trades * 100) if trades > 0 else 0.0 
            active_pos = []
            num_active = 0
            with real_positions_lock: 
                active_pos = list(real_positions.keys()) 
                num_active = len(real_positions) 
            asset_logger.info(f"Balance:{bal_str} {TARGET_ASSET}, Active Positions:{num_active} {active_pos}, Total Trades:{trades}(Delayed), Winning Trades:{wins}(Delayed), Win Rate:{win_rate:.2f}%")
            last_asset_log_time = now 
        except Exception as e:
            asset_logger.error(f"Error logging asset status: {e}", exc_info=True)

# ==============================================================================
# 상태 동기화 로직 헬퍼 함수들
# ==============================================================================
def _parse_exchange_positions():
    """거래소에서 가져온 원본 포지션 데이터를 내부 형식으로 파싱"""
    exchange_pos_dict = {}
    try:
        op_logger.debug("[SYNC_REST] Fetching current positions from exchange via REST API...")
        exchange_positions_raw = call_api_with_retry(lambda: binance_rest.fetch_positions(), error_message="fetch_positions")
        time.sleep(0.1)

        for pos in exchange_positions_raw:
            try:
                amount = float(pos.get('info', {}).get('positionAmt', 0))
                if abs(amount) < 1e-9:
                    continue
                symbol_ccxt_raw = pos.get('symbol')
                if not symbol_ccxt_raw: continue
                symbol_parts = symbol_ccxt_raw.split(':')
                symbol_ccxt_clean = symbol_parts[0]
                if '/' not in symbol_ccxt_clean: continue

                if symbol_ccxt_clean.endswith(f'/{TARGET_ASSET}'):
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
    except Exception as e:
        op_logger.error(f"[SYNC_REST] Failed to fetch or parse exchange positions: {e}")
        raise
    return exchange_pos_dict

def _handle_local_only_positions(local_only_symbols, local_pos_dict):
    """로컬 상태에만 있는 포지션 처리 (재확인 후 로컬 상태 제거 및 주문 취소)"""
    global total_trades
    if not local_only_symbols: return

    op_logger.warning(f"[SYNC_REST][WARN] Local positions not found on exchange (Initial Check): {local_only_symbols}")
    for symbol_ws in local_only_symbols:
        symbol_ccxt = convert_symbol_to_ccxt(symbol_ws)
        remove_local_state = False
        try:
            op_logger.debug(f"Re-checking position specifically for {symbol_ws} using fetch_positions(symbols=['{symbol_ccxt}'])")
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
                op_logger.info(f"[{symbol_ws}] Cancelling orphaned orders for {symbol_ccxt}...")
                cancel_open_orders_for_symbol(symbol_ccxt)
                unsubscribe_from_3m_kline(symbol_ws)


def _handle_exchange_only_positions(exchange_only_symbols, exchange_pos_dict):
    """거래소에만 있는 (봇이 추적하지 않는) 포지션 처리 (즉시 종료 시도)"""
    global total_trades
    if not exchange_only_symbols: return

    op_logger.error(f"[SYNC_REST][CRITICAL] Untracked positions on exchange: {exchange_only_symbols}. Attempting to close.")
    for symbol_ws in exchange_only_symbols:
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
                with stats_lock: total_trades += 1
        except Exception as close_err: op_logger.error(f"[SYNC_REST] Error closing untracked {symbol_ccxt}: {close_err}. MANUAL INTERVENTION.")
        time.sleep(0.5)

def _handle_positions_in_both(both_symbols, local_pos_dict, exchange_pos_dict):
    """로컬과 거래소 모두에 있는 포지션 검증 (수량/방향 불일치, SL/TP 주문 상태)"""
    if not both_symbols: return

    for symbol_ws in both_symbols:
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

# ==============================================================================
# 상태 동기화 로직 (State Synchronization Logic) - 메인 함수
# ==============================================================================
def sync_positions_with_exchange():
    """REST API를 사용하여 봇 내부 상태와 실제 거래소 포지션 상태를 동기화"""
    global real_positions, total_trades
    op_logger.info("[SYNC_REST] Starting state synchronization (REST API Based)...")
    if not binance_rest:
        op_logger.error("[SYNC_REST] CCXT REST instance not ready. Skipping sync.")
        return

    try:
        exchange_pos_dict = _parse_exchange_positions()

        with real_positions_lock:
            local_pos_dict = real_positions.copy()

        local_symbols = set(local_pos_dict.keys())
        exchange_symbols = set(exchange_pos_dict.keys())

        local_only = local_symbols - exchange_symbols
        exchange_only = exchange_symbols - local_symbols
        in_both = local_symbols.intersection(exchange_symbols)

        op_logger.info(f"[SYNC_REST] State Check: LocalOnly={len(local_only)}, ExchangeOnly={len(exchange_only)}, Both={len(in_both)}")
        if local_only: op_logger.info(f"[SYNC_REST] Local Only Symbols: {local_only}")
        if exchange_only: op_logger.info(f"[SYNC_REST] Exchange Only Symbols: {exchange_only}")

        _handle_local_only_positions(local_only, local_pos_dict)
        _handle_exchange_only_positions(exchange_only, exchange_pos_dict)
        _handle_positions_in_both(in_both, local_pos_dict, exchange_pos_dict)

        op_logger.info("[SYNC_REST] REST state synchronization finished.")

    except AuthenticationError:
        op_logger.error("[SYNC_REST] Authentication error during REST sync! Shutting down bot.")
        global shutdown_requested
        shutdown_requested = True
    except Exception as e:
        op_logger.error(f"[SYNC_REST] Critical error during REST state synchronization: {e}", exc_info=True)
        time.sleep(60)


def sync_state_periodically(interval_seconds):
    """주기적으로 상태 동기화 함수를 호출하는 스레드 함수"""
    global shutdown_requested
    op_logger.info(f"REST Sync thread started. Sync interval: {interval_seconds} seconds.")
    while not shutdown_requested:
        try:
            op_logger.debug(f"REST Sync thread: Waiting for {interval_seconds} seconds until next sync...")
            wait_until = time.time() + interval_seconds
            while time.time() < wait_until and not shutdown_requested:
                time.sleep(1)
            if shutdown_requested: break
            sync_positions_with_exchange()
        except Exception as e:
            op_logger.error(f"Error in REST sync loop: {e}", exc_info=True)
            time.sleep(60)
    op_logger.info("REST Sync thread finished.")


# ==============================================================================
# 심볼 목록 주기적 업데이트 로직 (Symbol List Periodic Update Logic)
# ==============================================================================
def update_top_symbols_periodically(interval_seconds):
    """주기적으로 거래량 상위 심볼 목록을 업데이트하고 웹소켓 구독/구독 해제"""
    global subscribed_symbols, historical_data, kline_websocket_running, kline_wsapp, shutdown_requested
    op_logger.info(f"Symbol Update thread started. Update interval: {interval_seconds} seconds.")
    while not shutdown_requested:
        try:
            op_logger.debug(f"Symbol Update thread: Waiting for {interval_seconds} seconds until next update...")
            wait_until = time.time() + interval_seconds
            while time.time() < wait_until and not shutdown_requested:
                time.sleep(1)
            if shutdown_requested: break

            if not kline_websocket_running or not kline_wsapp or not kline_wsapp.sock or not kline_wsapp.sock.connected:
                op_logger.warning("[Symbol Update] K-line WebSocket is not ready. Skipping symbol update cycle.")
                continue

            op_logger.info("[Symbol Update] Starting periodic symbol update process...")
            new_sym_ccxt = get_top_volume_symbols(TOP_N_SYMBOLS)
            if not new_sym_ccxt:
                op_logger.warning("[Symbol Update] Failed to fetch new top symbols. Skipping update cycle.")
                continue
            new_sym_ws = {convert_symbol_to_ws(s) for s in new_sym_ccxt}

            with subscribed_symbols_lock:
                current_subs = subscribed_symbols.copy()
            to_add = new_sym_ws - current_subs
            to_remove = current_subs - new_sym_ws

            if to_remove:
                op_logger.info(f"[Symbol Update] Symbols to remove (Main TF): {to_remove}")
                streams_to_unsub = [f"{s.lower()}@kline_{TIMEFRAME}" for s in to_remove]
                if streams_to_unsub:
                    msg = {"method": "UNSUBSCRIBE", "params": streams_to_unsub, "id": int(time.time())}
                    try:
                        if kline_wsapp and kline_wsapp.sock and kline_wsapp.sock.connected:
                            kline_wsapp.send(json.dumps(msg))
                            op_logger.info(f"[Symbol Update] Sent UNSUBSCRIBE request for {len(to_remove)} main streams.")
                        else:
                            op_logger.warning("[Symbol Update] K-line WebSocket disconnected before sending UNSUBSCRIBE.")
                    except Exception as e:
                        op_logger.error(f"[Symbol Update] Failed to send UNSUBSCRIBE message: {e}")

                with subscribed_symbols_lock:
                    subscribed_symbols -= to_remove
                removed_count = 0
                with data_lock:
                    for s in to_remove:
                        if historical_data.pop(s, None) is not None:
                            removed_count += 1
                op_logger.info(f"[Symbol Update] Removed historical data for {removed_count} symbols.")

            if to_add:
                op_logger.info(f"[Symbol Update] Symbols to add (Main TF): {to_add}")
                fetched_count, error_count = 0, 0
                added_to_data = set()
                for symbol_ws in to_add:
                    if shutdown_requested: break
                    ccxt_symbol = convert_symbol_to_ccxt(symbol_ws)
                    df = fetch_initial_ohlcv(ccxt_symbol, TIMEFRAME)
                    if df is not None and not df.empty:
                        with data_lock:
                            historical_data[symbol_ws] = df
                        fetched_count += 1
                        added_to_data.add(symbol_ws)
                    else:
                        error_count += 1
                        op_logger.warning(f"[Symbol Update] Failed to fetch initial data for new symbol {symbol_ws}.")
                    time.sleep(0.3)
                op_logger.info(f"[Symbol Update] Fetched initial data for {fetched_count} new symbols ({error_count} errors).")

                if added_to_data:
                    streams_to_sub = [f"{s.lower()}@kline_{TIMEFRAME}" for s in added_to_data]
                    msg = {"method": "SUBSCRIBE", "params": streams_to_sub, "id": int(time.time())}
                    try:
                        if kline_wsapp and kline_wsapp.sock and kline_wsapp.sock.connected:
                            kline_wsapp.send(json.dumps(msg))
                            op_logger.info(f"[Symbol Update] Sent SUBSCRIBE request for {len(added_to_data)} new main streams.")
                            with subscribed_symbols_lock:
                                subscribed_symbols.update(added_to_data)
                        else:
                            op_logger.warning("[Symbol Update] K-line WebSocket disconnected before sending SUBSCRIBE for new symbols.")
                            with data_lock:
                                for s in added_to_data: historical_data.pop(s, None)
                    except Exception as e:
                        op_logger.error(f"[Symbol Update] Failed to send SUBSCRIBE message for new symbols: {e}")
                        with data_lock:
                            for s in added_to_data: historical_data.pop(s, None)


            with subscribed_symbols_lock: 
                current_count = len(subscribed_symbols)
            op_logger.info(f"[Symbol Update] Finished symbol update cycle. Currently subscribed to {current_count} main timeframe symbols.")

        except Exception as e:
            op_logger.error(f"Error in symbol update loop: {e}", exc_info=True)
            time.sleep(60)
    op_logger.info("Symbol Update thread finished.")


# ==============================================================================
# 웹소켓 처리 로직 (K-line)
# ==============================================================================
def update_historical_data(symbol_ws, kline_data, data_dict_to_update, lock_for_data, max_history_len):
    """수신된 K-line 데이터로 해당 심볼의 과거 데이터 DataFrame을 업데이트"""
    try:
        with lock_for_data: 
            if symbol_ws not in data_dict_to_update:
                return False

            df = data_dict_to_update[symbol_ws]
            k_time = pd.to_datetime(kline_data['t'], unit='ms', utc=True)
            k_open = float(kline_data['o'])
            k_high = float(kline_data['h'])
            k_low = float(kline_data['l'])
            k_close = float(kline_data['c'])
            k_volume = float(kline_data['v'])

            new_data_row = pd.DataFrame([{
                'open': k_open, 'high': k_high, 'low': k_low, 'close': k_close, 'volume': k_volume
            }], index=[k_time])

            if k_time in df.index:
                df.loc[k_time] = new_data_row.iloc[0]
            else:
                df = pd.concat([df, new_data_row])
                df = df.iloc[-max_history_len:] 
            data_dict_to_update[symbol_ws] = df 
            return True
    except Exception as e:
        op_logger.error(f"[{symbol_ws}] Error updating historical data: {e}")
        return False

def _check_entry_conditions_main_tf(symbol_ws, idf):
    """진입 조건(MACD 교차 및 BBM 기준)을 확인하고 진입 방향과 새로운 TP 목표를 반환"""
    with subscribed_symbols_lock:
        if symbol_ws not in subscribed_symbols: 
            return None, None, None

    with data_lock: 
        df = historical_data.get(symbol_ws)
    if df is None: return None, None, None

    if idf is None or idf.empty or len(idf) < 2:
        return None, None, None

    try:
        last = idf.iloc[-1]
        prev = idf.iloc[-2]

        entry_px = last['close'] 
        bbm = last.get('BBM', np.nan)
        curr_macd_line, prev_macd_line = last.get('MACD_line', np.nan), prev.get('MACD_line', np.nan)
        curr_macd_signal, prev_macd_signal = last.get('MACD_signal', np.nan), prev.get('MACD_signal', np.nan)

        if any(pd.isna(v) for v in [entry_px, bbm, curr_macd_line, prev_macd_line, curr_macd_signal, prev_macd_signal]):
            op_logger.debug(f"[{symbol_ws}] Required values for MACD entry check contain NaN.")
            return None, None, None
    except IndexError:
         return None, None, None
    except Exception as e:
        op_logger.error(f"[{symbol_ws}] Error accessing indicator data: {e}")
        return None, None, None

    tgt_side = None
    tp_tgt = None

    macd_golden_cross = (prev_macd_line <= prev_macd_signal and curr_macd_line > curr_macd_signal)
    macd_dead_cross = (prev_macd_line >= prev_macd_signal and curr_macd_line < curr_macd_signal)

    if macd_golden_cross and entry_px < bbm:
        tgt_side = 'buy'
        tp_tgt = entry_px * 1.008 
        op_logger.info(f"[{symbol_ws}] Long entry: MACD Golden Cross (L:{curr_macd_line:.4f}, S:{curr_macd_signal:.4f}) AND Price ({entry_px:.5f}) < BBM ({bbm:.5f}). TP: {tp_tgt:.5f}.")
    elif macd_dead_cross and entry_px > bbm:
        tgt_side = 'sell'
        tp_tgt = entry_px * 0.992 
        op_logger.info(f"[{symbol_ws}] Short entry: MACD Dead Cross (L:{curr_macd_line:.4f}, S:{curr_macd_signal:.4f}) AND Price ({entry_px:.5f}) > BBM ({bbm:.5f}). TP: {tp_tgt:.5f}.")

    return tgt_side, tp_tgt, entry_px

def _check_3m_macd_histogram_condition(symbol_ws, sym_ccxt, proposed_side): # 함수명 및 로직 수정
    """3분봉 MACD 히스토그램을 확인하여 진입 조건 필터링"""
    op_logger.debug(f"[{symbol_ws}] Checking 3m MACD histogram condition for {proposed_side} entry.")
    
    required_3m_candles = MACD_SLOW_PERIOD + MACD_SIGNAL_PERIOD + 20 
    
    df_3m = None
    with data_3m_lock:
        if symbol_ws in historical_data_3m and len(historical_data_3m[symbol_ws]) >= required_3m_candles:
            df_3m = historical_data_3m[symbol_ws].copy()
            op_logger.debug(f"[{symbol_ws}] Using cached 3m data ({len(df_3m)} candles) for MACD histogram check.")

    if df_3m is None:
        op_logger.debug(f"[{symbol_ws}] Cached 3m data insufficient. Fetching fresh 3m data ({INITIAL_CANDLE_FETCH_LIMIT} candles).")
        raw_ohlcv_3m = fetch_ohlcv_data(sym_ccxt, TIMEFRAME_3M, limit=INITIAL_CANDLE_FETCH_LIMIT) 
        
        if raw_ohlcv_3m and len(raw_ohlcv_3m) >= required_3m_candles:
            df_3m = pd.DataFrame(raw_ohlcv_3m, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df_3m['timestamp'] = pd.to_datetime(df_3m['timestamp'], unit='ms', utc=True)
            df_3m.set_index('timestamp', inplace=True)
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df_3m[col] = pd.to_numeric(df_3m[col])
            op_logger.debug(f"[{symbol_ws}] Fetched and processed {len(df_3m)} fresh 3m candles for histogram check.")
        else:
            op_logger.warning(f"[{symbol_ws}] Could not fetch sufficient fresh 3m data for histogram (got {len(raw_ohlcv_3m) if raw_ohlcv_3m else 0}, needed {required_3m_candles}). Filter fails.")
            return False

    try:
        # calculate_indicators 함수는 MACD 히스토그램을 'MACD_hist'로 저장
        # 여기서는 3분봉 데이터에 대해 직접 MACD를 계산하고 히스토그램 컬럼 이름을 사용
        idf_3m_for_histo = calculate_indicators(df_3m.copy()) # calculate_indicators가 'MACD_hist'를 생성
        if idf_3m_for_histo is None or idf_3m_for_histo.empty:
            op_logger.warning(f"[{symbol_ws}] MACD calculation on 3m data returned empty for histogram. Columns: {df_3m.columns.tolist()}")
            return False
        
        if 'MACD_hist' not in idf_3m_for_histo.columns:
            op_logger.warning(f"[{symbol_ws}] 'MACD_hist' column not found after 3m indicator calculation. Available: {idf_3m_for_histo.columns.tolist()}")
            return False

        if len(idf_3m_for_histo['MACD_hist'].dropna()) < 2 or idf_3m_for_histo['MACD_hist'].iloc[-2:].isnull().any():
            op_logger.warning(f"[{symbol_ws}] Not enough valid 3m MACD histogram values. Last two: {idf_3m_for_histo['MACD_hist'].iloc[-2:].tolist()}")
            return False

        current_hist = idf_3m_for_histo['MACD_hist'].iloc[-1]
        prev_hist = idf_3m_for_histo['MACD_hist'].iloc[-2]

    except Exception as e:
        op_logger.error(f"[{symbol_ws}] Error during 3m MACD histogram calculation/access: {e}", exc_info=True)
        return False

    if proposed_side == 'buy':
        # 조건: 최근 히스토그램이 이전 히스토그램보다 커야 함
        if current_hist > prev_hist:
            op_logger.info(f"[{symbol_ws}] Long entry 3m MACD histogram PASSED (PrevH: {prev_hist:.4f}, CurrH: {current_hist:.4f} - current > prev).")
            return True
        else:
            op_logger.info(f"[{symbol_ws}] Long entry filtered out by 3m MACD histogram (PrevH: {prev_hist:.4f}, CurrH: {current_hist:.4f}). Condition current > prev FAILED.")
            return False
    elif proposed_side == 'short':
        # 조건: 최근 히스토그램이 이전 히스토그램보다 작아야 함
        if current_hist < prev_hist:
            op_logger.info(f"[{symbol_ws}] Short entry 3m MACD histogram PASSED (PrevH: {prev_hist:.4f}, CurrH: {current_hist:.4f} - current < prev).")
            return True
        else:
            op_logger.info(f"[{symbol_ws}] Short entry filtered out by 3m MACD histogram (PrevH: {prev_hist:.4f}, CurrH: {current_hist:.4f}). Condition current < prev FAILED.")
            return False
    
    return False


def _perform_entry_order(symbol_ws, sym_ccxt, tgt_side, entry_px, tp_tgt, now):
    """실제 진입 주문 및 SL/TP 주문 설정, 로컬 상태 저장"""
    global real_positions
    try:
        op_logger.info(f"[{symbol_ws}] ===> Starting Entry Process: {tgt_side.upper()} @ {entry_px:.5f}, Initial TP Target: {tp_tgt:.5f}")

        with real_positions_lock: oc = len(real_positions)
        if oc >= MAX_OPEN_POSITIONS: raise Exception("Max positions reached just before entry execution")

        portion = 1.0 / (MAX_OPEN_POSITIONS - oc) if MAX_OPEN_POSITIONS > oc else 1.0
        current_balance = get_current_balance()
        if current_balance <= 0: raise Exception(f"Insufficient balance ({current_balance:.2f} {TARGET_ASSET}) for entry.")

        margin_to_use = current_balance * portion
        notional_value = margin_to_use * LEVERAGE
        min_notional_required = 5.0
        if notional_value < min_notional_required: raise Exception(f"Calculated notional value ({notional_value:.2f}) is below minimum ({min_notional_required})")

        entry_amount = notional_value / entry_px if entry_px > 0 else 0
        if entry_amount <= 0: raise Exception("Calculated entry amount is zero or negative")

        if not set_isolated_margin(sym_ccxt, LEVERAGE): raise Exception("Failed to set isolated margin or leverage")

        entry_order = place_market_order_real(sym_ccxt, tgt_side, entry_amount, entry_px)
        if not entry_order or not entry_order.get('id'): raise Exception("Market entry order placement failed")
        entry_oid, entry_coid = entry_order['id'], entry_order.get('clientOrderId')
        op_logger.info(f"[{symbol_ws}] Market entry order placed successfully (ID:{entry_oid}, CliID:{entry_coid}). Assuming filled.")

        sl_order, tp_order = None, None
        final_sl_price = entry_px * (LONG_STOP_LOSS_FACTOR if tgt_side == 'buy' else SHORT_STOP_LOSS_FACTOR)
        final_tp_price = tp_tgt 
        sl_tp_side = 'sell' if tgt_side == 'buy' else 'buy'

        sl_order = place_stop_market_order(sym_ccxt, sl_tp_side, final_sl_price, entry_amount)
        if not sl_order or not sl_order.get('id'): raise Exception("Stop Loss (SL) order placement failed")
        op_logger.info(f"[{symbol_ws}] SL order placed successfully (ID:{sl_order['id']})")
        time.sleep(0.1)

        tp_order = place_take_profit_market_order(sym_ccxt, sl_tp_side, final_tp_price, entry_amount)
        if tp_order and tp_order.get('id'): op_logger.info(f"[{symbol_ws}] TP order placed successfully (ID:{tp_order['id']})")
        else: op_logger.warning(f"[{symbol_ws}] Take Profit (TP) order placement failed or returned no ID. Proceeding without TP order initially.")

        with real_positions_lock:
            if len(real_positions) < MAX_OPEN_POSITIONS:
                real_positions[symbol_ws] = {
                    'side': 'long' if tgt_side == 'buy' else 'short', 'entry_price': entry_px,
                    'amount': entry_amount, 'entry_time': now,
                    'entry_order_id': entry_oid, 'entry_client_order_id': entry_coid,
                    'sl_order_id': sl_order['id'], 'sl_client_order_id': sl_order.get('clientOrderId'),
                    'tp_order_id': tp_order['id'] if tp_order and tp_order.get('id') else None,
                    'tp_client_order_id': tp_order.get('clientOrderId') if tp_order and tp_order.get('id') else None,
                    'current_tp_price': final_tp_price if tp_order and tp_order.get('id') else None 
                }
                op_logger.info(f"[{symbol_ws}] <<< Entry successful and position state stored. Active Positions: {len(real_positions)} >>>")
                subscribe_to_3m_kline(symbol_ws, sym_ccxt)

            else: raise Exception("Max positions reached during final state storage")

    except Exception as sltp_e:
        op_logger.error(f"[{symbol_ws}] Error placing SL/TP orders or during entry: {sltp_e}. !!! INITIATING ROLLBACK !!!")
        if sl_order and sl_order.get('id'): Thread(target=cancel_order, args=(sym_ccxt,), kwargs={'order_id': sl_order['id'], 'client_order_id': sl_order.get('clientOrderId')}, daemon=True).start()
        if tp_order and tp_order.get('id'): Thread(target=cancel_order, args=(sym_ccxt,), kwargs={'order_id': tp_order['id'], 'client_order_id': tp_order.get('clientOrderId')}, daemon=True).start()
        if 'entry_oid' in locals() and entry_oid: Thread(target=cancel_order, args=(sym_ccxt,), kwargs={'order_id': entry_oid, 'client_order_id': entry_coid if 'entry_coid' in locals() else None}, daemon=True).start()
        op_logger.warning(f"[{symbol_ws}] Rollback process initiated. Position might need manual closing if entry was filled.")
        with real_positions_lock: real_positions.pop(symbol_ws, None)
        raise
def _execute_entry_strategy(symbol_ws, sym_ccxt, tgt_side, entry_px, tp_tgt, now):
    """진입 전략 실행 (주문 실행, 상태 저장)"""
    global entry_in_progress
    with entry_lock: entry_in_progress[symbol_ws] = True
    try:
        if entry_px <= 0:
            op_logger.warning(f"[{symbol_ws}] Invalid entry price ({entry_px}). Skipping entry.")
            return
        if tp_tgt <= 0:
            op_logger.warning(f"[{symbol_ws}] Invalid TP target ({tp_tgt}). Skipping entry.")
            return

        _perform_entry_order(symbol_ws, sym_ccxt, tgt_side, entry_px, tp_tgt, now)

    except Exception as entry_e:
        op_logger.error(f"[{symbol_ws}] Entry process failed: {entry_e}", exc_info=False)
    finally:
        with entry_lock: entry_in_progress.pop(symbol_ws, None)


def process_kline_message_main_tf(symbol_ws, kline_data):
    """K-line 웹소켓 메시지를 처리하여 지표 계산, 진입 조건 확인 및 실행"""
    global real_positions, entry_in_progress

    if not update_historical_data(symbol_ws, kline_data, historical_data, data_lock, MAX_CANDLE_HISTORY): return
    is_closed = kline_data.get('x', False)

    with data_lock: df = historical_data.get(symbol_ws)
    if df is None: return

    idf = calculate_indicators(df.copy())
    if idf is None or idf.empty or len(idf) < 2: return

    try:
        last_candle = idf.iloc[-1]
        current_price = last_candle['close']
        if pd.isna(current_price):
             return
    except Exception as e:
        op_logger.error(f"[{symbol_ws}] Error accessing indicator data: {e}")
        return

    now = datetime.now(UTC)
    sym_ccxt = convert_symbol_to_ccxt(symbol_ws)

    if is_closed:
        with entry_lock: is_entry_attempted = entry_in_progress.get(symbol_ws, False)
        with real_positions_lock: position_exists = symbol_ws in real_positions

        if not is_entry_attempted and not position_exists:
            with real_positions_lock: open_position_count = len(real_positions)
            if open_position_count >= MAX_OPEN_POSITIONS:
                return

            tgt_side, tp_tgt, entry_px = _check_entry_conditions_main_tf(symbol_ws, idf)

            if tgt_side and tp_tgt is not None and tp_tgt > 0 and entry_px > 0:
                # 3분봉 MACD 히스토그램 조건 확인 (함수명 변경 및 로직 수정됨)
                if _check_3m_macd_histogram_condition(symbol_ws, sym_ccxt, tgt_side):
                    _execute_entry_strategy(symbol_ws, sym_ccxt, tgt_side, entry_px, tp_tgt, now)
            elif tgt_side:
                op_logger.warning(f"[{symbol_ws}] Entry condition met for {tgt_side.upper()} but TP target is invalid (TP:{tp_tgt}, EntryPx:{entry_px}). Skipping entry.")

def process_kline_message_3m_tf(symbol_ws, kline_data):
    """3분봉 K-line 메시지를 처리하여 MACD 반대 신호 시 포지션 종료"""
    global real_positions, total_trades

    with subscribed_symbols_3m_lock:
        if symbol_ws not in subscribed_symbols_3m:
            return

    if not update_historical_data(symbol_ws, kline_data, historical_data_3m, data_3m_lock, MAX_CANDLE_HISTORY): 
        return
    
    is_closed_3m = kline_data.get('x', False)
    if not is_closed_3m: 
        return

    with data_3m_lock:
        df_3m = historical_data_3m.get(symbol_ws)
    if df_3m is None: return

    idf_3m = calculate_indicators(df_3m.copy()) 
    if idf_3m is None or idf_3m.empty or len(idf_3m) < 2: return

    try:
        last_3m = idf_3m.iloc[-1]
        prev_3m = idf_3m.iloc[-2]
        curr_macd_line_3m = last_3m.get('MACD_line', np.nan)
        prev_macd_line_3m = prev_3m.get('MACD_line', np.nan)
        curr_macd_signal_3m = last_3m.get('MACD_signal', np.nan)
        prev_macd_signal_3m = prev_3m.get('MACD_signal', np.nan)
        current_price_3m = last_3m.get('close', np.nan)
        if any(pd.isna(v) for v in [curr_macd_line_3m, prev_macd_line_3m, curr_macd_signal_3m, prev_macd_signal_3m, current_price_3m]):
            return
    except Exception as e:
        op_logger.error(f"[{symbol_ws}] Error accessing 3m indicator data for exit check: {e}")
        return

    with real_positions_lock:
        pos_info = real_positions.get(symbol_ws)
    if not pos_info: return 

    sym_ccxt = convert_symbol_to_ccxt(symbol_ws)
    exit_signal = False
    if pos_info['side'] == 'long' and (prev_macd_line_3m >= prev_macd_signal_3m and curr_macd_line_3m < curr_macd_signal_3m): # Dead Cross
        exit_signal = True; op_logger.info(f"[{symbol_ws}] 3m MACD Dead Cross detected for LONG position. Closing.")
    elif pos_info['side'] == 'short' and (prev_macd_line_3m <= prev_macd_signal_3m and curr_macd_line_3m > curr_macd_signal_3m): # Golden Cross
        exit_signal = True; op_logger.info(f"[{symbol_ws}] 3m MACD Golden Cross detected for SHORT position. Closing.")

    if exit_signal:
        op_logger.info(f"[{symbol_ws}] Closing position due to 3m MACD opposite signal. SL/TP orders will be cancelled.")
        Thread(target=cancel_order, args=(sym_ccxt,), kwargs={'order_id': pos_info.get('sl_order_id'), 'client_order_id': pos_info.get('sl_client_order_id')}, daemon=True).start()
        Thread(target=cancel_order, args=(sym_ccxt,), kwargs={'order_id': pos_info.get('tp_order_id'), 'client_order_id': pos_info.get('tp_client_order_id')}, daemon=True).start()
        time.sleep(0.2) 

        close_side = 'sell' if pos_info['side'] == 'long' else 'buy'
        close_order = place_market_order_real(sym_ccxt, close_side, pos_info['amount'], current_price_3m)

        if close_order and close_order.get('id'):
            op_logger.info(f"[{symbol_ws}] Position closed by 3m MACD signal. Order ID: {close_order['id']}.")
            with real_positions_lock: real_positions.pop(symbol_ws, None)
            with stats_lock: total_trades += 1
            trade_logger.info(f"3M_MACD_EXIT: {close_side.upper()} {sym_ccxt}, Amount: {pos_info['amount']:.8f}, ApproxPrice: {current_price_3m:.5f}, CloseOrdID: {close_order['id']}")
            unsubscribe_from_3m_kline(symbol_ws) 
        else:
            op_logger.error(f"[{symbol_ws}] FAILED to close position by 3m MACD signal. Manual check required!")


# ==============================================================================
# 웹소켓 콜백 함수 (K-line)
# ==============================================================================
def on_message_kline(wsapp, message):
    """K-line 웹소켓 메시지 수신 시 호출되는 콜백 함수"""
    try:
        data = json.loads(message)
        if 'stream' in data and 'data' in data:
            stream_name = data['stream']
            payload = data['data']
            if payload.get('e') == 'kline': 
                symbol_upper = stream_name.split('@')[0].upper() 
                tf_from_stream = stream_name.split('_')[-1] 
                if tf_from_stream == TIMEFRAME: 
                    process_kline_message_main_tf(symbol_upper, payload['k'])
                elif tf_from_stream == TIMEFRAME_3M: 
                    process_kline_message_3m_tf(symbol_upper, payload['k'])

        elif 'result' in data and data.get('id'):
            op_logger.info(f"K-line WebSocket subscription response: {data}")
        elif 'e' in data and data['e'] == 'error':
             op_logger.error(f"K-line WebSocket API Error received: {data}")

    except json.JSONDecodeError:
        op_logger.error(f"K-line WebSocket JSON Decode Error: {message[:100]}")
    except Exception as e:
        op_logger.error(f"Error processing K-line WebSocket message: {e}", exc_info=True)

def on_error_kline(wsapp, error):
    """K-line 웹소켓 오류 발생 시 호출되는 콜백 함수"""
    op_logger.error(f"K-line WebSocket Error: {error}")
    if isinstance(error, ConnectionRefusedError):
        op_logger.error("Connection refused by the server. Check network or Binance status.")

def on_close_kline(wsapp, close_status_code, close_msg):
    """K-line 웹소켓 연결 종료 시 호출되는 콜백 함수"""
    global kline_websocket_running
    if not shutdown_requested:
        op_logger.warning(f"K-line WebSocket connection closed unexpectedly! Code: {close_status_code}, Msg: {close_msg}. Will attempt to reconnect.")
        kline_websocket_running = False
    else:
        op_logger.info(f"K-line WebSocket connection closed gracefully.")
        kline_websocket_running = False

def _subscribe_kline_streams(wsapp, symbols_to_subscribe_ws, timeframe):
    """지정된 심볼 목록에 대해 K-line 웹소켓 스트림을 구독"""
    if not symbols_to_subscribe_ws:
        op_logger.warning(f"No symbols provided for {timeframe} K-line subscription.")
        return False
    streams = [f"{s.lower()}@kline_{timeframe}" for s in symbols_to_subscribe_ws]
    sub_id = int(time.time())
    msg = {"method": "SUBSCRIBE", "params": streams, "id": sub_id}
    try:
        wsapp.send(json.dumps(msg))
        op_logger.info(f"K-line subscription request sent for {len(symbols_to_subscribe_ws)} {timeframe} streams (ID:{sub_id}).")
        return True
    except Exception as e:
        op_logger.error(f"Failed to send {timeframe} K-line subscription request: {e}")
        return False

def subscribe_to_3m_kline(symbol_ws, symbol_ccxt):
    """특정 심볼의 3분봉 K-line 구독 및 초기 데이터 로드"""
    global kline_wsapp, historical_data_3m, subscribed_symbols_3m
    op_logger.info(f"[{symbol_ws}] Attempting to subscribe to 3m K-line.")
    with subscribed_symbols_3m_lock:
        if symbol_ws in subscribed_symbols_3m:
            op_logger.info(f"[{symbol_ws}] Already subscribed to 3m K-line.")
            return

    fetch_limit_3m = MACD_SLOW_PERIOD + MACD_SIGNAL_PERIOD + 50
    df_3m = fetch_initial_ohlcv(symbol_ccxt, timeframe=TIMEFRAME_3M, limit=fetch_limit_3m, for_3m_chart=True)
    if df_3m is not None and not df_3m.empty:
        with data_3m_lock:
            historical_data_3m[symbol_ws] = df_3m
        op_logger.info(f"[{symbol_ws}] Initial 3m historical data fetched ({len(df_3m)} candles).")
    else:
        op_logger.warning(f"[{symbol_ws}] Failed to fetch initial 3m data. Subscription will proceed without it for now.")

    if kline_wsapp and kline_wsapp.sock and kline_wsapp.sock.connected:
        stream_to_sub = [f"{symbol_ws.lower()}@kline_{TIMEFRAME_3M}"]
        msg = {"method": "SUBSCRIBE", "params": stream_to_sub, "id": int(time.time())}
        try:
            kline_wsapp.send(json.dumps(msg))
            with subscribed_symbols_3m_lock:
                subscribed_symbols_3m.add(symbol_ws)
            op_logger.info(f"[{symbol_ws}] Sent SUBSCRIBE request for 3m K-line stream.")
        except Exception as e:
            op_logger.error(f"[{symbol_ws}] Failed to send 3m K-line SUBSCRIBE message: {e}")
    else:
        op_logger.warning(f"[{symbol_ws}] K-line WebSocket not ready for 3m subscription.")

def unsubscribe_from_3m_kline(symbol_ws):
    """특정 심볼의 3분봉 K-line 구독 해제"""
    global kline_wsapp, subscribed_symbols_3m, historical_data_3m
    op_logger.info(f"[{symbol_ws}] Attempting to unsubscribe from 3m K-line.")
    with subscribed_symbols_3m_lock:
        if symbol_ws not in subscribed_symbols_3m:
            return

    if kline_wsapp and kline_wsapp.sock and kline_wsapp.sock.connected:
        stream_to_unsub = [f"{symbol_ws.lower()}@kline_{TIMEFRAME_3M}"]
        msg = {"method": "UNSUBSCRIBE", "params": stream_to_unsub, "id": int(time.time())}
        try:
            kline_wsapp.send(json.dumps(msg))
            op_logger.info(f"[{symbol_ws}] Sent UNSUBSCRIBE request for 3m K-line stream.")
        except Exception as e:
            op_logger.error(f"[{symbol_ws}] Failed to send 3m K-line UNSUBSCRIBE message: {e}")
    
    with subscribed_symbols_3m_lock: subscribed_symbols_3m.discard(symbol_ws)
    with data_3m_lock: historical_data_3m.pop(symbol_ws, None)


def _fetch_initial_data_for_subscribed_symbols(symbols_to_fetch_ws):
    """구독된 심볼들(메인 타임프레임)에 대한 초기 과거 데이터를 로드"""
    global shutdown_requested, historical_data 
    if not symbols_to_fetch_ws:
        op_logger.warning("No symbols provided to fetch initial data for.")
        return

    op_logger.info("Fetching initial historical data for subscribed symbols...")
    fetched_count, error_count = 0, 0
    with data_lock:
        historical_data.clear()
    for symbol_ws in symbols_to_fetch_ws:
        if shutdown_requested: break
        sym_ccxt = convert_symbol_to_ccxt(symbol_ws)
        df = fetch_initial_ohlcv(sym_ccxt, TIMEFRAME) 
        if df is not None and not df.empty:
            with data_lock:
                historical_data[symbol_ws] = df
            fetched_count += 1
        else:
            error_count += 1
            op_logger.warning(f"Failed to fetch initial data for {symbol_ws}.")
        time.sleep(0.3)
    op_logger.info(f"Initial historical data fetch complete ({fetched_count} symbols OK, {error_count} errors).")

def on_open_kline_initial(wsapp):
    """K-line 웹소켓 최초 연결 성공 시 호출되는 콜백 함수"""
    global subscribed_symbols, historical_data, kline_websocket_running, shutdown_requested
    kline_websocket_running = True
    op_logger.info("K-line WebSocket initial connection opened successfully.")

    op_logger.info("Fetching initial top symbols for subscription...")
    initial_sym_ccxt = get_top_volume_symbols(TOP_N_SYMBOLS)
    if not initial_sym_ccxt:
        op_logger.error("Could not fetch initial symbols. Shutting down.");
        shutdown_requested = True; wsapp.close(); return
    initial_sym_ws = {convert_symbol_to_ws(s) for s in initial_sym_ccxt}

    if not _subscribe_kline_streams(wsapp, initial_sym_ws, TIMEFRAME):
        op_logger.error("Failed to subscribe to initial K-line streams. Shutting down.");
        shutdown_requested = True; wsapp.close(); return

    with subscribed_symbols_lock: subscribed_symbols = initial_sym_ws.copy()

    _fetch_initial_data_for_subscribed_symbols(initial_sym_ws)
    print("-" * 80 + f"\nK-line WebSocket connected ({TIMEFRAME}). Bot is now listening for market data...\n" + "-" * 80)

def on_open_kline_reconnect(wsapp):
    """K-line 웹소켓 재연결 성공 시 호출되는 콜백 함수"""
    global kline_websocket_running, subscribed_symbols, subscribed_symbols_3m
    kline_websocket_running = True
    op_logger.info("K-line WebSocket RECONNECTED successfully.")
    
    with subscribed_symbols_lock:
        current_subs = subscribed_symbols.copy()
    if not current_subs:
        op_logger.warning("Main timeframe subscription list is empty on reconnect.")
    else:
        op_logger.info(f"Resubscribing to {len(current_subs)} main timeframe K-line streams...")
        if not _subscribe_kline_streams(wsapp, current_subs, TIMEFRAME): 
            op_logger.error("Failed to resubscribe to main K-line streams. Closing connection to retry.")
            wsapp.close(); return 
    with subscribed_symbols_3m_lock: current_subs_3m = subscribed_symbols_3m.copy()
    if current_subs_3m:
        op_logger.info(f"Resubscribing to {len(current_subs_3m)} active 3m K-line streams...")
        streams_3m = [f"{s.lower()}@kline_{TIMEFRAME_3M}" for s in current_subs_3m]
        msg_3m = {"method": "SUBSCRIBE", "params": streams_3m, "id": int(time.time())}
        try: wsapp.send(json.dumps(msg_3m)); op_logger.info("3m K-line resubscription request sent.")
        except Exception as e: op_logger.error(f"Failed to send 3m K-line resubscription request: {e}")


# ==============================================================================
# 메인 실행 로직 (Main Execution Logic)
# ==============================================================================
if __name__ == "__main__":
    start_time_str = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S %Z")
    op_logger.info(f"Bot starting at: {start_time_str}")

    if SIMULATION_MODE:
        op_logger.error("SIMULATION_MODE is True. Set to False for real trading. Exiting.")
        exit()
    if not API_KEY or API_KEY == "YOUR_BINANCE_API_KEY" or not API_SECRET or API_SECRET == "YOUR_BINANCE_API_SECRET":
        op_logger.error("API Key or Secret is not set or using placeholder values. Please configure them. Exiting.")
        exit()

    op_logger.warning("="*30 + f" REAL TRADING MODE - {log_prefix} " + "="*30)
    op_logger.warning("Strategy: 15m MACD Cross (Price vs BBM) Entry + 3m MACD Histogram Filter / Fixed TP (0.8%) / Fixed SL / 3m MACD Opposite Signal Exit") # 전략 설명 업데이트
    op_logger.warning(f"Key Settings: MACD({MACD_FAST_PERIOD},{MACD_SLOW_PERIOD},{MACD_SIGNAL_PERIOD}), BBands({BBANDS_PERIOD},{BBANDS_STDDEV})")
    op_logger.warning(f"             Main TF: {TIMEFRAME}, Exit Monitor TF: {TIMEFRAME_3M}")
    op_logger.warning(f"             MaxPos={MAX_OPEN_POSITIONS}, Leverage={LEVERAGE}x, Timeframe={TIMEFRAME}, SL={1-LONG_STOP_LOSS_FACTOR:.2%}/{SHORT_STOP_LOSS_FACTOR-1:.2%}")
    op_logger.warning(f"             SymbolUpdateInterval={SYMBOL_UPDATE_INTERVAL_HOURS}h, RESTSyncInterval={REST_SYNC_INTERVAL_MINUTES}min")
    op_logger.warning("!!! THIS BOT WILL USE REAL FUNDS - MONITOR CLOSELY !!!")
    op_logger.warning("="*80)
    for i in range(3, 0, -1):
        print(f"Starting in {i}...", end='\r')
        time.sleep(1)
    print("Starting now!          ") 

    if not initialize_binance_rest():
        op_logger.error("Exiting due to CCXT REST initialization failure.")
        exit()

    op_logger.info("Running initial REST state synchronization...")
    sync_positions_with_exchange()
    op_logger.info("Initial REST sync complete.")
    log_asset_status() 

    sync_thread = Thread(target=sync_state_periodically, args=(REST_SYNC_INTERVAL_MINUTES * 60,), daemon=True)
    sync_thread.start()

    ws_url_kline = f"wss://fstream.binance.com/stream"
    reconnect_delay = 5 

    try:
        while not shutdown_requested: 
            if not kline_websocket_running: 
                op_logger.info("Attempting K-line WebSocket connection/reconnection...")
                if kline_wsapp and kline_wsapp.sock:
                    try:
                        kline_wsapp.close()
                        time.sleep(1) 
                    except Exception as close_e:
                         op_logger.warning(f"Error closing previous WebSocket connection: {close_e}")

                current_on_open = on_open_kline_initial if kline_thread is None else on_open_kline_reconnect
                kline_wsapp = websocket.WebSocketApp(
                    ws_url_kline,
                    on_open=current_on_open,
                    on_message=on_message_kline,
                    on_error=on_error_kline,
                    on_close=on_close_kline
                )

                if kline_thread is None or not kline_thread.is_alive():
                    kline_thread = Thread(target=lambda: kline_wsapp.run_forever(ping_interval=0, ping_timeout=10), daemon=True) 
                    kline_thread.start()
                    op_logger.info("New K-line WebSocket thread started. Waiting for connection...")
                else:
                    op_logger.warning("K-line WebSocket thread seems to be still alive during reconnect attempt? This might indicate an issue.")

                if symbol_update_thread is None or not symbol_update_thread.is_alive():
                    symbol_update_thread = Thread(target=update_top_symbols_periodically, args=(SYMBOL_UPDATE_INTERVAL_HOURS * 60 * 60,), daemon=True)
                    symbol_update_thread.start()
                    op_logger.info("Symbol Update thread started/restarted.")

                connect_wait_start = time.time()
                while not kline_websocket_running and time.time() - connect_wait_start < 15:
                    if shutdown_requested: break 
                    time.sleep(0.5)

                if kline_websocket_running: 
                    op_logger.info("K-line WebSocket connection established/re-established successfully.")
                    reconnect_delay = 5 
                else: 
                    op_logger.error(f"K-line WebSocket connection failed after waiting. Retrying in {reconnect_delay} seconds...")
                    if kline_wsapp and kline_wsapp.sock: 
                        kline_wsapp.close()
                    time.sleep(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 2, 60) 

            else: 
                log_asset_status() 
                time.sleep(1) 

    except KeyboardInterrupt: 
        op_logger.info("Keyboard interrupt received. Initiating graceful shutdown...")
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
        
        with subscribed_symbols_3m_lock:
            active_3m_subs = list(subscribed_symbols_3m)
        if active_3m_subs:
            op_logger.info(f"Attempting to unsubscribe from {len(active_3m_subs)} active 3m streams...")
            for sym_ws in active_3m_subs: unsubscribe_from_3m_kline(sym_ws) 

        op_logger.info("Waiting for background threads to finish (max 5 seconds)...")
        time.sleep(5) 

        op_logger.warning("Attempting to cancel ALL remaining open orders across all USDT futures markets...")
        all_cancelled_final = True
        try:
            if binance_rest: 
                markets = binance_rest.fetch_markets()
                usdt_futures_symbols = [mkt['symbol'] for mkt in markets if mkt.get('type') == 'future' and mkt.get('quote') == TARGET_ASSET]
                op_logger.info(f"Checking for open orders in {len(usdt_futures_symbols)} USDT futures markets...")
                for symbol_ccxt in usdt_futures_symbols:
                    if not cancel_open_orders_for_symbol(symbol_ccxt):
                        op_logger.error(f"Failed to cancel orders for {symbol_ccxt} during final cleanup.")
                    time.sleep(0.3) 
            else:
                 op_logger.error("CCXT REST instance is not available for final order cancellation.")
                 all_cancelled_final = False

        except Exception as cancel_all_err:
            op_logger.error(f"Error occurred during final order cancellation process: {cancel_all_err}")
            all_cancelled_final = False

        if all_cancelled_final:
            op_logger.info("Finished attempting final order cancellation.")
        else:
            op_logger.error("Potential issues during final order cancellation. MANUAL CHECK OF OPEN ORDERS IS STRONGLY ADVISED on Binance.")

        op_logger.info("Fetching final balance...")
        final_balance = get_current_balance()
        bal_str = f"{final_balance:.2f}" if final_balance is not None else "Error"
        with stats_lock: 
            trades, wins = total_trades, winning_trades
            wr = (wins / trades * 100) if trades > 0 else 0.0
        final_msg = f"Final Balance:{bal_str} {TARGET_ASSET}, Total Trades:{trades}(Delayed), Winning Trades:{wins}(Delayed), Win Rate:{wr:.2f}%"
        op_logger.info(final_msg)
        asset_logger.info(final_msg) 

        op_logger.info(f"{log_prefix} Bot shutdown complete.")
        print(f"{log_prefix} Bot shutdown complete.")
