# -*- coding: utf-8 -*-
# === 최종 버전 V2 (REST 기반 + 자동 심볼 업데이트 + K-line 재연결 + 강화된 동기화 + MACD/BBM 진입 조건 + 오류 수정) ===
# === 수정: 진입 로직 변수명 오류 수정 (target_side -> tgt_side) ===
# === 수정: 진입 전 수수료 고려 최소 이익률 체크 로직 추가 ===
# === 수정: 동기화 로직 심볼 형식 불일치 문제 수정 ===

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
BBANDS_PERIOD = 20 # 볼린저 밴드 기간
BBANDS_STDDEV = 2.0 # 볼린저 밴드 표준편차
MACD_FAST = 12 # MACD 단기 이동평균선 기간
MACD_SLOW = 26 # MACD 장기 이동평균선 기간
MACD_SIGNAL = 9 # MACD 시그널선 기간
LONG_STOP_LOSS_FACTOR = 0.99 # 롱 포지션 손절 비율 (진입가 * 0.99)
SHORT_STOP_LOSS_FACTOR = 1.01 # 숏 포지션 손절 비율 (진입가 * 1.01)
POSITION_MONITORING_DELAY_MINUTES = 5 # 포지션 진입 후 TP 업데이트 시작까지 대기 시간(분)
WHIPSAW_BLACKLIST_HOURS = 2 # 휩쏘 등으로 인한 블랙리스트 지속 시간(시간)
TP_UPDATE_THRESHOLD_PERCENT = 0.1 # TP 업데이트를 위한 최소 가격 변동률 (%)
REST_SYNC_INTERVAL_MINUTES = 5 # REST API 상태 동기화 주기(분)
SYMBOL_UPDATE_INTERVAL_HOURS = 2 # 거래 대상 심볼 목록 업데이트 주기(시간)
API_RETRY_COUNT = 3 # API 호출 실패 시 재시도 횟수
API_RETRY_DELAY_SECONDS = 2 # API 호출 재시도 간격(초)
FEE_RATE = 0.0005 # 예상 수수료율 (시장가 기준, 필요시 조정, 예: 0.05% -> 0.0005)
INITIAL_CANDLE_FETCH_LIMIT = 100 # 초기 캔들 데이터 로드 개수
MAX_CANDLE_HISTORY = 200 # 메모리에 유지할 최대 캔들 개수
KST = ZoneInfo("Asia/Seoul") # 한국 시간대
UTC = timezone.utc # UTC 시간대
pd.set_option('display.max_rows', None); pd.set_option('display.max_columns', None); pd.set_option('display.width', None) # Pandas 출력 옵션

# ==============================================================================
# 로깅 설정 (Logging Setup)
# ==============================================================================
log_dir = os.path.dirname(os.path.abspath(__file__)) # 로그 파일 저장 디렉토리
log_filename_base = "bot_log" # 로그 파일 기본 이름
log_prefix = "[REST_MACD_BBM_V2_SYNC_FIX]" # 로그 메시지 접두사 (버전 명시)

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
real_positions = {} # 현재 보유 포지션 정보 (딕셔너리: {symbol_ws: position_info})
real_positions_lock = Lock() # real_positions 접근 동기화를 위한 Lock
total_trades = 0 # 총 거래 횟수 (동기화 시 업데이트)
winning_trades = 0 # 승리 거래 횟수 (구현 필요 시 추가)
stats_lock = Lock() # 통계 변수 접근 동기화를 위한 Lock
historical_data = {} # 심볼별 과거 캔들 데이터 (딕셔너리: {symbol_ws: DataFrame})
data_lock = Lock() # historical_data 접근 동기화를 위한 Lock
blacklist = {} # 거래 금지 심볼 목록 (딕셔너리: {symbol_ws: expiry_time})
blacklist_lock = Lock() # blacklist 접근 동기화를 위한 Lock
entry_in_progress = {} # 현재 진입 시도 중인 심볼 (딕셔너리: {symbol_ws: True})
entry_lock = Lock() # entry_in_progress 접근 동기화를 위한 Lock
last_asset_log_time = datetime.now(UTC) # 마지막 자산 로그 기록 시간
kline_websocket_running = False # K-line 웹소켓 실행 상태 플래그
kline_wsapp = None # K-line 웹소켓 앱 객체
subscribed_symbols = set() # 현재 구독 중인 심볼 목록 (웹소켓 형식, e.g., 'BTCUSDT')
subscribed_symbols_lock = Lock() # subscribed_symbols 접근 동기화를 위한 Lock
shutdown_requested = False # 봇 종료 요청 플래그
kline_thread = None # K-line 웹소켓 실행 스레드
symbol_update_thread = None # 심볼 목록 업데이트 스레드
sync_thread = None # REST 상태 동기화 스레드
binance_rest = None # CCXT 바이낸스 REST API 객체

# ==============================================================================
# API 호출 재시도 헬퍼 함수 (API Call Retry Helper Function)
# ==============================================================================
def call_api_with_retry(api_call, max_retries=API_RETRY_COUNT, delay_seconds=API_RETRY_DELAY_SECONDS, error_message="API call failed"):
    """지정된 횟수만큼 API 호출을 재시도하는 함수"""
    retries = 0
    while retries < max_retries:
        try:
            return api_call() # API 호출 시도
        except (RequestTimeout, ExchangeNotAvailable, OnMaintenance, NetworkError, ExchangeError) as e: # 재시도 가능한 네트워크/거래소 오류
            retries += 1
            op_logger.warning(f"{error_message}. Retry {retries}/{max_retries} after {delay_seconds}s. Error: {e}")
            if retries < max_retries:
                time.sleep(delay_seconds)
            else:
                op_logger.error(f"{error_message}. Max retries reached.")
                raise e # 최대 재시도 횟수 도달 시 예외 발생
        except AuthenticationError as auth_e: # 인증 오류는 재시도 불가
            op_logger.error(f"Auth Error during {error_message}: {auth_e}. Cannot retry.")
            raise auth_e
        except Exception as e: # 예상치 못한 오류
            op_logger.error(f"Unexpected error during {error_message}: {e}", exc_info=True)
            raise e
    # 루프를 비정상적으로 빠져나온 경우 (이론상 발생하면 안됨)
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
            'enableRateLimit': True, # API 속도 제한 자동 처리 활성화
            'options': {
                'defaultType': 'future', # 기본 거래 타입을 선물로 설정
                'adjustForTimeDifference': True # 클라이언트-서버 시간 차이 자동 보정
            }
        })
        binance_rest.load_markets() # 마켓 정보 로드
        # 서버 시간 확인 (API 연결 및 시간 동기화 검증)
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
        # fetch_balance API 호출 (재시도 포함)
        balance = call_api_with_retry(lambda: binance_rest.fetch_balance(params={'type': 'future'}), error_message="fetch_balance")
        # 'free' 딕셔너리에서 해당 자산의 잔고 반환, 없으면 0.0
        return float(balance['free'].get(asset, 0.0))
    except Exception as e:
        # 재시도 후에도 실패 시 에러 로그 기록
        op_logger.error(f"Unexpected error fetching balance after retries: {e}")
        return 0.0

def get_top_volume_symbols(n=TOP_N_SYMBOLS):
    """거래량 상위 N개의 USDT 선물 심볼 목록을 반환 (CCXT 형식)"""
    if not binance_rest: return []
    op_logger.info(f"Fetching top {n} symbols by volume...")
    try:
        # 모든 티커 정보 조회 (재시도 포함)
        tickers = call_api_with_retry(lambda: binance_rest.fetch_tickers(), error_message="fetch_tickers")
        # USDT 선물 마켓 필터링 (심볼 형식: BASE/QUOTE:SETTLE, e.g., BTC/USDT:USDT)
        futures_tickers = {
            s: t for s, t in tickers.items()
            if '/' in s and s.endswith(f"/{TARGET_ASSET}:{TARGET_ASSET}") and t.get('quoteVolume') is not None
        }
        if not futures_tickers:
            op_logger.warning("No USDT futures tickers found.")
            return []
        # 'quoteVolume' (거래대금) 기준으로 내림차순 정렬
        sorted_tickers = sorted(futures_tickers.values(), key=lambda x: x.get('quoteVolume', 0), reverse=True)
        # 상위 N개 심볼 추출 (CCXT 형식, e.g., 'BTC/USDT')
        top_symbols_ccxt = [t['symbol'].split(':')[0] for t in sorted_tickers[:n]]
        op_logger.info(f"Fetched top {len(top_symbols_ccxt)} symbols.")
        return top_symbols_ccxt
    except Exception as e:
        # 재시도 후에도 실패 시 에러 로그 기록
        op_logger.error(f"Error fetching top symbols after retries: {e}")
        return []

def fetch_initial_ohlcv(symbol_ccxt, timeframe=TIMEFRAME, limit=INITIAL_CANDLE_FETCH_LIMIT):
    """지정된 심볼의 초기 OHLCV 데이터를 조회하여 DataFrame으로 반환"""
    if not binance_rest: return None
    try:
        # 지표 계산에 필요한 최소 캔들 수 계산 (넉넉하게 버퍼 추가)
        macd_buffer = MACD_SLOW + MACD_SIGNAL + 50
        actual_limit = max(limit, BBANDS_PERIOD + 50, macd_buffer) # 설정값과 필요값 중 큰 값 사용
        op_logger.debug(f"Fetching initial {actual_limit} candles for {symbol_ccxt} ({timeframe})...")
        # fetch_ohlcv API 호출 (재시도 포함)
        ohlcv = call_api_with_retry(lambda: binance_rest.fetch_ohlcv(symbol_ccxt, timeframe=timeframe, limit=actual_limit),
                                    error_message=f"fetch_ohlcv for {symbol_ccxt}")
        if not ohlcv:
            op_logger.warning(f"No OHLCV data returned for {symbol_ccxt}.")
            return None
        # DataFrame 생성 및 전처리
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True) # 타임스탬프 변환 (UTC 기준)
        df.set_index('timestamp', inplace=True) # 타임스탬프를 인덱스로 설정
        op_logger.debug(f"Fetched {len(df)} candles for {symbol_ccxt}.")
        return df
    except Exception as e:
        # 재시도 후에도 실패 시 에러 로그 기록
        op_logger.error(f"Error fetching OHLCV for {symbol_ccxt} after retries: {e}")
        return None

def calculate_indicators(df):
    """주어진 DataFrame에 기술적 지표(BBands, MACD)를 계산하여 추가"""
    # 지표 계산에 필요한 최소 데이터 길이 확인
    macd_req_len = MACD_SLOW + MACD_SIGNAL
    required_len = max(BBANDS_PERIOD, macd_req_len) + 50 # 버퍼 포함
    if df is None or len(df) < required_len:
        op_logger.debug(f"Not enough data for indicators: Have {len(df) if df is not None else 0}, Need ~{required_len}")
        return None
    try:
        df_copy = df.copy() # 원본 DataFrame 변경 방지
        # pandas_ta 라이브러리를 사용하여 지표 계산 및 DataFrame에 추가 (append=True)
        df_copy.ta.bbands(length=BBANDS_PERIOD, std=BBANDS_STDDEV, append=True)
        df_copy.ta.macd(fast=MACD_FAST, slow=MACD_SLOW, signal=MACD_SIGNAL, append=True)

        # pandas_ta가 생성하는 컬럼 이름 확인 (라이브러리 버전에 따라 다를 수 있음)
        bbl_col = f'BBL_{BBANDS_PERIOD}_{float(BBANDS_STDDEV)}'
        bbm_col = f'BBM_{BBANDS_PERIOD}_{float(BBANDS_STDDEV)}'
        bbu_col = f'BBU_{BBANDS_PERIOD}_{float(BBANDS_STDDEV)}'
        macd_col = f'MACD_{MACD_FAST}_{MACD_SLOW}_{MACD_SIGNAL}'
        macdh_col = f'MACDh_{MACD_FAST}_{MACD_SLOW}_{MACD_SIGNAL}' # 히스토그램 (여기서는 사용 안함)
        macds_col = f'MACDs_{MACD_FAST}_{MACD_SLOW}_{MACD_SIGNAL}' # 시그널선

        # 컬럼 이름 변경 (짧고 일관된 이름 사용)
        rename_map = {
            bbl_col: 'BBL', bbm_col: 'BBM', bbu_col: 'BBU',
            macd_col: 'MACD', macdh_col: 'MACDh', macds_col: 'MACDs'
        }
        # 실제 존재하는 컬럼만 이름 변경 시도
        existing_rename_map = {k: v for k, v in rename_map.items() if k in df_copy.columns}
        if len(existing_rename_map) < 6: # BBands(3) + MACD(3) = 6개 컬럼 확인
             op_logger.warning(f"Indicator columns not fully generated. Available: {df_copy.columns.tolist()}")
             return None
        df_copy.rename(columns=existing_rename_map, inplace=True)

        # 필요한 지표 컬럼 존재 여부 확인
        required_cols = ['BBL', 'BBM', 'BBU', 'MACD', 'MACDs']
        if not all(col in df_copy.columns for col in required_cols):
            op_logger.warning(f"Required indicator columns missing after rename. Needed: {required_cols}, Have: {df_copy.columns.tolist()}")
            return None

        # 최근 2개 캔들의 지표 값에 NaN이 있는지 확인 (진입 조건 비교에 필요)
        if len(df_copy) < 2 or df_copy[required_cols].iloc[-2:].isnull().any().any():
            op_logger.debug("Latest or previous indicator values contain NaN.")
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
        # 1. 마진 모드 설정 시도
        try:
            # set_margin_mode API 호출
            binance_rest.set_margin_mode('ISOLATED', symbol_ccxt, params={})
            op_logger.info(f"Margin mode successfully set to ISOLATED for {symbol_ccxt}.")
            time.sleep(0.2) # API 호출 간 지연
        except ccxt.ExchangeError as e:
            # 이미 격리 모드이거나 변경할 필요 없는 경우 경고 로그
            if 'No need to change margin type' in str(e) or 'already isolated' in str(e):
                op_logger.warning(f"Margin mode for {symbol_ccxt} is already ISOLATED.")
            # 포지션이 존재하여 변경 불가 시 에러 로그 및 False 반환
            elif 'position exists' in str(e):
                 op_logger.error(f"Cannot change margin mode for {symbol_ccxt}, an open position exists.")
                 return False
            else: # 그 외 거래소 오류
                op_logger.error(f"Failed to set margin mode for {symbol_ccxt}: {e}")
                return False

        # 2. 레버리지 설정 시도
        try:
            # set_leverage API 호출
            binance_rest.set_leverage(leverage, symbol_ccxt, params={})
            op_logger.info(f"Leverage successfully set to {leverage}x for {symbol_ccxt}.")
            return True # 성공 시 True 반환
        except ccxt.ExchangeError as e:
            # 이미 해당 레버리지이거나 변경할 필요 없는 경우 경고 로그 및 True 반환
            if 'No need to change leverage' in str(e):
                op_logger.warning(f"Leverage for {symbol_ccxt} is already {leverage}x.")
                return True
            else: # 그 외 거래소 오류
                op_logger.error(f"Failed to set leverage for {symbol_ccxt}: {e}")
                return False
    except (RequestTimeout, ExchangeNotAvailable, OnMaintenance) as e: # 네트워크/거래소 오류
        op_logger.warning(f"Temporary issue setting isolated margin for {symbol_ccxt}: {e}")
        return False
    except Exception as e: # 예상치 못한 오류
        op_logger.error(f"Unexpected error setting isolated margin for {symbol_ccxt}: {e}", exc_info=True)
        return False

def place_market_order_real(symbol_ccxt, side, amount, current_price=None):
    """실제 시장가 주문을 실행 (진입 또는 종료)"""
    if not binance_rest or amount <= 0:
        op_logger.error(f"[{symbol_ccxt}] Invalid arguments for market order: amount={amount}")
        return None
    try:
        # 마켓 정보 가져오기 (수량/가격 정밀도, 최소 주문 금액 등)
        mkt = binance_rest.market(symbol_ccxt)
        # 주문 수량을 거래소 정밀도에 맞게 조정
        adj_amt_str = binance_rest.amount_to_precision(symbol_ccxt, amount)
        adj_amt = float(adj_amt_str)
        if adj_amt <= 0:
            op_logger.error(f"[{symbol_ccxt}] Adjusted amount '{adj_amt_str}' is <= 0.")
            return None

        # 최소 주문 금액 확인 (선물은 보통 5 USDT)
        min_notional = mkt.get('limits', {}).get('cost', {}).get('min', 5.0)
        # 현재 가격 정보가 있고, 예상 주문 금액이 최소 금액보다 작으면 오류 처리
        if current_price and adj_amt * current_price < min_notional:
            op_logger.error(f"[{symbol_ccxt}] Estimated order value ({adj_amt * current_price:.2f}) is less than minimum required ({min_notional}). Amount: {adj_amt_str}")
            return None

        op_logger.info(f"[REAL ORDER] Attempting {side.upper()} {adj_amt_str} {symbol_ccxt} @ MARKET")
        # 사용자 정의 Client Order ID 생성 (추적 용이성)
        coid = f"bot_{uuid.uuid4().hex[:16]}"
        params = {'newClientOrderId': coid}
        # create_market_order API 호출
        order = binance_rest.create_market_order(symbol_ccxt, side, adj_amt, params=params)
        oid = order.get('id') # 거래소 주문 ID
        op_logger.info(f"[REAL ORDER PLACED] ID:{oid} CliID:{coid} Sym:{symbol_ccxt} Side:{side} ReqAmt:{adj_amt_str}")
        # 매매 로그 기록
        trade_logger.info(f"REAL MARKET ORDER: {side.upper()} {symbol_ccxt}, ReqAmt:{adj_amt_str}, OrdID:{oid}, CliOrdID:{coid}")
        # 주문 정보 반환
        return {'id': oid, 'clientOrderId': coid, 'status': order.get('status', 'open')} # 상태 포함
    except ccxt.InsufficientFunds as e: # 잔고 부족 오류
        op_logger.error(f"[ORDER FAILED] Insufficient funds for {symbol_ccxt}: {e}")
        return None
    except ccxt.ExchangeError as e: # 그 외 거래소 오류 (e.g., 최소 주문 수량 미달 등)
        op_logger.error(f"[ORDER FAILED] Exchange error placing market order for {symbol_ccxt}: {e}")
        return None
    except (RequestTimeout, ExchangeNotAvailable, OnMaintenance) as e: # 네트워크/거래소 일시 오류
        op_logger.warning(f"[ORDER FAILED] Network/Exchange issue placing market order for {symbol_ccxt}: {e}")
        return None
    except Exception as e: # 예상치 못한 오류
        op_logger.error(f"[ORDER FAILED] Unexpected error placing market order for {symbol_ccxt}: {e}", exc_info=True)
        return None

def place_stop_market_order(symbol_ccxt, side, stop_price, amount):
    """실제 STOP_MARKET 주문을 실행 (손절 주문)"""
    if not binance_rest or amount <= 0 or stop_price <= 0:
        op_logger.error(f"[{symbol_ccxt}] Invalid arguments for stop market order: amount={amount}, stop_price={stop_price}")
        return None
    try:
        # 수량과 가격을 거래소 정밀도에 맞게 조정
        amt_str = binance_rest.amount_to_precision(symbol_ccxt, amount)
        sp_str = binance_rest.price_to_precision(symbol_ccxt, stop_price)
        amt = float(amt_str)
        if amt <= 0:
            op_logger.error(f"[{symbol_ccxt}] SL Adjusted amount '{amt_str}' is <= 0.")
            return None

        op_logger.info(f"[REAL SL ORDER] Attempting {side.upper()} {amt_str} {symbol_ccxt} if price hits {sp_str}")
        # 사용자 정의 Client Order ID 생성
        coid = f"sl_{uuid.uuid4().hex[:16]}"
        # 파라미터 설정: stopPrice(트리거 가격), reduceOnly(포지션 감소 전용)
        params = {'stopPrice': sp_str, 'reduceOnly': True, 'newClientOrderId': coid}
        # create_order API 호출 (유형: STOP_MARKET)
        order = binance_rest.create_order(symbol_ccxt, 'STOP_MARKET', side, amt, None, params)
        oid = order.get('id')
        op_logger.info(f"[REAL SL PLACED] ID:{oid} CliID:{coid} Sym:{symbol_ccxt} Side:{side} StopPx:{sp_str} Amt:{amt_str}")
        # 매매 로그 기록
        trade_logger.info(f"REAL SL ORDER SET: {side.upper()} {symbol_ccxt}, Amt:{amt_str}, StopPx:{sp_str}, OrdID:{oid}, CliOrdID:{coid}")
        # 주문 정보 반환
        return {'id': oid, 'clientOrderId': coid}
    except ccxt.ExchangeError as e: # 거래소 오류
        op_logger.error(f"[SL FAILED] Exchange error placing stop market order for {symbol_ccxt}: {e}")
        return None
    except (RequestTimeout, ExchangeNotAvailable, OnMaintenance) as e: # 네트워크/거래소 일시 오류
        op_logger.warning(f"[SL FAILED] Network/Exchange issue placing stop market order for {symbol_ccxt}: {e}")
        return None
    except Exception as e: # 예상치 못한 오류
        op_logger.error(f"[SL FAILED] Unexpected error placing stop market order for {symbol_ccxt}: {e}", exc_info=True)
        return None

def place_take_profit_market_order(symbol_ccxt, side, stop_price, amount):
    """실제 TAKE_PROFIT_MARKET 주문을 실행 (익절 주문)"""
    if not binance_rest or amount <= 0 or stop_price <= 0:
        op_logger.error(f"[{symbol_ccxt}] Invalid arguments for take profit market order: amount={amount}, stop_price={stop_price}")
        return None
    try:
        # 수량과 가격을 거래소 정밀도에 맞게 조정
        amt_str = binance_rest.amount_to_precision(symbol_ccxt, amount)
        sp_str = binance_rest.price_to_precision(symbol_ccxt, stop_price)
        amt = float(amt_str)
        if amt <= 0:
            op_logger.error(f"[{symbol_ccxt}] TP Adjusted amount '{amt_str}' is <= 0.")
            return None

        op_logger.info(f"[REAL TP ORDER] Attempting {side.upper()} {amt_str} {symbol_ccxt} if price hits {sp_str}")
        # 사용자 정의 Client Order ID 생성
        coid = f"tp_{uuid.uuid4().hex[:16]}"
        # 파라미터 설정: stopPrice(트리거 가격), reduceOnly(포지션 감소 전용)
        params = {'stopPrice': sp_str, 'reduceOnly': True, 'newClientOrderId': coid}
        # create_order API 호출 (유형: TAKE_PROFIT_MARKET)
        order = binance_rest.create_order(symbol_ccxt, 'TAKE_PROFIT_MARKET', side, amt, None, params)
        oid = order.get('id')
        op_logger.info(f"[REAL TP PLACED] ID:{oid} CliID:{coid} Sym:{symbol_ccxt} Side:{side} StopPx:{sp_str} Amt:{amt_str}")
        # 매매 로그 기록
        trade_logger.info(f"REAL TP ORDER SET: {side.upper()} {symbol_ccxt}, Amt:{amt_str}, StopPx:{sp_str}, OrdID:{oid}, CliOrdID:{coid}")
        # 주문 정보 반환
        return {'id': oid, 'clientOrderId': coid}
    except ccxt.ExchangeError as e: # 거래소 오류
        op_logger.error(f"[TP FAILED] Exchange error placing take profit market order for {symbol_ccxt}: {e}")
        return None
    except (RequestTimeout, ExchangeNotAvailable, OnMaintenance) as e: # 네트워크/거래소 일시 오류
        op_logger.warning(f"[TP FAILED] Network/Exchange issue placing take profit market order for {symbol_ccxt}: {e}")
        return None
    except Exception as e: # 예상치 못한 오류
        op_logger.error(f"[TP FAILED] Unexpected error placing take profit market order for {symbol_ccxt}: {e}", exc_info=True)
        return None

def cancel_order(symbol_ccxt, order_id=None, client_order_id=None):
    """지정된 주문 ID 또는 Client Order ID를 사용하여 주문을 취소"""
    if not binance_rest or (not order_id and not client_order_id):
        op_logger.warning(f"[{symbol_ccxt}] Cancel order called with no ID.")
        return True # 취소할 ID가 없으면 성공으로 간주
    target_id_str = f"ID={order_id}" if order_id else f"CliID={client_order_id}"
    op_logger.info(f"Attempting to cancel order {target_id_str} for {symbol_ccxt}...")
    try:
        if order_id:
            binance_rest.cancel_order(order_id, symbol_ccxt) # 거래소 ID로 취소
        else:
            # Client Order ID로 취소 시 파라미터 필요
            binance_rest.cancel_order(client_order_id, symbol_ccxt, params={'origClientOrderId': client_order_id})
        op_logger.info(f"Successfully cancelled order {target_id_str}.")
        return True
    except OrderNotFound: # 주문을 찾을 수 없음 (이미 체결/취소됨)
        op_logger.warning(f"Order {target_id_str} not found, likely already closed/cancelled.")
        return True # 성공으로 간주
    except (ExchangeNotAvailable, OnMaintenance, RequestTimeout) as e: # 네트워크/거래소 일시 오류
        op_logger.error(f"Cannot cancel order {target_id_str} due to temporary issue: {e}")
        return False # 실패
    except RateLimitExceeded as e: # API 속도 제한 초과
        op_logger.error(f"Rate limit exceeded cancelling order {target_id_str}: {e}")
        return False # 실패
    except ccxt.ExchangeError as e: # 그 외 거래소 오류
        # 주문이 존재하지 않는다는 오류 코드(-2011) 또는 메시지 포함 시 성공으로 간주
        if 'Order does not exist' in str(e) or '-2011' in str(e):
            op_logger.warning(f"Order {target_id_str} likely already gone (ExchangeError).")
            return True
        else:
            op_logger.error(f"Failed to cancel order {target_id_str}: {e}")
            return False # 실패
    except Exception as e: # 예상치 못한 오류
        op_logger.error(f"Unexpected error cancelling order {target_id_str}: {e}", exc_info=True)
        return False # 실패

def cancel_open_orders_for_symbol(symbol_ccxt):
    """지정된 심볼의 모든 미체결 주문을 취소"""
    if not binance_rest: return False
    op_logger.warning(f"Attempting to cancel ALL open orders for {symbol_ccxt}...")
    cancelled_count, success = 0, True
    try:
        # fetch_open_orders API 호출 (재시도 포함)
        open_orders = call_api_with_retry(lambda: binance_rest.fetch_open_orders(symbol_ccxt),
                                          error_message=f"fetch_open_orders for {symbol_ccxt}")
    except Exception as fetch_e:
        op_logger.error(f"Error fetching open orders for {symbol_ccxt} to cancel: {fetch_e}")
        return False # 주문 조회 실패 시 취소 불가

    if not open_orders:
        op_logger.info(f"No open orders found for {symbol_ccxt}.")
        return True # 취소할 주문 없으면 성공

    op_logger.info(f"Found {len(open_orders)} open orders for {symbol_ccxt}. Cancelling them...")
    # 각 주문에 대해 cancel_order 함수 호출
    for o in open_orders:
        if not cancel_order(symbol_ccxt, order_id=o.get('id'), client_order_id=o.get('clientOrderId')):
            success = False # 하나라도 취소 실패 시 False
        else:
            cancelled_count += 1
        time.sleep(0.2) # API 호출 간 지연

    op_logger.info(f"Finished cancellation attempt for {symbol_ccxt}. Cancelled {cancelled_count}/{len(open_orders)} orders.")
    return success

def check_symbol_in_blacklist(symbol_ws):
    """주어진 심볼이 블랙리스트에 있는지, 유효기간이 남았는지 확인"""
    with blacklist_lock: # Lock으로 보호
        expiry = blacklist.get(symbol_ws)
    if expiry and datetime.now(UTC) < expiry: # 만료 시간이 있고, 현재 시간보다 미래이면
        return True # 블랙리스트 유효
    elif expiry: # 만료 시간이 지났으면
        op_logger.info(f"Blacklist expired for {symbol_ws}. Removing from list.")
        with blacklist_lock: # Lock으로 보호
             # 만료된 항목 제거 (get으로 확인했으므로 존재 보장됨)
            if symbol_ws in blacklist:
                 del blacklist[symbol_ws]
        return False # 블랙리스트 만료
    return False # 블랙리스트에 없음

def add_to_blacklist(symbol_ws, reason=""):
    """주어진 심볼을 블랙리스트에 추가"""
    symbol_clean = symbol_ws.split(':')[0] # ':' 이후 부분 제거 (혹시 모를 경우 대비)
    expiry = datetime.now(UTC) + timedelta(hours=WHIPSAW_BLACKLIST_HOURS) # 만료 시간 계산
    with blacklist_lock: # Lock으로 보호
        blacklist[symbol_clean] = expiry
    op_logger.warning(f"Blacklisted {symbol_clean} until {expiry.astimezone(KST):%Y-%m-%d %H:%M:%S KST}. Reason: {reason}")

def log_asset_status():
    """현재 자산 상태 (잔고, 활성 포지션, 통계)를 주기적으로 로깅"""
    global last_asset_log_time
    now = datetime.now(UTC)
    # 마지막 로그 시간으로부터 1시간 이상 경과 시 실행
    if now - last_asset_log_time >= timedelta(hours=1):
        try:
            bal = get_current_balance() # 현재 잔고 조회
            bal_str = f"{bal:.2f}" if bal is not None else "Error"
            with stats_lock: # 통계 변수 접근 (Lock 사용)
                trades, wins = total_trades, winning_trades
            win_rate = (wins / trades * 100) if trades > 0 else 0.0 # 승률 계산
            active_pos = []
            num_active = 0
            with real_positions_lock: # 포지션 정보 접근 (Lock 사용)
                active_pos = list(real_positions.keys()) # 활성 포지션 심볼 목록
                num_active = len(real_positions) # 활성 포지션 개수
            # 자산 로그 기록
            asset_logger.info(f"Balance:{bal_str} {TARGET_ASSET}, Active Positions:{num_active} {active_pos}, Total Trades:{trades}(Delayed), Winning Trades:{wins}(Delayed), Win Rate:{win_rate:.2f}%")
            last_asset_log_time = now # 마지막 로그 시간 업데이트
        except Exception as e:
            asset_logger.error(f"Error logging asset status: {e}", exc_info=True)

# ==============================================================================
# 상태 동기화 로직 (State Synchronization Logic) - 수정됨
# ==============================================================================
def sync_positions_with_exchange():
    """REST API를 사용하여 봇 내부 상태와 실제 거래소 포지션 상태를 동기화"""
    global real_positions, total_trades # 전역 변수 사용 명시
    op_logger.info("[SYNC_REST] Starting state synchronization (REST API Based)...")
    if not binance_rest:
        op_logger.error("[SYNC_REST] CCXT REST instance not ready. Skipping sync.")
        return

    try:
        # 1. 거래소에서 현재 모든 포지션 정보 가져오기
        op_logger.debug("[SYNC_REST] Fetching current positions from exchange via REST API...")
        # fetch_positions API 호출 (재시도 포함)
        exchange_positions_raw = call_api_with_retry(lambda: binance_rest.fetch_positions(), error_message="fetch_positions")
        time.sleep(0.1) # API 호출 간 지연

        # 2. 거래소 포지션 정보를 봇 내부 형식에 맞게 가공 (딕셔너리: {symbol_ws: info})
        exchange_pos_dict = {}
        for pos in exchange_positions_raw:
            try:
                # 'info' 필드에서 실제 포지션 수량('positionAmt') 가져오기
                amount = float(pos.get('info', {}).get('positionAmt', 0))
                # 포지션 수량이 0에 가까우면 무시 (부동소수점 오차 감안)
                if abs(amount) < 1e-9:
                    continue

                symbol_ccxt_raw = pos.get('symbol') # 거래소에서 반환된 심볼 (e.g., BTC/USDT, BTC/USDT:USDT)
                if not symbol_ccxt_raw: continue # 심볼 없으면 스킵

                # *** FIX: 심볼 형식 일관성 처리 ***
                # CCXT는 보통 'BTC/USDT' 형식을 반환하지만, API 응답 원본은 다를 수 있음
                # ':'가 포함된 경우 제거하고, '/'를 기준으로 분리하여 웹소켓 형식(BTCUSDT) 생성
                symbol_parts = symbol_ccxt_raw.split(':') # ':' 제거
                symbol_ccxt_clean = symbol_parts[0] # 'BTC/USDT' 부분
                if '/' not in symbol_ccxt_clean: continue # '/' 없으면 예상 형식 아님

                base_asset = symbol_ccxt_clean.split('/')[0]
                quote_asset = symbol_ccxt_clean.split('/')[1]

                # 타겟 자산(USDT) 마켓인지 확인
                if quote_asset == TARGET_ASSET:
                    symbol_ws = base_asset + quote_asset # 웹소켓 형식 (e.g., BTCUSDT)
                    exchange_pos_dict[symbol_ws] = {
                        'side': 'long' if amount > 0 else 'short', # 포지션 방향
                        'amount': abs(amount), # 포지션 수량 (절대값)
                        'entry_price': float(pos.get('entryPrice', 0)), # 진입 가격
                        'symbol_ccxt': symbol_ccxt_clean, # 정제된 CCXT 형식 심볼 ('BTC/USDT') 저장
                        'unrealized_pnl': float(pos.get('unrealizedPnl', 0)) # 미실현 손익
                    }
                # *** FIX END ***

            except Exception as parse_err:
                op_logger.error(f"[SYNC_REST] Error parsing exchange position data: Raw Symbol='{pos.get('symbol')}', Info='{pos.get('info')}', Error: {parse_err}")


        # 3. 봇 내부 포지션 정보 복사 (비교용)
        with real_positions_lock: # Lock으로 보호
            local_pos_dict = real_positions.copy()

        # 4. 로컬 상태와 거래소 상태 비교 (이제 exchange_pos_dict의 키는 'BTCUSDT' 형식)
        local_pos_symbols = set(local_pos_dict.keys())
        exchange_pos_symbols = set(exchange_pos_dict.keys())
        L_only = local_pos_symbols - exchange_pos_symbols # 로컬에만 있는 심볼
        E_only = exchange_pos_symbols - local_pos_symbols # 거래소에만 있는 심볼
        Both = local_pos_symbols.intersection(exchange_pos_symbols) # 양쪽 모두에 있는 심볼
        op_logger.info(f"[SYNC_REST] State Check: Local_Only={len(L_only)}, Exchange_Only={len(E_only)}, Both={len(Both)}")
        if L_only: op_logger.info(f"[SYNC_REST] Local Only Symbols: {L_only}")
        if E_only: op_logger.info(f"[SYNC_REST] Exchange Only Symbols: {E_only}") # 이제 'BTCUSDT' 형식으로 나와야 함
        if Both: op_logger.info(f"[SYNC_REST] Symbols in Both: {Both}")


        # 4.1 로컬 O / 거래소 X (Local Only): 로컬 상태 오류 가능성 -> 재확인 후 로컬 상태 제거 및 주문 취소
        if L_only:
            op_logger.warning(f"[SYNC_REST][WARN] Local positions not found on exchange (Initial Check): {L_only}")
            for symbol_ws in L_only:
                # *** FIX: symbol_ws를 사용하여 ccxt 형식 생성 확인 ***
                symbol_ccxt = symbol_ws.replace(TARGET_ASSET, f'/{TARGET_ASSET}') # 'BTCUSDT' -> 'BTC/USDT'
                remove_local_state = False
                try:
                    # 해당 심볼만 지정하여 포지션 정보 다시 조회 (더 정확한 확인)
                    op_logger.debug(f"Re-checking position specifically for {symbol_ws} using fetch_positions(symbols=['{symbol_ccxt}'])")
                    specific_pos_list = call_api_with_retry(
                        lambda: binance_rest.fetch_positions(symbols=[symbol_ccxt]), # 'BTC/USDT' 사용
                        error_message=f"fetch_positions(symbol) for {symbol_ws}"
                    )
                    position_confirmed_gone = False
                    if isinstance(specific_pos_list, list):
                        if not specific_pos_list: # 빈 리스트 반환 시 포지션 없음 확인
                            position_confirmed_gone = True
                        else: # 리스트에 내용이 있으면 수량 재확인
                            try:
                                amount_recheck = float(specific_pos_list[0].get('info', {}).get('positionAmt', 0))
                                if abs(amount_recheck) < 1e-9: # 수량이 0이면 포지션 없음 확인
                                    position_confirmed_gone = True
                                else:
                                     # 이 경우는 거의 발생하지 않아야 함 (초기 fetch_positions와 개별 fetch_positions 결과 불일치)
                                     op_logger.error(f"[SYNC_REST] Discrepancy! Position for {symbol_ws} exists on re-check but not in initial fetch. Amount: {amount_recheck}. Manual check advised.")
                            except Exception as parse_e:
                                op_logger.error(f"[SYNC_REST] Error parsing re-checked position for {symbol_ws}: {parse_e}")
                                # 파싱 오류 시 안전하게 로컬 상태 제거 가정
                                position_confirmed_gone = True
                    else:
                         op_logger.error(f"[SYNC_REST] Unexpected response type during re-check for {symbol_ws}: {type(specific_pos_list)}. Assuming closure.")
                         position_confirmed_gone = True


                    if position_confirmed_gone:
                        op_logger.warning(f"[SYNC_REST] Confirmed no active position for {symbol_ws} on exchange after re-check. Removing local state.")
                        remove_local_state = True
                    # else: # 재확인 시 포지션이 존재하면 로컬 상태 유지 (드문 경우)
                    #    op_logger.error(f"[SYNC_REST] Discrepancy persists for {symbol_ws} after re-check. Position exists on exchange but wasn't in the initial bulk fetch. NOT removing local state. Manual check needed.")

                except Exception as recheck_e:
                    op_logger.error(f"[SYNC_REST] Error re-checking position for {symbol_ws}: {recheck_e}. Assuming closure and removing local state as a precaution.")
                    remove_local_state = True # 재확인 실패 시 안전하게 로컬 상태 제거

                # 로컬 상태 제거 결정 시
                if remove_local_state:
                    removed_info = None
                    with real_positions_lock: # Lock으로 보호
                        if symbol_ws in real_positions:
                            removed_info = real_positions.pop(symbol_ws, None) # 로컬 딕셔너리에서 제거
                    # Lock 해제 후 작업
                    if removed_info:
                        op_logger.info(f"[{symbol_ws}] Local position state removed successfully.")
                        # 통계 업데이트 (종료된 거래로 간주)
                        with stats_lock:
                            total_trades += 1
                            # TODO: 승패 여부 판단 로직 추가 필요 (removed_info 활용)
                        # 관련 미체결 주문 취소 시도 (SL/TP 등)
                        op_logger.info(f"[{symbol_ws}] Attempting to cancel any orphaned open orders for removed local position...")
                        cancel_open_orders_for_symbol(symbol_ccxt) # 'BTC/USDT' 형식 사용
                    else:
                         op_logger.warning(f"[{symbol_ws}] Tried to remove local state, but it was already gone.")


        # 4.2 거래소 O / 로컬 X (Exchange Only): 봇이 추적하지 못하는 포지션 -> 즉시 종료 시도 및 블랙리스트
        if E_only:
            op_logger.error(f"[SYNC_REST][CRITICAL] Untracked positions found on exchange: {E_only}. These positions were likely created outside the bot or due to a previous error.")
            for symbol_ws in E_only: # 이제 'BTCUSDT' 형식
                op_logger.error(f"[SYNC_REST][ACTION] -> Attempting to close untracked position for {symbol_ws} immediately and blacklisting.")
                pos_info = exchange_pos_dict.get(symbol_ws)
                time.sleep(0.1)
                if not pos_info:
                    op_logger.error(f"[SYNC_REST] Could not get info for untracked position {symbol_ws} from exchange data (logic error?).")
                    continue

                # *** FIX: pos_info에서 올바른 ccxt 심볼 사용 ***
                symbol_ccxt = pos_info.get('symbol_ccxt') # 'BTC/USDT' 형식
                if not symbol_ccxt:
                     op_logger.error(f"[SYNC_REST] Cannot get symbol_ccxt for untracked position {symbol_ws}. Skipping closure.")
                     continue

                # 1. 해당 심볼의 모든 미체결 주문 취소 (혹시 모를 관련 주문 제거)
                cancel_open_orders_for_symbol(symbol_ccxt) # 'BTC/USDT' 형식 사용
                time.sleep(0.5)

                # 2. 시장가로 포지션 종료 시도
                try:
                    # 현재가 조회 (최소 주문 금액 체크용)
                    ticker = call_api_with_retry(lambda: binance_rest.fetch_ticker(symbol_ccxt), error_message=f"fetch_ticker for closing {symbol_ws}")
                    current_price = ticker['last'] if ticker and 'last' in ticker else None
                    # 시장가 종료 주문 실행
                    close_order_result = place_market_order_real(
                        symbol_ccxt, # 'BTC/USDT' 형식 사용
                        'sell' if pos_info['side'] == 'long' else 'buy', # 반대 방향으로 주문
                        pos_info['amount'],
                        current_price
                    )

                    if not close_order_result or not close_order_result.get('id'):
                        op_logger.error(f"[SYNC_REST] Failed to place market close order for untracked position {symbol_ws}. MANUAL INTERVENTION REQUIRED.")
                    else:
                        op_logger.info(f"[SYNC_REST] Successfully placed market close order for untracked position {symbol_ws}. Order ID: {close_order_result.get('id')}")
                        # 통계 업데이트 (종료된 거래로 간주)
                        with stats_lock:
                            total_trades += 1
                            # TODO: 승패 여부 판단 로직 추가 필요 (수수료 고려 어려움)
                        # 로컬 상태는 원래 없었으므로 pop 불필요

                except Exception as close_err:
                    op_logger.error(f"[SYNC_REST] Error occurred while trying to close untracked position {symbol_ws}: {close_err}. MANUAL INTERVENTION REQUIRED.")

                # 3. 블랙리스트 추가 (재진입 방지) - 웹소켓 형식 사용
                add_to_blacklist(symbol_ws, reason="Untracked position closed via REST Sync")
                time.sleep(0.5)


        # 4.3 양쪽 모두 존재 (Both): 상세 비교 및 SL/TP 주문 상태 확인/보정 (이 부분은 변경 필요 없음)
        if Both:
            op_logger.info(f"[SYNC_REST] Verifying {len(Both)} positions present in both local state and on exchange...")
            for symbol_ws in Both: # 'BTCUSDT' 형식
                local_info = local_pos_dict.get(symbol_ws)
                exchange_info = exchange_pos_dict.get(symbol_ws)
                if not local_info or not exchange_info:
                    op_logger.error(f"[SYNC_REST] Inconsistency: Symbol {symbol_ws} in 'Both' list but missing info in local or exchange dict.")
                    continue

                # *** FIX: exchange_info에서 올바른 ccxt 심볼 사용 ***
                symbol_ccxt = exchange_info.get('symbol_ccxt') # 'BTC/USDT' 형식
                if not symbol_ccxt:
                     op_logger.error(f"[SYNC_REST] Cannot get symbol_ccxt for position {symbol_ws} in 'Both'. Skipping order check.")
                     continue

                # 수량 또는 방향 불일치 확인 (심각한 오류)
                amount_diff = abs(local_info.get('amount', 0) - exchange_info.get('amount', 0)) > 1e-6
                side_mismatch = local_info.get('side') != exchange_info.get('side')

                if amount_diff or side_mismatch:
                    op_logger.warning(f"[SYNC_REST][DISCREPANCY] Mismatch found for {symbol_ws}!")
                    op_logger.warning(f"  Local State : Side={local_info.get('side')}, Amount={local_info.get('amount', 0):.8f}")
                    op_logger.warning(f"  Exchange    : Side={exchange_info.get('side')}, Amount={exchange_info.get('amount', 0):.8f}")
                    op_logger.warning(f"  This indicates a potential issue. NOT auto-correcting amount/side. Manual check advised.")

                # SL/TP 주문 상태 확인 (로컬에 ID가 있는데 거래소에 없는 경우)
                try:
                    op_logger.debug(f"[SYNC_REST] Checking open orders for existing position {symbol_ws}...")
                    # 해당 심볼의 미체결 주문 목록 조회 ('BTC/USDT' 형식 사용)
                    open_orders = call_api_with_retry(lambda: binance_rest.fetch_open_orders(symbol_ccxt),
                                                      error_message=f"fetch_open_orders for sync check {symbol_ws}")
                    open_order_ids = {str(o['id']) for o in open_orders} # 조회된 미체결 주문 ID 집합

                    # 로컬 SL 주문 ID 확인
                    sl_id = str(local_info.get('sl_order_id')) if local_info.get('sl_order_id') else None
                    if sl_id and sl_id not in open_order_ids:
                        op_logger.warning(f"[SYNC_REST] Local SL order ID {sl_id} for {symbol_ws} not found among open orders on exchange. Clearing SL info locally.")
                        with real_positions_lock:
                            if symbol_ws in real_positions and str(real_positions[symbol_ws].get('sl_order_id')) == sl_id:
                                real_positions[symbol_ws]['sl_order_id'] = None
                                real_positions[symbol_ws]['sl_client_order_id'] = None

                    # 로컬 TP 주문 ID 확인
                    tp_id = str(local_info.get('tp_order_id')) if local_info.get('tp_order_id') else None
                    if tp_id and tp_id not in open_order_ids:
                        op_logger.warning(f"[SYNC_REST] Local TP order ID {tp_id} for {symbol_ws} not found among open orders on exchange. Clearing TP info locally.")
                        with real_positions_lock:
                            if symbol_ws in real_positions and str(real_positions[symbol_ws].get('tp_order_id')) == tp_id:
                                real_positions[symbol_ws]['tp_order_id'] = None
                                real_positions[symbol_ws]['tp_client_order_id'] = None
                                real_positions[symbol_ws]['current_tp_price'] = None

                except Exception as order_check_e:
                    op_logger.error(f"[SYNC_REST] Error checking open orders for {symbol_ws} during sync: {order_check_e}")

        op_logger.info("[SYNC_REST] REST state synchronization finished.")

    except AuthenticationError: # 인증 오류 발생 시 봇 종료
        op_logger.error("[SYNC_REST] Authentication error during REST sync! Shutting down bot.")
        global shutdown_requested
        shutdown_requested = True
    except Exception as e: # 그 외 예외 발생 시 에러 로그 기록
        op_logger.error(f"[SYNC_REST] Critical error during REST state synchronization: {e}", exc_info=True)
        time.sleep(60)


def sync_state_periodically(interval_seconds):
    """주기적으로 상태 동기화 함수를 호출하는 스레드 함수"""
    global shutdown_requested
    op_logger.info(f"REST Sync thread started. Sync interval: {interval_seconds} seconds.")
    while not shutdown_requested: # 종료 요청 없을 시 반복
        try:
            op_logger.debug(f"REST Sync thread: Waiting for {interval_seconds} seconds until next sync...")
            # 종료 요청을 확인하며 대기
            wait_until = time.time() + interval_seconds
            while time.time() < wait_until and not shutdown_requested:
                time.sleep(1) # 1초 간격으로 종료 플래그 확인
            if shutdown_requested: break # 종료 요청 시 루프 탈출

            # 동기화 함수 호출
            sync_positions_with_exchange()

        except Exception as e:
            op_logger.error(f"Error in REST sync loop: {e}", exc_info=True)
            time.sleep(60) # 예외 발생 시 60초 대기 후 재시도
    op_logger.info("REST Sync thread finished.")


# ==============================================================================
# 심볼 목록 주기적 업데이트 로직 (Symbol List Periodic Update Logic)
# ==============================================================================
def update_top_symbols_periodically(interval_seconds):
    """주기적으로 거래량 상위 심볼 목록을 업데이트하고 웹소켓 구독/구독 해제"""
    global subscribed_symbols, historical_data, kline_websocket_running, kline_wsapp, shutdown_requested # 전역 변수 사용
    op_logger.info(f"Symbol Update thread started. Update interval: {interval_seconds} seconds.")
    while not shutdown_requested: # 종료 요청 없을 시 반복
        try:
            op_logger.debug(f"Symbol Update thread: Waiting for {interval_seconds} seconds until next update...")
            # 종료 요청을 확인하며 대기
            wait_until = time.time() + interval_seconds
            while time.time() < wait_until and not shutdown_requested:
                time.sleep(1)
            if shutdown_requested: break

            # K-line 웹소켓 연결 상태 확인
            if not kline_websocket_running or not kline_wsapp or not kline_wsapp.sock or not kline_wsapp.sock.connected:
                op_logger.warning("[Symbol Update] K-line WebSocket is not ready. Skipping symbol update cycle.")
                continue # 웹소켓 연결 안되어 있으면 업데이트 건너뛰기

            op_logger.info("[Symbol Update] Starting periodic symbol update process...")
            # 1. 새로운 거래량 상위 심볼 목록 가져오기
            new_sym_ccxt = get_top_volume_symbols(TOP_N_SYMBOLS)
            if not new_sym_ccxt:
                op_logger.warning("[Symbol Update] Failed to fetch new top symbols. Skipping update cycle.")
                continue # 심볼 목록 조회 실패 시 건너뛰기
            # 웹소켓 형식으로 변환 (e.g., BTC/USDT -> BTCUSDT)
            new_sym_ws = {s.replace(f'/{TARGET_ASSET}', TARGET_ASSET) for s in new_sym_ccxt}

            # 2. 현재 구독 목록과 비교하여 추가/제거할 심볼 결정
            with subscribed_symbols_lock: # Lock으로 보호
                current_subs = subscribed_symbols.copy()
            to_add = new_sym_ws - current_subs # 새로 추가할 심볼
            to_remove = current_subs - new_sym_ws # 제거할 심볼

            # 3. 제거할 심볼 처리
            if to_remove:
                op_logger.info(f"[Symbol Update] Symbols to remove: {to_remove}")
                # 웹소켓 구독 해제 메시지 생성
                streams_to_unsub = [f"{s.lower()}@kline_{TIMEFRAME}" for s in to_remove]
                if streams_to_unsub:
                    msg = {"method": "UNSUBSCRIBE", "params": streams_to_unsub, "id": int(time.time())}
                    try:
                        # 웹소켓 연결 상태 재확인 후 메시지 전송
                        if kline_wsapp and kline_wsapp.sock and kline_wsapp.sock.connected:
                            kline_wsapp.send(json.dumps(msg))
                            op_logger.info(f"[Symbol Update] Sent UNSUBSCRIBE request for {len(to_remove)} streams.")
                        else:
                            op_logger.warning("[Symbol Update] K-line WebSocket disconnected before sending UNSUBSCRIBE.")
                    except Exception as e:
                        op_logger.error(f"[Symbol Update] Failed to send UNSUBSCRIBE message: {e}")

                # 내부 구독 목록에서 제거
                with subscribed_symbols_lock: # Lock으로 보호
                    subscribed_symbols -= to_remove
                # 과거 데이터 딕셔너리에서 제거
                removed_count = 0
                with data_lock: # Lock으로 보호
                    for s in to_remove:
                        if historical_data.pop(s, None) is not None:
                            removed_count += 1
                op_logger.info(f"[Symbol Update] Removed historical data for {removed_count} symbols.")

            # 4. 추가할 심볼 처리
            if to_add:
                op_logger.info(f"[Symbol Update] Symbols to add: {to_add}")
                fetched_count, error_count = 0, 0
                added_to_data = set() # 실제로 데이터 로드 성공 및 구독할 심볼
                # 각 심볼에 대해 초기 데이터 로드
                for symbol_ws in to_add:
                    if shutdown_requested: break # 종료 요청 시 중단
                    ccxt_symbol = symbol_ws.replace(TARGET_ASSET, f'/{TARGET_ASSET}')
                    # 지표 계산에 필요한 충분한 데이터 로드
                    df = fetch_initial_ohlcv(ccxt_symbol, TIMEFRAME, limit=max(INITIAL_CANDLE_FETCH_LIMIT, MACD_SLOW+MACD_SIGNAL+50))
                    if df is not None and not df.empty:
                        with data_lock: # Lock으로 보호
                            historical_data[symbol_ws] = df # 데이터 저장
                        fetched_count += 1
                        added_to_data.add(symbol_ws) # 성공 목록에 추가
                    else:
                        error_count += 1
                        op_logger.warning(f"[Symbol Update] Failed to fetch initial data for new symbol {symbol_ws}.")
                    time.sleep(0.3) # API 호출 간 지연
                op_logger.info(f"[Symbol Update] Fetched initial data for {fetched_count} new symbols ({error_count} errors).")

                # 데이터 로드 성공한 심볼만 웹소켓 구독
                if added_to_data:
                    streams_to_sub = [f"{s.lower()}@kline_{TIMEFRAME}" for s in added_to_data]
                    msg = {"method": "SUBSCRIBE", "params": streams_to_sub, "id": int(time.time())}
                    try:
                        # 웹소켓 연결 상태 재확인 후 메시지 전송
                        if kline_wsapp and kline_wsapp.sock and kline_wsapp.sock.connected:
                            kline_wsapp.send(json.dumps(msg))
                            op_logger.info(f"[Symbol Update] Sent SUBSCRIBE request for {len(added_to_data)} new streams.")
                            # 내부 구독 목록에 추가
                            with subscribed_symbols_lock: # Lock으로 보호
                                subscribed_symbols.update(added_to_data)
                        else:
                            op_logger.warning("[Symbol Update] K-line WebSocket disconnected before sending SUBSCRIBE for new symbols.")
                            # 구독 실패 시 데이터 롤백? (고려 필요)
                            with data_lock:
                                for s in added_to_data: historical_data.pop(s, None)
                    except Exception as e:
                        op_logger.error(f"[Symbol Update] Failed to send SUBSCRIBE message for new symbols: {e}")
                         # 구독 실패 시 데이터 롤백? (고려 필요)
                        with data_lock:
                            for s in added_to_data: historical_data.pop(s, None)


            with subscribed_symbols_lock: # 최종 구독 수 확인
                current_count = len(subscribed_symbols)
            op_logger.info(f"[Symbol Update] Finished symbol update cycle. Currently subscribed to {current_count} symbols.")

        except Exception as e:
            op_logger.error(f"Error in symbol update loop: {e}", exc_info=True)
            time.sleep(60) # 예외 발생 시 60초 대기 후 재시도
    op_logger.info("Symbol Update thread finished.")


# ==============================================================================
# 웹소켓 처리 로직 (K-line) - (WebSocket Processing Logic - K-line)
# ==============================================================================
def update_historical_data(symbol_ws, kline_data):
    """수신된 K-line 데이터로 해당 심볼의 과거 데이터 DataFrame을 업데이트"""
    global historical_data # 전역 변수 사용
    try:
        with data_lock: # Lock으로 보호
            if symbol_ws not in historical_data:
                # op_logger.debug(f"[{symbol_ws}] Received kline data but symbol not in historical_data (likely during removal). Skipping update.")
                return False # 데이터 딕셔너리에 없으면 업데이트 불가

            df = historical_data[symbol_ws]
            # K-line 데이터에서 시간 및 O, H, L, C, V 추출
            k_time = pd.to_datetime(kline_data['t'], unit='ms', utc=True) # 캔들 시작 시간
            k_open = float(kline_data['o'])
            k_high = float(kline_data['h'])
            k_low = float(kline_data['l'])
            k_close = float(kline_data['c'])
            k_volume = float(kline_data['v'])

            # 새 데이터 행 생성
            new_data_row = pd.DataFrame([{
                'open': k_open, 'high': k_high, 'low': k_low, 'close': k_close, 'volume': k_volume
            }], index=[k_time])

            # 기존 DataFrame에 업데이트 또는 추가
            if k_time in df.index: # 해당 시간의 데이터가 이미 있으면 업데이트 (캔들 진행 중)
                df.loc[k_time] = new_data_row.iloc[0]
            else: # 새로운 캔들이면 맨 뒤에 추가
                df = pd.concat([df, new_data_row])
                # 최대 캔들 개수 유지 (오래된 데이터 제거)
                df = df.iloc[-MAX_CANDLE_HISTORY:]

            historical_data[symbol_ws] = df # 업데이트된 DataFrame 저장
            return True
    except Exception as e:
        op_logger.error(f"[{symbol_ws}] Error updating historical data: {e}")
        return False

def try_update_tp(sym_ws, sym_ccxt, side, amt, tp_id, tp_coid, cur_tp, new_tp):
    """조건 만족 시 기존 TP 주문을 취소하고 새로운 TP 주문을 설정"""
    # 포지션 존재 여부 재확인 (Lock 사용)
    with real_positions_lock:
        if sym_ws not in real_positions:
            op_logger.warning(f"[{sym_ws}] TP update skipped: Position no longer exists in local state.")
            return False

    if not cur_tp or not new_tp or new_tp <= 0 or cur_tp <= 0:
        # op_logger.debug(f"[{sym_ws}] TP update skipped: Invalid current or new TP price (cur={cur_tp}, new={new_tp}).")
        return False

    # TP 가격 변동률 계산
    diff_percent = abs(new_tp - cur_tp) / cur_tp * 100
    # 변동률이 임계값 이상일 때만 업데이트 시도
    if diff_percent >= TP_UPDATE_THRESHOLD_PERCENT:
        op_logger.info(f"[{sym_ws}] TP target price moved significantly: {cur_tp:.5f} -> {new_tp:.5f} ({diff_percent:.2f}% change). Attempting to update TP order...")

        # 1. 기존 TP 주문 취소
        if not cancel_order(sym_ccxt, order_id=tp_id, client_order_id=tp_coid):
            op_logger.error(f"[{sym_ws}] Failed to cancel previous TP order (ID:{tp_id}/CliID:{tp_coid}) during TP update. Aborting update.")
            # TODO: 취소 실패 시 처리 로직 (e.g., 블랙리스트, 재시도)
            return False
        time.sleep(0.2) # 취소 처리 시간 확보

        # 2. 새로운 TP 주문 설정
        new_tp_info = None
        try:
            # 수량 및 가격 정밀도 조정
            amt_str = binance_rest.amount_to_precision(sym_ccxt, amt)
            tp_str = binance_rest.price_to_precision(sym_ccxt, new_tp)
            tp_px = float(tp_str)
            # 새로운 TP 주문 실행
            new_tp_info = place_take_profit_market_order(sym_ccxt, 'sell' if side == 'long' else 'buy', tp_px, float(amt_str))
        except Exception as e:
            op_logger.error(f"[{sym_ws}] Error placing new TP order during update: {e}")
            # TODO: 주문 실패 시 처리 로직 (e.g., 블랙리스트, 재시도)
            return False

        # 3. 새로운 TP 주문 성공 시 로컬 상태 업데이트
        if new_tp_info and new_tp_info.get('id'):
            new_id, new_coid = new_tp_info['id'], new_tp_info.get('clientOrderId')
            with real_positions_lock: # Lock으로 보호
                # 포지션 존재 여부 다시 확인 (그 사이에 종료되었을 수 있음)
                if sym_ws in real_positions:
                    # 로컬 포지션 정보에 새로운 TP 주문 정보 저장
                    real_positions[sym_ws]['tp_order_id'] = new_id
                    real_positions[sym_ws]['tp_client_order_id'] = new_coid
                    real_positions[sym_ws]['current_tp_price'] = tp_px
                    op_logger.info(f"[{sym_ws}] Successfully updated TP order to price {tp_px:.5f} (New ID:{new_id}).")
                    return True
                else: # 포지션이 사라졌으면
                    op_logger.warning(f"[{sym_ws}] Position disappeared while updating TP order. Attempting to cancel the newly placed TP order (ID:{new_id}).")
                    # 방금 생성한 TP 주문 취소 시도 (별도 스레드)
                    Thread(target=cancel_order, args=(sym_ccxt,), kwargs={'order_id': new_id, 'client_order_id': new_coid}, daemon=True).start()
                    return False
        else: # 새로운 TP 주문 실패 시
            op_logger.error(f"[{sym_ws}] Failed to place new TP order after cancelling the old one. TP is currently not set!")
            # TODO: TP 주문 재시도 로직 필요
            return False
    # else: # TP 가격 변동률이 임계값 미만이면 업데이트 안함
    #    op_logger.debug(f"[{sym_ws}] TP price change ({diff_percent:.2f}%) is below threshold ({TP_UPDATE_THRESHOLD_PERCENT}%). No TP update needed.")

    return False # 업데이트 조건 미충족 또는 실패 시

def process_kline_message(symbol_ws, kline_data):
    """K-line 웹소켓 메시지를 처리하여 지표 계산, TP 업데이트, 진입 조건 확인 및 실행"""
    global real_positions, entry_in_progress # 전역 변수 사용

    # 구독 중인 심볼인지 확인 (구독 해제된 심볼 처리 방지)
    with subscribed_symbols_lock:
        if symbol_ws not in subscribed_symbols:
            return

    # 과거 데이터 업데이트
    if not update_historical_data(symbol_ws, kline_data):
        return # 업데이트 실패 시 중단

    # 캔들 마감 여부 확인
    is_closed = kline_data.get('x', False)

    # 업데이트된 데이터 가져오기
    with data_lock: # Lock으로 보호
        df = historical_data.get(symbol_ws)
    if df is None: return # 데이터 없으면 중단

    # 지표 계산
    idf = calculate_indicators(df.copy()) # 복사본 사용
    if idf is None or idf.empty or len(idf) < 2:
        # op_logger.debug(f"[{symbol_ws}] Indicators could not be calculated or not enough data.")
        return # 지표 계산 불가 시 중단

    try:
        # 최신 캔들 및 이전 캔들 데이터 접근
        last = idf.iloc[-1]
        prev = idf.iloc[-2]

        # 필요한 값 추출
        price = last['close'] # 현재가 (마지막 종가)
        bbl, bbm, bbu = last.get('BBL', np.nan), last.get('BBM', np.nan), last.get('BBU', np.nan)
        macd_curr, macds_curr = last.get('MACD', np.nan), last.get('MACDs', np.nan) # 현재 MACD 값
        macd_prev, macds_prev = prev.get('MACD', np.nan), prev.get('MACDs', np.nan) # 이전 MACD 값

        # 필수 값들이 NaN인지 확인
        if any(pd.isna(v) for v in [price, bbl, bbm, bbu, macd_curr, macds_curr, macd_prev, macds_prev]):
            # op_logger.debug(f"[{symbol_ws}] Required indicator values contain NaN. Skipping logic.")
            return
    except IndexError:
         # op_logger.debug(f"[{symbol_ws}] IndexError accessing indicator data (likely not enough rows).")
         return # 데이터 부족으로 인덱스 오류 발생 시
    except Exception as e:
        op_logger.error(f"[{symbol_ws}] Error accessing indicator data: {e}")
        return

    now = datetime.now(UTC) # 현재 시간 (UTC)
    sym_ccxt = symbol_ws.replace(TARGET_ASSET, f'/{TARGET_ASSET}') # CCXT 형식 심볼

    # 현재 포지션 정보 복사 (읽기 전용)
    pos_copy = {}
    with real_positions_lock: # Lock으로 보호
        pos_copy = real_positions.copy()

    # --- TP 업데이트 로직 ---
    if symbol_ws in pos_copy: # 해당 심볼 포지션이 존재하면
        pinfo = pos_copy[symbol_ws]
        side, entry_time, amount = pinfo['side'], pinfo['entry_time'], pinfo['amount']
        tp_id, tp_coid, current_tp_price = pinfo.get('tp_order_id'), pinfo.get('tp_client_order_id'), pinfo.get('current_tp_price')

        # 포지션 진입 후 일정 시간 경과했고, TP 주문이 설정되어 있으면
        if now - entry_time >= timedelta(minutes=POSITION_MONITORING_DELAY_MINUTES) and tp_id and current_tp_price:
            # 새로운 TP 타겟 가격 계산 (Long: BBU, Short: BBL)
            new_tp_target = bbu if side == 'long' else bbl
            if not pd.isna(new_tp_target) and new_tp_target > 0:
                # TP 업데이트 시도 함수 호출
                try_update_tp(symbol_ws, sym_ccxt, side, amount, tp_id, tp_coid, current_tp_price, new_tp_target)


    # --- 진입 로직 (캔들 마감 시에만 실행) ---
    if is_closed:
        # 진입 시도 중인지 확인
        with entry_lock:
            is_entry_attempted = entry_in_progress.get(symbol_ws, False)
        # 이미 포지션 있는지 확인
        with real_positions_lock:
            position_exists = symbol_ws in real_positions
        # 블랙리스트 확인
        is_blacklisted = check_symbol_in_blacklist(symbol_ws)

        # 진입 조건: 시도 중X, 포지션X, 블랙리스트X
        if not is_entry_attempted and not position_exists and not is_blacklisted:
            # 최대 포지션 개수 확인
            with real_positions_lock:
                open_position_count = len(real_positions)
            if open_position_count >= MAX_OPEN_POSITIONS:
                # op_logger.debug(f"[{symbol_ws}] Entry condition met, but max positions ({MAX_OPEN_POSITIONS}) reached.")
                return # 최대 개수 도달 시 진입 불가

            # --- 진입 조건 계산 (MACD 크로스 + 가격 vs BBM) ---
            tgt_side = None # 진입 방향 (buy/sell) - 초기화
            tp_tgt = None   # TP 타겟 가격 - 초기화
            entry_px = price # 진입 가격 (현재 종가)

            # 조건 계산
            macd_cross_long = macd_prev <= macds_prev and macd_curr > macds_curr # 골든 크로스
            macd_cross_short = macd_prev >= macds_prev and macd_curr < macds_curr # 데드 크로스
            price_below_bbm = price < bbm # 가격이 BBM 아래
            price_above_bbm = price > bbm # 가격이 BBM 위

            long_entry_condition = macd_cross_long and price_below_bbm
            short_entry_condition = macd_cross_short and price_above_bbm

            # 조건 만족 시 진입 방향 및 TP 타겟 설정
            if long_entry_condition:
                tgt_side = 'buy'
                tp_tgt = bbu if not pd.isna(bbu) and bbu > 0 else None # BBU가 유효하면 TP로 설정
                op_logger.info(f"[{symbol_ws}] Long entry condition met: MACD crossed up AND Price ({price:.5f}) < BBM ({bbm:.5f}).")
            elif short_entry_condition:
                tgt_side = 'sell'
                tp_tgt = bbl if not pd.isna(bbl) and bbl > 0 else None # BBL이 유효하면 TP로 설정
                op_logger.info(f"[{symbol_ws}] Short entry condition met: MACD crossed down AND Price ({price:.5f}) > BBM ({bbm:.5f}).")

            # 진입 방향(tgt_side)이 결정되고 TP 타겟(tp_tgt)이 유효할 때
            if tgt_side and tp_tgt is not None:

                # *** 추가된 로직: 수수료 고려 최소 이익률 체크 ***
                if entry_px <= 0: # 진입 가격 유효성 체크
                    op_logger.warning(f"[{symbol_ws}] Invalid entry price ({entry_px}) for fee calculation. Skipping entry.")
                    return

                # 예상 이익률 계산
                expected_profit_ratio = abs(tp_tgt - entry_px) / entry_px
                # 총 수수료율 계산 (진입 + TP 종료 = 2번)
                total_fee_ratio = 2 * FEE_RATE

                # 예상 이익률이 총 수수료율보다 큰지 확인
                if expected_profit_ratio <= total_fee_ratio:
                    op_logger.info(f"[{symbol_ws}] Entry skipped: Expected profit ratio ({expected_profit_ratio:.4f}) is not greater than total fee ratio ({total_fee_ratio:.4f}). TP: {tp_tgt:.5f}, Entry: {entry_px:.5f}")
                    return # 이익률 낮으면 진입 안함
                # *** 수수료 체크 로직 끝 ***

                # 실제 진입 절차 시작
                with entry_lock: # 진입 시도 플래그 설정 (Lock 사용)
                    entry_in_progress[symbol_ws] = True
                try:
                    op_logger.info(f"[{symbol_ws}] ===> Starting Entry Process: {tgt_side.upper()} @ {entry_px:.5f}, Initial TP Target: {tp_tgt:.5f} (Profit Ratio {expected_profit_ratio:.4f} > Fee Ratio {total_fee_ratio:.4f})")

                    # 1. 잔고 확인
                    current_balance = get_current_balance()
                    if current_balance <= 10.0: # 최소 잔고 (임의값, 필요시 조정)
                        raise Exception(f"Insufficient balance ({current_balance:.2f} {TARGET_ASSET})")

                    # 2. 포지션 크기 계산
                    with real_positions_lock: # 현재 포지션 수 다시 확인 (Lock 사용)
                        oc = len(real_positions)
                    if oc >= MAX_OPEN_POSITIONS: # 그 사이에 다른 포지션이 진입했을 수 있음
                        raise Exception("Max positions reached just before entry execution")
                    # 사용 가능 슬롯 기반으로 자금 배분
                    portion = 1.0 / (MAX_OPEN_POSITIONS - oc) if MAX_OPEN_POSITIONS > oc else 1.0
                    margin_to_use = current_balance * portion # 사용할 증거금
                    notional_value = margin_to_use * LEVERAGE # 레버리지 적용한 포지션 가치
                    min_notional_required = 5.0 # 바이낸스 최소 주문 금액 (선물 기준)
                    if notional_value < min_notional_required:
                        raise Exception(f"Calculated notional value ({notional_value:.2f}) is below minimum ({min_notional_required})")
                    # 진입 수량 계산
                    entry_amount = notional_value / entry_px if entry_px > 0 else 0
                    if entry_amount <= 0:
                        raise Exception("Calculated entry amount is zero or negative")

                    # 3. 마진/레버리지 설정
                    if not set_isolated_margin(sym_ccxt, LEVERAGE):
                        raise Exception("Failed to set isolated margin or leverage")

                    # 4. 시장가 진입 주문 실행
                    entry_order = place_market_order_real(sym_ccxt, tgt_side, entry_amount, entry_px)
                    if not entry_order or not entry_order.get('id'):
                        raise Exception("Market entry order placement failed")
                    entry_oid, entry_coid = entry_order['id'], entry_order.get('clientOrderId')
                    op_logger.info(f"[{symbol_ws}] Market entry order placed successfully (ID:{entry_oid}, CliID:{entry_coid}). Waiting for fill confirmation is NOT implemented, assuming filled.")
                    # 참고: 실제로는 주문 체결 확인 로직이 필요할 수 있음

                    # 5. SL / TP 주문 설정
                    sl_order, tp_order = None, None
                    final_sl_price, final_tp_price = None, None
                    try:
                        # SL 가격 계산
                        final_sl_price = entry_px * (LONG_STOP_LOSS_FACTOR if tgt_side == 'buy' else SHORT_STOP_LOSS_FACTOR)
                        # TP 가격은 위에서 계산된 tp_tgt 사용
                        final_tp_price = tp_tgt

                        # SL/TP 주문 방향 결정 (진입 방향과 반대)
                        sl_tp_side = 'sell' if tgt_side == 'buy' else 'buy'

                        # SL 주문 실행
                        sl_order = place_stop_market_order(sym_ccxt, sl_tp_side, final_sl_price, entry_amount)
                        if not sl_order or not sl_order.get('id'):
                            raise Exception("Stop Loss (SL) order placement failed")
                        op_logger.info(f"[{symbol_ws}] SL order placed successfully (ID:{sl_order['id']})")
                        time.sleep(0.1) # API 호출 간 지연

                        # TP 주문 실행
                        tp_order = place_take_profit_market_order(sym_ccxt, sl_tp_side, final_tp_price, entry_amount)
                        if not tp_order or not tp_order.get('id'):
                            raise Exception("Take Profit (TP) order placement failed")
                        op_logger.info(f"[{symbol_ws}] TP order placed successfully (ID:{tp_order['id']})")

                        # 6. 모든 주문 성공 시 로컬 상태 저장
                        with real_positions_lock: # Lock으로 보호
                            # 저장 전 최대 포지션 수 다시 확인
                            if len(real_positions) < MAX_OPEN_POSITIONS:
                                real_positions[symbol_ws] = {
                                    'side': 'long' if tgt_side == 'buy' else 'short',
                                    'entry_price': entry_px,
                                    'amount': entry_amount, # 실제 주문 수량 저장 필요 (API 응답에서 가져오는 것이 더 정확)
                                    'entry_time': now,
                                    'entry_order_id': entry_oid,
                                    'entry_client_order_id': entry_coid,
                                    'sl_order_id': sl_order['id'],
                                    'sl_client_order_id': sl_order.get('clientOrderId'),
                                    'tp_order_id': tp_order['id'],
                                    'tp_client_order_id': tp_order.get('clientOrderId'),
                                    'current_tp_price': final_tp_price # 초기 TP 가격 저장
                                }
                                op_logger.info(f"[{symbol_ws}] <<< Entry successful and position state stored. Active Positions: {len(real_positions)} >>>")
                            else: # 그 사이에 최대치 도달 시 롤백
                                op_logger.error(f"[{symbol_ws}] Max positions reached just before storing state! Rolling back SL/TP orders.")
                                raise Exception("Max positions reached during final state storage")

                    except Exception as sltp_e: # SL 또는 TP 주문 실패 시 롤백
                        op_logger.error(f"[{symbol_ws}] Error placing SL/TP orders: {sltp_e}. !!! INITIATING ROLLBACK !!!")
                        # 실패 지점까지 생성된 주문들 취소 시도 (별도 스레드)
                        if sl_order and sl_order.get('id'):
                            op_logger.warning(f"[{symbol_ws}] Rollback: Cancelling SL order {sl_order['id']}")
                            Thread(target=cancel_order, args=(sym_ccxt,), kwargs={'order_id': sl_order['id'], 'client_order_id': sl_order.get('clientOrderId')}, daemon=True).start()
                        if tp_order and tp_order.get('id'):
                            op_logger.warning(f"[{symbol_ws}] Rollback: Cancelling TP order {tp_order['id']}")
                            Thread(target=cancel_order, args=(sym_ccxt,), kwargs={'order_id': tp_order['id'], 'client_order_id': tp_order.get('clientOrderId')}, daemon=True).start()
                        # 진입 주문도 취소 시도 (시장가라 이미 체결되었을 가능성 높음)
                        op_logger.warning(f"[{symbol_ws}] Rollback: Attempting to cancel entry order {entry_oid} (might be filled)")
                        Thread(target=cancel_order, args=(sym_ccxt,), kwargs={'order_id': entry_oid, 'client_order_id': entry_coid}, daemon=True).start()

                        # TODO: 만약 진입 주문이 이미 체결되었다면, 즉시 시장가로 종료하는 로직 필요
                        op_logger.warning(f"[{symbol_ws}] Rollback process initiated. Position might need manual closing if entry was filled.")
                        # 로컬 상태 제거 (혹시 저장되었다면)
                        with real_positions_lock:
                            real_positions.pop(symbol_ws, None)
                        # 블랙리스트 추가
                        add_to_blacklist(symbol_ws, reason=f"Entry failed during SL/TP placement: {sltp_e}")
                        # finally 블록에서 entry_in_progress는 제거됨

                except Exception as entry_e: # 진입 프로세스 전반의 예외 처리
                    op_logger.error(f"[{symbol_ws}] Entry process failed: {entry_e}", exc_info=False) # 스택 트레이스 없이 간단히 로깅
                    # 실패 시 로컬 상태 정리 (혹시 모를 부분적 저장 방지)
                    with real_positions_lock:
                        real_positions.pop(symbol_ws, None)
                    # 블랙리스트 추가 등 추가 조치 가능
                    add_to_blacklist(symbol_ws, reason=f"Entry process failed: {entry_e}")
                finally:
                    # 진입 시도 플래그 해제 (성공/실패/롤백 모든 경우)
                    with entry_lock: # Lock으로 보호
                        entry_in_progress.pop(symbol_ws, None) # 해당 심볼 플래그 제거

# ==============================================================================
# 웹소켓 콜백 함수 (K-line) - (WebSocket Callback Functions - K-line)
# ==============================================================================
def on_message_kline(wsapp, message):
    """K-line 웹소켓 메시지 수신 시 호출되는 콜백 함수"""
    try:
        data = json.loads(message) # JSON 파싱
        # 스트림 데이터 형식인지 확인 ('stream'과 'data' 필드 존재)
        if 'stream' in data and 'data' in data:
            stream_name = data['stream'] # 스트림 이름 (e.g., btcusdt@kline_15m)
            payload = data['data'] # 실제 데이터
            # K-line 이벤트인지 확인
            if payload.get('e') == 'kline':
                symbol_upper = stream_name.split('@')[0].upper() # 심볼 추출 (대문자)
                # K-line 데이터 처리 함수 호출
                process_kline_message(symbol_upper, payload['k'])
        # 구독 응답 메시지 처리
        elif 'result' in data and data.get('id'):
            op_logger.info(f"K-line WebSocket subscription response: {data}")
        # 에러 메시지 처리
        elif 'e' in data and data['e'] == 'error':
             op_logger.error(f"K-line WebSocket API Error received: {data}")
        # 그 외 알 수 없는 형식
        # else: op_logger.debug(f"Received unknown K-line WS message format: {message[:100]}")

    except json.JSONDecodeError: # JSON 파싱 오류
        op_logger.error(f"K-line WebSocket JSON Decode Error: {message[:100]}")
    except Exception as e: # 그 외 예외 처리
        op_logger.error(f"Error processing K-line WebSocket message: {e}", exc_info=True)

def on_error_kline(wsapp, error):
    """K-line 웹소켓 오류 발생 시 호출되는 콜백 함수"""
    op_logger.error(f"K-line WebSocket Error: {error}")
    # 연결 거부 오류 특별 처리
    if isinstance(error, ConnectionRefusedError):
        op_logger.error("Connection refused by the server. Check network or Binance status.")
    # 다른 오류 유형에 따른 추가 처리 가능 (e.g., TimeoutError)

def on_close_kline(wsapp, close_status_code, close_msg):
    """K-line 웹소켓 연결 종료 시 호출되는 콜백 함수"""
    global kline_websocket_running
    if not shutdown_requested: # 정상 종료 요청이 아닌 경우
        op_logger.warning(f"K-line WebSocket connection closed unexpectedly! Code: {close_status_code}, Msg: {close_msg}. Will attempt to reconnect.")
        kline_websocket_running = False # 실행 상태 플래그 False로 설정 (재연결 로직 트리거)
    else: # 정상 종료 시
        op_logger.info(f"K-line WebSocket connection closed gracefully.")
        kline_websocket_running = False

def on_open_kline_initial(wsapp):
    """K-line 웹소켓 최초 연결 성공 시 호출되는 콜백 함수"""
    global subscribed_symbols, historical_data, kline_websocket_running, shutdown_requested # 전역 변수 사용
    kline_websocket_running = True # 실행 상태 True로 설정
    op_logger.info("K-line WebSocket initial connection opened successfully.")

    # 1. 초기 거래 대상 심볼 목록 가져오기
    op_logger.info("Fetching initial top symbols for subscription...")
    initial_sym_ccxt = get_top_volume_symbols(TOP_N_SYMBOLS)
    if not initial_sym_ccxt:
        op_logger.error("Could not fetch initial symbols. Shutting down.")
        shutdown_requested = True # 종료 플래그 설정
        wsapp.close() # 웹소켓 연결 종료
        return
    # 웹소켓 형식으로 변환
    initial_sym_ws = {s.replace(f'/{TARGET_ASSET}', TARGET_ASSET) for s in initial_sym_ccxt}
    op_logger.info(f"Subscribing to initial {len(initial_sym_ws)} K-line streams...")

    # 2. 웹소켓 스트림 구독 요청
    streams = [f"{s.lower()}@kline_{TIMEFRAME}" for s in initial_sym_ws]
    if not streams:
        op_logger.error("No streams to subscribe to initially. Shutting down.")
        shutdown_requested = True
        wsapp.close()
        return
    sub_id = 1 # 구독 요청 ID
    msg = {"method": "SUBSCRIBE", "params": streams, "id": sub_id}
    try:
        wsapp.send(json.dumps(msg)) # 구독 메시지 전송
        time.sleep(1) # 응답 대기 시간 (필요시 조정)
        op_logger.info(f"Initial K-line subscription request sent (ID:{sub_id}).")
    except Exception as e:
        op_logger.error(f"Failed to send initial K-line subscription request: {e}. Shutting down.")
        shutdown_requested = True
        wsapp.close()
        return

    # 3. 내부 구독 목록 업데이트
    with subscribed_symbols_lock: # Lock으로 보호
        subscribed_symbols = initial_sym_ws

    # 4. 초기 과거 데이터 로드
    op_logger.info("Fetching initial historical data for subscribed symbols...")
    fetched_count, error_count = 0, 0
    with data_lock: # Lock으로 보호 (초기화)
        historical_data.clear()
    symbols_to_fetch = initial_sym_ws.copy() # 반복 중 변경 방지 위해 복사
    for symbol_ws in symbols_to_fetch:
        if shutdown_requested: break # 종료 요청 시 중단
        sym_ccxt = symbol_ws.replace(TARGET_ASSET, f'/{TARGET_ASSET}')
        # 지표 계산에 필요한 충분한 데이터 로드
        df = fetch_initial_ohlcv(sym_ccxt, TIMEFRAME, limit=max(INITIAL_CANDLE_FETCH_LIMIT, MACD_SLOW+MACD_SIGNAL+50))
        if df is not None and not df.empty:
            with data_lock: # Lock으로 보호
                historical_data[symbol_ws] = df # 데이터 저장
            fetched_count += 1
        else:
            error_count += 1
            op_logger.warning(f"Failed to fetch initial data for {symbol_ws}.")
            # 데이터 로드 실패 시 구독 목록에서도 제거? (고려 필요)
            # with subscribed_symbols_lock: subscribed_symbols.discard(symbol_ws)
        time.sleep(0.3) # API 호출 간 지연

    op_logger.info(f"Initial historical data fetch complete ({fetched_count} symbols OK, {error_count} errors).")
    print("-" * 80 + "\nK-line WebSocket connected. Bot is now listening for market data (REST Sync Mode)...\n" + "-" * 80)

def on_open_kline_reconnect(wsapp):
    """K-line 웹소켓 재연결 성공 시 호출되는 콜백 함수"""
    global kline_websocket_running, subscribed_symbols # 전역 변수 사용
    kline_websocket_running = True # 실행 상태 True로 설정
    op_logger.info("K-line WebSocket RECONNECTED successfully.")

    # 현재 구독 중인 심볼 목록 가져오기
    with subscribed_symbols_lock: # Lock으로 보호
        current_subs = subscribed_symbols.copy()

    if not current_subs:
        op_logger.warning("Subscription list is empty on reconnect. No streams to resubscribe.")
        return

    # 기존 구독 목록으로 재구독 요청
    op_logger.info(f"Resubscribing to {len(current_subs)} previously subscribed K-line streams...")
    streams = [f"{s.lower()}@kline_{TIMEFRAME}" for s in current_subs]
    if not streams:
        op_logger.warning("No streams found to resubscribe.")
        return

    resub_id = int(time.time()) # 재구독 요청 ID
    msg = {"method": "SUBSCRIBE", "params": streams, "id": resub_id}
    try:
        wsapp.send(json.dumps(msg)) # 재구독 메시지 전송
        op_logger.info(f"Resubscription request sent (ID:{resub_id}).")
        # 재연결 시에는 초기 데이터 로드는 하지 않음 (기존 데이터 유지)
    except Exception as e:
        op_logger.error(f"Failed to send resubscription request: {e}")
        # 재구독 실패 시 연결 다시 닫고 재시도 유도?
        wsapp.close()


# ==============================================================================
# 메인 실행 로직 (Main Execution Logic)
# ==============================================================================
if __name__ == "__main__":
    start_time_str = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S %Z")
    op_logger.info(f"Bot starting at: {start_time_str}")

    # 설정값 검증
    if SIMULATION_MODE:
        op_logger.error("SIMULATION_MODE is True. Set to False for real trading. Exiting.")
        exit()
    if not API_KEY or API_KEY == "YOUR_BINANCE_API_KEY" or not API_SECRET or API_SECRET == "YOUR_BINANCE_API_SECRET":
        op_logger.error("API Key or Secret is not set or using placeholder values. Please configure them. Exiting.")
        exit()

    # 실제 거래 모드 경고
    op_logger.warning("="*30 + f" REAL TRADING MODE - {log_prefix} " + "="*30)
    op_logger.warning("Strategy: MACD Cross + Price vs BBM Entry / Fixed SL / Dynamic TP (BBands) / REST Sync / Auto K-line Reconnect")
    op_logger.warning(f"Key Settings: MaxPos={MAX_OPEN_POSITIONS}, Leverage={LEVERAGE}x, Timeframe={TIMEFRAME}, SymbolUpdateInterval={SYMBOL_UPDATE_INTERVAL_HOURS}h, RESTSyncInterval={REST_SYNC_INTERVAL_MINUTES}min, FeeRate={FEE_RATE}") # FEE_RATE 추가
    op_logger.warning("!!! THIS BOT WILL USE REAL FUNDS - MONITOR CLOSELY !!!")
    op_logger.warning("="*80)
    # 시작 전 카운트다운
    for i in range(3, 0, -1):
        print(f"Starting in {i}...", end='\r')
        time.sleep(1)
    print("Starting now!          ") # 이전 메시지 덮어쓰기

    # 1. CCXT REST API 초기화
    if not initialize_binance_rest():
        op_logger.error("Exiting due to CCXT REST initialization failure.")
        exit()

    # 2. 초기 상태 동기화 실행
    op_logger.info("Running initial REST state synchronization...")
    sync_positions_with_exchange()
    op_logger.info("Initial REST sync complete.")
    log_asset_status() # 초기 자산 상태 로깅

    # 3. REST 동기화 스레드 시작
    sync_thread = Thread(target=sync_state_periodically, args=(REST_SYNC_INTERVAL_MINUTES * 60,), daemon=True)
    sync_thread.start()

    # K-line 웹소켓 URL
    ws_url_kline = f"wss://fstream.binance.com/stream"
    reconnect_delay = 5 # 초기 재연결 시도 간격(초)

    try:
        # 메인 루프: 웹소켓 연결 관리 및 유지
        while not shutdown_requested: # 종료 요청 없을 시 반복
            if not kline_websocket_running: # K-line 웹소켓 연결이 끊겼거나 시작 전이면
                op_logger.info("Attempting K-line WebSocket connection/reconnection...")
                # 기존 웹소켓 객체 정리 (재연결 시)
                if kline_wsapp and kline_wsapp.sock:
                    try:
                        kline_wsapp.close()
                        time.sleep(1) # 종료 시간 확보
                    except Exception as close_e:
                         op_logger.warning(f"Error closing previous WebSocket connection: {close_e}")

                # 연결 시 사용할 on_open 콜백 함수 결정 (최초 vs 재연결)
                current_on_open = on_open_kline_initial if kline_thread is None else on_open_kline_reconnect
                # 새 웹소켓 앱 객체 생성
                kline_wsapp = websocket.WebSocketApp(
                    ws_url_kline,
                    on_open=current_on_open,
                    on_message=on_message_kline,
                    on_error=on_error_kline,
                    on_close=on_close_kline
                )

                # 웹소켓 실행 스레드 시작 (최초 또는 재시작)
                if kline_thread is None or not kline_thread.is_alive():
                    kline_thread = Thread(target=lambda: kline_wsapp.run_forever(ping_interval=60, ping_timeout=10), daemon=True)
                    kline_thread.start()
                    op_logger.info("New K-line WebSocket thread started. Waiting for connection...")
                else:
                    # 이론상 이전 스레드가 종료되어야 하지만, 혹시 살아있으면 경고
                    op_logger.warning("K-line WebSocket thread seems to be still alive during reconnect attempt? This might indicate an issue.")
                    # 필요시 강제 종료 로직 추가?

                # 심볼 업데이트 스레드 시작 (최초 또는 재시작)
                if symbol_update_thread is None or not symbol_update_thread.is_alive():
                    symbol_update_thread = Thread(target=update_top_symbols_periodically, args=(SYMBOL_UPDATE_INTERVAL_HOURS * 60 * 60,), daemon=True)
                    symbol_update_thread.start()
                    op_logger.info("Symbol Update thread started/restarted.")

                # 연결 완료 대기 (최대 15초)
                connect_wait_start = time.time()
                while not kline_websocket_running and time.time() - connect_wait_start < 15:
                    if shutdown_requested: break # 종료 요청 시 대기 중단
                    time.sleep(0.5)

                if kline_websocket_running: # 연결 성공 시
                    op_logger.info("K-line WebSocket connection established/re-established successfully.")
                    reconnect_delay = 5 # 재연결 지연 시간 초기화
                else: # 연결 실패 시
                    op_logger.error(f"K-line WebSocket connection failed after waiting. Retrying in {reconnect_delay} seconds...")
                    if kline_wsapp and kline_wsapp.sock: # 실패 시 웹소켓 객체 다시 닫기
                        kline_wsapp.close()
                    time.sleep(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 2, 60) # 재연결 지연 시간 증가 (최대 60초)

            else: # K-line 웹소켓 정상 실행 중
                log_asset_status() # 주기적 자산 로깅
                time.sleep(1) # 메인 루프 잠시 대기

    except KeyboardInterrupt: # Ctrl+C 입력 시
        op_logger.info("Keyboard interrupt received. Initiating graceful shutdown...")
        shutdown_requested = True # 종료 플래그 설정
    except Exception as main_loop_err: # 메인 루프에서 예상치 못한 오류 발생 시
        op_logger.error(f"Critical error in main loop: {main_loop_err}", exc_info=True)
        shutdown_requested = True # 종료 플래그 설정
    finally:
        # --- 최종 종료 처리 ---
        op_logger.info("Initiating final shutdown sequence...")
        shutdown_requested = True # 모든 스레드에 종료 신호 확실히 전달
        kline_websocket_running = False # 웹소켓 상태 플래그 변경

        # 웹소켓 연결 종료 시도
        if kline_wsapp and kline_wsapp.sock:
            op_logger.info("Closing K-line WebSocket connection...")
            kline_wsapp.close()

        # 다른 스레드들이 종료될 시간 확보
        op_logger.info("Waiting for background threads to finish (max 5 seconds)...")
        time.sleep(5) # 필요시 시간 조정

        # 모든 미체결 주문 취소 시도 (매우 중요)
        op_logger.warning("Attempting to cancel ALL remaining open orders across all USDT futures markets...")
        all_cancelled_final = True
        try:
            if binance_rest: # REST API 객체 유효한지 확인
                # 모든 선물 마켓 정보 가져오기
                markets = binance_rest.fetch_markets()
                usdt_futures_symbols = [mkt['symbol'] for mkt in markets if mkt.get('type') == 'future' and mkt.get('quote') == TARGET_ASSET]
                op_logger.info(f"Checking for open orders in {len(usdt_futures_symbols)} USDT futures markets...")
                # 각 마켓별로 미체결 주문 취소 함수 호출
                for symbol_ccxt in usdt_futures_symbols:
                    if not cancel_open_orders_for_symbol(symbol_ccxt):
                        op_logger.error(f"Failed to cancel orders for {symbol_ccxt} during final cleanup.")
                        # 취소 실패 시에도 계속 진행
                    time.sleep(0.3) # API 호출 간 지연
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

        # 최종 자산 상태 로깅
        op_logger.info("Fetching final balance...")
        final_balance = get_current_balance()
        bal_str = f"{final_balance:.2f}" if final_balance is not None else "Error"
        with stats_lock: # 최종 통계 가져오기
            trades, wins = total_trades, winning_trades
            wr = (wins / trades * 100) if trades > 0 else 0.0
        final_msg = f"Final Balance:{bal_str} {TARGET_ASSET}, Total Trades:{trades}(Delayed), Winning Trades:{wins}(Delayed), Win Rate:{wr:.2f}%"
        op_logger.info(final_msg)
        asset_logger.info(final_msg) # 자산 로그에도 기록

        op_logger.info(f"{log_prefix} Bot shutdown complete.")
        print(f"{log_prefix} Bot shutdown complete.")

