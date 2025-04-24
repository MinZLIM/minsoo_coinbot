import pandas as pd
import pandas_ta as ta
from binance.client import Client
from binance.enums import *
from binance.exceptions import BinanceAPIException # API 에러 처리용
import time
import schedule
from datetime import datetime, timedelta
import math
import os
import threading
import queue
import traceback
import logging
import logging.handlers
import csv # CSV 파일 로깅 위해

# --- 로깅 설정 ---
# 기본 로그 (파일)
log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
log_file = 'trading_bot.log'
file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
file_handler.setFormatter(log_formatter)
file_handler.setLevel(logging.INFO)

logger = logging.getLogger('MainLogger') # 기본 로거 이름 지정
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)

# 거래 내역 로그 (CSV 파일)
trade_log_file = 'trade_history.csv'
trade_logger = logging.getLogger('TradeHistory')
trade_logger.setLevel(logging.INFO)
# CSV 파일 핸들러 (trade_logger에만 추가)
trade_file_handler = logging.FileHandler(trade_log_file, mode='a', encoding='utf-8') 
# CSV 포매터 불필요 (직접 csv.writer 사용)
trade_logger.addHandler(trade_file_handler)

# CSV 헤더 작성 (파일이 없거나 비어있을 경우)
if not os.path.exists(trade_log_file) or os.path.getsize(trade_log_file) == 0:
    with open(trade_log_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'Timestamp', 'Symbol', 'Side', 'EntryPrice', 'ClosePrice', 'Quantity',
            'Leverage', 'GrossPnL', 'Fee', 'NetPnL', 'FinalBalance', 'Reason'
        ])

# --- 설정 ---
# ⚠️⚠️⚠️ 실제 바이낸스 API 키 + 읽기 전용(Read-Only) 강력 권장! ⚠️⚠️⚠️
API_KEY = ""
API_SECRET = ""
USE_TESTNET = False # 실제 API 사용

# 전략 파라미터
TOP_N_SYMBOLS = 20
INTERVAL = Client.KLINE_INTERVAL_15MINUTE
BB_LENGTH = 20; BB_STD_DEV = 2.0
STOCH_RSI_K = 14; STOCH_RSI_D = 3; STOCH_RSI_RSI = 14; STOCH_RSI_STOCH = 14
MACD_FAST = 12; MACD_SLOW = 26; MACD_SIGNAL = 9
LEVERAGE = 5 # 포지션 크기 계산 및 PnL 계산에 사용
# ENTRY_BB_Nahe_PERCENT 조건 제거됨
STOP_LOSS_PERCENT = 0.03
TAKE_PROFIT_BB_Nahe_PERCENT = 0.005
MAX_POSITIONS = 4
CAPITAL_ALLOCATION_PER_POSITION = 0.25 # <<< 자본의 25%씩 분할
WHIPSAW_DURATION_MINUTES = 60
FEE_RATE = 0.0005
POSITION_MONITOR_INTERVAL_SECONDS = 30
REALTIME_CHECK_INTERVAL_SECONDS = 30

# 가상 계정 및 상태 관리
initial_capital = 10000.0
virtual_balance = initial_capital
open_positions = {}
indicator_states = {}
blacklist = {}
message_queue = queue.Queue()

# 스레드 안전성을 위한 Lock 객체
balance_lock = threading.Lock()
positions_lock = threading.Lock()
indicator_lock = threading.Lock()
blacklist_lock = threading.Lock()

# --- 바이낸스 클라이언트 생성 ---
if USE_TESTNET:
    client = Client(API_KEY, API_SECRET, testnet=True)
    logger.info("바이낸스 테스트넷 연결 시도...")
    try: client.futures_ping(); logger.info("테스트넷 연결 성공.")
    except Exception as e: logger.error(f"오류: 테스트넷 연결 실패 - {e}"); exit()
else:
    client = Client(API_KEY, API_SECRET)
    logger.warning("⚠️ 실제 바이낸스 서버 연결 시도...")
    try:
        account_info = client.futures_account()
        if account_info: logger.info("✅ 실제 서버 연결 및 API 키 인증 성공.")
        else: logger.error("❌ 계정 정보 조회 실패. API 키 권한 확인 필요."); exit()
    except Exception as e:
        logger.error(f"❌ 실제 서버 연결 또는 API 키 인증 실패 - {e}"); logger.exception(e); exit()

# --- 헬퍼 함수 ---
def log(message, level=logging.INFO):
    """설정된 로거를 사용하여 메시지 로깅"""
    # logger.log(level, message) # 이렇게 해도 됨
    if level == logging.DEBUG: logger.debug(message)
    elif level == logging.INFO: logger.info(message)
    elif level == logging.WARNING: logger.warning(message)
    elif level == logging.ERROR: logger.error(message)
    elif level == logging.CRITICAL: logger.critical(message)
    else: logger.info(message)

def log_trade_history(log_data):
     """거래 내역을 CSV 파일에 로깅"""
     try:
         # 로그 순서 정의 (CSV 헤더와 일치해야 함)
         field_order = [
            'Timestamp', 'Symbol', 'Side', 'EntryPrice', 'ClosePrice', 'Quantity',
            'Leverage', 'GrossPnL', 'Fee', 'NetPnL', 'FinalBalance', 'Reason'
         ]
         # 현재 시간을 Timestamp로 추가
         log_data['Timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

         # trade_logger를 통해 파일에 직접 쓰기 (포매터 사용 안 함)
         with open(trade_log_file, 'a', encoding='utf-8', newline='') as f:
              writer = csv.writer(f)
              writer.writerow([log_data.get(field, '') for field in field_order]) # 순서대로 값 가져와서 쓰기

     except Exception as e:
         log(f"!!! 오류: 거래 내역 로깅 실패 - {e}", logging.ERROR)
         logger.exception(e)


def get_top_volume_symbols(n):
    """거래대금 상위 N개 USDT Perpetual 심볼 조회"""
    # (이전 코드와 동일 - contractType 필터 제거, quoteVolume > 0 조건 유지)
    try:
        log("Ticker 정보 조회 시도...")
        tickers = client.futures_ticker()
        log(f"Ticker 정보 수신 완료 (총 {len(tickers) if isinstance(tickers, list) else 'N/A'}개 항목 수신됨).")
        if not tickers or not isinstance(tickers, list): log("경고: Ticker 정보가 비어있거나 리스트 형식이 아닙니다.", logging.WARNING); return []

        usdt_tickers = []
        filter_log_count = 0
        for t in tickers:
            passes = True; reason = []
            if not isinstance(t, dict): passes = False; reason.append("타입 Dict 아님")
            elif 'symbol' not in t: passes = False; reason.append("키 'symbol' 없음")
            elif not t['symbol'].endswith('USDT'): passes = False; reason.append("USDT 아님")
            elif '_' in t['symbol']: passes = False; reason.append("심볼에 '_' 포함")
            elif 'quoteVolume' not in t: passes = False; reason.append("키 'quoteVolume' 없음")
            elif not float(t.get('quoteVolume', 0)) > 0: passes = False; reason.append(f"quoteVolume 0 이하 ({t.get('quoteVolume')})") # 실제 데이터이므로 > 0 유지

            if passes: usdt_tickers.append(t)
            elif filter_log_count < 5: log(f"  [필터 제외됨] {t.get('symbol', 'N/A')} - 이유: {', '.join(reason)}", logging.DEBUG); filter_log_count += 1

        log(f"필터링 후 USDT Perpetual 심볼 개수: {len(usdt_tickers)}")
        if not usdt_tickers: log("경고: 필터링 후 남은 USDT 선물 심볼이 없습니다.", logging.WARNING); return []

        sorted_tickers = sorted(usdt_tickers, key=lambda x: float(x.get('quoteVolume', 0)), reverse=True)
        return [t['symbol'] for t in sorted_tickers[:n]]
    except Exception as e: log(f"!!! 오류: 거래대금 상위 심볼 조회 중 예외 발생 - {e}", logging.ERROR); logger.exception(e); return []


def get_historical_data(symbol, interval, start_str, limit=500):
    """과거 Klines 데이터 조회"""
    # (이전 코드와 동일)
    try:
        klines = client.futures_historical_klines(symbol=symbol, interval=interval, start_str=start_str, limit=limit)
        if not klines: return None
        df = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)
        for col in ['open', 'high', 'low', 'close', 'volume']: df[col] = df[col].astype(float)
        return df
    except Exception as e: log(f"오류: {symbol} {interval} 데이터 조회 실패 - {e}", logging.ERROR); return None


def calculate_indicators(df):
    """볼린저밴드, 스토캐스틱RSI, MACD 계산"""
    # (이전 코드와 동일)
    required_length = max(BB_LENGTH, MACD_SLOW + MACD_SIGNAL, STOCH_RSI_RSI + STOCH_RSI_STOCH) + 2
    if df is None or len(df) < required_length: return None
    try:
        df.ta.bbands(length=BB_LENGTH, std=BB_STD_DEV, append=True)
        stoch_rsi_df = df.ta.stochrsi(k=STOCH_RSI_K, d=STOCH_RSI_D, rsi_length=STOCH_RSI_RSI, stochastic_length=STOCH_RSI_STOCH)
        macd_df = df.ta.macd(fast=MACD_FAST, slow=MACD_SLOW, signal=MACD_SIGNAL)
        lower_col=f'BBL_{BB_LENGTH}_{BB_STD_DEV:.1f}'; middle_col=f'BBM_{BB_LENGTH}_{BB_STD_DEV:.1f}'; upper_col=f'BBU_{BB_LENGTH}_{BB_STD_DEV:.1f}'
        k_col=f'STOCHRSIk_{STOCH_RSI_STOCH}_{STOCH_RSI_RSI}_{STOCH_RSI_K}_{STOCH_RSI_D}'; d_col=f'STOCHRSId_{STOCH_RSI_STOCH}_{STOCH_RSI_RSI}_{STOCH_RSI_K}_{STOCH_RSI_D}'
        macd_line_col=f'MACD_{MACD_FAST}_{MACD_SLOW}_{MACD_SIGNAL}'; signal_line_col=f'MACDs_{MACD_FAST}_{MACD_SLOW}_{MACD_SIGNAL}'
        df.rename(columns={lower_col: 'bb_lower', middle_col: 'bb_middle', upper_col: 'bb_upper'}, inplace=True, errors='ignore')
        if k_col in stoch_rsi_df.columns: df['stoch_k'] = stoch_rsi_df[k_col]
        if d_col in stoch_rsi_df.columns: df['stoch_d'] = stoch_rsi_df[d_col]
        if macd_line_col in macd_df.columns: df['macd_line'] = macd_df[macd_line_col]
        if signal_line_col in macd_df.columns: df['macd_signal'] = macd_df[signal_line_col]
        required_cols = ['bb_lower', 'bb_middle', 'bb_upper', 'stoch_k', 'stoch_d', 'macd_line', 'macd_signal']
        if not all(col in df.columns for col in required_cols): return None
        if df[required_cols].iloc[-2:].isnull().any().any(): return None
        return df
    except Exception as e: log(f"!!! 오류: 지표 계산 중 예외 발생 - {e}", logging.ERROR); logger.exception(e); return None


def calculate_fees(entry_price, quantity):
    """예상 수수료 계산 (진입+청산)"""
    # (이전 코드와 동일)
    position_value = entry_price * quantity
    total_fee = position_value * FEE_RATE * 2
    return total_fee

# --- 포지션 모니터링 스레드 ---
def position_monitor_thread(symbol, side, entry_price, quantity, stop_loss_price, msg_q):
    """개별 포지션 모니터링 및 청산 신호 전송 스레드"""
    # (이전 코드와 동일)
    log(f"[{symbol}] 모니터링 스레드 시작 ({side}). SL: {stop_loss_price:.4f}")
    while True:
        try:
            time.sleep(POSITION_MONITOR_INTERVAL_SECONDS)
            start_str = f"{BB_LENGTH * 15 + 60} minutes ago UTC"
            df = get_historical_data(symbol, INTERVAL, start_str, limit=BB_LENGTH + 5)
            if df is None or len(df) < 1: continue
            df.ta.bbands(length=BB_LENGTH, std=BB_STD_DEV, append=True)
            lower_col=f'BBL_{BB_LENGTH}_{BB_STD_DEV:.1f}'; upper_col=f'BBU_{BB_LENGTH}_{BB_STD_DEV:.1f}'
            df.rename(columns={lower_col:'bb_lower', upper_col:'bb_upper'}, inplace=True, errors='ignore')
            if 'bb_lower' not in df.columns or 'bb_upper' not in df.columns or df.iloc[-1].isnull().any(): continue
            last_candle = df.iloc[-1]; current_price = last_candle['close']
            bb_upper = last_candle['bb_upper']; bb_lower = last_candle['bb_lower']
            # 손절매 조건
            if (side == 'LONG' and current_price <= stop_loss_price) or (side == 'SHORT' and current_price >= stop_loss_price):
                log(f"!!! [{symbol}] 손절매 조건 도달! 현재가: {current_price:.4f}", logging.INFO)
                msg_q.put({'type': 'close', 'symbol': symbol, 'close_price': current_price, 'reason': 'Stop Loss'})
                break
            # 익절 조건
            tp_hit = False; reason = ''
            if side == 'LONG' and pd.notna(bb_upper) and abs(current_price - bb_upper) / bb_upper <= TAKE_PROFIT_BB_Nahe_PERCENT: tp_hit = True; reason = 'Take Profit (BB상단 근접)'
            elif side == 'SHORT' and pd.notna(bb_lower) and abs(current_price - bb_lower) / bb_lower <= TAKE_PROFIT_BB_Nahe_PERCENT: tp_hit = True; reason = 'Take Profit (BB하단 근접)'
            if tp_hit:
                log(f"!!! [{symbol}] 익절 조건 도달! 현재가: {current_price:.4f}", logging.INFO)
                msg_q.put({'type': 'close', 'symbol': symbol, 'close_price': current_price, 'reason': reason})
                break
        except Exception as e: log(f"!!! 오류: [{symbol}] 모니터링 스레드 내 오류 발생 - {e}", logging.ERROR); logger.exception(e); time.sleep(60)
    log(f"[{symbol}] 모니터링 스레드 종료.")

# --- 메인 로직 함수 ---
def run_strategy_indicators():
    """15분 주기 실행: 지표 계산 및 상태 업데이트"""
    # (이전 코드와 동일)
    global indicator_states
    log(f"===== 지표 계산 사이클 시작 =====")
    top_symbols = get_top_volume_symbols(TOP_N_SYMBOLS)
    if not top_symbols: log("경고: 지표 계산 위한 상위 심볼 정보 없음.", logging.WARNING); return
    log(f"상위 {len(top_symbols)}개 심볼 지표 계산 시작...")
    new_states = {}
    for symbol in top_symbols:
        required_candles = max(BB_LENGTH, MACD_SLOW + MACD_SIGNAL, STOCH_RSI_RSI + STOCH_RSI_STOCH) + 2
        start_str = f"{math.ceil(required_candles * 15 / 60) + 1} hours ago UTC"
        df = get_historical_data(symbol, INTERVAL, start_str, limit=required_candles + 50)
        if df is None: continue
        indicators_df = calculate_indicators(df)
        if indicators_df is None: continue
        try:
            last=indicators_df.iloc[-1]; prev=indicators_df.iloc[-2]
            bb_lower=last['bb_lower']; bb_middle=last['bb_middle']; bb_upper=last['bb_upper']
            k_now=last['stoch_k']; d_now=last['stoch_d']; k_prev=prev['stoch_k']; d_prev=prev['stoch_d']
            stoch_crossed_up = k_now > d_now and k_prev <= d_prev and k_now < 25
            stoch_crossed_down = k_now < d_now and k_prev >= d_prev and k_now > 75
            macd_now=last['macd_line']; signal_now=last['macd_signal']; macd_prev=prev['macd_line']; signal_prev=prev['macd_signal']
            macd_crossed_up = macd_now > signal_now and macd_prev <= signal_prev
            macd_crossed_down = macd_now < signal_now and macd_prev >= signal_prev
            new_states[symbol] = { 'bb_lower': bb_lower, 'bb_middle': bb_middle, 'bb_upper': bb_upper, 'stoch_crossed_up': stoch_crossed_up, 'stoch_crossed_down': stoch_crossed_down, 'macd_crossed_up': macd_crossed_up, 'macd_crossed_down': macd_crossed_down, 'last_update': datetime.now() }
        except Exception as e: log(f"!!! 오류: {symbol} 지표 상태 추출 중 오류 - {e}", logging.ERROR); logger.exception(e)
    with indicator_lock: indicator_states = new_states; log(f"총 {len(indicator_states)}개 심볼 지표 상태 업데이트 완료.")
    log(f"===== 지표 계산 사이클 완료 =====")


def check_realtime_entry():
    """짧은 주기 실행: 실시간 가격 확인 및 진입 조건 판단/실행"""
    global virtual_balance, open_positions, blacklist
    # log(f"--- 실시간 진입 체크 시작 ---") # 로그 너무 많으면 주석 처리

    # --- 메시지 큐 처리 & Whipsaw & 블랙리스트 정리 ---
    positions_closed_in_cycle = []
    while not message_queue.empty():
        try:
            message = message_queue.get_nowait()
            if message.get('type') == 'close':
                symbol=message['symbol']; close_price=message['close_price']; reason=message['reason']
                log(f"메시지 수신: {symbol} 포지션 청산 요청 (사유: {reason}, 가격: {close_price:.4f})")
                trade_log_entry = {} # 거래 기록용 딕셔너리
                with positions_lock:
                    if symbol in open_positions:
                        pos_data=open_positions[symbol]['details']
                        side=pos_data['side']; entry_price=pos_data['entry_price']; quantity=pos_data['quantity']
                        entry_time=pos_data['entry_time']; leverage=pos_data['leverage']

                        # PnL 계산
                        if side == 'LONG': gross_pnl = (close_price - entry_price) * quantity * leverage
                        else: gross_pnl = (entry_price - close_price) * quantity * leverage
                        fee = calculate_fees(entry_price, quantity); net_pnl = gross_pnl - fee

                        current_balance_before_close = virtual_balance # 로그용 현재 잔고
                        with balance_lock: virtual_balance += net_pnl
                        final_balance = virtual_balance # 최종 잔고

                        log(f"포지션 청산 완료: {symbol} ({side}) | PNL: {net_pnl:.2f} (수수료 {fee:.2f}) | 잔고: {final_balance:.2f}")
                        positions_closed_in_cycle.append({'symbol': symbol, 'net_pnl': net_pnl, 'entry_time': entry_time, 'close_time': datetime.now()})

                        # 거래 내역 로깅 데이터 준비
                        trade_log_entry = {
                            'Symbol': symbol, 'Side': side, 'EntryPrice': f"{entry_price:.6f}", 'ClosePrice': f"{close_price:.6f}",
                            'Quantity': f"{quantity:.8f}", 'Leverage': leverage, 'GrossPnL': f"{gross_pnl:.4f}",
                            'Fee': f"{fee:.4f}", 'NetPnL': f"{net_pnl:.4f}", 'FinalBalance': f"{final_balance:.2f}", 'Reason': reason
                        }

                        del open_positions[symbol]
                    else: pass # 이미 처리됨
            # else: 알 수 없는 메시지
        except queue.Empty: break
        except Exception as e: log(f"!!! 오류: 메시지 큐 처리 중 오류 - {e}", logging.ERROR); logger.exception(e)

    # 거래 내역 파일 로깅 (Lock 바깥에서 수행)
    if trade_log_entry:
        log_trade_history(trade_log_entry)

    # Whipsaw 처리
    now_dt = datetime.now()
    for closed_pos in positions_closed_in_cycle:
        symbol=closed_pos['symbol']; net_pnl=closed_pos['net_pnl']; entry_time=closed_pos['entry_time']; close_time=closed_pos['close_time']
        if net_pnl < 0 and (close_time - entry_time) < timedelta(minutes=WHIPSAW_DURATION_MINUTES):
             expiry = close_time + timedelta(minutes=WHIPSAW_DURATION_MINUTES)
             with blacklist_lock: blacklist[symbol] = expiry
             log(f"Whipsaw 감지: {symbol} 을(를) {expiry.strftime('%H:%M:%S')} 까지 블랙리스트에 추가.")

    # 블랙리스트 정리
    symbols_to_remove_from_blacklist = []
    with blacklist_lock:
        for symbol, expiry_time in list(blacklist.items()):
            if now_dt >= expiry_time: symbols_to_remove_from_blacklist.append(symbol)
    if symbols_to_remove_from_blacklist:
         with blacklist_lock:
              for symbol in symbols_to_remove_from_blacklist:
                   if symbol in blacklist: del blacklist[symbol]; log(f"{symbol} 블랙리스트 기간 만료, 제거됨.")

    # --- 신규 진입 조건 확인 ---
    current_pos_count = len(open_positions)
    if MAX_POSITIONS - current_pos_count <= 0: return

    current_position_symbols = list(open_positions.keys())
    with blacklist_lock: blacklisted_symbols = list(blacklist.keys())

    try:
        all_tickers = client.futures_symbol_ticker()
        if not all_tickers: return
        current_prices = {ticker['symbol']: float(ticker['price']) for ticker in all_tickers if isinstance(ticker, dict) and 'symbol' in ticker and 'price' in ticker}
    except Exception as e: log(f"!!! 오류: 실시간 Ticker 가격 조회 실패 - {e}", logging.ERROR); return

    with indicator_lock: relevant_symbols = list(indicator_states.keys())

    for symbol in relevant_symbols:
        if len(open_positions) >= MAX_POSITIONS: break
        if symbol in current_position_symbols: continue
        if symbol in blacklisted_symbols: continue
        if symbol not in current_prices: continue

        current_price = current_prices[symbol]
        if current_price <= 0: continue # 가격 오류 방지

        with indicator_lock: state = indicator_states.get(symbol)
        if state is None or (datetime.now() - state['last_update']) > timedelta(minutes=20): continue

        bb_middle=state['bb_middle']
        stoch_crossed_up=state['stoch_crossed_up']; stoch_crossed_down=state['stoch_crossed_down']
        macd_crossed_up=state['macd_crossed_up']; macd_crossed_down=state['macd_crossed_down']

        # --- 진입 시도 공통 로직 ---
        def attempt_entry(side):
            global virtual_balance, open_positions # 전역 변수 수정 명시
            nonlocal symbol, current_price, bb_middle # 상위 스코프 변수 사용 명시

            if side == 'LONG': potential_profit_per_unit = bb_middle - current_price
            else: potential_profit_per_unit = current_price - bb_middle

            if pd.isna(potential_profit_per_unit) or potential_profit_per_unit <= 0:
                 log(f"  - {symbol} {side} 진입 보류: 잠재 이익 계산 불가 또는 0 이하", logging.DEBUG)
                 return False # 잠재 이익 없으면 진입 불가

            # --- 포지션 크기 계산 (자산 25% 분할) ---
            with balance_lock: current_capital = virtual_balance # 현재 자산 읽기 (Lock 필요)
            capital_for_position = current_capital * CAPITAL_ALLOCATION_PER_POSITION
            target_notional_value = capital_for_position * LEVERAGE # 레버리지 적용된 목표 포지션 가치
            quantity = target_notional_value / current_price
            # ----------------------------------------

            fee = calculate_fees(current_price, quantity)
            estimated_profit = potential_profit_per_unit * quantity * LEVERAGE

            if estimated_profit > fee:
                 log(f"  >>> {symbol} {side} 진입 조건 최종 만족! (예상수익 {estimated_profit:.2f} > 수수료 {fee:.2f})")

                 # --- 격리 마진 설정 시도 ---
                 try:
                      log(f"  - {symbol} 격리(ISOLATED) 마진 설정 시도...")
                      client.futures_change_margin_type(symbol=symbol, marginType='ISOLATED')
                      log(f"  - {symbol} 격리 마진 설정 완료.")
                 except BinanceAPIException as api_e:
                      # 이미 격리거나, 포지션/주문 존재로 변경 불가 시 오류 발생 가능
                      if api_e.code == -4046: # No need to change margin type
                           log(f"  - {symbol} 이미 격리 마진이거나 변경 불필요.")
                      else:
                           log(f"  - 경고: {symbol} 격리 마진 설정 실패 - {api_e}. 교차(CROSS)로 진행될 수 있음.", logging.WARNING)
                 except Exception as e:
                      log(f"  - 경고: {symbol} 격리 마진 설정 중 예상치 못한 오류 - {e}", logging.WARNING)
                 # -------------------------

                 with positions_lock:
                      if symbol not in open_positions and len(open_positions) < MAX_POSITIONS:
                           entry_price=current_price
                           if side == 'LONG': stop_loss_price=entry_price*(1-STOP_LOSS_PERCENT)
                           else: stop_loss_price=entry_price*(1+STOP_LOSS_PERCENT)

                           pos_details={'symbol': symbol, 'side': side, 'entry_price': entry_price, 'quantity': quantity,'entry_time': datetime.now(), 'stop_loss_price': stop_loss_price, 'leverage': LEVERAGE }
                           monitor = threading.Thread(target=position_monitor_thread, args=(symbol, side, entry_price, quantity, stop_loss_price, message_queue), daemon=True)
                           open_positions[symbol] = {'thread': monitor, 'details': pos_details}; monitor.start()
                           log(f"  +++ 가상 {side} 진입 및 모니터링 스레드 시작: {symbol}, 수량: {quantity:.8f}, SL: {stop_loss_price:.4f}")
                           return True # 진입 성공
                      else: return False # 자리 없음 또는 중복
            else:
                 log(f"  - {symbol} {side} 진입 보류: 예상 수익({estimated_profit:.2f}) <= 수수료({fee:.2f})", logging.DEBUG)
                 return False # 수수료 조건 불만족
            return False

        # --- Long/Short 조건 확인 후 진입 시도 ---
        entry_attempted = False
        if stoch_crossed_up and macd_crossed_up:
            entry_attempted = attempt_entry('LONG')

        # Long 진입 안됐고, Short 조건 맞으면 Short 시도
        if not entry_attempted and stoch_crossed_down and macd_crossed_down:
            attempt_entry('SHORT')

    # log(f"--- 실시간 진입 체크 완료 ---") # 로그 너무 많으면 주석 처리


# --- 스케줄링 설정 ---
if __name__ == "__main__":
    log("=== 신규 전략 테스트 프로그램 시작 (실제 API, BB제외, 자산분할, 파일로그) ===")
    log(f"초기 가상 자본: {initial_capital:.2f} USDT")
    log("⚠️ 실제 API 사용 중입니다. API 키 보안 및 권한 설정을 반드시 확인하세요!")
    log("⚠️ 읽기 전용(Read-Only) API 키 사용을 강력히 권장합니다!")

    run_strategy_indicators() # 초기 지표 계산

    schedule.every(15).minutes.at(":01").do(run_strategy_indicators)
    schedule.every(REALTIME_CHECK_INTERVAL_SECONDS).seconds.do(check_realtime_entry)

    log(f"스케줄러 시작: {15}분마다 지표 계산, {REALTIME_CHECK_INTERVAL_SECONDS}초마다 실시간 진입 체크")
    log(f"기본 로그는 {log_file}, 거래 내역은 {trade_log_file} 파일에 저장됩니다.")

    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            log("사용자에 의해 프로그램 종료.")
            break
        except Exception as e:
            log(f"!!! 메인 루프 오류 발생: {e}", logging.CRITICAL); logger.exception(e)
            log("30초 후 재시도..."); time.sleep(30)