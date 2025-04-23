import pandas as pd
import pandas_ta as ta
from binance.client import Client
from binance.enums import *
import time
import schedule
from datetime import datetime, timedelta
import math
import os
import threading
import queue
import traceback

# --- 설정 ---
# ⚠️⚠️⚠️ 실제 바이낸스 API 키를 사용합니다. 보안에 극도로 유의하세요! ⚠️⚠️⚠️
# ⚠️⚠️⚠️ 읽기 전용(Read-Only) API 키 사용을 강력히 권장합니다! ⚠️⚠️⚠️
# API_KEY = os.environ.get('BINANCE_API_KEY', "YOUR_BINANCE_LIVE_API_KEY")
# API_SECRET = os.environ.get('BINANCE_SECRET_KEY', "YOUR_BINANCE_LIVE_SECRET_KEY")
API_KEY = ""    # <<<< 실제 바이낸스 API 키 입력
API_SECRET = "" # <<<< 실제 바이낸스 시크릿 키 입력
USE_TESTNET = False                # <<<< 실제 API 사용 설정

# 전략 파라미터 (이전과 동일)
TOP_N_SYMBOLS = 20
INTERVAL = Client.KLINE_INTERVAL_15MINUTE
BB_LENGTH = 20; BB_STD_DEV = 2.0
STOCH_RSI_K = 14; STOCH_RSI_D = 3; STOCH_RSI_RSI = 14; STOCH_RSI_STOCH = 14
MACD_FAST = 12; MACD_SLOW = 26; MACD_SIGNAL = 9
LEVERAGE = 5
ENTRY_BB_Nahe_PERCENT = 0.01
STOP_LOSS_PERCENT = 0.03
TAKE_PROFIT_BB_Nahe_PERCENT = 0.005
MAX_POSITIONS = 4
WHIPSAW_DURATION_MINUTES = 60
FEE_RATE = 0.0005
POSITION_MONITOR_INTERVAL_SECONDS = 30
REALTIME_CHECK_INTERVAL_SECONDS = 30

# 가상 계정 및 상태 관리 (이전과 동일)
initial_capital = 10000.0
virtual_balance = initial_capital
open_positions = {}
indicator_states = {}
blacklist = {}
message_queue = queue.Queue()

# 스레드 안전성을 위한 Lock 객체 (이전과 동일)
balance_lock = threading.Lock()
positions_lock = threading.Lock()
indicator_lock = threading.Lock()
blacklist_lock = threading.Lock()

# --- 바이낸스 클라이언트 생성 (실제 API 사용) ---
if USE_TESTNET:
     # 이 부분은 이제 실행되지 않음
    client = Client(API_KEY, API_SECRET, testnet=True)
    print("바이낸스 테스트넷 연결 시도...")
    try: client.futures_ping(); print("테스트넷 연결 성공.")
    except Exception as e: print(f"오류: 테스트넷 연결 실패 - {e}"); exit()
else:
    # ⚠️ 실제 서버 연결
    client = Client(API_KEY, API_SECRET)
    print("⚠️ 실제 바이낸스 서버 연결 시도...")
    try:
        # 선물 계정 정보 조회로 연결 및 API 키 유효성 확인 시도
        account_info = client.futures_account()
        if account_info:
             print("✅ 실제 서버 연결 및 API 키 인증 성공.")
             # print(f"선물 계정 자산: {account_info.get('totalWalletBalance', 'N/A')}") # 필요시 잔고 확인
        else:
             print("❌ 실제 서버 연결 성공했으나 계정 정보 조회 실패. API 키 권한 확인 필요.")
             exit()
    except Exception as e:
        print(f"❌ 오류: 실제 서버 연결 또는 API 키 인증 실패 - {e}")
        print("   - API 키/시크릿 키가 정확한지 확인하세요.")
        print("   - API 키 설정에서 IP 접근 제한이 있다면 허용 목록에 현재 IP를 추가했는지 확인하세요.")
        print("   - API 키에 '선물 활성화(Enable Futures)' 또는 최소한 '읽기(Enable Reading)' 권한이 있는지 확인하세요.")
        exit()


# --- 헬퍼 함수 ---
def log(message):
    # (이전 코드와 동일)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def get_top_volume_symbols(n):
    """거래대금 상위 N개 USDT Perpetual 심볼 조회 (실제 서버용)"""
    try:
        log("Ticker 정보 조회 시도...")
        tickers = client.futures_ticker()
        log(f"Ticker 정보 수신 완료 (총 {len(tickers) if isinstance(tickers, list) else 'N/A'}개 항목 수신됨).")

        if not tickers or not isinstance(tickers, list):
            log("경고: Ticker 정보가 비어있거나 리스트 형식이 아닙니다.")
            return []

        usdt_tickers = []
        filter_log_count = 0
        for t in tickers:
            passes = True; reason = []
            if not isinstance(t, dict): passes = False; reason.append("타입 Dict 아님")
            elif 'symbol' not in t: passes = False; reason.append("키 'symbol' 없음")
            elif not t['symbol'].endswith('USDT'): passes = False; reason.append("USDT 아님")
            elif '_' in t['symbol']: passes = False; reason.append("심볼에 '_' 포함")
            # 실제 서버에서는 contractType 필드가 있을 것으로 기대하지만, 안전하게 .get() 사용
            # elif t.get('contractType') != 'PERPETUAL': passes = False; reason.append(f"contractType 아님 ({t.get('contractType')})")
            elif 'quoteVolume' not in t: passes = False; reason.append("키 'quoteVolume' 없음")
            # 실제 서버 데이터이므로 거래량 0보다 큰 조건 복원
            elif not float(t.get('quoteVolume', 0)) > 0:
                 passes = False; reason.append(f"quoteVolume 0 이하 ({t.get('quoteVolume')})")

            if passes: usdt_tickers.append(t)
            elif filter_log_count < 5:
                log(f"  [필터 제외됨] {t.get('symbol', 'N/A')} - 이유: {', '.join(reason)}")
                filter_log_count += 1

        log(f"필터링 후 USDT Perpetual 심볼 개수: {len(usdt_tickers)}")
        if not usdt_tickers:
            log("경고: 필터링 후 남은 USDT 선물 심볼이 없습니다.")
            return []

        sorted_tickers = sorted(usdt_tickers, key=lambda x: float(x.get('quoteVolume', 0)), reverse=True)
        return [t['symbol'] for t in sorted_tickers[:n]]

    except Exception as e:
        log(f"!!! 오류: 거래대금 상위 심볼 조회 중 예외 발생 - {e}")
        traceback.print_exc()
        return []

def get_historical_data(symbol, interval, start_str, limit=500):
    """과거 Klines 데이터 조회 (start_str 필수)"""
    # (이전 코드와 동일)
    try:
        klines = client.futures_historical_klines(symbol=symbol, interval=interval, start_str=start_str, limit=limit)
        if not klines: return None
        df = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)
        for col in ['open', 'high', 'low', 'close', 'volume']: df[col] = df[col].astype(float)
        return df
    except Exception as e:
        # 실제 API 사용 시 Rate Limit 에러가 발생할 수 있음
        log(f"오류: {symbol} {interval} 데이터 조회 실패 (start='{start_str}', limit={limit}) - {e}")
        if 'Rate limit exceeded' in str(e):
             log("경고: API Rate Limit 초과! 요청 빈도 조절 필요.")
             # 필요한 경우 여기서 프로그램 중지 또는 대기 로직 추가
        return None

def calculate_indicators(df):
    """볼린저밴드, 스토캐스틱RSI, MACD 계산"""
    # (이전 코드와 동일)
    required_length = max(BB_LENGTH, MACD_SLOW + MACD_SIGNAL, STOCH_RSI_RSI + STOCH_RSI_STOCH) + 2
    if df is None or len(df) < required_length:
        # log(f"경고: 지표 계산 위한 데이터 부족 (필요: {required_length}, 현재: {len(df) if df is not None else 0})") # 로그 너무 많으면 주석처리
        return None
    try:
        df.ta.bbands(length=BB_LENGTH, std=BB_STD_DEV, append=True)
        stoch_rsi_df = df.ta.stochrsi(k=STOCH_RSI_K, d=STOCH_RSI_D, rsi_length=STOCH_RSI_RSI, stochastic_length=STOCH_RSI_STOCH)
        macd_df = df.ta.macd(fast=MACD_FAST, slow=MACD_SLOW, signal=MACD_SIGNAL)

        lower_col = f'BBL_{BB_LENGTH}_{BB_STD_DEV:.1f}'; middle_col = f'BBM_{BB_LENGTH}_{BB_STD_DEV:.1f}'; upper_col = f'BBU_{BB_LENGTH}_{BB_STD_DEV:.1f}'
        k_col = f'STOCHRSIk_{STOCH_RSI_STOCH}_{STOCH_RSI_RSI}_{STOCH_RSI_K}_{STOCH_RSI_D}'
        d_col = f'STOCHRSId_{STOCH_RSI_STOCH}_{STOCH_RSI_RSI}_{STOCH_RSI_K}_{STOCH_RSI_D}'
        macd_line_col = f'MACD_{MACD_FAST}_{MACD_SLOW}_{MACD_SIGNAL}'; signal_line_col = f'MACDs_{MACD_FAST}_{MACD_SLOW}_{MACD_SIGNAL}'

        df.rename(columns={lower_col: 'bb_lower', middle_col: 'bb_middle', upper_col: 'bb_upper'}, inplace=True, errors='ignore')
        if k_col in stoch_rsi_df.columns: df['stoch_k'] = stoch_rsi_df[k_col]
        if d_col in stoch_rsi_df.columns: df['stoch_d'] = stoch_rsi_df[d_col]
        if macd_line_col in macd_df.columns: df['macd_line'] = macd_df[macd_line_col]
        if signal_line_col in macd_df.columns: df['macd_signal'] = macd_df[signal_line_col]

        required_cols = ['bb_lower', 'bb_middle', 'bb_upper', 'stoch_k', 'stoch_d', 'macd_line', 'macd_signal']
        if not all(col in df.columns for col in required_cols): return None
        if df[required_cols].iloc[-2:].isnull().any().any(): return None # NaN 체크

        return df
    except Exception as e:
        log(f"!!! 오류: 지표 계산 중 예외 발생 - {e}"); traceback.print_exc()
        return None

def calculate_fees(entry_price, quantity):
    """예상 수수료 계산 (진입+청산)"""
    # (이전 코드와 동일)
    position_value = entry_price * quantity
    total_fee = position_value * FEE_RATE * 2
    return total_fee

# --- 포지션 모니터링 스레드 ---
def position_monitor_thread(symbol, side, entry_price, quantity, stop_loss_price, msg_q):
    """개별 포지션 모니터링 및 청산 신호 전송 스레드"""
    # (이전 코드와 동일 - 단, 실시간 가격 사용 주석 처리 유지)
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

            last_candle = df.iloc[-1]
            current_price = last_candle['close'] # 종가 기준 모니터링
            # 실시간 가격 사용 원할 경우 아래 주석 해제 및 테스트 필요
            # try:
            #     current_ticker = client.futures_symbol_ticker(symbol=symbol)
            #     current_price = float(current_ticker['price'])
            # except Exception as ticker_e:
            #     log(f"[{symbol}] 실시간 가격 조회 실패: {ticker_e}")
            #     continue # 가격 조회 실패 시 이번 사이클 건너뛰기

            bb_upper = last_candle['bb_upper']; bb_lower = last_candle['bb_lower']

            # log(f"[{symbol}] 모니터링: 현재가 {current_price:.4f}, SL: {stop_loss_price:.4f}, BB: {bb_lower:.4f} / {bb_upper:.4f}") # 로그 너무 많으면 주석처리

            # 손절매 조건
            if (side == 'LONG' and current_price <= stop_loss_price) or \
               (side == 'SHORT' and current_price >= stop_loss_price):
                log(f"!!! [{symbol}] 손절매 조건 도달! 현재가: {current_price:.4f}")
                msg_q.put({'type': 'close', 'symbol': symbol, 'close_price': current_price, 'reason': 'Stop Loss'})
                break
            # 익절 조건
            tp_hit = False
            if side == 'LONG' and pd.notna(bb_upper) and abs(current_price - bb_upper) / bb_upper <= TAKE_PROFIT_BB_Nahe_PERCENT: tp_hit = True; reason = 'Take Profit (BB상단 근접)'
            elif side == 'SHORT' and pd.notna(bb_lower) and abs(current_price - bb_lower) / bb_lower <= TAKE_PROFIT_BB_Nahe_PERCENT: tp_hit = True; reason = 'Take Profit (BB하단 근접)'
            if tp_hit:
                log(f"!!! [{symbol}] 익절 조건 도달! 현재가: {current_price:.4f}")
                msg_q.put({'type': 'close', 'symbol': symbol, 'close_price': current_price, 'reason': reason})
                break
        except Exception as e:
            log(f"!!! 오류: [{symbol}] 모니터링 스레드 내 오류 발생 - {e}"); traceback.print_exc()
            time.sleep(60)
    log(f"[{symbol}] 모니터링 스레드 종료.")


# --- 메인 로직 함수 ---
def run_strategy_indicators():
    """15분 주기 실행: 지표 계산 및 상태 업데이트"""
    # (이전 코드와 동일)
    global indicator_states
    log(f"===== 지표 계산 사이클 시작 (Time: {datetime.now()}) =====")
    top_symbols = get_top_volume_symbols(TOP_N_SYMBOLS)
    if not top_symbols: log("경고: 지표 계산 위한 상위 심볼 정보 없음."); return
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
            new_states[symbol] = {
                'bb_lower': bb_lower, 'bb_middle': bb_middle, 'bb_upper': bb_upper,
                'stoch_crossed_up': stoch_crossed_up, 'stoch_crossed_down': stoch_crossed_down,
                'macd_crossed_up': macd_crossed_up, 'macd_crossed_down': macd_crossed_down,
                'last_update': datetime.now()
            }
        except Exception as e: log(f"!!! 오류: {symbol} 지표 상태 추출 중 오류 - {e}")
    with indicator_lock: indicator_states = new_states; log(f"총 {len(indicator_states)}개 심볼 지표 상태 업데이트 완료.")
    log(f"===== 지표 계산 사이클 완료 (Time: {datetime.now()}) =====")

def check_realtime_entry():
    """짧은 주기 실행: 실시간 가격 확인 및 진입 조건 판단/실행"""
    # (이전 코드와 동일)
    global virtual_balance, open_positions, blacklist
    # log(f"--- 실시간 진입 체크 시작 (Time: {datetime.now()}) ---") # 로그 너무 많으면 주석 처리

    # 메시지 큐 처리
    positions_closed_in_cycle = []
    while not message_queue.empty():
        try:
            message = message_queue.get_nowait()
            if message.get('type') == 'close':
                symbol=message['symbol']; close_price=message['close_price']; reason=message['reason']
                log(f"메시지 수신: {symbol} 포지션 청산 요청 (사유: {reason}, 가격: {close_price:.4f})")
                with positions_lock:
                    if symbol in open_positions:
                        pos_data=open_positions[symbol]['details']
                        side=pos_data['side']; entry_price=pos_data['entry_price']; quantity=pos_data['quantity']; entry_time=pos_data['entry_time']; leverage=pos_data['leverage']
                        if side == 'LONG': pnl = (close_price - entry_price) * quantity * leverage
                        else: pnl = (entry_price - close_price) * quantity * leverage
                        fee = calculate_fees(entry_price, quantity); net_pnl = pnl - fee
                        with balance_lock: virtual_balance += net_pnl
                        log(f"포지션 청산 완료: {symbol} ({side}) | PNL: {net_pnl:.2f} | 잔고: {virtual_balance:.2f}")
                        positions_closed_in_cycle.append({'symbol': symbol, 'net_pnl': net_pnl, 'entry_time': entry_time, 'close_time': datetime.now()})
                        del open_positions[symbol]
                    # else: 이미 처리됨
            # else: 알 수 없는 메시지
        except queue.Empty: break
        except Exception as e: log(f"!!! 오류: 메시지 큐 처리 중 오류 - {e}"); traceback.print_exc()

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
        for symbol, expiry_time in list(blacklist.items()): # 순회 중 변경 위해 list() 사용
            if now_dt >= expiry_time: symbols_to_remove_from_blacklist.append(symbol)
    if symbols_to_remove_from_blacklist:
         with blacklist_lock:
              for symbol in symbols_to_remove_from_blacklist:
                   if symbol in blacklist: del blacklist[symbol]; log(f"{symbol} 블랙리스트 기간 만료, 제거됨.")

    # 신규 진입 조건 확인
    current_pos_count = len(open_positions)
    if MAX_POSITIONS - current_pos_count <= 0: return # 슬롯 없음

    current_position_symbols = list(open_positions.keys())
    with blacklist_lock: blacklisted_symbols = list(blacklist.keys())

    try: # 실시간 가격 조회
        all_tickers = client.futures_symbol_ticker()
        if not all_tickers: return
        current_prices = {ticker['symbol']: float(ticker['price']) for ticker in all_tickers if isinstance(ticker, dict) and 'symbol' in ticker and 'price' in ticker}
    except Exception as e: log(f"!!! 오류: 실시간 Ticker 가격 조회 실패 - {e}"); return

    with indicator_lock: relevant_symbols = list(indicator_states.keys())

    checked_count = 0
    for symbol in relevant_symbols:
        if len(open_positions) >= MAX_POSITIONS: break
        if symbol in current_position_symbols: continue
        if symbol in blacklisted_symbols: continue
        if symbol not in current_prices: continue

        checked_count += 1
        current_price = current_prices[symbol]

        with indicator_lock: state = indicator_states.get(symbol)
        if state is None or (datetime.now() - state['last_update']) > timedelta(minutes=20): continue

        bb_lower=state['bb_lower']; bb_middle=state['bb_middle']; bb_upper=state['bb_upper']
        stoch_crossed_up=state['stoch_crossed_up']; stoch_crossed_down=state['stoch_crossed_down']
        macd_crossed_up=state['macd_crossed_up']; macd_crossed_down=state['macd_crossed_down']

        # Long 진입
        long_bb_cond = pd.notna(bb_lower) and abs(current_price - bb_lower) / bb_lower <= ENTRY_BB_Nahe_PERCENT
        if long_bb_cond and stoch_crossed_up and macd_crossed_up:
            if pd.notna(bb_middle):
                 potential_profit_per_unit = bb_middle - current_price
                 position_value_usdt=100.0; quantity=position_value_usdt/current_price
                 fee=calculate_fees(current_price, quantity); estimated_profit=potential_profit_per_unit*quantity*LEVERAGE
                 if estimated_profit > fee:
                      log(f"  >>> {symbol} 롱 진입 조건 최종 만족!")
                      with positions_lock:
                           if symbol not in open_positions and len(open_positions) < MAX_POSITIONS:
                                entry_price=current_price; stop_loss_price=entry_price*(1-STOP_LOSS_PERCENT)
                                pos_details={'symbol': symbol, 'side': 'LONG', 'entry_price': entry_price, 'quantity': quantity,'entry_time': datetime.now(), 'stop_loss_price': stop_loss_price, 'leverage': LEVERAGE }
                                monitor = threading.Thread(target=position_monitor_thread, args=(symbol, 'LONG', entry_price, quantity, stop_loss_price, message_queue), daemon=True)
                                open_positions[symbol] = {'thread': monitor, 'details': pos_details}; monitor.start()
                                log(f"  +++ 가상 롱 진입 및 모니터링 스레드 시작: {symbol}, SL: {stop_loss_price:.4f}")

        # Short 진입
        short_bb_cond = pd.notna(bb_upper) and abs(current_price - bb_upper) / bb_upper <= ENTRY_BB_Nahe_PERCENT
        if short_bb_cond and stoch_crossed_down and macd_crossed_down:
             if pd.notna(bb_middle):
                 potential_profit_per_unit = current_price - bb_middle
                 position_value_usdt=100.0; quantity=position_value_usdt/current_price
                 fee=calculate_fees(current_price, quantity); estimated_profit=potential_profit_per_unit*quantity*LEVERAGE
                 if estimated_profit > fee:
                      log(f"  >>> {symbol} 숏 진입 조건 최종 만족!")
                      with positions_lock:
                           if symbol not in open_positions and len(open_positions) < MAX_POSITIONS:
                                entry_price=current_price; stop_loss_price=entry_price*(1+STOP_LOSS_PERCENT)
                                pos_details={'symbol': symbol, 'side': 'SHORT', 'entry_price': entry_price, 'quantity': quantity,'entry_time': datetime.now(), 'stop_loss_price': stop_loss_price, 'leverage': LEVERAGE }
                                monitor = threading.Thread(target=position_monitor_thread, args=(symbol, 'SHORT', entry_price, quantity, stop_loss_price, message_queue), daemon=True)
                                open_positions[symbol] = {'thread': monitor, 'details': pos_details}; monitor.start()
                                log(f"  +++ 가상 숏 진입 및 모니터링 스레드 시작: {symbol}, SL: {stop_loss_price:.4f}")

    # log(f"--- 실시간 진입 체크 완료 ({checked_count}개 심볼 확인) ---") # 로그 너무 많으면 주석 처리


# --- 스케줄링 설정 ---
if __name__ == "__main__":
    log("=== 신규 전략 테스트 프로그램 시작 (실제 API 데이터 사용) ===")
    log(f"초기 가상 자본: {initial_capital:.2f} USDT")
    log("⚠️ 실제 API 사용 중입니다. API 키 보안 및 권한 설정을 반드시 확인하세요!")
    log("⚠️ 읽기 전용(Read-Only) API 키 사용을 강력히 권장합니다!")

    run_strategy_indicators() # 초기 지표 계산

    schedule.every(15).minutes.at(":01").do(run_strategy_indicators)
    schedule.every(REALTIME_CHECK_INTERVAL_SECONDS).seconds.do(check_realtime_entry)

    log(f"스케줄러 시작: {15}분마다 지표 계산, {REALTIME_CHECK_INTERVAL_SECONDS}초마다 실시간 진입 체크")

    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            log("사용자에 의해 프로그램 종료.")
            break
        except Exception as e:
            log(f"!!! 메인 루프 오류 발생: {e}"); traceback.print_exc()
            log("30초 후 재시도..."); time.sleep(30)