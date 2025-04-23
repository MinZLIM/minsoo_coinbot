import pandas as pd
import pandas_ta as ta
from binance.client import Client
from binance.enums import *
import time
import schedule # 주기적 실행을 위해
from datetime import datetime
import math

# --- 설정 ---
API_KEY = "8ba1c0b2aba42aa05d70c854ea0f507c75cb894dd72e8b4077cc485d689a8f74"    # 테스트넷 API 키 입력
API_SECRET = "C2GGjvR7nfC099bJwe3MxfUkQKcHIpYQZdyaX5nTyfVQN6lVfUO3qBdGWc0vJHFI" # 테스트넷 시크릿 키 입력
USE_TESTNET = True                 # 테스트넷 사용

# 전략 파라미터
TOP_N_SYMBOLS = 20             # 모니터링할 상위 거래대금 심볼 개수
BB_LENGTH = 20                 # 볼린저 밴드 기간 (15분 봉 기준)
BB_STD_DEV = 2.0               # 볼린저 밴드 표준편차 배수
ENTRY_THRESHOLD_PERCENT = 0.90 # 진입: 밴드 상/하단에서 (중심선 기준 거리의) 90% 지점
STOP_LOSS_THRESHOLD_PERCENT = 0.85 # 손절: 밴드 상/하단 밖으로 (밴드폭의) 85% 거리
LEVERAGE = 10                  # 레버리지
MAX_POSITIONS = 4              # 최대 동시 진입 포지션 개수
FEE_RATE = 0.0005              # 수수료율 (시장가 Taker 0.04% + 약간의 여유 = 0.05% 가정, 진입/청산 총 0.1%) -> 레버리지 미고려, 포지션 가치 기준
INTERVAL_15M = Client.KLINE_INTERVAL_15MINUTE # 15분 봉
INTERVAL_1D = Client.KLINE_INTERVAL_1DAY   # 일봉

# 가상 계정 설정
initial_capital = 10000.0 # 초기 가상 자본 (USDT)
virtual_balance = initial_capital
open_positions = [] # { 'symbol': str, 'side': 'LONG'/'SHORT', 'entry_price': float, 'quantity': float, 'entry_time': datetime, 'stop_loss_price': float, 'take_profit_target': float }

# --- 바이낸스 클라이언트 생성 ---
if USE_TESTNET:
    client = Client(API_KEY, API_SECRET, testnet=True)
    client.futures_change_leverage(symbol='BTCUSDT', leverage=LEVERAGE) # 대표 심볼 레버리지 설정 (다른 심볼도 필요시 설정)
    print("바이낸스 테스트넷 연결 완료.")
else:
    # 실제 거래용 클라이언트 (주의!)
    # client = Client(API_KEY, API_SECRET)
    print("경고: 실서버 설정입니다. 테스트 목적으로는 USE_TESTNET=True로 변경하세요.")
    exit() # 실서버 실행 방지

# --- 헬퍼 함수 ---

def log(message):
    """간단한 로깅 함수"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")
    # TODO: 나중에 Telegram/카카오톡 등으로 보내는 로직 추가 가능

def get_top_volume_symbols(n):
    """24시간 거래대금 상위 N개 USDT 선물 심볼 조회"""
    try:
        tickers = client.futures_ticker()
        # USDT 무기한 선물만 필터링 ('contractType' == 'PERPETUAL' and symbol ends with 'USDT')
        usdt_tickers = [t for t in tickers if t['symbol'].endswith('USDT') and '_' not in t['symbol']] # 인덱스 상품 등 제외
        # quoteVolume 기준으로 정렬 (거래대금)
        sorted_tickers = sorted(usdt_tickers, key=lambda x: float(x['quoteVolume']), reverse=True)
        return [t['symbol'] for t in sorted_tickers[:n]]
    except Exception as e:
        log(f"오류: 거래대금 상위 심볼 조회 실패 - {e}")
        return []

def get_historical_data(symbol, interval, lookback):
    """과거 Klines 데이터 조회"""
    try:
        klines = client.futures_historical_klines(symbol, interval, lookback)
        df = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time',
                                           'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume',
                                           'taker_buy_quote_asset_volume', 'ignore'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)
        df['open'] = df['open'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['close'] = df['close'].astype(float)
        df['volume'] = df['volume'].astype(float)
        return df
    except Exception as e:
        log(f"오류: {symbol} {interval} 데이터 조회 실패 - {e}")
        return None

def calculate_indicators(df):
    """볼린저 밴드 계산"""
    if df is None or len(df) < BB_LENGTH:
        return df
    try:
        # pandas_ta 사용하여 볼린저 밴드 계산
        df.ta.bbands(length=BB_LENGTH, std=BB_STD_DEV, append=True)
        # 컬럼 이름이 다를 수 있으니 확인 필요 (예: 'BBL_20_2.0', 'BBM_20_2.0', 'BBU_20_2.0')
        # 필요시 컬럼 이름 명시적으로 변경
        df.rename(columns={
            f'BBL_{BB_LENGTH}_{BB_STD_DEV}': 'bb_lower',
            f'BBM_{BB_LENGTH}_{BB_STD_DEV}': 'bb_middle',
            f'BBU_{BB_LENGTH}_{BB_STD_DEV}': 'bb_upper'
        }, inplace=True)
        # 밴드 폭 계산
        if 'bb_upper' in df.columns and 'bb_lower' in df.columns:
             df['bb_width'] = df['bb_upper'] - df['bb_lower']
        return df
    except Exception as e:
        log(f"오류: 지표 계산 실패 - {e}")
        return df # 지표 계산 실패 시 원본 반환

def check_trend(symbol):
    """일봉 기준 추세 판단"""
    df_1d = get_historical_data(symbol, INTERVAL_1D, "3 day ago UTC") # 최근 3일 데이터 (오늘, 어제, 그제)
    if df_1d is None or len(df_1d) < 2:
        return None # 데이터 부족

    # 가장 최근 봉 (오늘, 아직 마감 안됨) 과 전날 봉 비교
    current_price = df_1d['close'].iloc[-1]
    previous_close = df_1d['close'].iloc[-2]

    if current_price > previous_close:
        return 'UPTREND'
    elif current_price < previous_close:
        return 'DOWNTREND'
    else:
        return 'SIDEWAYS'

def calculate_fees(entry_price, quantity, leverage):
    """예상 수수료 계산 (진입+청산)"""
    position_value = entry_price * quantity # 레버리지 적용 전 가치
    # 실제 수수료는 (포지션 가치 / 레버리지) * 수수료율 이지만,
    # 바이낸스는 포지션 가치(entry_price * quantity) 기준으로 계산하므로 아래와 같이 계산
    entry_fee = position_value * FEE_RATE
    exit_fee = position_value * FEE_RATE # 청산 시 가격 변동 무시하고 단순 계산
    return entry_fee + exit_fee

# --- 주기적으로 실행될 메인 로직 ---
def run_strategy():
    global virtual_balance, open_positions
    log(f"전략 실행 시작. 현재 가상 잔고: {virtual_balance:.2f} USDT")

    # 1. 포지션 관리 (청산 조건 확인)
    positions_to_remove = []
    for i, position in enumerate(open_positions):
        symbol = position['symbol']
        side = position['side']
        log(f"포지션 확인 중: {symbol} ({side})")
        df_15m = get_historical_data(symbol, INTERVAL_15M, f"{BB_LENGTH + 5} candles ago UTC") # 지표 계산 위해 충분한 데이터 로드
        if df_15m is None or df_15m.empty:
            continue
        df_15m = calculate_indicators(df_15m)
        if df_15m is None or 'bb_upper' not in df_15m.columns or df_15m.iloc[-1].isnull().any():
             log(f"경고: {symbol} 지표 데이터 부족 또는 오류로 포지션 관리 건너뜀.")
             continue

        last_candle = df_15m.iloc[-1]
        current_price = last_candle['close']
        bb_upper = last_candle['bb_upper']
        bb_lower = last_candle['bb_lower']
        bb_middle = last_candle['bb_middle']
        # 밴드 폭의 절반 (중심선 ~ 상/하단 거리)
        half_bb_width = (bb_upper - bb_lower) / 2.0 if bb_upper > bb_lower else 0

        # 청산 조건 확인
        closed = False
        pnl = 0.0

        # 7. 목표가 도달 확인
        if side == 'SHORT' and current_price <= bb_lower: # 하단 밴드 도달/하회 시 익절
            pnl = (position['entry_price'] - current_price) * position['quantity'] * LEVERAGE
            fee = calculate_fees(position['entry_price'], position['quantity'], LEVERAGE) # 가정
            pnl -= fee # 수수료 차감
            virtual_balance += pnl
            log(f"익절 청산: {symbol} ({side}). 진입가: {position['entry_price']:.4f}, 청산가: {current_price:.4f}, PNL: {pnl:.2f} USDT (수수료 포함)")
            closed = True
        elif side == 'LONG' and current_price >= bb_upper: # 상단 밴드 도달/상회 시 익절
            pnl = (current_price - position['entry_price']) * position['quantity'] * LEVERAGE
            fee = calculate_fees(position['entry_price'], position['quantity'], LEVERAGE) # 가정
            pnl -= fee # 수수료 차감
            virtual_balance += pnl
            log(f"익절 청산: {symbol} ({side}). 진입가: {position['entry_price']:.4f}, 청산가: {current_price:.4f}, PNL: {pnl:.2f} USDT (수수료 포함)")
            closed = True

        # 6. 손절가 도달 확인 (익절 조건보다 먼저 확인)
        if not closed: # 익절되지 않았을 경우 손절 확인
            stop_price_short = bb_upper + half_bb_width * STOP_LOSS_THRESHOLD_PERCENT # 상단 밴드 위쪽
            stop_price_long = bb_lower - half_bb_width * STOP_LOSS_THRESHOLD_PERCENT # 하단 밴드 아래쪽

            if side == 'SHORT' and current_price >= stop_price_short:
                pnl = (position['entry_price'] - current_price) * position['quantity'] * LEVERAGE
                fee = calculate_fees(position['entry_price'], position['quantity'], LEVERAGE) # 가정
                pnl -= fee # 수수료 차감
                virtual_balance += pnl
                log(f"손절 청산: {symbol} ({side}). 진입가: {position['entry_price']:.4f}, 청산가: {current_price:.4f}, 손절 기준가: {stop_price_short:.4f}, PNL: {pnl:.2f} USDT (수수료 포함)")
                closed = True
            elif side == 'LONG' and current_price <= stop_price_long:
                pnl = (current_price - position['entry_price']) * position['quantity'] * LEVERAGE
                fee = calculate_fees(position['entry_price'], position['quantity'], LEVERAGE) # 가정
                pnl -= fee # 수수료 차감
                virtual_balance += pnl
                log(f"손절 청산: {symbol} ({side}). 진입가: {position['entry_price']:.4f}, 청산가: {current_price:.4f}, 손절 기준가: {stop_price_long:.4f}, PNL: {pnl:.2f} USDT (수수료 포함)")
                closed = True

        if closed:
            positions_to_remove.append(i)

    # 실제 리스트에서 제거 (뒤에서부터 제거해야 인덱스 문제 없음)
    for i in sorted(positions_to_remove, reverse=True):
        del open_positions[i]

    # 2. 신규 진입 조건 확인
    if len(open_positions) < MAX_POSITIONS:
        log(f"신규 진입 가능 슬롯: {MAX_POSITIONS - len(open_positions)}개")
        top_symbols = get_top_volume_symbols(TOP_N_SYMBOLS)
        if not top_symbols:
             log("경고: 거래대금 상위 심볼 정보를 가져오지 못했습니다.")
             return # 심볼 정보 없으면 진입 불가

        symbols_in_position = [p['symbol'] for p in open_positions]

        for symbol in top_symbols:
            if len(open_positions) >= MAX_POSITIONS:
                log("최대 포지션 개수에 도달하여 신규 진입 중단.")
                break
            if symbol in symbols_in_position:
                continue # 이미 포지션 보유 중인 심볼은 건너뛰기

            log(f"심볼 확인 중: {symbol}")

            # 2) 추세 확인
            trend = check_trend(symbol)
            if trend is None:
                log(f"경고: {symbol} 추세 확인 실패.")
                continue
            log(f"{symbol} 일봉 추세: {trend}")

            # 15분봉 데이터 및 지표 계산
            df_15m = get_historical_data(symbol, INTERVAL_15M, f"{BB_LENGTH + 5} candles ago UTC")
            if df_15m is None or df_15m.empty:
                 continue
            df_15m = calculate_indicators(df_15m)
            if df_15m is None or 'bb_upper' not in df_15m.columns or df_15m.iloc[-1].isnull().any():
                 log(f"경고: {symbol} 15분봉 지표 데이터 부족 또는 오류.")
                 continue

            last_candle = df_15m.iloc[-1]
            current_price = last_candle['close']
            bb_upper = last_candle['bb_upper']
            bb_lower = last_candle['bb_lower']
            bb_middle = last_candle['bb_middle']
            bb_width = last_candle['bb_width']

            # 밴드 폭의 절반 (중심선 ~ 상/하단 거리)
            half_bb_width = (bb_upper - bb_lower) / 2.0 if bb_upper > bb_lower else 0

            # 진입 가격대 계산
            # 90% 조건 해석: 중심선에서 밴드 끝까지 거리의 90% 지점 도달 시
            short_entry_price_threshold = bb_middle + half_bb_width * ENTRY_THRESHOLD_PERCENT
            long_entry_price_threshold = bb_middle - half_bb_width * ENTRY_THRESHOLD_PERCENT

            # 3) 숏 포지션 진입 조건 확인
            if trend == 'DOWNTREND' and current_price >= short_entry_price_threshold:
                log(f"{symbol} 숏 진입 조건 확인: 현재가({current_price:.4f}) >= 진입 기준가({short_entry_price_threshold:.4f})")
                # 5) 수수료 조건 확인 (목표가: 하단 밴드)
                potential_profit_per_unit = current_price - bb_lower
                # 가상 진입 수량 계산 (예: 가용 자본의 1% 사용)
                # risk_amount = virtual_balance * 0.01
                # quantity = (risk_amount * LEVERAGE) / current_price
                # 여기서는 단순화를 위해 고정 수량 또는 가치 사용 (실제로는 최소 주문량 등 고려 필요)
                # 예시: 100 USDT 가치의 포지션 진입
                position_value_usdt = 100.0
                quantity = position_value_usdt / current_price

                estimated_fee = calculate_fees(current_price, quantity, LEVERAGE)
                estimated_target_profit = potential_profit_per_unit * quantity * LEVERAGE

                if bb_width > 0 and estimated_target_profit > estimated_fee:
                    log(f"{symbol} 숏 진입! 예상 수익({estimated_target_profit:.2f}) > 예상 수수료({estimated_fee:.2f})")
                    # 가상 포지션 진입
                    entry_price = current_price # 실제로는 약간의 슬리피지 발생 가능
                    stop_loss_price = bb_upper + half_bb_width * STOP_LOSS_THRESHOLD_PERCENT
                    take_profit_target = bb_lower # 목표가

                    open_positions.append({
                        'symbol': symbol,
                        'side': 'SHORT',
                        'entry_price': entry_price,
                        'quantity': quantity,
                        'entry_time': datetime.now(),
                        'stop_loss_price': stop_loss_price,
                        'take_profit_target': take_profit_target # 목표가는 계속 변할 수 있음 (다음 캔들에서 재확인)
                    })
                    log(f"가상 숏 포지션 진입: {symbol}, 수량: {quantity:.6f}, 진입가: {entry_price:.4f}, 손절가: {stop_loss_price:.4f}, 목표가(현재): {take_profit_target:.4f}")
                    # 가상 잔고에서 수수료 차감 (진입 시 절반만 차감 가정)
                    # virtual_balance -= estimated_fee / 2.0
                else:
                    log(f"{symbol} 숏 진입 보류: 예상 수익({estimated_target_profit:.2f}) <= 예상 수수료({estimated_fee:.2f}) 또는 밴드폭 0")


            # 4) 롱 포지션 진입 조건 확인
            elif trend == 'UPTREND' and current_price <= long_entry_price_threshold:
                log(f"{symbol} 롱 진입 조건 확인: 현재가({current_price:.4f}) <= 진입 기준가({long_entry_price_threshold:.4f})")
                # 5) 수수료 조건 확인 (목표가: 상단 밴드)
                potential_profit_per_unit = bb_upper - current_price
                position_value_usdt = 100.0 # 예시
                quantity = position_value_usdt / current_price
                estimated_fee = calculate_fees(current_price, quantity, LEVERAGE)
                estimated_target_profit = potential_profit_per_unit * quantity * LEVERAGE

                if bb_width > 0 and estimated_target_profit > estimated_fee:
                    log(f"{symbol} 롱 진입! 예상 수익({estimated_target_profit:.2f}) > 예상 수수료({estimated_fee:.2f})")
                    # 가상 포지션 진입
                    entry_price = current_price
                    stop_loss_price = bb_lower - half_bb_width * STOP_LOSS_THRESHOLD_PERCENT
                    take_profit_target = bb_upper

                    open_positions.append({
                        'symbol': symbol,
                        'side': 'LONG',
                        'entry_price': entry_price,
                        'quantity': quantity,
                        'entry_time': datetime.now(),
                        'stop_loss_price': stop_loss_price,
                        'take_profit_target': take_profit_target
                    })
                    log(f"가상 롱 포지션 진입: {symbol}, 수량: {quantity:.6f}, 진입가: {entry_price:.4f}, 손절가: {stop_loss_price:.4f}, 목표가(현재): {take_profit_target:.4f}")
                    # virtual_balance -= estimated_fee / 2.0
                else:
                     log(f"{symbol} 롱 진입 보류: 예상 수익({estimated_target_profit:.2f}) <= 예상 수수료({estimated_fee:.2f}) 또는 밴드폭 0")

    # 9) 리포팅 (현재 상태)
    log(f"전략 실행 완료. 현재 가상 잔고: {virtual_balance:.2f} USDT")
    log(f"현재 보유 포지션: {len(open_positions)}개")
    for pos in open_positions:
        log(f"  - {pos['symbol']} ({pos['side']}), 진입가: {pos['entry_price']:.4f}, 수량: {pos['quantity']:.6f}")


# --- 스케줄링 설정 ---
log("테스트 프로그램 시작.")
log(f"초기 가상 자본: {initial_capital:.2f} USDT")

# 처음 한 번 즉시 실행
run_strategy()

# 매 15분마다 실행 (실제 환경에 맞게 조절 필요)
# 예: schedule.every(15).minutes.do(run_strategy)
# 테스트를 위해 짧은 간격으로 설정 가능
schedule.every(1).minutes.do(run_strategy) # 매 1분마다 실행 (테스트용)

while True:
    schedule.run_pending()
    time.sleep(1) # CPU 사용량 줄이기 위해 잠시 대기