from fileinput import close
from pyupbit import * 
import time
import requests
import json
from datetime import datetime
from multiprocessing import Process
from threading import Thread
import schedule
import datetime
import traceback
import os

## 원화 거래 코인 중 거래량 상위 30 개 코인을 리턴
## 최종 30 개 코인 리스트 전역 변수
## 4 시간 봉 추세 확인. 
def tickerfinder():
    global last_list
    last_list=[]
    tickers_krw = get_tickers(fiat="KRW")
    ticker_list = {}
    for tickers in tickers_krw:
        db = get_ohlcv(tickers,"minutes240",3)
        tr = db['close']
        time.sleep(0.1)
        #if tr[1] < tr[2]: 
        such_daily_volume=db["value"]
        daily_volume= sum(such_daily_volume)
        ticker_list[tickers]=daily_volume
        #else:
        #    pass
    
    ## 모든 코인 준 거래량으로 list 순차 정렬
    ticker_list=sorted(ticker_list.items(),key=lambda x: x[1],reverse=True)

    ## 상위 30 개 코인 return 
    for ticker in range(30):
        last_list.append(ticker_list[ticker][0])

    ## 비트코인 리스트에서 제거 
    if "KRW-BTC" in last_list:
        last_list.remove("KRW-BTC")
    
    print(last_list)

    return last_list

## 스토캐스틱 RSI 지표
## term = fast 스토캐스틱 기간 , n = slow스토캐스틱 기간 , period =  rsi 기간 , MACD_L =MACD 곡선 장기 이동 지수 , MACD_S = MACD 단기 이동 지수 
## return  값 [0] = slwo_k , [1] = slow_d , [2] = RSI , [3] = MACD , [4] = MACD 기준선, [5] = obv , [6]= obv 이평선 , [7] = 볼린저 upper,[8] = 볼린저 lower , [9] = pdi , [10] = mdi , [11] = adx 
def stocatic_rsi(ticker,term,n,period , MACD_L,MACD_S):
    db = get_ohlcv(ticker, interval='minutes5',count=200)
    time.sleep(0.15)
    fast_k = ( (db['close'] - db['low'].rolling(term).min()) / (db['high'].rolling(term).max() - db['low'].rolling(term).min()) ) * 100
    # macd 계산 
    exp12 = db['close'].ewm(span=MACD_S, adjust=False).mean()
    exp26 = db['close'].ewm(span=MACD_L ,adjust=False).mean()
    macd = exp12 - exp26
    exp = macd.ewm(span=period,adjust=False).mean()
    # 스토캐스틱 계산
    slow_k = fast_k.rolling(n).mean()
    slow_d = slow_k.rolling(n).mean()
    # rsi 계산
    delta = db['close'].diff()
    up, down = delta.copy(), delta.copy()
    up[up < 0] = 0
    down[down > 0] = 0
    AVG_Gain = up.ewm(com=(period - 1), min_periods=period).mean()
    AVG_LOss = down.abs().ewm(com=(period - 1), min_periods=period).mean()
    RS=AVG_Gain/AVG_LOss
    RSI = 100.0 - (100.0/(1.0+RS))
    # OBV 계산
    obv=[]
    obv.append(0)
    for i in range(1,200):
        if float(db['close'][i]) > float(db['close'][i-1]):
            obv.append(obv[-1] + float(db['volume'][i]))
        elif float(db['close'][i]) < float(db['close'][i-1]):
            obv.append(obv[-1] - float(db['volume'][i]))
        else:
            obv.append(obv[-1])

    # 볼린저 밴드 계산
    ma20 = db['close'].rolling(window=20).mean()
    stdev = db['close'].rolling(window=20).std()
    upper = ma20 + (2*stdev)
    lower = ma20 - (2*stdev)

    # adx 계산
    tr1 = db['high'] - db['low']
    tr2 = abs(db['high'] - db['close'].shift(1))
    tr3 = abs(db['low'] - db['close'].shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(14).mean()
    dm_pos = db['high'].diff()
    dm_neg = -db['low'].diff()
    dm_pos[dm_pos < 0] = 0
    dm_neg[dm_neg < 0] = 0
    plus_di = 100 * (dm_pos.ewm(alpha = 1/14).mean() / atr)
    minus_di = abs(100 * (dm_neg.ewm(alpha = 1/14).mean() / atr))
    dx = (abs(plus_di - minus_di) / abs(plus_di + minus_di)) * 100
    adx = ((dx.shift(1) * (14 - 1)) + dx) / 14 -1 
    adx_smooth = adx.ewm(alpha = 1/14).mean()

    # 120 이평선
    average_120 = db['close'].rolling(120).mean()



    # dataframe 에 지표 추가
    db['obv'] = obv
    obv_ema=db['obv'].rolling(10).mean()
    db['obv_ema'] = obv_ema
    db['slow_k'] = slow_k
    db['slow_d'] = slow_d
    db['RSI'] = RSI
    db['macd'] = macd
    db['exp'] = exp
    db['upper'] = upper
    db['lower'] = lower
    db['bol_ma'] = ma20
    db['average_120'] = average_120
    db['pdi'] = plus_di
    db['mdi'] = minus_di
    db['adx'] = adx_smooth

    return slow_k[199] , slow_d[199] , RSI[199] , macd[199] , exp[199] , obv[199] , obv_ema[199] , upper[199] ,lower[199] , plus_di[199] , minus_di[199] , adx[199]

## rsi 만 계산
## rsi 15 분 봉 최근 8 개 값 중 최소값이 25 보다 작고 rsi 15 분봉 최근 값이 최소 값보다 작아야 함.  
def rsi_calculate(ticker,period):
    db = get_ohlcv(ticker, interval='minutes5',count=200)
    delta = db['close'].diff()
    up, down = delta.copy(), delta.copy()
    up[up < 0] = 0
    down[down > 0] = 0
    AVG_Gain = up.ewm(com=(period - 1), min_periods=period).mean()
    AVG_LOss = down.abs().ewm(com=(period - 1), min_periods=period).mean()
    RS=AVG_Gain/AVG_LOss
    RSI = 100.0 - (100.0/(1.0+RS))
    db['RSI'] = RSI

    if float(min(RSI.iloc[192:199])) < 30 and float(min(RSI.iloc[192:199])) > float(RSI[199]) :
        return 1
    else:
        print("rsi 매수 조건 매칭하지 않음.")
        return 0 

## 해머 판별
def hammer_founder(ticker, ohlcv, i):
    open = float(ohlcv["open"][i])
    close = float(ohlcv["close"][i])
    highest = float(ohlcv["high"][i])
    low = float(ohlcv["low"][i])
    if open - close > 0:
        hammer_size = open - close
        tail = close - low
        top = highest - open
    else:
        hammer_size = close - open
        tail = open - low
        top = highest - close
    if tail > hammer_size * 2.5 and top < tail / 2:
        return rsi_calculate(ticker)
    else:
        return None

## 매수 조건 진입 확인. 
def hammer_checker(ticker):
    print("망치형 조회 : {}".format(ticker))
    ohlcv = get_ohlcv(ticker, "minutes30", count=6)
    num_hammers = []  # list init. 
    for i in range(5):
        rsi = hammer_founder(ticker, ohlcv, i)
        if rsi is not None:
            num_hammers.append(rsi)
    now_rsi = hammer_founder(ticker, ohlcv, 5)
    if now_rsi is not None and float(now_rsi) < float(min(num_hammers)):
        return 1
    else:
        return 0



