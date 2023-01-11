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

## 스토캐스틱 RSI 지표
## term = fast 스토캐스틱 기간 , n = slow스토캐스틱 기간 , period =  rsi 기간 , MACD_L =MACD 곡선 장기 이동 지수 , MACD_S = MACD 단기 이동 지수 
## return  값 [0] = slwo_k , [1] = slow_d , [2] = RSI , [3] = MACD , [4] = MACD 기준선, [5] = obv , [6]= obv 이평선 , [7] = 볼린저 upper,[8] = 볼린저 lower , [9] = 120 이평선 , [10] 상승 추세 여부 , [11] 5 분봉 RSI
def stocatic_rsi(ticker,term,n,period , MACD_L,MACD_S):
    db = get_ohlcv(ticker, interval='minutes1',count=200)
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

    # 상향 추세 체크
    trend_check=upper_trend_check(ticker)

    return slow_k[199] , slow_d[199] , RSI[199] , macd[199] , exp[199] , obv[199] , obv_ema[199] , ma20[199] ,lower[199] , average_120[199] , trend_check[0] , trend_check[1] 

