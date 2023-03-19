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
## 호가 창(oreder book)의 ask_price 기준 현재가 가져옴. 시장가 매수 진행
## 매도는 subprocess 로 수행

access_key = ""
secret_key = ""

## 업비트 인증
bit = Upbit(access_key, secret_key)

## 모니터링 종목 리스트 
last_list=[]

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



## 코인 주문 및 매도 함수
## 중복 코인 , 보유 코인 5 개 이상일 시 구매하지 않음.
## 코인 지정가 구매 후 1 초 대기 주문 조회 후 없으면 구매 완료 된 것으로 판단 , 목표가 매도 주문 수행
def buy_sell(ticker,bol_upper,bol_lower):
    #print("매수 조건 확인...")
    my_coin = real_coin_list(ticker)
    if my_coin[0] == True:
        #print("중복된 코인 입니다. 구매하지 않습니다.:{}".format(ticker))
        return 0 
    elif my_coin[1] >= 11 :
        #print("구매된 코인 개수가 5 개 입니다.")
        return 0
    
    target_price = get_tick_size(bol_upper )  ##  볼린저 상단. 
    loss_price = get_tick_size(bol_lower*0.998)
    bit_price = float(get_orderbook(ticker)['orderbook_units'][0]['ask_price'])
    
    if 100 <= bit_price < 1000 :
        return 0
    if loss_price*0.995 > bit_price:
        return 0
    if target_price*0.999 <= bit_price or loss_price*0.995 > bit_price: # or bit_15_price <= bit_price:
        #print("수수료 손해 예상 구매하지 않습니다.:{} : {}".format(ticker,bit_price))
        return 0
    print('구매 진행합니다. :{}'.format(ticker))
    buy_price = int(float(my_coin[2])/(10-my_coin[1]))
    market_buy(ticker,buy_price) ## 시장가 매수
    check = Thread(target=buy_status_check(ticker,target_price,loss_price))
    check.start()
    return "own_coin:{}".format(my_coin) ##  코인 계좌 상태

## 코인 주문 ## 
## 내 KRW 자산 조회
def my_krw_balance():
    try:
        global bit
        my_krw = bit.get_balances()[0]['balance']

        return my_krw
    except:
        my_krw_balance()

## 시장가 매수
def market_buy(ticker,buy_price):
    global bit
    bit.buy_market_order(ticker,buy_price)

## 지정가 매수
## limit_buy(코인 종류 , 매수 가격 ,주문 가격)
def limit_buy(ticker, bit_price, buy_price):
    global bit
    volume = buy_price/bit_price
    bit.buy_limit_order(ticker,bit_price,volume)

## 시장가 매도
def market_sell(ticker,sell_volume):
    global bit
    sell_volume = str(sell_volume.replace(',',''))
    bit.sell_market_order(ticker,sell_volume)


## 지정가 매도
def limit_sell(ticker, sell_price, sell_volume):
    global bit
    try:
        sell_volume = str(sell_volume.replace(',',''))
        sell_check = bit.sell_limit_order(ticker,sell_price,sell_volume)
        if 'uuid' in sell_check:
            return True
        else:
            time.sleep(5)
            print("매도 재시도")
            print(ticker , sell_price, sell_volume)
            limit_sell(ticker, sell_price, sell_volume)
    except:
        print(sell_check, ticker, sell_price,sell_volume)
        traceback.print_exc()
        print(datetime.datetime.now())
        pass


## 계좌 코인 현황
## 해당 코인 보유 유무 / 코인 개수 , 자산 , 해당 코인 개수(있을 시)
def real_coin_list(ticker):
    ticker = ticker.replace("KRW-",'')
    global bit
    my_balance = bit.get_balances()
    own_check = False
    for x in my_balance:
        if str(x['currency']) == "KRW":
            my_KRW = str(x['balance'])
        elif str(x['currency']) == ticker:
                own_check = True
                return  own_check,  len(my_balance) , my_KRW, str(x['balance'])
            
    return own_check , len(my_balance) , my_KRW # [0] 해당 코인 보유 여부 , [1] 코인 개수 . [2] 해당 코인 개수 

## 매수 주문 5 초 후 체결 되지 않았으면 주문 취소. 
def buy_status_check(ticker,bol_upper,loss_price):
    ## 계좌에 코인 조회 될 시 목표가 매도 주문 수행
    try:
        time.sleep(5)
        global bit
        my_coin = real_coin_list(ticker)
        if  my_coin[0] == False:
            order_list = bit.get_order(ticker)
            if len(order_list) == 0:
                return 0
            cancle = bit.cancel_order(order_list[0]['uuid'])
            if 'uuid' in cancle:
                print("매수 취소합니다. :{}".format(ticker))
                return 0 
            else: 
                buy_status_check(ticker,bol_upper,loss_price)

        elif my_coin[0] == True:
            sell = limit_sell(ticker,bol_upper, my_coin[3])
            if sell == True: ## 목표가 매도 주문 후 손절가 확인을 위한 시장가 확인 주문 서브 프로세스 시작
                time.sleep(2)
                order_list = bit.get_order(ticker)
                pr_market_sell = Process(target=own_coin_marker_sell, args= (ticker,my_coin[3],loss_price,order_list[0]['uuid']))
                pr_market_sell.start()
            else:
                buy_status_check(ticker,bol_upper,loss_price)
    except:
        traceback.print_exc()
        print(datetime.datetime.now())
        buy_status_check(ticker,bol_upper,loss_price)

## 보유코인 시장가 매도 
def own_coin_marker_sell(ticker,volume,loss_price,uuid):
    global bit
    while True:
        try:
            time.sleep(2)
            new_target = stocatic_rsi(ticker,9,3,9,26,12)
            bit_price = float(get_current_price(ticker))
            my_coin = real_coin_list(ticker)
            if my_coin[0]== False:
                print("매도 완료:{}".format(ticker))
                break
            if new_target[7] < bit_price:
                bit.cancel_order(uuid)
                print("새 목표가 매도 {}".format(ticker))
                market_sell(ticker,volume)
            if bit_price <= float(loss_price):
                bit.cancel_order(uuid)
                print("손절가 매도{} ".format(ticker))
                market_sell(ticker,volume)
               
        except:
            traceback.print_exc()
            pass

if __name__ == '__main__':
    try:
        last_list=tickerfinder()
        schedule.every(4).hours.do(tickerfinder)
        while True:
            try:
                schedule.run_pending()
                for i in last_list:
                    st_rsi=stocatic_rsi(i,9,3,9,26,12)
                    time.sleep(0.2)
                    ## return  값 [0] = slwo_k , [1] = slow_d , [2] = RSI , [3] = MACD , [4] = MACD 기준선, [5] = obv , [6]= obv 이평선 , [7] = 볼린저 upper,[8] = 볼린저 lower , [9] = pdi , [10] = mdi , [11] = adx 
                    if   st_rsi[2] < 50 and st_rsi[9] > st_rsi[10] and st_rsi[9] > st_rsi[11] and st_rsi[10] < st_rsi[11]: 
                        #if rsi_calculate(i,9) == 1:
                            #if hammer_checker(i) == 1:
                        buyinfo=buy_sell(i,st_rsi[7],st_rsi[8])
                        if buyinfo == 0 :
                            pass
                        else:
                            print(buyinfo,datetime.datetime.now())
            except:
                traceback.print_exc()
                print(datetime.datetime.now())
                pass
    except:
        traceback.print_exc()
        print(datetime.datetime.now())
        pass 