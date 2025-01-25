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
import indicator

## 코인 주문 및 매도 함수
## 중복 코인 , 보유 코인 5 개 이상일 시 구매하지 않음.
## 코인 지정가 구매 후 1 초 대기 주문 조회 후 없으면 구매 완료 된 것으로 판단 , 목표가 매도 주문 수행
def buy_sell(bit, ticker,bol_upper,bol_lower):
    #print("매수 조건 확인...")
    my_coin = real_coin_list(bit,ticker)
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
    market_buy(bit,ticker,buy_price) ## 시장가 매수
    check = Thread(target=buy_status_check(bit,ticker,target_price,loss_price))
    check.start()
    return "own_coin:{}".format(my_coin) ##  코인 계좌 상태

## 코인 주문 ## 
## 내 KRW 자산 조회
def my_krw_balance(bit):
    try:
        my_krw = bit.get_balances()[0]['balance']

        return my_krw
    except:
        my_krw_balance()

## 시장가 매수
def market_buy(bit,ticker,buy_price):
    bit.buy_market_order(ticker,buy_price)

## 지정가 매수
## limit_buy(코인 종류 , 매수 가격 ,주문 가격)
def limit_buy(bit,ticker, bit_price, buy_price):
    volume = buy_price/bit_price
    bit.buy_limit_order(ticker,bit_price,volume)

## 시장가 매도
def market_sell(bit,ticker,sell_volume):
    sell_volume = str(sell_volume.replace(',',''))
    bit.sell_market_order(ticker,sell_volume)


## 지정가 매도
def limit_sell(bit, ticker, sell_price, sell_volume):
    try:
        sell_volume = str(sell_volume.replace(',',''))
        sell_check = bit.sell_limit_order(ticker,sell_price,sell_volume)
        if 'uuid' in sell_check:
            return True
        else:
            time.sleep(5)
            print("매도 재시도")
            print(ticker , sell_price, sell_volume)
            limit_sell(bit,ticker, sell_price, sell_volume)
    except:
        print(sell_check, ticker, sell_price,sell_volume)
        traceback.print_exc()
        print(datetime.datetime.now())
        pass


## 계좌 코인 현황
## 해당 코인 보유 유무 / 코인 개수 , 자산 , 해당 코인 개수(있을 시)
def real_coin_list(bit,ticker):
    ticker = ticker.replace("KRW-",'')
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
def buy_status_check(bit,ticker,bol_upper,loss_price):
    ## 계좌에 코인 조회 될 시 목표가 매도 주문 수행
    try:
        time.sleep(5)
        my_coin = real_coin_list(bit,ticker)
        if  my_coin[0] == False:
            order_list = bit.get_order(ticker)
            if len(order_list) == 0:
                return 0
            cancle = bit.cancel_order(order_list[0]['uuid'])
            if 'uuid' in cancle:
                print("매수 취소합니다. :{}".format(ticker))
                return 0 
            else: 
                buy_status_check(bit,ticker,bol_upper,loss_price)

        elif my_coin[0] == True:
            sell = limit_sell(bit,ticker,bol_upper, my_coin[3])
            if sell == True: ## 목표가 매도 주문 후 손절가 확인을 위한 시장가 확인 주문 서브 프로세스 시작
                time.sleep(2)
                order_list = bit.get_order(ticker)
                pr_market_sell = Process(target=own_coin_marker_sell, args= (bit,ticker,my_coin[3],loss_price,order_list[0]['uuid']))
                pr_market_sell.start()
            else:
                buy_status_check(bit,ticker,bol_upper,loss_price)
    except:
        traceback.print_exc()
        print(datetime.datetime.now())
        buy_status_check(bit,ticker,bol_upper,loss_price)

## 보유코인 시장가 매도 
def own_coin_marker_sell(bit,ticker,volume,loss_price,uuid):
    while True:
        try:
            time.sleep(2)
            new_target = indicator.stocatic_rsi(ticker,9,3,9,26,12)
            bit_price = float(get_current_price(ticker))
            my_coin = real_coin_list(bit,ticker)
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

def testtt():
    print(10)
    time.sleep(10)