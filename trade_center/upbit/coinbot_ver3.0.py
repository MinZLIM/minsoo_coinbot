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
from fucntion import trade , indicator

access_key = ""
secret_key = ""

## 업비트 인증
bit = Upbit(access_key, secret_key)


if __name__ == '__main__':
    try:
        last_list=indicator.tickerfinder()
        schedule.every(4).hours.do(indicator.tickerfinder())
        while True:
            try:
                schedule.run_pending()
                for i in last_list:
                    st_rsi= indicator.stocatic_rsi(i,9,3,9,26,12)
                    time.sleep(0.2)
                    ## return  값 [0] = slwo_k , [1] = slow_d , [2] = RSI , [3] = MACD , [4] = MACD 기준선, [5] = obv , [6]= obv 이평선 , [7] = 볼린저 upper,[8] = 볼린저 lower , [9] = pdi , [10] = mdi , [11] = adx 
                    if   st_rsi[2] < 50 and st_rsi[9] > st_rsi[10] and st_rsi[9] > st_rsi[11] and st_rsi[10] < st_rsi[11]: 
                        buyinfo=trade.buy_sell(i,st_rsi[7],st_rsi[8])
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