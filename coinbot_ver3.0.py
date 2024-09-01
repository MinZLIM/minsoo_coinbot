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
from fucntion import trade

## 호가 창(oreder book)의 ask_price 기준 현재가 가져옴. 시장가 매수 진행
## 매도는 subprocess 로 수행
