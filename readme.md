[<img src="https://github.com/MinZLIM/minsoo_coinbot/blob/main/img/ko.png">](https://github.com/MinZLIM/minsoo_coinbot/blob/main/readme/readme_ko.md)

# What?

This project is an automatic trading program for cryptocurrency through the API provided by upbit.

---
# How?
## Acting Indicator
### Moving Average
 > When data (price) moves directionally, the average obtained as it moves
### MACD
 > A technique that attempts to capture trading signals by using the difference between moving average lines. It is commonly used to measure the strength, direction, and time of a stock price trend.
### RSI
 > RSI is a method of calculating the average value of the increase and decrease in the stock price over a certain period of time compared to the previous day's price, and judging it as overbought if the increase in the increase is large, and oversold if the amount of decrease in the price is large.


## Flow 
 > Create a list of the top 30 bit trading coms <br />
 > list looping rsi, macd, etc. indicator collection <br />
 > Whether it matches the indicator, etc. (whether it has it, is reviewing it) <br />
 > Transactions traded by the market (cancelled if not concluded after a certain period of time) <br />
 > Setting a snail goal <br />
 > MaintenanceListLooping and component collection <br />
 > Selling at market price when maintenance indicators are measurable <br />
--- 


# Test Scenarios : Init Setup & Cleaning Run

## Test Case1. Execute Robot Vacuum SW
### step1 : Excute Robot Vacuum SW
### data : None
### Result : Including SW Process's Status is Running

## Test Case2. Connect to App
- Env : IOS/Android Device
> Step1 : Install Asigned App     data : None   Result: App is Installed in Control Device

> Step2 : Searching Robot
> data : None
### Result : App Detect Robot 

### Step3 : Searching Robot
### data : None
### Result : App Detect Robot 

### Step4 : Connect to Robot
### data : None
### Result : App showing Robot Status(Model, Charging, and so on) 

