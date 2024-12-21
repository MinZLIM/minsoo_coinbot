import ccxt

# 바이낸스 API 정보 설정
binance = ccxt.binance({
    'apiKey': 'JZbn0C117U035JhXqGHS5VMorcgLB0ENLj10PC72cjnWWyNk7i1WJWpeFqkWvnJo',
    'secret': 'R3r2577yoETYFCQLFwHv5tIutB7e1kxmWr1kdbNqBOtRWPSdcVV66djMfeEu11yH',
})

# 자산 조회
balances = binance.fetch_balance()

print(balances)
# 자산 출력
for asset, balance in balances['total'].items():
    if balance > 0:
        print(f"Asset: {asset}, Balance: {balance}")
    else:
        print(1)
