import time
import threading
import ccxt
import sys
import os
import pandas as pd
import ccxt_function

virt_account = 1000000.0
open_positions = []
account_file = "./trade_log/virtual_account.txt"

def update_account_file():
    """Update the account file with current balance and open positions."""
    with open(account_file, "w") as file:
        file.write(f"Balance: {virt_account:.2f}\n")
        file.write("Open Positions:\n")
        for pos in open_positions:
            file.write(f"{pos}\n")

def execute_trade(symbol, action, position_size, entry_price, target_price, stop_loss_price, log_file):
    """
    Execute a trade and log it.
    """
    global virt_account

    # Check if already in position for this symbol
    if any(pos['symbol'] == symbol for pos in open_positions):
        return

    # Calculate fee and effective profit margin
    fee = position_size * 0.0005
    effective_profit = (
        (entry_price - target_price if action == "short" else target_price - entry_price) * position_size - fee
    )

    if effective_profit <= 0:
        return

    # Log trade details
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    trade_details = (
        f"[{timestamp}] {action.upper()} {symbol} with position size {position_size:.2f}, "
        f"Entry: {entry_price:.2f}, Target: {target_price:.2f}, Stop Loss: {stop_loss_price:.2f}\n"
    )

    with open(log_file, "a") as file:
        file.write(trade_details)

    # Update virtual account balance
    virt_account -= position_size
    open_positions.append({
        "symbol": symbol,
        "action": action,
        "size": position_size,
        "entry_price": entry_price,
        "target_price": target_price,
        "stop_loss_price": stop_loss_price,
        "log_file": log_file  # Store log file for proper logging
    })
    update_account_file()

def check_positions(exchange):
    """
    Check open positions and update the virtual account if targets or stop losses are hit.
    """
    global virt_account, open_positions
    
    for position in open_positions[:]:
        symbol = position['symbol']
        action = position['action']
        size = position['size']
        entry_price = position['entry_price']
        target_price = position['target_price']
        stop_loss_price = position['stop_loss_price']
        log_file = position['log_file']

        # Fetch the latest price for the symbol
        try:
            ticker = exchange.fetch_ticker(symbol)
            current_price = ticker['last']
        except Exception as e:
            print(f"Error fetching price for {symbol}: {e}")
            continue

        # Check if target or stop loss is hit
        if (action == "long" and current_price >= target_price) or (action == "short" and current_price <= target_price):
            profit = (target_price - entry_price if action == "long" else entry_price - target_price) * size
            virt_account += size + profit
            status = "TARGET HIT"
        elif (action == "long" and current_price <= stop_loss_price) or (action == "short" and current_price >= stop_loss_price):
            loss = (entry_price - stop_loss_price if action == "long" else stop_loss_price - entry_price) * size
            virt_account += size - loss
            status = "STOP LOSS HIT"
        else:
            continue

        # Log position closure
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        closure_details = (
            f"[{timestamp}] {status} for {symbol}: Closed position with {action.upper()} at {current_price:.2f}.\n"
        )

        with open(log_file, "a") as file:
            file.write(closure_details)

        # Remove the closed position
        open_positions.remove(position)
        update_account_file()

def analyze_and_record():
    try:
        # Initialize Binance Futures API
        exchange = ccxt.binanceusdm()
        os.makedirs("./trade_log", exist_ok=True)  # Create directory if it doesn't exist

        while True:
            top_coins = ccxt_function.get_top_30_coins()

            for symbol in top_coins:
                try:
                    # Fetch 5-minute interval data and calculate indicators
                    indicators = ccxt_function.calculate_indicators(symbol, exchange)
                    action = ccxt_function.decide_trade_action(indicators)
                    log_file = f"./trade_log/{symbol.replace('/', '_')}_log.txt"

                    if action != "none":
                        position_size = ccxt_function.calculate_position_size(virt_account, risk_percentage=10)  # 10% risk
                        entry_price = indicators['close'].iloc[-1]
                        target_price, stop_loss_price = ccxt_function.calculate_targets(
                            entry_price, risk_reward_ratio=2, stop_loss_percentage=1, action=action
                        )

                        execute_trade(symbol, action, position_size, entry_price, target_price, stop_loss_price, log_file)

                    # Check open positions for closures
                    check_positions(exchange)

                except Exception as e:
                    print(f"Error analyzing symbol {symbol}: {e}")

            # Wait briefly before checking for updates again
            time.sleep(10)  # Check for updates every 10 seconds

    except Exception as e:
        print(f"Error fetching top coins: {e}")

if __name__ == "__main__":
    analyze_and_record()
