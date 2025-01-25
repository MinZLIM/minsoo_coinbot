import ccxt
import pandas as pd
import numpy as np
from flask import Flask, jsonify, render_template_string
from fucntion import ccxt_function
from urllib.parse import unquote, quote
import threading
import time

app = Flask(__name__)

# Global cache for pre-analyzed coin data
coin_data_cache = {}
transaction_log = []  # Store transactions
virtual_balance = 100.0  # Initial virtual balance

def execute_trade(symbol, market_flow, entry_price):
    global virtual_balance, transaction_log
    quantity = 1  # Fixed quantity for simplicity

    if market_flow == "long":
        # Buy action
        cost = entry_price * quantity
        if virtual_balance >= cost:
            virtual_balance -= cost
            transaction_log.append({
                "symbol": symbol,
                "action": "buy",
                "price": entry_price,
                "quantity": quantity,
                "balance": virtual_balance
            })
    elif market_flow == "short":
        # Short action (hypothetical logic for selling first)
        gain = entry_price * quantity
        virtual_balance += gain
        transaction_log.append({
            "symbol": symbol,
            "action": "short",
            "price": entry_price,
            "quantity": quantity,
            "balance": virtual_balance
        })

# Pre-analyze coin data
def pre_analyze_coins():
    """
    Continuously fetch and analyze top coins and execute trades.
    """
    global coin_data_cache
    exchange = ccxt.binanceusdm()
    while True:
        try:
            top_coins = ccxt_function.get_top_30_coins()
            for symbol in top_coins:
                try:
                    analysis = ccxt_function.analyze_coin(symbol, exchange)
                    coin_data_cache[symbol] = analysis

                    # Execute trade based on analysis
                    if analysis["market_flow"] in ["long", "short"]:
                        execute_trade(symbol, analysis["market_flow"], analysis["entry_price"])

                except Exception as e:
                    coin_data_cache[symbol] = {"error": str(e)}
            time.sleep(3600)  # Update every hour
        except Exception as e:
            print(f"Error in pre_analyze_coins: {e}")

# Flask Routes
@app.route("/coins", methods=['GET'])
def get_coins():
    try:
        # Filter coins with buy signals
        coins_with_signals = {symbol: data for symbol, data in coin_data_cache.items() if data.get("market_flow") in ["long", "short"]}
        html_template = """
        <html>
        <head><title>Coin Trading Signals</title></head>
        <body>
            <h1>Coins with Trading Signals</h1>
            <ul>
            {% for symbol, data in coins.items() %}
                <li>
                    <a href="/coins/{{ symbol | urlencode }}">{{ symbol }}</a> - {{ data["market_flow"] | capitalize }}
                </li>
            {% endfor %}
            </ul>
        </body>
        </html>
        """
        return render_template_string(html_template, coins=coins_with_signals)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/coins/<path:symbol>", methods=['GET'])
def get_coin_data(symbol):
    try:
        symbol = unquote(symbol)  # Decode the URL-encoded symbol
        if symbol in coin_data_cache:
            coin_data = coin_data_cache[symbol]
            return jsonify({
                "entry_price": coin_data.get("entry_price"),
                "target_price": coin_data.get("target_price"),
                "stop_loss": coin_data.get("stop_loss"),
                "market_flow": coin_data.get("market_flow")
            })
        else:
            return jsonify({"error": f"Coin data for {symbol} not found."}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/transactions", methods=['GET'])
def get_transactions():
    try:
        return jsonify({
            "virtual_balance": virtual_balance,
            "transaction_log": transaction_log
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    # Start pre-analysis in a separate thread
    analysis_thread = threading.Thread(target=pre_analyze_coins, daemon=True)
    analysis_thread.start()
    app.run(host="0.0.0.0", port=5000)
