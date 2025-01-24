from flask import Flask, jsonify, render_template_string
from analyze import coin_data_cache
import threading
import analyze

app = Flask(__name__)

@app.route("/coins", methods=['GET'])
def get_coins():
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

@app.route("/coins/<path:symbol>", methods=['GET'])
def get_coin_data(symbol):
    symbol = unquote(symbol)
    if symbol in coin_data_cache:
        coin_data = coin_data_cache[symbol]
        return jsonify({
            "entry_price": coin_data.get("entry_price"),
            "target_price": coin_data.get("target_price"),
            "stop_loss": coin_data.get("stop_loss"),
            "market_flow": coin_data.get("market_flow"),
        })
    return jsonify({"error": f"Coin data for {symbol} not found."}), 404

if __name__ == "__main__":
    # Start the analysis in a separate thread
    analysis_thread = threading.Thread(target=analyze.analyze_coins, daemon=True)
    analysis_thread.start()

    # Run Flask server
    app.run(host="0.0.0.0", port=5000)
