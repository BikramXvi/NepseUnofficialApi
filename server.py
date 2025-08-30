from flask import Flask, request, redirect, jsonify, render_template_string
import pandas as pd
import os
from datetime import datetime

app = Flask(__name__)


NEPSE_FILE = os.path.join("to-csv", "NepseAPI", "NepseIndex.csv")
USER_FILE = "user_portfolio.csv"


if not os.path.exists(USER_FILE):
    pd.DataFrame(columns=[
        "boid", "username", "stock", "buy_price_stock", "quantity",
        "current_stock_price",
        "stock_profit_loss", "stock_percent_change",
        "nepse_investment_value", "nepse_profit_loss", "nepse_percent_change",
        "date"
    ]).to_csv(USER_FILE, index=False)


def get_latest_nepse_index():
    """Read live NEPSE CSV and filter only 'NEPSE Index'."""
    if not os.path.exists(NEPSE_FILE):
        raise FileNotFoundError(f"{NEPSE_FILE} not found.")
    df = pd.read_csv(NEPSE_FILE)
    nepse_row = df[df['index'] == 'NEPSE Index']
    if nepse_row.empty:
        raise ValueError("NEPSE Index row not found in CSV.")
    latest = nepse_row.iloc[-1]
    previous_close = float(latest['previousClose'])
    current_value = float(latest['currentValue'])
    return previous_close, current_value


@app.route('/', methods=['GET'])
def home():
    """Clean UI to add multiple stock transactions."""
    html_form = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Portfolio Entry</title>
        <style>
            body {
                font-family: Arial, sans-serif;
                background-color: #FFFFFF;
                color: #333;
                padding: 20px;
            }
            h2 {
                color: #35B14F;
            }
            table {
                width: 100%;
                border-collapse: collapse;
                margin-bottom: 20px;
            }
            table, th, td {
                border: 1px solid #35B14F;
            }
            th, td {
                padding: 8px;
                text-align: center;
            }
            tr:nth-child(even) {background-color: #f9f9f9;}
            tr:hover {background-color: #e8f5e9;}
            input[type=text], input[type=number] {
                width: 90%;
                padding: 5px;
                box-sizing: border-box;
                border: 1px solid #35B14F;
                border-radius: 4px;
            }
            input[type=submit] {
                background-color: #35B14F;
                color: #FFFFFF;
                border: none;
                padding: 10px 20px;
                font-size: 16px;
                border-radius: 5px;
                cursor: pointer;
            }
            input[type=submit]:hover {
                background-color: #2e993f;
            }
            .error {
                color: #DC3545;
                font-weight: bold;
            }
        </style>
    </head>
    <body>
        <h2>Add Multiple Share Transactions</h2>
        <form method="post" action="/add_transaction">
            BOID: <input type="text" name="boid" required><br><br>
            Username: <input type="text" name="username" required><br><br>

            <table>
                <tr style="background-color:#35B14F; color:#FFFFFF;">
                    <th>Stock Symbol</th>
                    <th>Buy Price</th>
                    <th>Quantity</th>
                    <th>Current Price</th>
                </tr>
                {% for i in range(5) %}
                <tr>
                    <td><input type="text" name="stock_{{i}}"></td>
                    <td><input type="number" step="any" name="buy_price_stock_{{i}}"></td>
                    <td><input type="number" step="any" name="quantity_{{i}}"></td>
                    <td><input type="number" step="any" name="current_stock_price_{{i}}"></td>
                </tr>
                {% endfor %}
            </table>

            <input type="submit" value="Submit">
        </form>
    </body>
    </html>
    """
    return render_template_string(html_form)

@app.route('/add_transaction', methods=['POST'])
def add_transaction():
    """Add multiple transactions for a user at once."""
    try:
        boid = request.form.get("boid")
        username = request.form.get("username")
        nepse_prev, nepse_current = get_latest_nepse_index()
        new_rows = []

        
        for i in range(5):
            stock = request.form.get(f"stock_{i}")
            buy_price_stock = request.form.get(f"buy_price_stock_{i}")
            quantity = request.form.get(f"quantity_{i}")
            current_stock_price = request.form.get(f"current_stock_price_{i}")

            # Skip empty rows
            if not stock or not buy_price_stock or not quantity or not current_stock_price:
                continue

            buy_price_stock = float(buy_price_stock)
            quantity = float(quantity)
            current_stock_price = float(current_stock_price)
            buy_amount = buy_price_stock * quantity
            market_value_stock = current_stock_price * quantity
            stock_profit_loss = market_value_stock - buy_amount
            stock_percent_change = ((current_stock_price - buy_price_stock) / buy_price_stock) * 100

            nepse_units = buy_amount / nepse_prev
            nepse_investment_value = nepse_units * nepse_current
            nepse_profit_loss = nepse_investment_value - buy_amount
            nepse_percent_change = ((nepse_investment_value - buy_amount) / buy_amount) * 100

            new_row = {
                "boid": boid,
                "username": username,
                "stock": stock,
                "buy_price_stock": buy_price_stock,
                "quantity": quantity,
                "current_stock_price": current_stock_price,
                "stock_profit_loss": stock_profit_loss,
                "stock_percent_change": stock_percent_change,
                "nepse_investment_value": nepse_investment_value,
                "nepse_profit_loss": nepse_profit_loss,
                "nepse_percent_change": nepse_percent_change,
                "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            new_rows.append(new_row)

        if not new_rows:
            return '<p class="error">No valid transactions to add!</p>'

        
        df = pd.read_csv(USER_FILE)
        df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
        df.to_csv(USER_FILE, index=False)

        return redirect('/')

    except Exception as e:
        return f'<p class="error">Error: {str(e)}</p>'

@app.route('/portfolio', methods=['GET'])
def portfolio():
    """Return all transactions as JSON (for Power BI)."""
    try:
        df = pd.read_csv(USER_FILE)
        return jsonify(df.to_dict(orient="records"))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/portfolio_vs_nepse/<boid>', methods=['GET'])
def portfolio_vs_nepse(boid):
    """Compare a user's portfolio vs NEPSE using all transactions."""
    try:
        df = pd.read_csv(USER_FILE)
        user_df = df[df['boid'] == boid]
        if user_df.empty:
            return jsonify({"error": "No transactions for this BOID"}), 404

        invested_total = (user_df['buy_price_stock'] * user_df['quantity']).sum()
        stock_current_total = (user_df['current_stock_price'] * user_df['quantity']).sum()
        portfolio_return_percent = ((stock_current_total - invested_total) / invested_total) * 100

        nepse_current_total = user_df['nepse_investment_value'].sum()
        nepse_return_percent = ((nepse_current_total - invested_total) / invested_total) * 100

        return jsonify({
            "boid": boid,
            "portfolio_return_percent": round(portfolio_return_percent, 2),
            "nepse_return_percent": round(nepse_return_percent, 2),
            "invested_total": invested_total,
            "current_stock_value_total": stock_current_total
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True)

