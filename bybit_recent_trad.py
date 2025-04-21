import pandas as pd
from collections import defaultdict
import datetime
from pybit.unified_trading import HTTP
import requests

BYBIT_API_KEY = os.getenv("BYBIT_API_KEY")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

# ---- CONFIG ----
BYBIT_API_KEY = "3wn4lzTEtKF595utDe"
BYBIT_API_SECRET = "dgSAVZpHtTuurQe6WyUDXivGzS3eZcRKKYTj"
SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/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"  # 🔁 Replace with your Slack webhook

# ---- STEP 1: Fetch Public Trade Data ----
session = HTTP(
    testnet=False,
    api_key=BYBIT_API_KEY,
    api_secret=BYBIT_API_SECRET
)

response = session.get_public_trade_history(
    category="spot",
    symbol="MONUSDT",
    limit=1000  # Max allowed
)

trades = response.get("result", {}).get("list", [])

# ---- STEP 2: Aggregate Per Minute ----
grouped = defaultdict(lambda: {
    "buy_volume": 0.0,
    "sell_volume": 0.0,
    "total_volume": 0.0,
    "price_volume": 0.0,
    "buy_count": 0,
    "sell_count": 0
})

for trade in trades:
    ts = int(trade["time"]) // 1000  # Convert to seconds
    minute = ts - (ts % 60)  # Round to nearest minute
    side = trade["side"]
    qty = float(trade["size"])
    price = float(trade["price"])
    entry = grouped[minute]

    if side == "Buy":
        entry["buy_volume"] += qty
        entry["buy_count"] += 1
    elif side == "Sell":
        entry["sell_volume"] += qty
        entry["sell_count"] += 1

    entry["total_volume"] += qty
    entry["price_volume"] += qty * price

# ---- STEP 3: Convert to DataFrame ----
rows = []
for ts, data in grouped.items():
    dt = datetime.datetime.fromtimestamp(ts)
    total_trades = data["buy_count"] + data["sell_count"]
    avg_price = data["price_volume"] / data["total_volume"] if data["total_volume"] else 0
    usd_volume = avg_price * data["total_volume"]
    rows.append({
        "timestamp": dt,
        "buy_volume": round(data["buy_volume"], 2),
        "sell_volume": round(data["sell_volume"], 2),
        "price": round(avg_price, 4),
        "usd_volume": round(usd_volume, 2),
        "trade_count": total_trades
    })

df = pd.DataFrame(rows).sort_values("timestamp")

# ---- STEP 4: Calculate Summary ----
if not df.empty:
    start_time = df["timestamp"].min()
    end_time = df["timestamp"].max()
    total_minutes = int((end_time - start_time).total_seconds() / 60)
    active_minutes = df["timestamp"].dt.floor("min").nunique()
    trading_freq_pct = (active_minutes / total_minutes) * 100 if total_minutes > 0 else 0

    summary = {
        "Start Time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
        "End Time": end_time.strftime("%Y-%m-%d %H:%M:%S"),
        "Total Buy Volume": round(df["buy_volume"].sum(), 2),
        "Total Sell Volume": round(df["sell_volume"].sum(), 2),
        "Total USD Volume": round(df["usd_volume"].sum(), 2),
        "Trading Frequency %": round(trading_freq_pct, 2)
    }

    # ---- STEP 5: Send Slack Alert ----
    slack_message = "*MONUSDT Spot Trade Summary*\n"
    for k, v in summary.items():
        slack_message += f"> *{k}*: `{v}`\n"

    slack_payload = {"text": slack_message}
    response = requests.post(SLACK_WEBHOOK_URL, json=slack_payload)

    if response.status_code == 200:
        print("✅ Slack alert sent.")
    else:
        print(f"⚠️ Slack error: {response.status_code} - {response.text}")
else:
    print("⚠️ No data available to summarize.")
