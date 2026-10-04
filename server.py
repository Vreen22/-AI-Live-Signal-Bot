import asyncio
import json
import os
import time
import urllib.request
from collections import deque
from pathlib import Path

import websockets
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse

BASE = Path(__file__).resolve().parent
INDEX = BASE / "index.html"

app = FastAPI(title="AI Live Signal Bot V17")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Keep the provider registry available to the mobile UI.
DEFAULT_SOURCES = [
    "Binance", "Coinbase", "Kraken", "OKX", "Bybit", "KuCoin",
    "Gate.io", "Bitfinex", "Bitstamp", "Crypto.com",
    "OANDA", "FXCM", "IG", "Twelve Data", "Finnhub",
    "Alpaca", "Polygon/Massive", "Alpha Vantage", "Intrinio",
    "dxFeed", "Barchart", "EODHD",
]

SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD",
    "USDCAD", "NZDUSD", "XAUUSD",
]
INTERVALS = ["1m", "5m", "15m", "30m", "1h", "2h", "4h"]

bars = {(s, tf): deque(maxlen=500) for s in SYMBOLS for tf in INTERVALS}
latest = {}
feed_heartbeat = {"Binance": 0.0}
background_tasks = []


def load_json(name, fallback):
    try:
        p = BASE / name
        if p.exists():
            return json.loads(p.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"Could not read {name}: {exc}")
    return fallback


PROVIDERS = load_json("providers.json", {})
INSTRUMENTS = load_json("instruments.json", {})


def ema(values, period):
    if len(values) < period:
        return None
    e = sum(values[:period]) / period
    k = 2 / (period + 1)
    for x in values[period:]:
        e = x * k + e * (1 - k)
    return e


def rsi(values, period=14):
    if len(values) < period + 1:
        return None
    gains = losses = 0.0
    for a, b in zip(values[-period-1:-1], values[-period:]):
        d = b - a
        if d > 0:
            gains += d
        else:
            losses -= d
    if losses == 0:
        return 100.0
    return 100 - 100 / (1 + gains / losses)


def atr(candles, period=14):
    if len(candles) < period + 1:
        return None
    tr = []
    for i in range(1, len(candles)):
        c, p = candles[i], candles[i - 1]
        tr.append(max(
            c["high"] - c["low"],
            abs(c["high"] - p["close"]),
            abs(c["low"] - p["close"]),
        ))
    return sum(tr[-period:]) / period


def analyze(symbol, tf):
    data = list(bars.get((symbol, tf), []))
    if len(data) < 30:
        return {
            "symbol": symbol, "timeframe": tf, "signal": "NO TRADE",
            "confidence": 0, "reason": "Waiting for closed candles",
            "candles": len(data), "source": "Binance",
            "timestamp": int(time.time()),
        }

    closes = [x["close"] for x in data]
    e9 = ema(closes, 9)
    e21 = ema(closes, 21)
    e50 = ema(closes, 50)
    e200 = ema(closes, 200)
    r = rsi(closes)
    a = atr(data)

    last = data[-1]
    prev = data[-2]
    buy = sell = 0
    confirmations = []

    if e50 is not None and e200 is not None:
        if e50 > e200:
            buy += 2
            confirmations.append("EMA 50/200 bullish")
        elif e50 < e200:
            sell += 2
            confirmations.append("EMA 50/200 bearish")

    if e9 is not None and e21 is not None:
        if e9 > e21:
            buy += 1
        elif e9 < e21:
            sell += 1

    if r is not None:
        if 52 <= r <= 70:
            buy += 1
            confirmations.append("RSI bullish")
        elif 30 <= r <= 48:
            sell += 1
            confirmations.append("RSI bearish")

    if last["close"] > last["open"] and last["close"] > prev["high"]:
        buy += 2
        confirmations.append("BOS / momentum bullish")
    elif last["close"] < last["open"] and last["close"] < prev["low"]:
        sell += 2
        confirmations.append("BOS / momentum bearish")

    # Simple liquidity sweep approximation.
    recent = data[-10:-1]
    if recent:
        prev_low = min(x["low"] for x in recent)
        prev_high = max(x["high"] for x in recent)
        if last["low"] < prev_low and last["close"] > prev_low:
            buy += 2
            confirmations.append("Liquidity grab bullish")
        if last["high"] > prev_high and last["close"] < prev_high:
            sell += 2
            confirmations.append("Liquidity grab bearish")

    if len(data) >= 3:
        c1, c2, c3 = data[-3], data[-2], data[-1]
        if c1["high"] < c3["low"]:
            buy += 1
            confirmations.append("FVG bullish")
        if c1["low"] > c3["high"]:
            sell += 1
            confirmations.append("FVG bearish")

    if buy >= 5 and buy > sell:
        signal = "BUY"
        score = buy
    elif sell >= 5 and sell > buy:
        signal = "SELL"
        score = sell
    else:
        signal = "NO TRADE"
        score = max(buy, sell)

    confidence = min(95, max(0, 45 + score * 7))
    entry = last["close"]
    sl = tp1 = tp2 = None
    if a:
        if signal == "BUY":
            sl, tp1, tp2 = entry - 1.5*a, entry + 1.5*a, entry + 3*a
        elif signal == "SELL":
            sl, tp1, tp2 = entry + 1.5*a, entry - 1.5*a, entry - 3*a

    return {
        "symbol": symbol,
        "timeframe": tf,
        "signal": signal,
        "confidence": confidence,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "price": entry,
        "ema9": e9,
        "ema21": e21,
        "ema50": e50,
        "ema200": e200,
        "rsi14": r,
        "atr14": a,
        "confirmations": confirmations,
        "auto_trade": False,
        "source": "Binance",
        "candles": len(data),
        "timestamp": int(time.time()),
    }


def fetch_binance(symbol, interval, limit=300):
    url = (
        "https://data-api.binance.vision/api/v3/klines"
        f"?symbol={symbol}&interval={interval}&limit={limit}"
    )
    with urllib.request.urlopen(url, timeout=15) as response:
        raw = json.loads(response.read().decode())
    return [
        {
            "time": int(x[0]),
            "open": float(x[1]),
            "high": float(x[2]),
            "low": float(x[3]),
            "close": float(x[4]),
            "volume": float(x[5]),
        }
        for x in raw
    ]


async def load_history():
    # Only Binance crypto symbols are loaded here. Other providers remain
    # credential/API dependent and are never falsely marked LIVE.
    crypto = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]
    for symbol in crypto:
        for tf in INTERVALS:
            try:
                data = await asyncio.to_thread(fetch_binance, symbol, tf)
                bars[(symbol, tf)].clear()
                # Only completed candles are used by the signal engine.
                bars[(symbol, tf)].extend(data[:-1])
                latest[(symbol, tf)] = analyze(symbol, tf)
            except Exception as exc:
                latest[(symbol, tf)] = {
                    "symbol": symbol, "timeframe": tf,
                    "signal": "NO TRADE", "confidence": 0,
                    "reason": f"Data error: {exc}",
                    "source": "Binance",
                    "timestamp": int(time.time()),
                }


async def binance_stream():
    crypto = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]
    streams = "/".join(
        f"{s.lower()}@kline_{tf}"
        for s in crypto for tf in INTERVALS
    )
    url = f"wss://stream.binance.com:9443/stream?streams={streams}"

    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                print("Binance WebSocket connected")
                async for raw in ws:
                    msg = json.loads(raw)
                    k = msg.get("data", {}).get("k")
                    if not k:
                        continue
                    feed_heartbeat["Binance"] = time.time()

                    # Never analyze an unfinished candle.
                    if not k.get("x"):
                        continue

                    symbol = k["s"]
                    tf = k["i"]
                    c = {
                        "time": int(k["t"]),
                        "open": float(k["o"]),
                        "high": float(k["h"]),
                        "low": float(k["l"]),
                        "close": float(k["c"]),
                        "volume": float(k["v"]),
                    }
                    q = bars[(symbol, tf)]
                    if q and q[-1]["time"] == c["time"]:
                        q[-1] = c
                    else:
                        q.append(c)
                    latest[(symbol, tf)] = analyze(symbol, tf)
        except Exception as exc:
            print("Binance stream reconnect:", exc)
            await asyncio.sleep(5)


@app.on_event("startup")
async def startup():
    await load_history()
    background_tasks.append(asyncio.create_task(binance_stream()))


@app.on_event("shutdown")
async def shutdown():
    for task in background_tasks:
        task.cancel()


@app.get("/")
async def home():
    # This is the fix for Render's 404 at GET /.
    if INDEX.exists():
        return FileResponse(INDEX, media_type="text/html")
    return JSONResponse({
        "status": "online",
        "message": "AI Live Signal Bot is running, but index.html is missing.",
    })


@app.get("/api/health")
async def health():
    age = time.time() - feed_heartbeat["Binance"] if feed_heartbeat["Binance"] else None
    return {
        "status": "ok",
        "service": "AI Live Signal Bot V17",
        "binance": "LIVE" if age is not None and age < 30 else "WAITING",
        "seconds_since_feed": age,
        "auto_trade": False,
        "timestamp": int(time.time()),
    }


@app.get("/api/live-status")
async def live_status():
    age = time.time() - feed_heartbeat["Binance"] if feed_heartbeat["Binance"] else None
    return {
        "Binance": {
            "status": "LIVE" if age is not None and age < 30 else "WAITING",
            "seconds_since_data": age,
        }
    }


@app.get("/api/sources")
async def sources():
    names = DEFAULT_SOURCES
    if isinstance(PROVIDERS, list):
        names = [p.get("name", str(p)) if isinstance(p, dict) else str(p) for p in PROVIDERS]
    elif isinstance(PROVIDERS, dict):
        names = list(PROVIDERS.keys()) or DEFAULT_SOURCES

    return {
        "sources": [
            {
                "name": name,
                "status": "LIVE" if name == "Binance" and feed_heartbeat["Binance"] else
                          "API KEY / CONNECTOR REQUIRED",
            }
            for name in names
        ]
    }


@app.get("/api/instruments")
async def instruments():
    return {"symbols": SYMBOLS, "timeframes": INTERVALS, "registry": INSTRUMENTS}


@app.get("/api/candles/{symbol}/{timeframe}")
async def candles(symbol: str, timeframe: str):
    key = (symbol.upper(), timeframe)
    if key not in bars:
        return JSONResponse({"error": "Unsupported symbol/timeframe"}, status_code=400)
    return {"symbol": key[0], "timeframe": key[1], "candles": list(bars[key])}


@app.get("/api/signal/{symbol}/{timeframe}")
async def signal(symbol: str, timeframe: str):
    key = (symbol.upper(), timeframe)
    if key not in bars:
        return JSONResponse({"error": "Unsupported symbol/timeframe"}, status_code=400)
    return latest.get(key, analyze(*key))


# Backward-compatible endpoints.
@app.get("/signal/{symbol}/{interval}")
async def legacy_signal(symbol: str, interval: str):
    return await signal(symbol, interval)


@app.get("/sources")
async def legacy_sources():
    return await sources()


@app.get("/health")
async def legacy_health():
    return await health()
