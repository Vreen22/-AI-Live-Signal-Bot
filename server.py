import asyncio
import json
import time
import urllib.parse
import urllib.request
from collections import deque
from pathlib import Path

import websockets
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse

BASE = Path(__file__).resolve().parent
INDEX = BASE / "index.html"

app = FastAPI(title="AI Live Signal Bot V18")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

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

CRYPTO_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]

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
            "symbol": symbol,
            "timeframe": tf,
            "signal": "NO TRADE",
            "confidence": 0,
            "reason": "Waiting for enough closed candles",
            "candles": len(data),
            "source": "Binance",
            "timestamp": int(time.time()),
            "auto_trade": False,
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

    buy = 0
    sell = 0
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
            sl = entry - 1.5 * a
            tp1 = entry + 1.5 * a
            tp2 = entry + 3 * a
        elif signal == "SELL":
            sl = entry + 1.5 * a
            tp1 = entry - 1.5 * a
            tp2 = entry - 3 * a

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
    """
    Binance Spot REST klines.
    We use api.binance.com instead of the old data-api host.
    """
    query = urllib.parse.urlencode({
        "symbol": symbol,
        "interval": interval,
        "limit": limit,
    })

    url = f"https://api.binance.com/api/v3/klines?{query}"

    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "AI-Live-Signal-Bot/18",
            "Accept": "application/json",
        },
    )

    last_error = None

    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=20) as response:
                raw = json.loads(response.read().decode("utf-8"))

            if not isinstance(raw, list):
                raise RuntimeError(f"Unexpected Binance response: {raw}")

            result = []

            for x in raw:
                result.append({
                    "time": int(x[0]),
                    "open": float(x[1]),
                    "high": float(x[2]),
                    "low": float(x[3]),
                    "close": float(x[4]),
                    "volume": float(x[5]),
                })

            return result

        except Exception as exc:
            last_error = exc
            time.sleep(1 + attempt)

    raise RuntimeError(
        f"Binance REST failed for {symbol} {interval}: {last_error}"
    )


async def load_one_history(symbol, tf):
    try:
        data = await asyncio.to_thread(
            fetch_binance,
            symbol,
            tf,
            300,
        )

        # The last REST kline can still be open.
        # The signal engine uses CLOSED candles only.
        closed = data[:-1] if len(data) > 1 else []

        q = bars[(symbol, tf)]
        q.clear()
        q.extend(closed)

        if q:
            latest[(symbol, tf)] = analyze(symbol, tf)

        print(
            f"Loaded {symbol} {tf}: "
            f"{len(closed)} closed candles"
        )

        return len(closed)

    except Exception as exc:
        print(f"History error {symbol} {tf}: {exc}")
        return 0


async def load_history():
    """
    Load BTCUSDT 1m first so the mobile UI becomes useful immediately.
    Then load the remaining crypto/timeframe combinations.
    """
    await load_one_history("BTCUSDT", "1m")

    jobs = []

    for symbol in CRYPTO_SYMBOLS:
        for tf in INTERVALS:
            if symbol == "BTCUSDT" and tf == "1m":
                continue
            jobs.append(load_one_history(symbol, tf))

    # Small batches reduce the chance of hitting REST limits.
    for i in range(0, len(jobs), 7):
        await asyncio.gather(*jobs[i:i + 7])
        await asyncio.sleep(0.15)


async def binance_stream():
    streams = "/".join(
        f"{symbol.lower()}@kline_{tf}"
        for symbol in CRYPTO_SYMBOLS
        for tf in INTERVALS
    )

    url = (
        "wss://stream.binance.com:9443/stream"
        f"?streams={streams}"
    )

    while True:
        try:
            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=10,
                max_size=2**20,
            ) as ws:

                print("Binance WebSocket connected")
                feed_heartbeat["Binance"] = time.time()

                async for raw in ws:
                    msg = json.loads(raw)

                    k = msg.get("data", {}).get("k")

                    if not k:
                        continue

                    # The connection itself is alive.
                    feed_heartbeat["Binance"] = time.time()

                    symbol = k.get("s")
                    tf = k.get("i")

                    if not symbol or not tf:
                        continue

                    # Ignore unsupported combinations.
                    if (symbol, tf) not in bars:
                        continue

                    candle = {
                        "time": int(k["t"]),
                        "open": float(k["o"]),
                        "high": float(k["h"]),
                        "low": float(k["l"]),
                        "close": float(k["c"]),
                        "volume": float(k["v"]),
                    }

                    # We display/analyze CLOSED candles only.
                    if not k.get("x"):
                        continue

                    q = bars[(symbol, tf)]

                    if q and q[-1]["time"] == candle["time"]:
                        q[-1] = candle
                    else:
                        q.append(candle)

                    latest[(symbol, tf)] = analyze(symbol, tf)

        except Exception as exc:
            print(f"Binance WebSocket reconnect: {exc}")
            feed_heartbeat["Binance"] = 0.0
            await asyncio.sleep(5)


@app.on_event("startup")
async def startup():
    # History first, then stream.
    await load_history()

    background_tasks.append(
        asyncio.create_task(binance_stream())
    )


@app.on_event("shutdown")
async def shutdown():
    for task in background_tasks:
        task.cancel()


@app.get("/")
async def home():
    if INDEX.exists():
        return FileResponse(
            INDEX,
            media_type="text/html",
        )

    return JSONResponse({
        "status": "online",
        "message": "index.html is missing.",
    })


@app.get("/api/health")
async def health():
    age = (
        time.time() - feed_heartbeat["Binance"]
        if feed_heartbeat["Binance"]
        else None
    )

    return {
        "status": "ok",
        "service": "AI Live Signal Bot V18",
        "binance": (
            "LIVE"
            if age is not None and age < 30
            else "WAITING"
        ),
        "seconds_since_feed": age,
        "auto_trade": False,
        "btc_1m_closed_candles": len(
            bars[("BTCUSDT", "1m")]
        ),
        "timestamp": int(time.time()),
    }


@app.get("/api/live-status")
async def live_status():
    age = (
        time.time() - feed_heartbeat["Binance"]
        if feed_heartbeat["Binance"]
        else None
    )

    btc_count = len(
        bars[("BTCUSDT", "1m")]
    )

    return {
        "Binance": {
            "status": (
                "LIVE"
                if age is not None and age < 30
                else "WAITING"
            ),
            "seconds_since_data": age,
            "closed_candles": btc_count,
        }
    }


@app.get("/api/sources")
async def sources():
    names = DEFAULT_SOURCES

    if isinstance(PROVIDERS, list):
        names = [
            p.get("name", str(p))
            if isinstance(p, dict)
            else str(p)
            for p in PROVIDERS
        ]

    elif isinstance(PROVIDERS, dict):
        names = (
            list(PROVIDERS.keys())
            or DEFAULT_SOURCES
        )

    return {
        "sources": [
            {
                "name": name,
                "status": (
                    "LIVE"
                    if name == "Binance"
                    and feed_heartbeat["Binance"]
                    else "API KEY / CONNECTOR REQUIRED"
                ),
            }
            for name in names
        ]
    }


@app.get("/api/instruments")
async def instruments():
    return {
        "symbols": SYMBOLS,
        "timeframes": INTERVALS,
        "registry": INSTRUMENTS,
    }


@app.get("/api/candles/{symbol}/{timeframe}")
async def candles(
    symbol: str,
    timeframe: str,
):
    key = (
        symbol.upper(),
        timeframe,
    )

    if key not in bars:
        return JSONResponse(
            {
                "error":
                "Unsupported symbol/timeframe"
            },
            status_code=400,
        )

    return {
        "symbol": key[0],
        "timeframe": key[1],
        "candles": list(bars[key]),
    }


@app.get("/api/signal/{symbol}/{timeframe}")
async def signal(
    symbol: str,
    timeframe: str,
):
    key = (
        symbol.upper(),
        timeframe,
    )

    if key not in bars:
        return JSONResponse(
            {
                "error":
                "Unsupported symbol/timeframe"
            },
            status_code=400,
        )

    return latest.get(
        key,
        analyze(*key),
    )


@app.get("/api/refresh/{symbol}/{timeframe}")
async def refresh(
    symbol: str,
    timeframe: str,
):
    key = (
        symbol.upper(),
        timeframe,
    )

    if key not in bars:
        return JSONResponse(
            {
                "error":
                "Unsupported symbol/timeframe"
            },
            status_code=400,
        )

    count = await load_one_history(
        key[0],
        key[1],
    )

    return {
        "symbol": key[0],
        "timeframe": key[1],
        "closed_candles": count,
        "signal": latest.get(
            key,
            analyze(*key),
        ),
    }


# Backward-compatible routes.
@app.get("/health")
async def legacy_health():
    return await health()


@app.get("/sources")
async def legacy_sources():
    return await sources()


@app.get("/signal/{symbol}/{interval}")
async def legacy_signal(
    symbol: str,
    interval: str,
):
    return await signal(
        symbol,
        interval,
    )
