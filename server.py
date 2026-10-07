import asyncio
import json
import time
import urllib.parse
import urllib.request
import urllib.error
from collections import deque
from pathlib import Path

import websockets
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse

BASE = Path(__file__).resolve().parent
INDEX = BASE / "index.html"

app = FastAPI(title="AI Live Signal Bot V21")
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

# Keep only CLOSED candles here.
bars = {(s, tf): deque(maxlen=500) for s in SYMBOLS for tf in INTERVALS}
latest = {}
feed_heartbeat = {"Binance": 0.0}
background_tasks = []

# Binance public market-data fallbacks.
# data-api.binance.vision is preferred for public market data.
REST_BASES = [
    "https://data-api.binance.vision/api/v3/klines",
    "https://api.binance.com/api/v3/klines",
    "https://api1.binance.com/api/v3/klines",
    "https://api2.binance.com/api/v3/klines",
    "https://api3.binance.com/api/v3/klines",
    "https://api4.binance.com/api/v3/klines",
]

USER_AGENT = "Mozilla/5.0 AI-Live-Signal-Bot/19"

# Prevent hammering REST if the hosting IP is temporarily rate-limited.
rest_backoff_until = 0.0


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

    gains = []
    losses = []

    for a, b in zip(values[-period - 1:-1], values[-period:]):
        d = b - a
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0

    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def atr(candles, period=14):
    if len(candles) < period + 1:
        return None

    tr = []

    for i in range(1, len(candles)):
        c = candles[i]
        p = candles[i - 1]

        tr.append(max(
            c["high"] - c["low"],
            abs(c["high"] - p["close"]),
            abs(c["low"] - p["close"]),
        ))

    return sum(tr[-period:]) / period


def detect_liquidity(data):
    if len(data) < 11:
        return None

    last = data[-1]
    recent = data[-11:-1]

    prev_low = min(x["low"] for x in recent)
    prev_high = max(x["high"] for x in recent)

    # Sweep below liquidity then close back above it.
    if last["low"] < prev_low and last["close"] > prev_low:
        return "bullish"

    # Sweep above liquidity then close back below it.
    if last["high"] > prev_high and last["close"] < prev_high:
        return "bearish"

    return None


def detect_bos_choch(data):
    if len(data) < 5:
        return None

    last = data[-1]
    recent = data[-5:-1]

    swing_high = max(x["high"] for x in recent)
    swing_low = min(x["low"] for x in recent)

    if last["close"] > swing_high:
        return "bullish"

    if last["close"] < swing_low:
        return "bearish"

    return None


def detect_fvg(data):
    if len(data) < 3:
        return None

    c1, c2, c3 = data[-3], data[-2], data[-1]

    # Three-candle imbalance.
    if c1["high"] < c3["low"]:
        return "bullish"

    if c1["low"] > c3["high"]:
        return "bearish"

    return None


def detect_momentum(last, prev):
    if last["close"] > last["open"] and last["close"] > prev["close"]:
        return "bullish"

    if last["close"] < last["open"] and last["close"] < prev["close"]:
        return "bearish"

    return None


def recent_swing_levels(data, lookback=30, pivot=2):
    """Return recent confirmed swing high/low from CLOSED candles only."""
    if len(data) < (pivot * 2 + 5):
        return None, None

    window = data[-lookback:] if len(data) > lookback else data
    swing_highs = []
    swing_lows = []

    for i in range(pivot, len(window) - pivot):
        h = window[i]["high"]
        l = window[i]["low"]
        left = window[i-pivot:i]
        right = window[i+1:i+pivot+1]
        if all(h > x["high"] for x in left + right):
            swing_highs.append(h)
        if all(l < x["low"] for x in left + right):
            swing_lows.append(l)

    return (swing_highs[-1] if swing_highs else None,
            swing_lows[-1] if swing_lows else None)


def analyze(symbol, tf):
    data = list(bars.get((symbol, tf), []))

    # EMA 50/200 needs at least 200 CLOSED candles.
    if len(data) < 200:
        return {
            "symbol": symbol,
            "timeframe": tf,
            "signal": "NO TRADE",
            "signal_strength": "WAITING",
            "confidence": 0,
            "reason": f"Waiting for 200 closed candles ({len(data)}/200)",
            "candles": len(data),
            "source": "Binance",
            "timestamp": int(time.time()),
            "auto_trade": False,
            "entry": None,
            "sl": None,
            "tp1": None,
            "tp2": None,
            "price": data[-1]["close"] if data else None,
            "ema9": None,
            "ema21": None,
            "ema50": None,
            "ema200": None,
            "rsi14": None,
            "atr14": None,
            "swing_high": None,
            "swing_low": None,
            "risk": None,
            "rr_tp1": None,
            "rr_tp2": None,
            "confirmations": [],
            "smc": {
                "liquidity_grab": "WAITING",
                "bos_choch": "WAITING",
                "fvg": "WAITING",
                "ema_50_200": "WAITING",
                "rsi14": "WAITING",
                "momentum": "WAITING",
            },
        }

    closes = [x["close"] for x in data]
    e9 = ema(closes, 9)
    e21 = ema(closes, 21)
    e50 = ema(closes, 50)
    e200 = ema(closes, 200)
    r = rsi(closes, 14)
    a = atr(data, 14)

    last = data[-1]
    prev = data[-2]

    liquidity = detect_liquidity(data)
    bos = detect_bos_choch(data)
    fvg = detect_fvg(data)
    momentum = detect_momentum(last, prev)

    buy_score = 0
    sell_score = 0
    confirmations = []

    # ------------------------------------------------------------
    # 1) EMA TREND — mandatory directional filter
    # ------------------------------------------------------------
    if e50 > e200:
        ema_state = "BULLISH"
        buy_score += 2
        confirmations.append("EMA 50 > EMA 200")
    elif e50 < e200:
        ema_state = "BEARISH"
        sell_score += 2
        confirmations.append("EMA 50 < EMA 200")
    else:
        ema_state = "NEUTRAL"

    # Fast EMA is a small bonus only.
    if e9 > e21:
        buy_score += 1
    elif e9 < e21:
        sell_score += 1

    # ------------------------------------------------------------
    # 2) RSI — direction-aware and not falsely marked neutral at 70+
    # ------------------------------------------------------------
    if 55 <= r <= 75:
        rsi_state = "BULLISH"
        buy_score += 1
        confirmations.append("RSI bullish")
    elif 25 <= r <= 45:
        rsi_state = "BEARISH"
        sell_score += 1
        confirmations.append("RSI bearish")
    elif r > 75:
        # Very strong momentum, but overbought. It is NOT a fresh
        # confirmation by itself; require structure/momentum to agree.
        rsi_state = "BULLISH_OVERBOUGHT"
        if e50 > e200:
            buy_score += 1
    elif r < 25:
        rsi_state = "BEARISH_OVERSOLD"
        if e50 < e200:
            sell_score += 1
    else:
        rsi_state = "NEUTRAL"

    # ------------------------------------------------------------
    # 3) BOS / CHoCH — mandatory structure confirmation
    # ------------------------------------------------------------
    if bos == "bullish":
        buy_score += 2
        confirmations.append("BOS/CHoCH bullish")
    elif bos == "bearish":
        sell_score += 2
        confirmations.append("BOS/CHoCH bearish")

    # ------------------------------------------------------------
    # 4) Liquidity grab — bonus confirmation
    # ------------------------------------------------------------
    if liquidity == "bullish":
        buy_score += 2
        confirmations.append("Liquidity grab bullish")
    elif liquidity == "bearish":
        sell_score += 2
        confirmations.append("Liquidity grab bearish")

    # ------------------------------------------------------------
    # 5) FVG — mandatory imbalance confirmation for a trade
    # ------------------------------------------------------------
    if fvg == "bullish":
        buy_score += 1
        confirmations.append("FVG bullish")
    elif fvg == "bearish":
        sell_score += 1
        confirmations.append("FVG bearish")

    # ------------------------------------------------------------
    # 6) Momentum — mandatory directional confirmation
    # ------------------------------------------------------------
    if momentum == "bullish":
        buy_score += 1
        confirmations.append("Momentum bullish")
    elif momentum == "bearish":
        sell_score += 1
        confirmations.append("Momentum bearish")

    # ------------------------------------------------------------
    # TRADE RULE
    # A BUY/SELL is only allowed when the important confirmations
    # actually agree. This prevents the old situation where the UI
    # showed BUY while RSI/Liquidity were still neutral/waiting.
    # ------------------------------------------------------------
    bullish_core = (
        e50 > e200
        and bos == "bullish"
        and fvg == "bullish"
        and momentum == "bullish"
        and rsi_state in ("BULLISH", "BULLISH_OVERBOUGHT")
    )

    bearish_core = (
        e50 < e200
        and bos == "bearish"
        and fvg == "bearish"
        and momentum == "bearish"
        and rsi_state in ("BEARISH", "BEARISH_OVERSOLD")
    )

    if bullish_core and buy_score > sell_score:
        signal = "BUY"
        score = buy_score
        core_count = 5
        signal_strength = "STRONG" if liquidity == "bullish" else "CONFIRMED"
    elif bearish_core and sell_score > buy_score:
        signal = "SELL"
        score = sell_score
        core_count = 5
        signal_strength = "STRONG" if liquidity == "bearish" else "CONFIRMED"
    else:
        signal = "NO TRADE"
        score = max(buy_score, sell_score)
        core_count = 0
        signal_strength = "WAIT"

    # Confidence is an agreement score, not a probability or guarantee.
    # A confirmed 5-part setup starts at 80%; liquidity can increase it.
    if signal == "BUY" or signal == "SELL":
        confidence = 80
        if liquidity == ("bullish" if signal == "BUY" else "bearish"):
            confidence += 8
        if (signal == "BUY" and rsi_state == "BULLISH") or (signal == "SELL" and rsi_state == "BEARISH"):
            confidence += 4
        if e9 > e21 and signal == "BUY":
            confidence += 2
        if e9 < e21 and signal == "SELL":
            confidence += 2
        confidence = min(95, confidence)
    else:
        # Show useful confidence while explicitly refusing a trade.
        confidence = min(79, max(0, int(45 + score * 5)))

    entry = last["close"]
    sl = tp1 = tp2 = None
    risk = None
    rr_tp1 = rr_tp2 = None
    swing_high, swing_low = recent_swing_levels(data, lookback=40, pivot=2)

    # ------------------------------------------------------------
    # STRUCTURE + ATR RISK MANAGEMENT
    # BUY: stop below swing low + ATR buffer, with at least 1 ATR room.
    # SELL: stop above swing high + ATR buffer, with at least 1 ATR room.
    # TP1 = 1R, TP2 = 2R.
    # ------------------------------------------------------------
    if a and a > 0 and signal == "BUY":
        atr_floor = entry - 1.0 * a
        structure_sl = (swing_low - 0.25 * a) if swing_low is not None else atr_floor
        sl = min(structure_sl, atr_floor)
        risk = entry - sl
        if risk > 0:
            tp1 = entry + risk
            tp2 = entry + (2.0 * risk)
            rr_tp1 = 1.0
            rr_tp2 = 2.0

    elif a and a > 0 and signal == "SELL":
        atr_ceiling = entry + 1.0 * a
        structure_sl = (swing_high + 0.25 * a) if swing_high is not None else atr_ceiling
        sl = max(structure_sl, atr_ceiling)
        risk = sl - entry
        if risk > 0:
            tp1 = entry - risk
            tp2 = entry - (2.0 * risk)
            rr_tp1 = 1.0
            rr_tp2 = 2.0

    # Make the displayed SMC states match the actual signal logic.
    return {
        "symbol": symbol,
        "timeframe": tf,
        "signal": signal,
        "signal_strength": signal_strength,
        "confidence": confidence,
        "reason": " + ".join(confirmations[-6:]) if confirmations else "No sufficient confirmation",
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
        "swing_high": swing_high,
        "swing_low": swing_low,
        "risk": risk,
        "rr_tp1": rr_tp1,
        "rr_tp2": rr_tp2,
        "core_confirmations": core_count,
        "confirmations": confirmations,
        "smc": {
            "liquidity_grab": liquidity.upper() if liquidity else "NONE",
            "bos_choch": bos.upper() if bos else "NONE",
            "fvg": fvg.upper() if fvg else "NONE",
            "ema_50_200": ema_state,
            "rsi14": rsi_state,
            "momentum": momentum.upper() if momentum else "NEUTRAL",
        },
        "auto_trade": False,
        "source": "Binance",
        "candles": len(data),
        "timestamp": int(time.time()),
    }


def _request_json(url, timeout=15):
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
            "Cache-Control": "no-cache",
        },
    )

    with urllib.request.urlopen(req, timeout=timeout) as response:
        status = getattr(response, "status", 200)
        body = response.read().decode("utf-8")

        if status != 200:
            raise RuntimeError(f"HTTP {status}: {body[:300]}")

        return json.loads(body)


def fetch_binance(symbol, interval, limit=300):
    """
    Fetch public Binance Spot klines.

    Important:
    Render/shared hosting IPs can receive HTTP 418 from one Binance REST
    endpoint. We therefore try Binance's public market-data endpoint first,
    then several API hosts. WebSocket remains the live feed.
    """
    global rest_backoff_until

    if time.time() < rest_backoff_until:
        raise RuntimeError(
            f"REST temporarily backed off for "
            f"{int(rest_backoff_until - time.time())}s"
        )

    query = urllib.parse.urlencode({
        "symbol": symbol,
        "interval": interval,
        "limit": min(int(limit), 1000),
    })

    errors = []

    for base in REST_BASES:
        url = f"{base}?{query}"

        for attempt in range(2):
            try:
                raw = _request_json(url, timeout=15)

                if not isinstance(raw, list):
                    raise RuntimeError(
                        f"Unexpected response type: {type(raw).__name__}"
                    )

                result = []

                for x in raw:
                    if not isinstance(x, list) or len(x) < 6:
                        continue

                    result.append({
                        "time": int(x[0]),
                        "open": float(x[1]),
                        "high": float(x[2]),
                        "low": float(x[3]),
                        "close": float(x[4]),
                        "volume": float(x[5]),
                    })

                if result:
                    rest_backoff_until = 0.0
                    return result

                raise RuntimeError("Empty kline response")

            except urllib.error.HTTPError as exc:
                body = ""
                try:
                    body = exc.read().decode("utf-8")[:300]
                except Exception:
                    pass

                msg = f"{base}: HTTP {exc.code} {body}"
                errors.append(msg)

                # 418/429 normally means the current REST route/IP is
                # rate-limited. Do not hammer the same endpoint.
                if exc.code in (418, 429):
                    break

            except Exception as exc:
                errors.append(f"{base}: {exc}")

            if attempt == 0:
                awaitable_sleep = 0.6
                time.sleep(awaitable_sleep)

    # Back off for 30 seconds so a shared Render IP is not hammered.
    rest_backoff_until = time.time() + 30

    raise RuntimeError(
        "All Binance REST endpoints failed. "
        + " | ".join(errors[-6:])
    )


async def fetch_binance_ws_history(symbol, interval, limit=300):
    """
    Bootstrap historical klines through Binance's WebSocket API.

    This is deliberately used before REST because Render/shared hosting IPs
    can receive HTTP 418/429 from Binance REST. The normal market WebSocket
    does NOT provide historical candles, so the WebSocket API is used for the
    initial 300-candle snapshot.
    """
    url = "wss://ws-api.binance.com:443/ws-api/v3"
    request_id = f"history-{symbol}-{interval}-{int(time.time() * 1000)}"

    async with websockets.connect(
        url,
        ping_interval=20,
        ping_timeout=20,
        close_timeout=10,
        max_size=2**20,
    ) as ws:
        await ws.send(json.dumps({
            "id": request_id,
            "method": "klines",
            "params": {
                "symbol": symbol,
                "interval": interval,
                "limit": min(int(limit), 1000),
            },
        }))

        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            raw = await asyncio.wait_for(
                ws.recv(),
                timeout=max(1.0, deadline - time.monotonic()),
            )
            msg = json.loads(raw)

            if msg.get("id") != request_id:
                continue

            if msg.get("status") != 200:
                raise RuntimeError(
                    f"Binance WS API error: {msg.get('error', msg)}"
                )

            raw_klines = msg.get("result") or []
            result = []

            for x in raw_klines:
                if not isinstance(x, list) or len(x) < 6:
                    continue
                result.append({
                    "time": int(x[0]),
                    "open": float(x[1]),
                    "high": float(x[2]),
                    "low": float(x[3]),
                    "close": float(x[4]),
                    "volume": float(x[5]),
                })

            if not result:
                raise RuntimeError("Binance WS API returned no klines")

            return result

    raise RuntimeError("Timed out waiting for Binance WS API history")


async def load_one_history(symbol, tf):
    # WebSocket API first: avoids Render shared-IP REST 418/429 problems.
    try:
        data = await fetch_binance_ws_history(symbol, tf, 300)

        # The last kline may still be open. Keep CLOSED candles only.
        closed = data[:-1] if len(data) > 1 else []

        q = bars[(symbol, tf)]
        q.clear()
        q.extend(closed)

        if q:
            latest[(symbol, tf)] = analyze(symbol, tf)

        print(
            f"Loaded {symbol} {tf} via Binance WS API: "
            f"{len(closed)} closed candles"
        )
        return len(closed)

    except Exception as ws_exc:
        print(f"WS history error {symbol} {tf}: {ws_exc}")

    # REST fallback remains available if the WebSocket API is unavailable.
    try:
        data = await asyncio.to_thread(
            fetch_binance,
            symbol,
            tf,
            300,
        )

        closed = data[:-1] if len(data) > 1 else []
        q = bars[(symbol, tf)]
        q.clear()
        q.extend(closed)

        if q:
            latest[(symbol, tf)] = analyze(symbol, tf)

        print(
            f"Loaded {symbol} {tf} via REST fallback: "
            f"{len(closed)} closed candles"
        )
        return len(closed)

    except Exception as rest_exc:
        print(f"History error {symbol} {tf}: {rest_exc}")
        return 0


async def load_history():
    """
    Load BTCUSDT 1m first, then the remaining crypto/timeframe combinations.

    If REST is temporarily blocked, the service still starts and WebSocket
    continues. We do not crash the Render service because history failed.
    """
    first = await load_one_history("BTCUSDT", "1m")

    if first == 0:
        print(
            "WARNING: BTCUSDT 1m history could not be loaded. "
            "WebSocket will continue collecting closed candles."
        )

    jobs = []

    for symbol in CRYPTO_SYMBOLS:
        for tf in INTERVALS:
            if symbol == "BTCUSDT" and tf == "1m":
                continue
            jobs.append((symbol, tf))

    # Small batches reduce rate-limit pressure.
    for i in range(0, len(jobs), 4):
        batch = [
            load_one_history(symbol, tf)
            for symbol, tf in jobs[i:i + 4]
        ]

        await asyncio.gather(*batch)
        await asyncio.sleep(0.5)


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
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    k = msg.get("data", {}).get("k")

                    if not k:
                        continue

                    feed_heartbeat["Binance"] = time.time()

                    symbol = k.get("s")
                    tf = k.get("i")

                    if not symbol or not tf:
                        continue

                    if (symbol, tf) not in bars:
                        continue

                    # ONLY CLOSED CANDLES.
                    if not k.get("x"):
                        continue

                    candle = {
                        "time": int(k["t"]),
                        "open": float(k["o"]),
                        "high": float(k["h"]),
                        "low": float(k["l"]),
                        "close": float(k["c"]),
                        "volume": float(k["v"]),
                    }

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
    # Start the WebSocket immediately so Render is live even if REST fails.
    background_tasks.append(
        asyncio.create_task(binance_stream())
    )

    # History runs in the background instead of blocking application startup.
    background_tasks.append(
        asyncio.create_task(load_history())
    )


@app.on_event("shutdown")
async def shutdown():
    for task in background_tasks:
        task.cancel()

    if background_tasks:
        await asyncio.gather(
            *background_tasks,
            return_exceptions=True,
        )


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
        "service": "AI Live Signal Bot V19",
        "binance": (
            "LIVE"
            if age is not None and age < 30
            else "WAITING"
        ),
        "seconds_since_feed": age,
        "rest_backoff_seconds": max(
            0,
            int(rest_backoff_until - time.time()),
        ),
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

    return {
        "Binance": {
            "status": (
                "LIVE"
                if age is not None and age < 30
                else "WAITING"
            ),
            "seconds_since_data": age,
            "closed_candles": len(
                bars[("BTCUSDT", "1m")]
            ),
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
        names = list(PROVIDERS.keys()) or DEFAULT_SOURCES

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
async def candles(symbol: str, timeframe: str):
    key = (symbol.upper(), timeframe)

    if key not in bars:
        return JSONResponse(
            {"error": "Unsupported symbol/timeframe"},
            status_code=400,
        )

    return {
        "symbol": key[0],
        "timeframe": key[1],
        "candles": list(bars[key]),
    }


@app.get("/api/signal/{symbol}/{timeframe}")
async def signal(symbol: str, timeframe: str):
    key = (symbol.upper(), timeframe)

    if key not in bars:
        return JSONResponse(
            {"error": "Unsupported symbol/timeframe"},
            status_code=400,
        )

    return latest.get(key, analyze(*key))


@app.get("/api/refresh/{symbol}/{timeframe}")
async def refresh(symbol: str, timeframe: str):
    key = (symbol.upper(), timeframe)

    if key not in bars:
        return JSONResponse(
            {"error": "Unsupported symbol/timeframe"},
            status_code=400,
        )

    count = await load_one_history(key[0], key[1])

    return {
        "symbol": key[0],
        "timeframe": key[1],
        "closed_candles": count,
        "signal": latest.get(key, analyze(*key)),
    }


# Backward-compatible routes.
@app.get("/health")
async def legacy_health():
    return await health()


@app.get("/sources")
async def legacy_sources():
    return await sources()


@app.get("/signal/{symbol}/{interval}")
async def legacy_signal(symbol: str, interval: str):
    return await signal(symbol, interval)
