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

APP_TITLE = "AI Live Signal Bot V23"

# Binance public market-data API
BINANCE_REST = "https://data-api.binance.vision/api/v3/klines"

# Binance public WebSocket
BINANCE_WS = "wss://data-stream.binance.vision/stream"


TIMEFRAMES = {
    "1m": 60_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
}


DEFAULT_SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "ADAUSDT",
    "DOGEUSDT",
    "AVAXUSDT",
    "LINKUSDT",
    "LTCUSDT",
]


bars = {}
locks = {}

ws_task = None
history_task = None

last_ws_message_ms = 0
ws_connected = False
last_error = ""


for symbol in DEFAULT_SYMBOLS:
    for tf in TIMEFRAMES:
        bars[(symbol, tf)] = deque(maxlen=600)
        locks[(symbol, tf)] = asyncio.Lock()


app = FastAPI(title=APP_TITLE)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def now_ms():
    return int(time.time() * 1000)


# =========================
# EMA
# =========================

def ema(values, period):
    if len(values) < period:
        return None

    k = 2.0 / (period + 1.0)

    e = sum(values[:period]) / period

    for value in values[period:]:
        e = value * k + e * (1.0 - k)

    return e


# =========================
# RSI
# =========================

def rsi(values, period=14):
    if len(values) <= period:
        return None

    gains = []
    losses = []

    for i in range(1, period + 1):
        d = values[i] - values[i - 1]

        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    for i in range(period + 1, len(values)):
        d = values[i] - values[i - 1]

        gain = max(d, 0.0)
        loss = max(-d, 0.0)

        avg_gain = ((avg_gain * (period - 1)) + gain) / period
        avg_loss = ((avg_loss * (period - 1)) + loss) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss

    return 100.0 - (100.0 / (1.0 + rs))


# =========================
# ATR
# =========================

def atr(candles, period=14):
    if len(candles) < period + 1:
        return None

    trs = []

    for i in range(1, len(candles)):
        cur = candles[i]
        prev = candles[i - 1]

        tr = max(
            cur["high"] - cur["low"],
            abs(cur["high"] - prev["close"]),
            abs(cur["low"] - prev["close"]),
        )

        trs.append(tr)

    if len(trs) < period:
        return None

    value = sum(trs[:period]) / period

    for tr in trs[period:]:
        value = ((value * (period - 1)) + tr) / period

    return value


# =========================
# SAFE ROUND
# =========================

def safe_round(value, digits=4):
    if value is None:
        return None

    return round(float(value), digits)


# =========================
# BINANCE REST
# =========================

def request_klines(symbol, interval, limit=1000):

    params = urllib.parse.urlencode(
        {
            "symbol": symbol,
            "interval": interval,
            "limit": min(int(limit), 1000),
        }
    )

    url = f"{BINANCE_REST}?{params}"

    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "AI-Live-Signal-Bot/23",
            "Accept": "application/json",
        },
    )

    with urllib.request.urlopen(req, timeout=20) as response:
        raw = response.read()

    return json.loads(raw)


# =========================
# NORMALIZE CANDLE
# =========================

def normalize_row(row):

    return {
        "time": int(row[0]),
        "open": float(row[1]),
        "high": float(row[2]),
        "low": float(row[3]),
        "close": float(row[4]),
        "volume": float(row[5]),
        "closed": True,
    }


# =========================
# LOAD HISTORY
# =========================

async def load_history(symbol, tf):

    key = (symbol, tf)

    try:

        rows = await asyncio.to_thread(
            request_klines,
            symbol,
            tf,
            1000,
        )

        step = TIMEFRAMES[tf]
        current = now_ms()

        cleaned = []

        for row in rows:

            candle = normalize_row(row)

            # Current/open candle is ignored.
            if candle["time"] + step <= current:
                cleaned.append(candle)

        cleaned = cleaned[-500:]

        async with locks[key]:

            bars[key].clear()

            bars[key].extend(cleaned)

        print(
            f"History loaded {symbol} {tf}: "
            f"{len(cleaned)} closed candles"
        )

        return len(cleaned)

    except Exception as exc:

        print(
            f"History error {symbol} {tf}: {exc}"
        )

        return 0


# =========================
# INITIAL HISTORY
# =========================

async def load_initial_history():

    priority = [
        "1m",
        "5m",
        "15m",
        "30m",
        "1h",
        "2h",
        "4h",
    ]

    # BTC first
    for tf in priority:

        await load_history(
            "BTCUSDT",
            tf,
        )

    # Other crypto
    for symbol in DEFAULT_SYMBOLS[1:]:

        await load_history(
            symbol,
            "1m",
        )

        await load_history(
            symbol,
            "5m",
        )


# =========================
# BINANCE WEBSOCKET
# =========================

async def websocket_loop():

    global ws_connected
    global last_ws_message_ms
    global last_error

    streams = []

    for symbol in DEFAULT_SYMBOLS:

        for tf in TIMEFRAMES:

            streams.append(
                f"{symbol.lower()}@kline_{tf}"
            )

    url = (
        BINANCE_WS
        + "?streams="
        + "/".join(streams)
    )

    while True:

        try:

            print(
                "Connecting Binance public WebSocket..."
            )

            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
                max_size=8 * 1024 * 1024,
            ) as ws:

                ws_connected = True
                last_error = ""

                print(
                    "Binance WebSocket connected"
                )

                async for message in ws:

                    last_ws_message_ms = now_ms()

                    payload = json.loads(message)

                    data = payload.get(
                        "data",
                        payload,
                    )

                    if data.get("e") != "kline":
                        continue

                    k = data.get("k", {})

                    symbol = str(
                        k.get("s", "")
                    ).upper()

                    tf = k.get("i")

                    if symbol not in DEFAULT_SYMBOLS:
                        continue

                    if tf not in TIMEFRAMES:
                        continue

                    candle = {
                        "time": int(k["t"]),
                        "open": float(k["o"]),
                        "high": float(k["h"]),
                        "low": float(k["l"]),
                        "close": float(k["c"]),
                        "volume": float(k["v"]),
                        "closed": bool(k["x"]),
                    }

                    key = (symbol, tf)

                    async with locks[key]:

                        existing = list(
                            bars[key]
                        )

                        if (
                            existing
                            and existing[-1]["time"]
                            == candle["time"]
                        ):

                            existing[-1] = candle

                        elif (
                            not existing
                            or candle["time"]
                            > existing[-1]["time"]
                        ):

                            existing.append(candle)

                        else:

                            continue

                        bars[key].clear()

                        bars[key].extend(
                            existing[-500:]
                        )

        except Exception as exc:

            ws_connected = False

            last_error = str(exc)

            print(
                f"WebSocket error: {exc}"
            )

        ws_connected = False

        await asyncio.sleep(3)


# =========================
# MARKET STRUCTURE
# =========================

def find_structure(candles):

    if len(candles) < 12:

        return {
            "bos": False,
            "choch": False,
            "direction": "NEUTRAL",
        }

    recent = candles[-8:]

    previous = (
        candles[-16:-8]
        if len(candles) >= 16
        else candles[:-8]
    )

    if not previous:

        return {
            "bos": False,
            "choch": False,
            "direction": "NEUTRAL",
        }

    recent_high = max(
        x["high"] for x in recent
    )

    recent_low = min(
        x["low"] for x in recent
    )

    prev_high = max(
        x["high"] for x in previous
    )

    prev_low = min(
        x["low"] for x in previous
    )

    close = candles[-1]["close"]

    bullish_break = close > prev_high
    bearish_break = close < prev_low

    direction = (
        "BULLISH"
        if bullish_break
        else "BEARISH"
        if bearish_break
        else "NEUTRAL"
    )

    return {
        "bos": bullish_break or bearish_break,
        "choch": bullish_break or bearish_break,
        "direction": direction,
    }


# =========================
# LIQUIDITY
# =========================

def find_liquidity(candles):

    if len(candles) < 20:

        return False, "NONE"

    lookback = candles[-12:-2]

    highs = [
        x["high"]
        for x in lookback
    ]

    lows = [
        x["low"]
        for x in lookback
    ]

    last = candles[-1]

    high_level = max(highs)
    low_level = min(lows)

    tolerance = max(
        (high_level - low_level) * 0.03,
        last["close"] * 0.00015,
    )

    swept_high = (
        last["high"]
        > high_level + tolerance
        and last["close"]
        < high_level
    )

    swept_low = (
        last["low"]
        < low_level - tolerance
        and last["close"]
        > low_level
    )

    if swept_low:

        return (
            True,
            "SELL-SIDE LIQUIDITY GRAB",
        )

    if swept_high:

        return (
            True,
            "BUY-SIDE LIQUIDITY GRAB",
        )

    return False, "NONE"


# =========================
# FVG
# =========================

def find_fvg(candles):

    if len(candles) < 5:

        return False, "NONE"

    a = candles[-3]
    c = candles[-1]

    bullish = c["low"] > a["high"]
    bearish = c["high"] < a["low"]

    if bullish:

        return True, "BULLISH FVG"

    if bearish:

        return True, "BEARISH FVG"

    return False, "NONE"


# =========================
# MOMENTUM
# =========================

def momentum_state(candles):

    if len(candles) < 6:

        return "NEUTRAL"

    last = candles[-1]
    prev = candles[-4]

    move = (
        last["close"]
        - prev["close"]
    )

    rng = max(
        last["high"] - last["low"],
        1e-12,
    )

    if move > rng * 0.25:

        return "BULLISH"

    if move < -rng * 0.25:

        return "BEARISH"

    return "NEUTRAL"


# =========================
# SIGNAL ENGINE
# =========================

def analyze(candles):

    if len(candles) < 210:

        return {
            "signal": "NO TRADE",
            "side": "NONE",
            "confidence": 0,
            "reason": (
                "Need at least 210 closed "
                f"candles (have {len(candles)})"
            ),
            "trend": "NEUTRAL",
            "entry": None,
            "stop_loss": None,
            "tp1": None,
            "tp2": None,
            "rr": None,
            "rsi": None,
            "atr": None,
            "smc": {},
            "confirmations": [],
        }

    closes = [
        x["close"]
        for x in candles
    ]

    price = closes[-1]

    e50 = ema(
        closes,
        50,
    )

    e200 = ema(
        closes,
        200,
    )

    r = rsi(
        closes,
        14,
    )

    a = atr(
        candles,
        14,
    )

    structure = find_structure(
        candles
    )

    liquidity, liquidity_type = find_liquidity(
        candles
    )

    fvg, fvg_type = find_fvg(
        candles
    )

    momentum = momentum_state(
        candles
    )

    bullish_trend = (
        e50 is not None
        and e200 is not None
        and e50 > e200
    )

    bearish_trend = (
        e50 is not None
        and e200 is not None
        and e50 < e200
    )

    bullish_rsi = (
        r is not None
        and 50 <= r <= 72
    )

    bearish_rsi = (
        r is not None
        and 28 <= r < 50
    )

    buy_score = 0
    sell_score = 0

    buy_reasons = []
    sell_reasons = []

    # EMA
    if bullish_trend:

        buy_score += 2

        buy_reasons.append(
            "EMA50 > EMA200"
        )

    if bearish_trend:

        sell_score += 2

        sell_reasons.append(
            "EMA50 < EMA200"
        )

    # RSI
    if bullish_rsi:

        buy_score += 1

        buy_reasons.append(
            "RSI bullish"
        )

    if bearish_rsi:

        sell_score += 1

        sell_reasons.append(
            "RSI bearish"
        )

    # Structure
    if structure["direction"] == "BULLISH":

        buy_score += 2

        buy_reasons.append(
            "BOS/CHoCH bullish"
        )

    elif structure["direction"] == "BEARISH":

        sell_score += 2

        sell_reasons.append(
            "BOS/CHoCH bearish"
        )

    # Liquidity
    if (
        liquidity
        and liquidity_type
        == "SELL-SIDE LIQUIDITY GRAB"
    ):

        buy_score += 1

        buy_reasons.append(
            "Sell-side liquidity grab"
        )

    elif (
        liquidity
        and liquidity_type
        == "BUY-SIDE LIQUIDITY GRAB"
    ):

        sell_score += 1

        sell_reasons.append(
            "Buy-side liquidity grab"
        )

    # FVG
    if (
        fvg
        and fvg_type
        == "BULLISH FVG"
    ):

        buy_score += 1

        buy_reasons.append(
            "Bullish FVG"
        )

    elif (
        fvg
        and fvg_type
        == "BEARISH FVG"
    ):

        sell_score += 1

        sell_reasons.append(
            "Bearish FVG"
        )

    # Momentum
    if momentum == "BULLISH":

        buy_score += 1

        buy_reasons.append(
            "Bullish momentum"
        )

    elif momentum == "BEARISH":

        sell_score += 1

        sell_reasons.append(
            "Bearish momentum"
        )

    side = "NONE"
    signal = "NO TRADE"
    confidence = 0
    reasons = []

    # BUY
    if (
        buy_score >= 6
        and bullish_trend
        and momentum == "BULLISH"
    ):

        side = "BUY"
        signal = "BUY"

        confidence = min(
            96,
            58 + buy_score * 5,
        )

        reasons = buy_reasons

    # SELL
    elif (
        sell_score >= 6
        and bearish_trend
        and momentum == "BEARISH"
    ):

        side = "SELL"
        signal = "SELL"

        confidence = min(
            96,
            58 + sell_score * 5,
        )

        reasons = sell_reasons

    else:

        best = max(
            buy_score,
            sell_score,
        )

        confidence = min(
            79,
            45 + best * 4,
        )

        reasons = (
            buy_reasons
            if buy_score >= sell_score
            else sell_reasons
        )

    entry = (
        price
        if side != "NONE"
        else None
    )

    stop = None
    tp1 = None
    tp2 = None
    rr = None

    if side != "NONE" and a:

        risk = max(
            a * 1.25,
            price * 0.001,
        )

        if side == "BUY":

            stop = price - risk

            tp1 = price + risk * 1.5

            tp2 = price + risk * 2.2

        else:

            stop = price + risk

            tp1 = price - risk * 1.5

            tp2 = price - risk * 2.2

        rr = 1.5

    confirmations = [

        (
            "EMA 50/200 CONFIRMED"
            if (
                bullish_trend
                or bearish_trend
            )
            else
            "EMA 50/200 —"
        ),

        (
            "RSI 14 CONFIRMED"
            if (
                (
                    bullish_rsi
                    and bullish_trend
                )
                or
                (
                    bearish_rsi
                    and bearish_trend
                )
            )
            else
            "RSI 14 —"
        ),

        (
            "BOS / CHoCH CONFIRMED"
            if structure["bos"]
            else
            "BOS / CHoCH —"
        ),

        (
            "Liquidity Grab CONFIRMED"
            if liquidity
            else
            "Liquidity Grab —"
        ),

        (
            "FVG CONFIRMED"
            if fvg
            else
            "FVG —"
        ),

        (
            "Momentum CONFIRMED"
            if momentum != "NEUTRAL"
            else
            "Momentum —"
        ),
    ]

    return {

        "signal": signal,

        "side": side,

        "confidence": int(
            confidence
        ),

        "reason": (
            " + ".join(reasons)
            if reasons
            else
            "No complete setup"
        ),

        "trend": (
            "BULLISH"
            if bullish_trend
            else
            "BEARISH"
            if bearish_trend
            else
            "NEUTRAL"
        ),

        "entry": safe_round(
            entry,
            2,
        ),

        "stop_loss": safe_round(
            stop,
            2,
        ),

        "tp1": safe_round(
            tp1,
            2,
        ),

        "tp2": safe_round(
            tp2,
            2,
        ),

        "rr": rr,

        "rsi": safe_round(
            r,
            2,
        ),

        "atr": safe_round(
            a,
            4,
        ),

        "smc": {

            "liquidity": liquidity,

            "liquidity_type":
                liquidity_type,

            "bos":
                structure["bos"],

            "choch":
                structure["choch"],

            "fvg":
                fvg,

            "fvg_type":
                fvg_type,

            "momentum":
                momentum,
        },

        "confirmations":
            confirmations,

        "ema50":
            safe_round(
                e50,
                2,
            ),

        "ema200":
            safe_round(
                e200,
                2,
            ),
    }


# =========================
# PUBLIC CANDLE
# =========================

def public_candle(c):

    return {
        "time": c["time"],
        "open": c["open"],
        "high": c["high"],
        "low": c["low"],
        "close": c["close"],
        "volume": c["volume"],
    }


# =========================
# HOME
# =========================

@app.get("/")
async def root():

    if INDEX.exists():

        return FileResponse(
            INDEX
        )

    return JSONResponse(
        {
            "status": "ok",
            "app": APP_TITLE,
        }
    )


# =========================
# HEALTH
# =========================

@app.get("/api/health")
async def health():

    return {

        "ok": True,

        "app":
            APP_TITLE,

        "binance_rest":
            BINANCE_REST,

        "websocket_connected":
            ws_connected,

        "last_ws_message_ms":
            last_ws_message_ms,

        "last_error":
            last_error,
    }


# =========================
# SOURCES
# =========================

@app.get("/api/sources")
async def sources():

    return {

        "sources": [

            {
                "id": "binance",
                "name": "Binance",
                "type": "crypto",
                "live": True,
                "status":
                    (
                        "LIVE"
                        if ws_connected
                        else
                        "CONNECTING"
                    ),
            },

            {
                "id": "forex",
                "name": "Forex",
                "type": "forex",
                "live": False,
                "status":
                    "NOT_CONNECTED",
            },

            {
                "id": "metals",
                "name": "Metals",
                "type": "metals",
                "live": False,
                "status":
                    "NOT_CONNECTED",
            },
        ]
    }


# =========================
# INSTRUMENTS
# =========================

@app.get("/api/instruments")
async def instruments():

    return {

        "crypto":
            DEFAULT_SYMBOLS,

        "forex": [
            "EURUSD",
            "GBPUSD",
            "USDJPY",
            "AUDUSD",
            "USDCAD",
        ],

        "metals": [
            "XAUUSD",
            "XAGUSD",
        ],
    }


# =========================
# LIVE STATUS
# =========================

@app.get("/api/live-status")
async def live_status():

    counts = {}

    for tf in TIMEFRAMES:

        counts[tf] = len(
            bars[
                ("BTCUSDT", tf)
            ]
        )

    return {

        "source":
            "Binance",

        "live":
            ws_connected,

        "websocket":
            ws_connected,

        "last_message_ms":
            last_ws_message_ms,

        "last_error":
            last_error,

        "history":
            counts,
    }


# =========================
# CANDLES API
# =========================

@app.get("/api/candles/{symbol}/{tf}")
async def candles(
    symbol: str,
    tf: str,
):

    symbol = symbol.upper()

    if tf not in TIMEFRAMES:

        return JSONResponse(
            {
                "error":
                    "Unsupported timeframe"
            },
            status_code=400,
        )

    key = (
        symbol,
        tf,
    )

    if key not in bars:

        return JSONResponse(
            {
                "error":
                    "Unsupported symbol"
            },
            status_code=404,
        )

    async with locks[key]:

        data = [
            public_candle(x)
            for x in list(
                bars[key]
            )[-500:]
        ]

    return {

        "symbol":
            symbol,

        "timeframe":
            tf,

        "closed_only":
            True,

        "count":
            len(data),

        "candles":
            data,
    }


# =========================
# SIGNAL API
# =========================

@app.get("/api/signal/{symbol}/{tf}")
async def signal(
    symbol: str,
    tf: str,
):

    symbol = symbol.upper()

    if tf not in TIMEFRAMES:

        return JSONResponse(
            {
                "error":
                    "Unsupported timeframe"
            },
            status_code=400,
        )

    key = (
        symbol,
        tf,
    )

    if key not in bars:

        return JSONResponse(
            {
                "error":
                    "Unsupported symbol"
            },
            status_code=404,
        )

    async with locks[key]:

        data = list(
            bars[key]
        )

    result = analyze(data)

    result.update(

        {
            "symbol":
                symbol,

            "timeframe":
                tf,

            "candle_count":
                len(data),

            "generated_at":
                now_ms(),

            "data_source":
                "Binance LIVE",

            "auto_trade":
                False,
        }
    )

    return result


# =========================
# STARTUP
# =========================

@app.on_event("startup")
async def startup():

    global ws_task
    global history_task

    history_task = asyncio.create_task(
        load_initial_history()
    )

    await asyncio.sleep(0.2)

    ws_task = asyncio.create_task(
        websocket_loop()
    )


# =========================
# SHUTDOWN
# =========================

@app.on_event("shutdown")
async def shutdown():

    for task in (
        ws_task,
        history_task,
    ):

        if task:

            task.cancel()

    await asyncio.gather(

        *(
            task
            for task in (
                ws_task,
                history_task,
            )
            if task
        ),

        return_exceptions=True,
    )
