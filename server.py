import asyncio
import json
import time
from collections import deque
from pathlib import Path

import websockets
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse


# =========================================================
# AI LIVE SIGNAL BOT V24
# Binance WebSocket History + Binance Live WebSocket
# =========================================================

BASE = Path(__file__).resolve().parent
INDEX = BASE / "index.html"

APP_TITLE = "AI Live Signal Bot V24"

# Binance WebSocket API for historical klines
BINANCE_API_WS = "wss://ws-api.binance.com:443/ws-api/v3"

# Binance public stream for live candles
BINANCE_STREAM_WS = "wss://data-stream.binance.vision/stream"


TIMEFRAMES = {
    "1m": 60_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
}


SYMBOLS = [
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

for symbol in SYMBOLS:
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


ws_task = None
history_task = None

ws_connected = False
last_ws_message_ms = 0
last_error = ""


# =========================================================
# TIME
# =========================================================

def now_ms():
    return int(time.time() * 1000)


# =========================================================
# EMA
# =========================================================

def ema(values, period):

    if len(values) < period:
        return None

    value = sum(values[:period]) / period
    multiplier = 2 / (period + 1)

    for price in values[period:]:
        value = (
            price * multiplier
            + value * (1 - multiplier)
        )

    return value


# =========================================================
# RSI
# =========================================================

def rsi(values, period=14):

    if len(values) <= period:
        return None

    gains = []
    losses = []

    for i in range(1, period + 1):

        change = values[i] - values[i - 1]

        gains.append(max(change, 0))
        losses.append(max(-change, 0))

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    for i in range(period + 1, len(values)):

        change = values[i] - values[i - 1]

        gain = max(change, 0)
        loss = max(-change, 0)

        avg_gain = (
            avg_gain * (period - 1) + gain
        ) / period

        avg_loss = (
            avg_loss * (period - 1) + loss
        ) / period

    if avg_loss == 0:
        return 100

    rs = avg_gain / avg_loss

    return 100 - (100 / (1 + rs))


# =========================================================
# ATR
# =========================================================

def atr(candles, period=14):

    if len(candles) < period + 1:
        return None

    trs = []

    for i in range(1, len(candles)):

        current = candles[i]
        previous = candles[i - 1]

        tr = max(
            current["high"] - current["low"],
            abs(
                current["high"]
                - previous["close"]
            ),
            abs(
                current["low"]
                - previous["close"]
            ),
        )

        trs.append(tr)

    value = sum(trs[:period]) / period

    for tr in trs[period:]:

        value = (
            value * (period - 1) + tr
        ) / period

    return value


# =========================================================
# BINANCE WEBSOCKET HISTORY
# =========================================================

async def request_history(symbol, tf, limit=500):

    request_id = f"{symbol}_{tf}_{int(time.time()*1000)}"

    request = {
        "id": request_id,
        "method": "klines",
        "params": {
            "symbol": symbol,
            "interval": tf,
            "limit": limit,
        },
    }

    try:

        async with websockets.connect(
            BINANCE_API_WS,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=5,
            max_size=8 * 1024 * 1024,
        ) as ws:

            await ws.send(
                json.dumps(request)
            )

            while True:

                raw = await asyncio.wait_for(
                    ws.recv(),
                    timeout=20,
                )

                response = json.loads(raw)

                if response.get("id") != request_id:
                    continue

                if response.get("status") != 200:

                    raise RuntimeError(
                        response.get(
                            "error",
                            "Binance WebSocket API error",
                        )
                    )

                return response.get(
                    "result",
                    []
                )

    except Exception as exc:

        print(
            f"History WS error "
            f"{symbol} {tf}: {exc}"
        )

        return []


# =========================================================
# LOAD HISTORY
# =========================================================

async def load_history(symbol, tf):

    key = (symbol, tf)

    try:

        rows = await request_history(
            symbol,
            tf,
            500,
        )

        if not rows:

            print(
                f"NO HISTORY "
                f"{symbol} {tf}"
            )

            return 0

        step = TIMEFRAMES[tf]
        current = now_ms()

        candles = []

        for row in rows:

            open_time = int(row[0])

            # Do NOT use the currently open candle.
            if open_time + step > current:
                continue

            candles.append(
                {
                    "time": open_time,
                    "open": float(row[1]),
                    "high": float(row[2]),
                    "low": float(row[3]),
                    "close": float(row[4]),
                    "volume": float(row[5]),
                    "closed": True,
                }
            )

        candles = candles[-500:]

        async with locks[key]:

            bars[key].clear()

            bars[key].extend(candles)

        print(
            f"History loaded "
            f"{symbol} {tf}: "
            f"{len(candles)} closed candles"
        )

        return len(candles)

    except Exception as exc:

        print(
            f"History error "
            f"{symbol} {tf}: {exc}"
        )

        return 0


# =========================================================
# INITIAL HISTORY
# =========================================================

async def load_initial_history():

    # Main BTC timeframes
    for tf in TIMEFRAMES:

        await load_history(
            "BTCUSDT",
            tf,
        )

    # Other coins
    for symbol in SYMBOLS[1:]:

        await load_history(
            symbol,
            "1m",
        )

        await load_history(
            symbol,
            "5m",
        )


# =========================================================
# LIVE BINANCE STREAM
# =========================================================

async def live_stream():

    global ws_connected
    global last_ws_message_ms
    global last_error

    streams = []

    for symbol in SYMBOLS:

        for tf in TIMEFRAMES:

            streams.append(
                f"{symbol.lower()}@kline_{tf}"
            )

    url = (
        BINANCE_STREAM_WS
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

                    payload = json.loads(
                        message
                    )

                    data = payload.get(
                        "data",
                        payload,
                    )

                    if data.get("e") != "kline":
                        continue

                    kline = data.get("k", {})

                    symbol = str(
                        kline.get("s", "")
                    ).upper()

                    tf = kline.get("i")

                    if symbol not in SYMBOLS:
                        continue

                    if tf not in TIMEFRAMES:
                        continue

                    candle = {
                        "time": int(kline["t"]),
                        "open": float(kline["o"]),
                        "high": float(kline["h"]),
                        "low": float(kline["l"]),
                        "close": float(kline["c"]),
                        "volume": float(kline["v"]),
                        "closed": bool(kline["x"]),
                    }

                    key = (
                        symbol,
                        tf,
                    )

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

                            existing.append(
                                candle
                            )

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


# =========================================================
# MARKET STRUCTURE
# =========================================================

def market_structure(candles):

    if len(candles) < 20:

        return {
            "bos": False,
            "choch": False,
            "direction": "NEUTRAL",
        }

    previous = candles[-16:-6]
    recent = candles[-6:]

    previous_high = max(
        x["high"]
        for x in previous
    )

    previous_low = min(
        x["low"]
        for x in previous
    )

    close = candles[-1]["close"]

    if close > previous_high:

        return {
            "bos": True,
            "choch": True,
            "direction": "BULLISH",
        }

    if close < previous_low:

        return {
            "bos": True,
            "choch": True,
            "direction": "BEARISH",
        }

    return {
        "bos": False,
        "choch": False,
        "direction": "NEUTRAL",
    }


# =========================================================
# LIQUIDITY GRAB
# =========================================================

def liquidity_grab(candles):

    if len(candles) < 20:
        return False, "NONE"

    lookback = candles[-12:-2]

    high = max(
        x["high"]
        for x in lookback
    )

    low = min(
        x["low"]
        for x in lookback
    )

    last = candles[-1]

    tolerance = max(
        (high - low) * 0.03,
        last["close"] * 0.00015,
    )

    if (
        last["low"] < low - tolerance
        and last["close"] > low
    ):

        return (
            True,
            "SELL-SIDE LIQUIDITY GRAB",
        )

    if (
        last["high"] > high + tolerance
        and last["close"] < high
    ):

        return (
            True,
            "BUY-SIDE LIQUIDITY GRAB",
        )

    return False, "NONE"


# =========================================================
# FVG
# =========================================================

def detect_fvg(candles):

    if len(candles) < 5:
        return False, "NONE"

    first = candles[-3]
    last = candles[-1]

    if last["low"] > first["high"]:

        return True, "BULLISH FVG"

    if last["high"] < first["low"]:

        return True, "BEARISH FVG"

    return False, "NONE"


# =========================================================
# MOMENTUM
# =========================================================

def momentum(candles):

    if len(candles) < 6:
        return "NEUTRAL"

    old = candles[-4]
    last = candles[-1]

    move = (
        last["close"]
        - old["close"]
    )

    candle_range = max(
        last["high"] - last["low"],
        0.00000001,
    )

    if move > candle_range * 0.25:
        return "BULLISH"

    if move < -candle_range * 0.25:
        return "BEARISH"

    return "NEUTRAL"


# =========================================================
# SIGNAL ENGINE
# =========================================================

def analyze(candles):

    if len(candles) < 210:

        return {
            "signal": "NO TRADE",
            "side": "NONE",
            "confidence": 0,
            "reason": (
                f"Need 210 closed candles. "
                f"Current: {len(candles)}"
            ),
            "trend": "NEUTRAL",
            "entry": None,
            "stop_loss": None,
            "tp1": None,
            "tp2": None,
            "rr": None,
            "rsi": None,
            "atr": None,
            "confirmations": [],
        }

    closes = [
        x["close"]
        for x in candles
    ]

    price = closes[-1]

    ema50 = ema(
        closes,
        50,
    )

    ema200 = ema(
        closes,
        200,
    )

    rsi14 = rsi(
        closes,
        14,
    )

    atr14 = atr(
        candles,
        14,
    )

    structure = market_structure(
        candles
    )

    liquidity, liquidity_type = (
        liquidity_grab(candles)
    )

    fvg, fvg_type = detect_fvg(
        candles
    )

    mom = momentum(
        candles
    )

    bullish = ema50 > ema200
    bearish = ema50 < ema200

    buy_score = 0
    sell_score = 0

    buy_reasons = []
    sell_reasons = []

    # EMA
    if bullish:

        buy_score += 2
        buy_reasons.append(
            "EMA50 > EMA200"
        )

    if bearish:

        sell_score += 2
        sell_reasons.append(
            "EMA50 < EMA200"
        )

    # RSI
    if 50 <= rsi14 <= 72:

        buy_score += 1
        buy_reasons.append(
            "RSI bullish"
        )

    if 28 <= rsi14 < 50:

        sell_score += 1
        sell_reasons.append(
            "RSI bearish"
        )

    # BOS
    if structure["direction"] == "BULLISH":

        buy_score += 2
        buy_reasons.append(
            "Bullish BOS/CHoCH"
        )

    elif structure["direction"] == "BEARISH":

        sell_score += 2
        sell_reasons.append(
            "Bearish BOS/CHoCH"
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

    if (
        liquidity
        and liquidity_type
        == "BUY-SIDE LIQUIDITY GRAB"
    ):

        sell_score += 1
        sell_reasons.append(
            "Buy-side liquidity grab"
        )

    # FVG
    if fvg_type == "BULLISH FVG":

        buy_score += 1
        buy_reasons.append(
            "Bullish FVG"
        )

    if fvg_type == "BEARISH FVG":

        sell_score += 1
        sell_reasons.append(
            "Bearish FVG"
        )

    # Momentum
    if mom == "BULLISH":

        buy_score += 1
        buy_reasons.append(
            "Bullish momentum"
        )

    if mom == "BEARISH":

        sell_score += 1
        sell_reasons.append(
            "Bearish momentum"
        )

    signal = "NO TRADE"
    side = "NONE"

    confidence = min(
        79,
        45 + max(
            buy_score,
            sell_score
        ) * 4,
    )

    reasons = []

    # Strong BUY
    if (
        buy_score >= 6
        and bullish
        and mom == "BULLISH"
    ):

        signal = "BUY"
        side = "BUY"

        confidence = min(
            96,
            60 + buy_score * 5,
        )

        reasons = buy_reasons

    # Strong SELL
    elif (
        sell_score >= 6
        and bearish
        and mom == "BEARISH"
    ):

        signal = "SELL"
        side = "SELL"

        confidence = min(
            96,
            60 + sell_score * 5,
        )

        reasons = sell_reasons

    entry = None
    stop_loss = None
    tp1 = None
    tp2 = None
    rr = None

    if side != "NONE" and atr14:

        risk = max(
            atr14 * 1.25,
            price * 0.001,
        )

        entry = price

        if side == "BUY":

            stop_loss = price - risk
            tp1 = price + risk * 1.5
            tp2 = price + risk * 2.2

        else:

            stop_loss = price + risk
            tp1 = price - risk * 1.5
            tp2 = price - risk * 2.2

        rr = 1.5

    confirmations = [

        (
            "EMA 50/200 CONFIRMED"
            if bullish or bearish
            else "EMA 50/200 —"
        ),

        (
            "RSI 14 CONFIRMED"
            if (
                (
                    bullish
                    and rsi14 >= 50
                )
                or
                (
                    bearish
                    and rsi14 < 50
                )
            )
            else "RSI 14 —"
        ),

        (
            "BOS / CHoCH CONFIRMED"
            if structure["bos"]
            else "BOS / CHoCH —"
        ),

        (
            "Liquidity Grab CONFIRMED"
            if liquidity
            else "Liquidity Grab —"
        ),

        (
            "FVG CONFIRMED"
            if fvg
            else "FVG —"
        ),

        (
            "Momentum CONFIRMED"
            if mom != "NEUTRAL"
            else "Momentum —"
        ),
    ]

    return {
        "signal": signal,
        "side": side,
        "confidence": int(confidence),

        "reason": (
            " + ".join(reasons)
            if reasons
            else "No complete setup"
        ),

        "trend": (
            "BULLISH"
            if bullish
            else "BEARISH"
        ),

        "entry": (
            round(entry, 2)
            if entry else None
        ),

        "stop_loss": (
            round(stop_loss, 2)
            if stop_loss else None
        ),

        "tp1": (
            round(tp1, 2)
            if tp1 else None
        ),

        "tp2": (
            round(tp2, 2)
            if tp2 else None
        ),

        "rr": rr,

        "rsi": (
            round(rsi14, 2)
            if rsi14 is not None
            else None
        ),

        "atr": (
            round(atr14, 4)
            if atr14 is not None
            else None
        ),

        "ema50": round(ema50, 2),
        "ema200": round(ema200, 2),

        "smc": {
            "liquidity": liquidity,
            "liquidity_type": liquidity_type,
            "bos": structure["bos"],
            "choch": structure["choch"],
            "fvg": fvg,
            "fvg_type": fvg_type,
            "momentum": mom,
        },

        "confirmations": confirmations,
    }


# =========================================================
# API
# =========================================================

@app.get("/")
async def home():

    if INDEX.exists():

        return FileResponse(INDEX)

    return {
        "status": "online",
        "app": APP_TITLE,
    }


@app.get("/api/health")
async def health():

    return {
        "ok": True,
        "app": APP_TITLE,
        "history_source":
            "Binance WebSocket API",
        "live_source":
            "Binance WebSocket Stream",
        "websocket_connected":
            ws_connected,
        "last_ws_message":
            last_ws_message_ms,
        "last_error":
            last_error,
    }


@app.get("/api/sources")
async def sources():

    return {
        "sources": [
            {
                "id": "binance",
                "name": "Binance",
                "type": "crypto",
                "live": True,
                "status": (
                    "LIVE"
                    if ws_connected
                    else "CONNECTING"
                ),
            },
            {
                "id": "forex",
                "name": "Forex",
                "type": "forex",
                "live": False,
                "status": "NOT_CONNECTED",
            },
            {
                "id": "metals",
                "name": "Metals",
                "type": "metals",
                "live": False,
                "status": "NOT_CONNECTED",
            },
        ]
    }


@app.get("/api/instruments")
async def instruments():

    return {
        "crypto": SYMBOLS,

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


@app.get("/api/live-status")
async def live_status():

    history = {}

    for tf in TIMEFRAMES:

        history[tf] = len(
            bars[
                ("BTCUSDT", tf)
            ]
        )

    return {
        "source": "Binance",
        "live": ws_connected,
        "websocket": ws_connected,
        "history": history,
        "last_message_ms":
            last_ws_message_ms,
        "last_error":
            last_error,
    }


@app.get("/api/candles/{symbol}/{tf}")
async def get_candles(
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

        candles = list(
            bars[key]
        )

    return {
        "symbol": symbol,
        "timeframe": tf,
        "closed_only": True,
        "count": len(candles),
        "candles": candles[-500:],
    }


@app.get("/api/signal/{symbol}/{tf}")
async def get_signal(
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

        candles = list(
            bars[key]
        )

    result = analyze(candles)

    result.update(
        {
            "symbol": symbol,
            "timeframe": tf,
            "candle_count":
                len(candles),
            "generated_at":
                now_ms(),
            "data_source":
                "Binance LIVE",
            "auto_trade": False,
        }
    )

    return result


# =========================================================
# STARTUP
# =========================================================

@app.on_event("startup")
async def startup():

    global history_task
    global ws_task

    print("=" * 60)
    print(APP_TITLE)
    print("Starting Binance WebSocket history...")
    print("=" * 60)

    history_task = asyncio.create_task(
        load_initial_history()
    )

    await asyncio.sleep(1)

    ws_task = asyncio.create_task(
        live_stream()
    )


# =========================================================
# SHUTDOWN
# =========================================================

@app.on_event("shutdown")
async def shutdown():

    for task in (
        history_task,
        ws_task,
    ):

        if task:

            task.cancel()

    await asyncio.gather(
        *(
            task
            for task in (
                history_task,
                ws_task,
            )
            if task
        ),
        return_exceptions=True,
    )
