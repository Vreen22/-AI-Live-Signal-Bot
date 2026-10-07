import asyncio
import csv
import io
import json
import time
import urllib.parse
import urllib.request
import urllib.error
import zipfile
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

import websockets
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse


# ============================================================
# AI LIVE SIGNAL BOT V22
# Binance Archive History + Binance Live WebSocket
# SIGNAL ONLY / AUTO TRADE OFF
# ============================================================

BASE = Path(__file__).resolve().parent
INDEX = BASE / "index.html"

app = FastAPI(title="AI Live Signal Bot V22")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# CONFIG
# ============================================================

DEFAULT_SOURCES = [
    "Binance",
    "Coinbase",
    "Kraken",
    "OKX",
    "Bybit",
    "KuCoin",
    "Gate.io",
    "Bitfinex",
    "Bitstamp",
    "Crypto.com",
    "OANDA",
    "FXCM",
    "IG",
    "Twelve Data",
    "Finnhub",
    "Alpaca",
    "Polygon/Massive",
    "Alpha Vantage",
    "Intrinio",
    "dxFeed",
    "Barchart",
    "EODHD",
]

SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "BNBUSDT",
    "XRPUSDT",
    "EURUSD",
    "GBPUSD",
    "USDJPY",
    "USDCHF",
    "AUDUSD",
    "USDCAD",
    "NZDUSD",
    "XAUUSD",
]

INTERVALS = [
    "1m",
    "5m",
    "15m",
    "30m",
    "1h",
    "2h",
    "4h",
]

CRYPTO_SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "BNBUSDT",
    "XRPUSDT",
]

# Only CLOSED candles are stored.
bars = {
    (symbol, tf): deque(maxlen=500)
    for symbol in SYMBOLS
    for tf in INTERVALS
}

latest = {}

feed_heartbeat = {
    "Binance": 0.0
}

background_tasks = []


# ============================================================
# LOAD OPTIONAL JSON FILES
# ============================================================

def load_json(name, fallback):
    try:
        p = BASE / name

        if p.exists():
            return json.loads(
                p.read_text(
                    encoding="utf-8"
                )
            )

    except Exception as exc:
        print(
            f"Could not read {name}: {exc}"
        )

    return fallback


PROVIDERS = load_json(
    "providers.json",
    {}
)

INSTRUMENTS = load_json(
    "instruments.json",
    {}
)


# ============================================================
# INDICATORS
# ============================================================

def ema(values, period):
    if len(values) < period:
        return None

    e = sum(
        values[:period]
    ) / period

    k = 2 / (period + 1)

    for x in values[period:]:
        e = (
            x * k
            + e * (1 - k)
        )

    return e


def rsi(values, period=14):
    if len(values) < period + 1:
        return None

    gains = []
    losses = []

    for a, b in zip(
        values[-period - 1:-1],
        values[-period:]
    ):
        d = b - a

        gains.append(
            max(d, 0.0)
        )

        losses.append(
            max(-d, 0.0)
        )

    avg_gain = (
        sum(gains) / period
    )

    avg_loss = (
        sum(losses) / period
    )

    if avg_loss == 0:
        return (
            100.0
            if avg_gain > 0
            else 50.0
        )

    rs = (
        avg_gain / avg_loss
    )

    return (
        100.0
        - (
            100.0
            / (1.0 + rs)
        )
    )


def atr(candles, period=14):
    if len(candles) < period + 1:
        return None

    tr = []

    for i in range(1, len(candles)):

        c = candles[i]
        p = candles[i - 1]

        tr.append(
            max(
                c["high"] - c["low"],
                abs(
                    c["high"]
                    - p["close"]
                ),
                abs(
                    c["low"]
                    - p["close"]
                ),
            )
        )

    return (
        sum(tr[-period:])
        / period
    )


# ============================================================
# SMC DETECTION
# ============================================================

def detect_liquidity(data):

    if len(data) < 11:
        return None

    last = data[-1]

    recent = data[-11:-1]

    prev_low = min(
        x["low"]
        for x in recent
    )

    prev_high = max(
        x["high"]
        for x in recent
    )

    # Bullish liquidity sweep
    if (
        last["low"] < prev_low
        and last["close"] > prev_low
    ):
        return "bullish"

    # Bearish liquidity sweep
    if (
        last["high"] > prev_high
        and last["close"] < prev_high
    ):
        return "bearish"

    return None


def detect_bos_choch(data):

    if len(data) < 5:
        return None

    last = data[-1]

    recent = data[-5:-1]

    swing_high = max(
        x["high"]
        for x in recent
    )

    swing_low = min(
        x["low"]
        for x in recent
    )

    if last["close"] > swing_high:
        return "bullish"

    if last["close"] < swing_low:
        return "bearish"

    return None


def detect_fvg(data):

    if len(data) < 3:
        return None

    c1 = data[-3]
    c2 = data[-2]
    c3 = data[-1]

    # Bullish FVG
    if c1["high"] < c3["low"]:
        return "bullish"

    # Bearish FVG
    if c1["low"] > c3["high"]:
        return "bearish"

    return None


def detect_momentum(last, prev):

    if (
        last["close"] > last["open"]
        and last["close"] > prev["close"]
    ):
        return "bullish"

    if (
        last["close"] < last["open"]
        and last["close"] < prev["close"]
    ):
        return "bearish"

    return None


def recent_swing_levels(
    data,
    lookback=40,
    pivot=2
):

    if len(data) < (
        pivot * 2 + 5
    ):
        return None, None

    window = (
        data[-lookback:]
        if len(data) > lookback
        else data
    )

    swing_highs = []
    swing_lows = []

    for i in range(
        pivot,
        len(window) - pivot
    ):

        h = window[i]["high"]
        l = window[i]["low"]

        left = window[
            i - pivot:i
        ]

        right = window[
            i + 1:i + pivot + 1
        ]

        if all(
            h > x["high"]
            for x in left + right
        ):
            swing_highs.append(h)

        if all(
            l < x["low"]
            for x in left + right
        ):
            swing_lows.append(l)

    return (
        swing_highs[-1]
        if swing_highs
        else None,

        swing_lows[-1]
        if swing_lows
        else None,
    )


# ============================================================
# ANALYSIS ENGINE
# ============================================================

def analyze(symbol, tf):

    data = list(
        bars.get(
            (symbol, tf),
            []
        )
    )

    if len(data) < 200:

        return {
            "symbol": symbol,
            "timeframe": tf,
            "signal": "NO TRADE",
            "signal_strength": "WAITING",
            "confidence": 0,
            "reason": (
                "Waiting for 200 "
                "closed candles "
                f"({len(data)}/200)"
            ),
            "candles": len(data),
            "source": "Binance",
            "timestamp": int(time.time()),
            "auto_trade": False,

            "entry": (
                data[-1]["close"]
                if data
                else None
            ),

            "sl": None,
            "tp1": None,
            "tp2": None,
            "price": (
                data[-1]["close"]
                if data
                else None
            ),

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

    closes = [
        x["close"]
        for x in data
    ]

    e9 = ema(
        closes,
        9
    )

    e21 = ema(
        closes,
        21
    )

    e50 = ema(
        closes,
        50
    )

    e200 = ema(
        closes,
        200
    )

    r = rsi(
        closes,
        14
    )

    a = atr(
        data,
        14
    )

    last = data[-1]
    prev = data[-2]

    liquidity = detect_liquidity(
        data
    )

    bos = detect_bos_choch(
        data
    )

    fvg = detect_fvg(
        data
    )

    momentum = detect_momentum(
        last,
        prev
    )

    buy_score = 0
    sell_score = 0

    confirmations = []

    # --------------------------------------------------------
    # EMA
    # --------------------------------------------------------

    if e50 > e200:

        ema_state = "BULLISH"

        buy_score += 2

        confirmations.append(
            "EMA 50 > EMA 200"
        )

    elif e50 < e200:

        ema_state = "BEARISH"

        sell_score += 2

        confirmations.append(
            "EMA 50 < EMA 200"
        )

    else:

        ema_state = "NEUTRAL"


    # Fast EMA bonus

    if e9 > e21:
        buy_score += 1

    elif e9 < e21:
        sell_score += 1


    # --------------------------------------------------------
    # RSI
    # --------------------------------------------------------

    if 55 <= r <= 75:

        rsi_state = "BULLISH"

        buy_score += 1

        confirmations.append(
            "RSI bullish"
        )

    elif 25 <= r <= 45:

        rsi_state = "BEARISH"

        sell_score += 1

        confirmations.append(
            "RSI bearish"
        )

    elif r > 75:

        rsi_state = (
            "BULLISH_OVERBOUGHT"
        )

        if e50 > e200:
            buy_score += 1

    elif r < 25:

        rsi_state = (
            "BEARISH_OVERSOLD"
        )

        if e50 < e200:
            sell_score += 1

    else:

        rsi_state = "NEUTRAL"


    # --------------------------------------------------------
    # BOS / CHoCH
    # --------------------------------------------------------

    if bos == "bullish":

        buy_score += 2

        confirmations.append(
            "BOS/CHoCH bullish"
        )

    elif bos == "bearish":

        sell_score += 2

        confirmations.append(
            "BOS/CHoCH bearish"
        )


    # --------------------------------------------------------
    # LIQUIDITY
    # --------------------------------------------------------

    if liquidity == "bullish":

        buy_score += 2

        confirmations.append(
            "Liquidity grab bullish"
        )

    elif liquidity == "bearish":

        sell_score += 2

        confirmations.append(
            "Liquidity grab bearish"
        )


    # --------------------------------------------------------
    # FVG
    # --------------------------------------------------------

    if fvg == "bullish":

        buy_score += 1

        confirmations.append(
            "FVG bullish"
        )

    elif fvg == "bearish":

        sell_score += 1

        confirmations.append(
            "FVG bearish"
        )


    # --------------------------------------------------------
    # MOMENTUM
    # --------------------------------------------------------

    if momentum == "bullish":

        buy_score += 1

        confirmations.append(
            "Momentum bullish"
        )

    elif momentum == "bearish":

        sell_score += 1

        confirmations.append(
            "Momentum bearish"
        )


    # ========================================================
    # STRICT TRADE RULE
    # ========================================================

    bullish_core = (
        e50 > e200
        and bos == "bullish"
        and fvg == "bullish"
        and momentum == "bullish"
        and rsi_state in (
            "BULLISH",
            "BULLISH_OVERBOUGHT",
        )
    )

    bearish_core = (
        e50 < e200
        and bos == "bearish"
        and fvg == "bearish"
        and momentum == "bearish"
        and rsi_state in (
            "BEARISH",
            "BEARISH_OVERSOLD",
        )
    )


    if (
        bullish_core
        and buy_score > sell_score
    ):

        signal = "BUY"

        score = buy_score

        core_count = 5

        signal_strength = (
            "STRONG"
            if liquidity == "bullish"
            else "CONFIRMED"
        )

    elif (
        bearish_core
        and sell_score > buy_score
    ):

        signal = "SELL"

        score = sell_score

        core_count = 5

        signal_strength = (
            "STRONG"
            if liquidity == "bearish"
            else "CONFIRMED"
        )

    else:

        signal = "NO TRADE"

        score = max(
            buy_score,
            sell_score
        )

        core_count = 0

        signal_strength = "WAIT"


    # ========================================================
    # CONFIDENCE
    # ========================================================

    if signal in (
        "BUY",
        "SELL"
    ):

        confidence = 80

        wanted_liq = (
            "bullish"
            if signal == "BUY"
            else "bearish"
        )

        if liquidity == wanted_liq:
            confidence += 8

        if (
            signal == "BUY"
            and rsi_state == "BULLISH"
        ):
            confidence += 4

        if (
            signal == "SELL"
            and rsi_state == "BEARISH"
        ):
            confidence += 4

        if (
            e9 > e21
            and signal == "BUY"
        ):
            confidence += 2

        if (
            e9 < e21
            and signal == "SELL"
        ):
            confidence += 2

        confidence = min(
            95,
            confidence
        )

    else:

        confidence = min(
            79,
            max(
                0,
                int(
                    45
                    + score * 5
                )
            )
        )


    # ========================================================
    # ENTRY / SL / TP
    # ========================================================

    entry = last["close"]

    sl = None
    tp1 = None
    tp2 = None

    risk = None

    rr_tp1 = None
    rr_tp2 = None

    swing_high, swing_low = (
        recent_swing_levels(
            data,
            lookback=40,
            pivot=2
        )
    )


    # BUY

    if (
        a
        and a > 0
        and signal == "BUY"
    ):

        atr_floor = (
            entry - 1.0 * a
        )

        if swing_low is not None:

            structure_sl = (
                swing_low
                - 0.25 * a
            )

        else:

            structure_sl = (
                atr_floor
            )

        sl = min(
            structure_sl,
            atr_floor
        )

        risk = (
            entry - sl
        )

        if risk > 0:

            tp1 = (
                entry + risk
            )

            tp2 = (
                entry
                + 2.0 * risk
            )

            rr_tp1 = 1.0
            rr_tp2 = 2.0


    # SELL

    elif (
        a
        and a > 0
        and signal == "SELL"
    ):

        atr_ceiling = (
            entry + 1.0 * a
        )

        if swing_high is not None:

            structure_sl = (
                swing_high
                + 0.25 * a
            )

        else:

            structure_sl = (
                atr_ceiling
            )

        sl = max(
            structure_sl,
            atr_ceiling
        )

        risk = (
            sl - entry
        )

        if risk > 0:

            tp1 = (
                entry - risk
            )

            tp2 = (
                entry
                - 2.0 * risk
            )

            rr_tp1 = 1.0
            rr_tp2 = 2.0


    # ========================================================
    # RESULT
    # ========================================================

    return {

        "symbol": symbol,

        "timeframe": tf,

        "signal": signal,

        "signal_strength":
            signal_strength,

        "confidence":
            confidence,

        "reason":
            (
                " + ".join(
                    confirmations[-6:]
                )
                if confirmations
                else
                "No sufficient confirmation"
            ),

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

        "swing_high":
            swing_high,

        "swing_low":
            swing_low,

        "risk": risk,

        "rr_tp1":
            rr_tp1,

        "rr_tp2":
            rr_tp2,

        "core_confirmations":
            core_count,

        "confirmations":
            confirmations,

        "smc": {

            "liquidity_grab":
                liquidity.upper()
                if liquidity
                else "NONE",

            "bos_choch":
                bos.upper()
                if bos
                else "NONE",

            "fvg":
                fvg.upper()
                if fvg
                else "NONE",

            "ema_50_200":
                ema_state,

            "rsi14":
                rsi_state,

            "momentum":
                momentum.upper()
                if momentum
                else "NEUTRAL",
        },

        "auto_trade": False,

        "source": "Binance",

        "candles":
            len(data),

        "timestamp":
            int(time.time()),
    }


# ============================================================
# BINANCE ARCHIVE
# ============================================================

ARCHIVE_BASE = (
    "https://data.binance.vision/"
    "data/spot/daily/klines"
)

USER_AGENT = (
    "Mozilla/5.0 "
    "AI-Live-Signal-Bot-V22"
)


def interval_minutes(interval):

    mapping = {
        "1m": 1,
        "5m": 5,
        "15m": 15,
        "30m": 30,
        "1h": 60,
        "2h": 120,
        "4h": 240,
    }

    return mapping.get(
        interval,
        1
    )


def archive_days_needed(
    interval,
    needed=300
):

    mins = interval_minutes(
        interval
    )

    candles_per_day = (
        1440 // mins
    )

    return (
        needed
        // candles_per_day
    ) + 2


def download_archive_day(
    symbol,
    interval,
    date_string
):

    filename = (
        f"{symbol}-{interval}-"
        f"{date_string}.zip"
    )

    url = (
        f"{ARCHIVE_BASE}/"
        f"{symbol}/"
        f"{interval}/"
        f"{filename}"
    )

    req = urllib.request.Request(
        url,
        headers={
            "User-Agent":
                USER_AGENT,
            "Accept":
                "*/*",
        },
    )

    try:

        with urllib.request.urlopen(
            req,
            timeout=30
        ) as response:

            content = (
                response.read()
            )

        return content

    except Exception as exc:

        print(
            f"Archive download failed "
            f"{filename}: {exc}"
        )

        return None


def parse_archive_zip(
    content
):

    if not content:
        return []

    candles = []

    try:

        with zipfile.ZipFile(
            io.BytesIO(content)
        ) as z:

            names = z.namelist()

            if not names:
                return []

            with z.open(
                names[0]
            ) as f:

                text = (
                    io.TextIOWrapper(
                        f,
                        encoding="utf-8"
                    )
                )

                reader = csv.reader(
                    text
                )

                for row in reader:

                    if not row:
                        continue

                    # Binance CSV can have header
                    if (
                        row[0]
                        .lower()
                        .startswith(
                            "open time"
                        )
                    ):
                        continue

                    if len(row) < 6:
                        continue

                    try:

                        candles.append({

                            "time":
                                int(row[0]),

                            "open":
                                float(row[1]),

                            "high":
                                float(row[2]),

                            "low":
                                float(row[3]),

                            "close":
                                float(row[4]),

                            "volume":
                                float(row[5]),
                        })

                    except Exception:
                        continue

    except Exception as exc:

        print(
            f"Archive parse error: {exc}"
        )

    return candles


async def load_archive_history(
    symbol,
    interval,
    needed=300
):

    days = archive_days_needed(
        interval,
        needed
    )

    today = datetime.now(
        timezone.utc
    ).date()

    all_candles = []

    print(
        f"Loading archive history "
        f"{symbol} {interval} "
        f"({days} days)"
    )

    for offset in range(
        days
    ):

        day = (
            today
            - timedelta(days=offset)
        )

        date_string = day.isoformat()

        content = (
            await asyncio.to_thread(
                download_archive_day,
                symbol,
                interval,
                date_string
            )
        )

        if content:

            data = (
                await asyncio.to_thread(
                    parse_archive_zip,
                    content
                )
            )

            all_candles.extend(
                data
            )

        # Small pause between archive files.
        await asyncio.sleep(
            0.15
        )

    # Remove duplicates.
    unique = {}

    for candle in all_candles:

        unique[
            candle["time"]
        ] = candle

    ordered = sorted(
        unique.values(),
        key=lambda x:
            x["time"]
    )

    # Remove currently open candle.
    now_ms = (
        int(time.time() * 1000)
    )

    minutes = interval_minutes(
        interval
    )

    interval_ms = (
        minutes
        * 60
        * 1000
    )

    closed = []

    for candle in ordered:

        candle_close_time = (
            candle["time"]
            + interval_ms
        )

        if candle_close_time <= now_ms:
            closed.append(
                candle
            )

    closed = closed[-500:]

    q = bars[
        (symbol, interval)
    ]

    q.clear()

    q.extend(
        closed
    )

    if q:

        latest[
            (symbol, interval)
        ] = analyze(
            symbol,
            interval
        )

    print(
        f"Archive loaded "
        f"{symbol} {interval}: "
        f"{len(closed)} closed candles"
    )

    return len(closed)


# ============================================================
# LOAD ONE HISTORY
# ============================================================

async def load_one_history(
    symbol,
    tf
):

    try:

        count = (
            await load_archive_history(
                symbol,
                tf,
                300
            )
        )

        if count > 0:
            return count

    except Exception as exc:

        print(
            f"Archive history error "
            f"{symbol} {tf}: {exc}"
        )


    print(
        f"WARNING: no history "
        f"loaded for {symbol} {tf}"
    )

    return 0


# ============================================================
# LOAD ALL HISTORY
# ============================================================

async def load_history():

    # First load BTC 1m.
    first = (
        await load_one_history(
            "BTCUSDT",
            "1m"
        )
    )

    if first == 0:

        print(
            "WARNING: BTCUSDT 1m "
            "history unavailable."
        )

    jobs = []

    for symbol in CRYPTO_SYMBOLS:

        for tf in INTERVALS:

            if (
                symbol == "BTCUSDT"
                and tf == "1m"
            ):
                continue

            jobs.append(
                (symbol, tf)
            )


    # Small batches.
    for i in range(
        0,
        len(jobs),
        2
    ):

        batch = [

            load_one_history(
                symbol,
                tf
            )

            for symbol, tf
            in jobs[i:i + 2]
        ]

        await asyncio.gather(
            *batch
        )

        await asyncio.sleep(
            0.5
        )


# ============================================================
# LIVE BINANCE WEBSOCKET
# ============================================================

async def binance_stream():

    streams = "/".join(

        f"{symbol.lower()}"
        f"@kline_{tf}"

        for symbol
        in CRYPTO_SYMBOLS

        for tf
        in INTERVALS
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

                print(
                    "Binance WebSocket connected"
                )

                feed_heartbeat[
                    "Binance"
                ] = time.time()


                async for raw in ws:

                    try:

                        msg = json.loads(
                            raw
                        )

                    except Exception:
                        continue


                    k = (
                        msg
                        .get("data", {})
                        .get("k")
                    )

                    if not k:
                        continue


                    feed_heartbeat[
                        "Binance"
                    ] = time.time()


                    symbol = k.get("s")
                    tf = k.get("i")


                    if not symbol or not tf:
                        continue


                    if (
                        symbol,
                        tf
                    ) not in bars:
                        continue


                    # VERY IMPORTANT:
                    # Only CLOSED candles.
                    if not k.get("x"):
                        continue


                    candle = {

                        "time":
                            int(k["t"]),

                        "open":
                            float(k["o"]),

                        "high":
                            float(k["h"]),

                        "low":
                            float(k["l"]),

                        "close":
                            float(k["c"]),

                        "volume":
                            float(k["v"]),
                    }


                    q = bars[
                        (symbol, tf)
                    ]


                    if (
                        q
                        and
                        q[-1]["time"]
                        == candle["time"]
                    ):

                        q[-1] = candle

                    else:

                        q.append(
                            candle
                        )


                    latest[
                        (symbol, tf)
                    ] = analyze(
                        symbol,
                        tf
                    )


        except Exception as exc:

            print(
                "Binance WebSocket "
                f"reconnect: {exc}"
            )

            feed_heartbeat[
                "Binance"
            ] = 0.0

            await asyncio.sleep(
                5
            )


# ============================================================
# STARTUP
# ============================================================

@app.on_event("startup")
async def startup():

    print(
        "Starting AI Live Signal Bot V22..."
    )

    # Live WebSocket immediately.
    background_tasks.append(
        asyncio.create_task(
            binance_stream()
        )
    )

    # Historical archive in background.
    background_tasks.append(
        asyncio.create_task(
            load_history()
        )
    )


# ============================================================
# SHUTDOWN
# ============================================================

@app.on_event("shutdown")
async def shutdown():

    for task in background_tasks:
        task.cancel()

    if background_tasks:

        await asyncio.gather(
            *background_tasks,
            return_exceptions=True
        )


# ============================================================
# HOME
# ============================================================

@app.get("/")
async def home():

    if INDEX.exists():

        return FileResponse(
            INDEX,
            media_type="text/html"
        )

    return JSONResponse({

        "status": "online",

        "service":
            "AI Live Signal Bot V22",

        "message":
            "index.html is missing."

    })


# ============================================================
# HEALTH
# ============================================================

@app.get("/api/health")
async def health():

    age = (

        time.time()
        - feed_heartbeat["Binance"]

        if feed_heartbeat["Binance"]
        else None
    )


    return {

        "status": "ok",

        "service":
            "AI Live Signal Bot V22",

        "binance":
            (
                "LIVE"
                if (
                    age is not None
                    and age < 30
                )
                else "WAITING"
            ),

        "seconds_since_feed":
            age,

        "auto_trade":
            False,

        "btc_1m_closed_candles":
            len(
                bars[
                    ("BTCUSDT", "1m")
                ]
            ),

        "timestamp":
            int(time.time())
    }


# ============================================================
# LIVE STATUS
# ============================================================

@app.get("/api/live-status")
async def live_status():

    age = (

        time.time()
        - feed_heartbeat["Binance"]

        if feed_heartbeat["Binance"]
        else None
    )


    return {

        "Binance": {

            "status":
                (
                    "LIVE"
                    if (
                        age is not None
                        and age < 30
                    )
                    else "WAITING"
                ),

            "seconds_since_data":
                age,

            "closed_candles":
                len(
                    bars[
                        ("BTCUSDT", "1m")
                    ]
                ),
        }
    }


# ============================================================
# SOURCES
# ============================================================

@app.get("/api/sources")
async def sources():

    names = DEFAULT_SOURCES


    if isinstance(
        PROVIDERS,
        list
    ):

        names = [

            p.get(
                "name",
                str(p)
            )

            if isinstance(
                p,
                dict
            )

            else str(p)

            for p in PROVIDERS
        ]


    elif isinstance(
        PROVIDERS,
        dict
    ):

        names = (
            list(
                PROVIDERS.keys()
            )
            or DEFAULT_SOURCES
        )


    return {

        "sources": [

            {

                "name": name,

                "status":

                    (
                        "LIVE"

                        if (
                            name
                            == "Binance"
                            and
                            feed_heartbeat[
                                "Binance"
                            ]
                        )

                        else
                        "API KEY / CONNECTOR REQUIRED"
                    )

            }

            for name in names
        ]
    }


# ============================================================
# INSTRUMENTS
# ============================================================

@app.get("/api/instruments")
async def instruments():

    return {

        "symbols":
            SYMBOLS,

        "timeframes":
            INTERVALS,

        "registry":
            INSTRUMENTS
    }


# ============================================================
# CANDLES
# ============================================================

@app.get(
    "/api/candles/{symbol}/{timeframe}"
)
async def candles(
    symbol: str,
    timeframe: str
):

    key = (
        symbol.upper(),
        timeframe
    )


    if key not in bars:

        return JSONResponse(

            {
                "error":
                    "Unsupported "
                    "symbol/timeframe"
            },

            status_code=400
        )


    return {

        "symbol":
            key[0],

        "timeframe":
            key[1],

        "candles":
            list(
                bars[key]
            )
    }


# ============================================================
# SIGNAL
# ============================================================

@app.get(
    "/api/signal/{symbol}/{timeframe}"
)
async def signal(
    symbol: str,
    timeframe: str
):

    key = (
        symbol.upper(),
        timeframe
    )


    if key not in bars:

        return JSONResponse(

            {
                "error":
                    "Unsupported "
                    "symbol/timeframe"
            },

            status_code=400
        )


    return latest.get(
        key,
        analyze(*key)
    )


# ============================================================
# MANUAL REFRESH
# ============================================================

@app.get(
    "/api/refresh/{symbol}/{timeframe}"
)
async def refresh(
    symbol: str,
    timeframe: str
):

    key = (
        symbol.upper(),
        timeframe
    )


    if key not in bars:

        return JSONResponse(

            {
                "error":
                    "Unsupported "
                    "symbol/timeframe"
            },

            status_code=400
        )


    count = await load_one_history(
        key[0],
        key[1]
    )


    return {

        "symbol":
            key[0],

        "timeframe":
            key[1],

        "closed_candles":
            count,

        "signal":
            latest.get(
                key,
                analyze(*key)
            )
    }


# ============================================================
# LEGACY ROUTES
# ============================================================

@app.get("/health")
async def legacy_health():
    return await health()


@app.get("/sources")
async def legacy_sources():
    return await sources()


@app.get(
    "/signal/{symbol}/{interval}"
)
async def legacy_signal(
    symbol: str,
    interval: str
):
    return await signal(
        symbol,
        interval
    )
