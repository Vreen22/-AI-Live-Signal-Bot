import asyncio, json, time
from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
import websockets
from signal_engine import analyze

app = FastAPI(title="AI Live Signal Bot V16")

@app.get("/api/live-status")
async def live_status():
    return {k: feed_status(k) for k in FEED_HEARTBEATS.keys()}


@app.get("/api/provider-health")
async def provider_health():
    return provider_health_snapshot()


@app.get("/api/instruments")
async def api_instruments():
    return json.loads(Path("instruments.json").read_text(encoding="utf-8"))

@app.get("/api/provider-status")
async def provider_status():
    import os
    return {
        "oanda": {"configured": bool(os.getenv("OANDA_API_TOKEN") and os.getenv("OANDA_ACCOUNT_ID"))},
        "fxcm": {"configured": bool(os.getenv("FXCM_ACCESS_TOKEN") and os.getenv("FXCM_CANDLE_URL"))},
        "ig": {"configured": bool(os.getenv("IG_ACCESS_TOKEN") and os.getenv("IG_ACCOUNT_ID"))}
    }
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


def norm(symbol, provider):
    if provider == "kraken":
        return symbol.replace("USDT", "/USD").replace("BTC/USD", "BTC/USD").replace("ETH/USD", "ETH/USD")
    if provider == "okx":
        return symbol.replace("USDT", "-USDT")
    if provider == "bybit":
        return symbol.replace("/", "").replace("-", "").upper()
    if provider == "kucoin":
        return symbol.replace("USDT", "-USDT")
    return symbol


def interval_minutes(interval):
    return {"1m": 1, "5m": 5, "15m": 15, "30m": 30, "1h": 60, "2h": 120, "4h": 240}.get(interval, 5)


async def reconnecting(url, handler):
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=2**20) as ws:
                async for item in handler(ws):
                    yield item
        except Exception:
            await asyncio.sleep(2)


async def binance(symbol, interval):
    url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@kline_{interval}"
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                async for raw in ws:
                    k = json.loads(raw)["k"]
                    yield {"timestamp": k["t"], "open": float(k["o"]), "high": float(k["h"]),
                           "low": float(k["l"]), "close": float(k["c"]), "volume": float(k["v"]),
                           "closed": bool(k["x"])}
        except Exception:
            await asyncio.sleep(2)


async def coinbase(symbol, interval):
    product = symbol.replace("USDT", "-USD")
    url = "wss://advanced-trade-ws.coinbase.com"
    minutes = interval_minutes(interval)
    bucket_size = minutes * 60
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                await ws.send(json.dumps({"type": "subscribe", "product_ids": [product], "channel": "market_trades"}))
                bucket = None
                cur = None
                async for raw in ws:
                    d = json.loads(raw)
                    if d.get("channel") != "market_trades":
                        continue
                    for ev in d.get("events", []):
                        for t in ev.get("trades", []):
                            p = float(t["price"]); v = float(t.get("size", 0)); now = int(time.time())
                            b = now // bucket_size * bucket_size
                            if bucket is not None and b != bucket and cur:
                                cur["closed"] = True
                                yield cur
                                cur = None
                            if cur is None:
                                bucket = b
                                cur = {"timestamp": b * 1000, "open": p, "high": p, "low": p, "close": p, "volume": v, "closed": False}
                            else:
                                cur["high"] = max(cur["high"], p); cur["low"] = min(cur["low"], p)
                                cur["close"] = p; cur["volume"] += v
        except Exception:
            await asyncio.sleep(2)


async def kraken(symbol, interval):
    # Kraken WebSocket v2 public OHLC. Public market data requires no API key.
    url = "wss://ws.kraken.com/v2"
    ksym = norm(symbol, "kraken")
    kint = {1: 1, 5: 5, 15: 15, 30: 30, 60: 60}.get(interval_minutes(interval), 5)
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=2**20) as ws:
                await ws.send(json.dumps({"method": "subscribe", "params": {"channel": "ohlc", "symbol": [ksym], "interval": kint, "snapshot": True}}))
                current = {}
                async for raw in ws:
                    d = json.loads(raw)
                    if d.get("channel") != "ohlc" or "data" not in d:
                        continue
                    for x in d["data"]:
                        ts = int(float(x["interval_begin"]) * 1000)
                        c = {"timestamp": ts, "open": float(x["open"]), "high": float(x["high"]),
                             "low": float(x["low"]), "close": float(x["close"]), "volume": float(x.get("volume", 0)), "closed": False}
                        prev = current.get("ts")
                        if prev is not None and ts > prev:
                            old = current["candle"].copy(); old["closed"] = True; yield old
                        current = {"ts": ts, "candle": c}
        except Exception:
            await asyncio.sleep(2)


async def okx(symbol, interval):
    url = "wss://ws.okx.com/ws/v5/business"
    inst = norm(symbol, "okx")
    ch = {"1m": "candle1m", "5m": "candle5m", "15m": "candle15m", "30m": "candle30m", "1h": "candle1H"}.get(interval, "candle5m")
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=2**20) as ws:
                await ws.send(json.dumps({"op": "subscribe", "args": [{"channel": ch, "instId": inst}]}))
                async for raw in ws:
                    d = json.loads(raw)
                    if d.get("arg", {}).get("channel") != ch or not d.get("data"):
                        continue
                    for x in d["data"]:
                        yield {"timestamp": int(x[0]), "open": float(x[1]), "high": float(x[2]), "low": float(x[3]),
                               "close": float(x[4]), "volume": float(x[5]), "closed": str(x[-1]) == "1"}
        except Exception:
            await asyncio.sleep(2)


async def bybit(symbol, interval):
    url = "wss://stream.bybit.com/v5/public/spot"
    bsym = norm(symbol, "bybit")
    bi = {"1m": "1", "5m": "5", "15m": "15", "30m": "30", "1h": "60"}.get(interval, "5")
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=2**20) as ws:
                await ws.send(json.dumps({"op": "subscribe", "args": [f"kline.{bi}.{bsym}"]}))
                async for raw in ws:
                    d = json.loads(raw)
                    if not d.get("topic", "").startswith("kline."):
                        continue
                    for x in d.get("data", []):
                        yield {"timestamp": int(x["start"]), "open": float(x["open"]), "high": float(x["high"]),
                               "low": float(x["low"]), "close": float(x["close"]), "volume": float(x["volume"]),
                               "closed": bool(x.get("confirm", False))}
        except Exception:
            await asyncio.sleep(2)


async def kucoin(symbol, interval):
    url = "wss://ws-api-spot.kucoin.com"
    ksym = norm(symbol, "kucoin")
    kt = {"1m": "1min", "5m": "5min", "15m": "15min", "30m": "30min", "1h": "1hour"}.get(interval, "5min")
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=2**20) as ws:
                await ws.send(json.dumps({"id": int(time.time()*1000), "type": "subscribe", "topic": f"/market/candles:{ksym}_{kt}", "response": True}))
                current = {}
                async for raw in ws:
                    d = json.loads(raw)
                    if d.get("type") != "message" or not d.get("data", {}).get("candles"):
                        continue
                    a = d["data"]["candles"]
                    ts = int(float(a[0]) * 1000)
                    c = {"timestamp": ts, "open": float(a[1]), "close": float(a[2]), "high": float(a[3]),
                         "low": float(a[4]), "volume": float(a[5]), "closed": False}
                    prev = current.get("ts")
                    if prev is not None and ts > prev:
                        old = current["candle"].copy(); old["closed"] = True; yield old
                    current = {"ts": ts, "candle": c}
        except Exception:
            await asyncio.sleep(2)


async def gateio(symbol, interval):
    url = "wss://api.gateio.ws/ws/v4/"
    gsym = symbol.replace("USDT", "_USDT").replace("/", "_").upper()
    gi = {"1m":"1m","5m":"5m","15m":"15m","30m":"30m","1h":"1h"}.get(interval, "5m")
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=2**20) as ws:
                await ws.send(json.dumps({"time": int(time.time()), "channel":"spot.candlesticks",
                                          "event":"subscribe", "payload":[gi, gsym]}))
                async for raw in ws:
                    d = json.loads(raw)
                    if d.get("channel") != "spot.candlesticks" or d.get("event") != "update":
                        continue
                    x = d.get("result", {})
                    if x:
                        yield {"timestamp":int(float(x["t"]))*1000, "open":float(x["o"]),
                               "high":float(x["h"]), "low":float(x["l"]), "close":float(x["c"]),
                               "volume":float(x.get("v",0)), "closed":False}
        except Exception:
            await asyncio.sleep(2)

async def bitfinex(symbol, interval):
    url = "wss://api-pub.bitfinex.com/ws/2"
    tf = {"1m":"1m","5m":"5m","15m":"15m","30m":"30m","1h":"1h"}.get(interval, "5m")
    bsym = symbol.replace("USDT","USD").replace("/","").upper()
    key = f"trade:{tf}:t{bsym}"
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=2**20) as ws:
                await ws.send(json.dumps({"event":"subscribe","channel":"candles","key":key}))
                cid = None; current_ts = None; current = None
                async for raw in ws:
                    d = json.loads(raw)
                    if isinstance(d, dict):
                        if d.get("event") == "subscribed" and d.get("channel") == "candles":
                            cid = d.get("chanId")
                        continue
                    if not isinstance(d, list) or len(d) < 2 or (cid is not None and d[0] != cid):
                        continue
                    payload = d[1]
                    rows = payload if isinstance(payload, list) and payload and isinstance(payload[0], list) else [payload]
                    for x in rows:
                        if not isinstance(x, list) or len(x) < 6: continue
                        ts = int(x[0])
                        c = {"timestamp":ts,"open":float(x[1]),"close":float(x[2]),
                             "high":float(x[3]),"low":float(x[4]),"volume":float(x[5]),"closed":False}
                        if current_ts is not None and ts > current_ts and current is not None:
                            old = current.copy(); old["closed"] = True; yield old
                        current_ts, current = ts, c
        except Exception:
            await asyncio.sleep(2)

async def bitstamp(symbol, interval):
    url = "wss://ws.bitstamp.net"
    pair = symbol.replace("USDT","USD").replace("/","").lower()
    bucket_size = interval_minutes(interval) * 60
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=2**20) as ws:
                await ws.send(json.dumps({"event":"bts:subscribe","data":{"channel":f"live_trades_{pair}"}}))
                bucket = None; cur = None
                async for raw in ws:
                    d = json.loads(raw)
                    if d.get("event") != "trade": continue
                    t = d.get("data", {})
                    try:
                        p = float(t["price"]); v = float(t.get("amount",0))
                        ts = int(float(t.get("microtimestamp", time.time()*1e6))/1_000_000)
                    except Exception:
                        continue
                    b = ts // bucket_size * bucket_size
                    if bucket is not None and b != bucket and cur:
                        old = cur.copy(); old["closed"] = True; yield old; cur = None
                    if cur is None:
                        bucket = b
                        cur = {"timestamp":b*1000,"open":p,"high":p,"low":p,"close":p,"volume":v,"closed":False}
                    else:
                        cur["high"] = max(cur["high"],p); cur["low"] = min(cur["low"],p)
                        cur["close"] = p; cur["volume"] += v
        except Exception:
            await asyncio.sleep(2)

async def crypto_com(symbol, interval):
    url = "wss://stream.crypto.com/exchange/v1/market"
    inst = symbol.replace("/","_").upper()
    tf = {"1m":"1m","5m":"5m","15m":"15m","30m":"30m","1h":"1h"}.get(interval, "5m")
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=2**20) as ws:
                now = int(time.time()*1000)
                await ws.send(json.dumps({"id":now,"method":"subscribe",
                                          "params":{"channels":[f"candlestick.{tf}.{inst}"]},
                                          "nonce":now}))
                async for raw in ws:
                    d = json.loads(raw)
                    if d.get("method") != "subscribe": continue
                    for x in d.get("result",{}).get("data",[]):
                        yield {"timestamp":int(x["t"]),"open":float(x["o"]),"high":float(x["h"]),
                               "low":float(x["l"]),"close":float(x["c"]),
                               "volume":float(x.get("v",0)),"closed":False}
        except Exception:
            await asyncio.sleep(2)



def validate_market_selection(provider, symbol):
    crypto = {"BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT","XRPUSDT"}
    forex = {"EURUSD","GBPUSD","USDJPY","USDCHF","AUDUSD","USDCAD","NZDUSD"}
    metals = {"XAUUSD"}
    if provider in {"binance","coinbase","kraken","okx","bybit","kucoin","gateio","bitfinex","bitstamp","crypto_com"}:
        if symbol not in crypto:
            raise ValueError(f"{provider} supports configured crypto symbols only")
    elif provider in {"oanda","fxcm","ig"}:
        if symbol not in forex | metals:
            raise ValueError(f"{provider} supports configured FX/metal symbols only")
    return True

# Canonical symbols -> provider-specific symbols.
SYMBOL_MAP = {
    "oanda": {"EURUSD":"EUR_USD","GBPUSD":"GBP_USD","USDJPY":"USD_JPY","USDCHF":"USD_CHF",
              "AUDUSD":"AUD_USD","USDCAD":"USD_CAD","NZDUSD":"NZD_USD","XAUUSD":"XAU_USD"},
    "fxcm": {"EURUSD":"EUR/USD","GBPUSD":"GBP/USD","USDJPY":"USD/JPY","USDCHF":"USD/CHF",
             "AUDUSD":"AUD/USD","USDCAD":"USD/CAD","NZDUSD":"NZD/USD","XAUUSD":"XAU/USD"},
    "ig": {"EURUSD":"CS.D.EURUSD.CFD.IP","GBPUSD":"CS.D.GBPUSD.CFD.IP","USDJPY":"CS.D.USDJPY.CFD.IP",
           "USDCHF":"CS.D.USDCHF.CFD.IP","AUDUSD":"CS.D.AUDUSD.CFD.IP","USDCAD":"CS.D.USDCAD.CFD.IP",
           "NZDUSD":"CS.D.NZDUSD.CFD.IP","XAUUSD":"CS.D.XAUUSD.CFD.IP"}
}

def map_symbol(provider, canonical):
    return SYMBOL_MAP.get(provider, {}).get(canonical, canonical)


def build_chart_overlay(signal, candles):
    """Return chart-friendly annotations derived from the same closed-candle signal."""
    if not candles:
        return {"markers":[],"levels":[],"zones":[]}
    last=candles[-1]
    conf=signal.get("confirmations",[])
    def ok(name, side):
        return any(x.get("name")==name and x.get(side) for x in conf)
    markers=[]
    if signal.get("signal") in {"BUY","SELL"}:
        markers.append({
            "type":"signal_arrow",
            "side":signal["signal"],
            "timestamp":last.get("timestamp"),
            "price":last.get("close")
        })
    if ok("BOS / CHoCH","bull") or ok("BOS / CHoCH","bear"):
        markers.append({"type":"bos","timestamp":last.get("timestamp"),"price":last.get("close")})
    levels=[]
    for k,label in [("entry","Entry"),("sl","SL"),("tp1","TP1"),("tp2","TP2")]:
        if signal.get(k) is not None:
            levels.append({"label":label,"price":signal[k]})
    zones=[]
    if ok("FVG","bull") or ok("FVG","bear"):
        zones.append({"type":"FVG","side":"BUY" if ok("FVG","bull") else "SELL"})
    if ok("Liquidity Grab","bull") or ok("Liquidity Grab","bear"):
        zones.append({"type":"Liquidity","side":"BUY" if ok("Liquidity Grab","bull") else "SELL"})
    return {"markers":markers,"levels":levels,"zones":zones}



# ---- V16 real-feed heartbeat registry ----
FEED_HEARTBEATS = {}

def mark_live(provider, symbol=None, interval=None):
    FEED_HEARTBEATS[provider] = {
        "live": True,
        "last_data_ms": int(time.time()*1000),
        "symbol": symbol,
        "interval": interval
    }

def feed_status(provider):
    x=FEED_HEARTBEATS.get(provider)
    if not x:
        return {"live":False,"last_data_ms":None,"age_seconds":None}
    age=max(0,(time.time()*1000-x["last_data_ms"])/1000)
    return {**x,"age_seconds":round(age,1),"live":age<=20}

# ---- V16 provider health / configuration status ----
def provider_health_snapshot():
    import os
    def cfg(*names): return all(os.getenv(n) for n in names)
    return {
        "twelvedata":{"configured":cfg("TWELVE_DATA_API_KEY"),"mode":"websocket","key_env":"TWELVE_DATA_API_KEY"},
        "finnhub":{"configured":cfg("FINNHUB_API_KEY"),"mode":"websocket","key_env":"FINNHUB_API_KEY"},
        "alpaca":{"configured":cfg("ALPACA_API_KEY","ALPACA_SECRET_KEY"),"mode":"websocket","key_env":"ALPACA_API_KEY + ALPACA_SECRET_KEY"},
        "polygon":{"configured":cfg("POLYGON_API_KEY"),"mode":"websocket","key_env":"POLYGON_API_KEY"},
        "alphavantage":{"configured":cfg("ALPHAVANTAGE_API_KEY"),"mode":"api","key_env":"ALPHAVANTAGE_API_KEY"},
        "intrinio":{"configured":cfg("INTRINIO_API_KEY"),"mode":"credentialed","key_env":"INTRINIO_API_KEY"},
        "dxfeed":{"configured":cfg("DXFEED_USERNAME","DXFEED_PASSWORD"),"mode":"credentialed","key_env":"DXFEED_USERNAME + DXFEED_PASSWORD"},
        "barchart":{"configured":cfg("BARCHART_API_KEY"),"mode":"credentialed","key_env":"BARCHART_API_KEY"},
        "eodhd":{"configured":cfg("EODHD_API_TOKEN"),"mode":"websocket","key_env":"EODHD_API_TOKEN"},
    }

# ---- Broker adapters (credentials stay server-side via environment variables) ----
async def oanda(symbol, interval):
    import os, aiohttp
    token = os.getenv("OANDA_API_TOKEN")
    account = os.getenv("OANDA_ACCOUNT_ID")
    if not token or not account:
        raise RuntimeError("OANDA requires OANDA_API_TOKEN and OANDA_ACCOUNT_ID")
    instrument = map_symbol("oanda", symbol).upper()
    gran = {"1m":"M1","5m":"M5","15m":"M15","30m":"M30","1h":"H1"}.get(interval, "M5")
    base = os.getenv("OANDA_API_BASE", "https://api-fxpractice.oanda.com")
    url = f"{base}/v3/accounts/{account}/instruments/{instrument}/candles"
    headers = {"Authorization": f"Bearer {token}"}
    last = None
    async with aiohttp.ClientSession(headers=headers) as s:
        while True:
            try:
                async with s.get(url, params={"granularity":gran, "count":100, "price":"M"}) as r:
                    if r.status != 200:
                        raise RuntimeError(f"OANDA HTTP {r.status}: {await r.text()}")
                    data = await r.json()
                    for c in data.get("candles", []):
                        if not c.get("complete"): continue
                        mid = c.get("mid", {})
                        ts = int(__import__("datetime").datetime.fromisoformat(c["time"].replace("Z","+00:00")).timestamp()*1000)
                        if ts == last: continue
                        last = ts
                        yield {"timestamp":ts,"open":float(mid["o"]),"high":float(mid["h"]),
                               "low":float(mid["l"]),"close":float(mid["c"]),
                               "volume":float(c.get("volume",0)),"closed":True}
                await asyncio.sleep(2)
            except Exception:
                await asyncio.sleep(3)


async def fxcm(symbol, interval):
    import os, aiohttp
    token = os.getenv("FXCM_ACCESS_TOKEN")
    if not token:
        raise RuntimeError("FXCM requires FXCM_ACCESS_TOKEN")
    # FXCM's supported API products are authenticated. This adapter uses the REST-style
    # price/candle gateway configured by FXCM accounts that expose the endpoint.
    url = os.getenv("FXCM_CANDLE_URL")
    if not url:
        raise RuntimeError("Set FXCM_CANDLE_URL for your FXCM API gateway")
    pair = map_symbol("fxcm", symbol)
    period = {"1m":"m1","5m":"m5","15m":"m15","30m":"m30","1h":"H1"}.get(interval,"m5")
    last = None
    async with aiohttp.ClientSession(headers={"Authorization":f"Bearer {token}"}) as s:
        while True:
            try:
                async with s.get(url, params={"symbol":pair,"period":period,"count":100}) as r:
                    if r.status != 200: raise RuntimeError(f"FXCM HTTP {r.status}")
                    data = await r.json()
                    rows = data.get("candles", data if isinstance(data,list) else [])
                    for c in rows:
                        ts = int(c.get("timestamp", c.get("time",0)))
                        if ts < 10**12: ts *= 1000
                        if ts == last: continue
                        last = ts
                        yield {"timestamp":ts,"open":float(c["open"]),"high":float(c["high"]),
                               "low":float(c["low"]),"close":float(c["close"]),
                               "volume":float(c.get("volume",0)),"closed":bool(c.get("complete",True))}
                await asyncio.sleep(2)
            except Exception:
                await asyncio.sleep(3)


async def ig(symbol, interval):
    import os, aiohttp
    token = os.getenv("IG_ACCESS_TOKEN")
    account = os.getenv("IG_ACCOUNT_ID")
    epic = os.getenv("IG_EPIC") or map_symbol("ig", symbol)
    if not token or not account or not epic:
        raise RuntimeError("IG requires IG_ACCESS_TOKEN, IG_ACCOUNT_ID and IG_EPIC")
    base = os.getenv("IG_API_BASE", "https://demo-api.ig.com/gateway/deal")
    tf = {"1m":"MINUTE","5m":"MINUTE_5","15m":"MINUTE_15","30m":"MINUTE_30","1h":"HOUR"}.get(interval,"MINUTE_5")
    url = f"{base}/prices/{epic}"
    headers = {"X-IG-API-KEY":os.getenv("IG_API_KEY",""), "CST":token, "X-SECURITY-TOKEN":os.getenv("IG_SECURITY_TOKEN",""), "Version":"3"}
    last = None
    async with aiohttp.ClientSession(headers=headers) as s:
        while True:
            try:
                async with s.get(url, params={"resolution":tf,"max":100}) as r:
                    if r.status != 200: raise RuntimeError(f"IG HTTP {r.status}")
                    data = await r.json()
                    for p in data.get("prices",[]):
                        ts = int(__import__("datetime").datetime.fromisoformat(p["snapshotTimeUTC"].replace("Z","+00:00")).timestamp()*1000)
                        if ts == last: continue
                        last = ts
                        def val(side, field):
                            x = p.get(side,{}).get(field)
                            return float(x) if x is not None else 0.0
                        yield {"timestamp":ts,"open":val("openPrice","bid"),"high":val("highPrice","bid"),
                               "low":val("lowPrice","bid"),"close":val("closePrice","bid"),
                               "volume":float(p.get("lastTradedVolume",0)),"closed":True}
                await asyncio.sleep(2)
            except Exception:
                await asyncio.sleep(3)




async def tracked_adapter(provider, fn, symbol, interval):
    async for candle in fn(symbol, interval):
        mark_live(provider, symbol, interval)
        yield candle

ADAPTERS = {"binance": lambda symbol, interval, _p="binance", _f=binance: tracked_adapter(_p, _f, symbol, interval), "coinbase": lambda symbol, interval, _p="coinbase", _f=coinbase: tracked_adapter(_p, _f, symbol, interval), "kraken": lambda symbol, interval, _p="kraken", _f=kraken: tracked_adapter(_p, _f, symbol, interval), "okx": lambda symbol, interval, _p="okx", _f=okx: tracked_adapter(_p, _f, symbol, interval), "bybit": lambda symbol, interval, _p="bybit", _f=bybit: tracked_adapter(_p, _f, symbol, interval), "kucoin": lambda symbol, interval, _p="kucoin", _f=kucoin: tracked_adapter(_p, _f, symbol, interval), "gateio": lambda symbol, interval, _p="gateio", _f=gateio: tracked_adapter(_p, _f, symbol, interval), "bitfinex": lambda symbol, interval, _p="bitfinex", _f=bitfinex: tracked_adapter(_p, _f, symbol, interval), "bitstamp": lambda symbol, interval, _p="bitstamp", _f=bitstamp: tracked_adapter(_p, _f, symbol, interval), "crypto_com": lambda symbol, interval, _p="crypto_com", _f=crypto_com: tracked_adapter(_p, _f, symbol, interval), "oanda": lambda symbol, interval, _p="oanda", _f=oanda: tracked_adapter(_p, _f, symbol, interval), "fxcm": lambda symbol, interval, _p="fxcm", _f=fxcm: tracked_adapter(_p, _f, symbol, interval), "ig": lambda symbol, interval, _p="ig", _f=ig: tracked_adapter(_p, _f, symbol, interval)}


@app.get("/api/providers")
def get_providers():
    return json.load(open("providers.json", "r", encoding="utf-8"))


@app.get("/api/health")
def health():
    return {"ok": True, "version": "V16", "auto_trade": False, "live_adapters": list(ADAPTERS.keys())}


@app.websocket("/ws/live")
async def live(ws: WebSocket):
    validate_market_selection(provider, symbol)

    await ws.accept()
    provider = ws.query_params.get("provider", "binance")
    symbol = ws.query_params.get("symbol", "BTCUSDT")
    interval = ws.query_params.get("interval", "5m")
    await ws.send_json({"type": "status", "provider": provider, "symbol": symbol, "interval": interval, "auto_trade": False})
    if provider not in ADAPTERS:
        await ws.send_json({"type": "adapter", "status": "not_activated", "provider": provider,
                            "message": "Provider is registered, but its live adapter is not activated in V16."})
        await ws.close(); return
    await ws.send_json({"type": "adapter", "status": "live", "provider": provider})
    candles = []
    stream = ADAPTERS[provider](symbol, interval)
    async for c in stream:
        if c["closed"]:
            # Avoid duplicate closed candles from providers that can repeat the same update.
            if candles and candles[-1]["timestamp"] == c["timestamp"]:
                candles[-1] = c
            else:
                candles.append(c)
            candles = candles[-300:]
            await ws.send_json({"type": "analysis", "candle": c, "result": analyze(candles)})
        else:
            await ws.send_json({"type": "live_price", "price": c["close"]})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
