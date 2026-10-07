from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

BASE = Path(__file__).resolve().parent
INDEX = BASE / "index.html"

app = FastAPI(title="AI Live Signal Bot V25")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def home():
    return FileResponse(INDEX)

@app.get("/api/health")
async def health():
    return {
        "ok": True,
        "app": "AI Live Signal Bot V25",
        "architecture": "Browser -> Binance public market data",
        "auto_trade": False,
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
                "status": "BROWSER LIVE",
            }
        ]
    }

@app.get("/api/instruments")
async def instruments():
    return {
        "crypto": [
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
        ],
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
}
