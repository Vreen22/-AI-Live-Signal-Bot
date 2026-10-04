# AI Live Signal Bot V17 — iPhone Mobile Bot

This version packages the previous bot as an iPhone/PWA-first app.

Included:
- Multi-source architecture and provider selector
- Crypto + Forex + Gold instrument registry
- Live feed heartbeat/status layer
- SMC-style confirmations: Liquidity, BOS/CHoCH, FVG, Retest, EMA, RSI, Momentum
- BUY / SELL / NO TRADE
- Confidence, Entry, SL, TP1, TP2
- Chart annotation payload
- iPhone PWA manifest + service worker
- AUTO TRADE OFF

Important:
- A provider is LIVE only after its adapter receives real market data.
- Credential-gated providers need server-side credentials.
- This project does not place trading orders.
