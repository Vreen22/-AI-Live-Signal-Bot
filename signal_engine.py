import math

def ema(vals, n):
    if not vals: return 0.0
    k=2/(n+1); e=float(vals[0])
    for x in vals[1:]: e=k*float(x)+(1-k)*e
    return e

def rsi(vals, n=14):
    if len(vals)<n+1: return 50.0
    gains=[]; losses=[]
    for i in range(1,len(vals)):
        d=vals[i]-vals[i-1]
        gains.append(max(d,0)); losses.append(max(-d,0))
    ag=sum(gains[-n:])/n; al=sum(losses[-n:])/n
    if al==0: return 100.0 if ag>0 else 50.0
    return 100-(100/(1+ag/al))

def atr(candles,n=14):
    if len(candles)<2: return 0.0
    trs=[]
    for i in range(1,len(candles)):
        c=candles[i]; p=candles[i-1]
        trs.append(max(c["high"]-c["low"],abs(c["high"]-p["close"]),abs(c["low"]-p["close"])))
    return sum(trs[-n:])/min(n,len(trs)) if trs else 0.0

def analyze(candles):
    if len(candles)<30:
        return {"signal":"NO TRADE","confidence":0,"reason":"Need more closed candles","confirmations":[]}
    c=candles[-1]; prev=candles[-2]
    closes=[x["close"] for x in candles]
    e9,e21,e50,e200=[ema(closes,n) for n in (9,21,50,200)]
    rr=rsi(closes,14); a=atr(candles,14)
    recent=candles[-7:-1]
    recent_hi=max(x["high"] for x in recent); recent_lo=min(x["low"] for x in recent)

    # Liquidity sweep/reclaim approximation.
    bull_sweep=c["low"]<recent_lo and c["close"]>recent_lo
    bear_sweep=c["high"]>recent_hi and c["close"]<recent_hi

    # BOS approximation using the recent structure range.
    bos_up=c["close"]>recent_hi
    bos_down=c["close"]<recent_lo

    # FVG approximation: current candle leaves a three-candle imbalance.
    fvg_up=len(candles)>=3 and c["low"]>candles[-3]["high"]
    fvg_down=len(candles)>=3 and c["high"]<candles[-3]["low"]

    # Retest approximation: price returns toward the last displacement midpoint.
    mid=(c["open"]+c["close"])/2
    retest_up=(c["close"]>=e21 and prev["low"]<=e21)
    retest_down=(c["close"]<=e21 and prev["high"]>=e21)

    ema_up=e9>e21>e50
    ema_down=e9<e21<e50
    rsi_up=rr>=52
    rsi_down=rr<=48
    momentum_up=c["close"]>c["open"] and c["close"]>prev["close"]
    momentum_down=c["close"]<c["open"] and c["close"]<prev["close"]

    bull = sum([bull_sweep,bos_up,fvg_up,retest_up,ema_up,rsi_up,momentum_up])
    bear = sum([bear_sweep,bos_down,fvg_down,retest_down,ema_down,rsi_down,momentum_down])

    # Require structure + trend/momentum confirmations, not a single indicator.
    if bull>=5 and ema_up and (rsi_up or momentum_up):
        signal="BUY"; score=bull
    elif bear>=5 and ema_down and (rsi_down or momentum_down):
        signal="SELL"; score=bear
    else:
        signal="NO TRADE"; score=max(bull,bear)

    confidence=min(95, 45 + score*7) if signal!="NO TRADE" else min(55,35+score*4)

    entry=c["close"]
    if a>0 and signal=="BUY":
        sl=entry-1.5*a; tp1=entry+2*a; tp2=entry+3*a
    elif a>0 and signal=="SELL":
        sl=entry+1.5*a; tp1=entry-2*a; tp2=entry-3*a
    else:
        sl=tp1=tp2=None

    confirmations=[
      {"name":"Liquidity Grab","bull":bull_sweep,"bear":bear_sweep},
      {"name":"BOS / CHoCH","bull":bos_up,"bear":bos_down},
      {"name":"FVG","bull":fvg_up,"bear":fvg_down},
      {"name":"FVG Retest / EMA21 Retest","bull":retest_up,"bear":retest_down},
      {"name":"EMA 9/21/50","bull":ema_up,"bear":ema_down},
      {"name":"RSI 14","bull":rsi_up,"bear":rsi_down},
      {"name":"Momentum / Price Action","bull":momentum_up,"bear":momentum_down},
    ]
    reason=" • ".join(x["name"] for x in confirmations if x["bull"] if signal=="BUY") if signal=="BUY" else (
        " • ".join(x["name"] for x in confirmations if x["bear"]) if signal=="SELL" else
        "Insufficient SMC confirmations"
    )
    return {
      "signal":signal,"confidence":confidence,"entry":entry,"sl":sl,"tp1":tp1,"tp2":tp2,
      "ema9":e9,"ema21":e21,"ema50":e50,"ema200":e200,"rsi":rr,"atr":a,
      "confirmations":confirmations,"bull_score":bull,"bear_score":bear,
      "reason":reason,"auto_trade":False
    }
