# -*- coding: utf-8 -*-
"""tv_weekly.py — Gavin's TradingView signals (ie_v20 "BUY ^ / SELL v" and ie_v22 APEX botFire / topFire) computed
on WEEKLY bars, the way his chart runs them (2026-09-24).

Why this exists: the 2026-09-16 full-system backtest ran the Pine symbol layer on DAILY bars and retired it (every
class NULL). Gavin's chart is WEEKLY (DECK screenshot, 2026-09-24: the bar countdown and the 200-bar average at the
200-week level). Re-run on weekly bars, the same code has a real edge (Investing/backtest/weekly_timing*.py):
  weekly BUY (v20 or v22), all tape          +1.46% vs the universe at 63 sessions [week-clustered CI +0.57,+2.45], up 67% (random 58%)
  weekly BUY & VIX >= 25                     +2.79%, up 79%
  weekly BUY & 30%+ drawdown & VIX >= 20     +3.77% [+1.74,+5.72], up 77%, +12.5% at 252 sessions
  weekly BUY & VIX < 20                      +0.72% ns, then -4.0% at 126 sessions (calm-tape BUYs fade)
  decay: entering 0/1/2/4 weeks after the fire +1.46 / +1.38 / +1.19 / +0.24%  -> a BUY is live for 2 completed weeks
  weekly topScore >= 5 & top regime          -1.81% [-3.32,-0.34] at 63 sessions (a trim read, not an exit)

This module is the SAME code the backtest graded: every function below the ruler is copied verbatim from
Investing/backtest/rules_timing.py (itself parity-checked against the vault's engine/timing/symbol_layer.py), and
tv_indicators.py is a verbatim copy of engine/timing/indicators.py. `parity_test()` in tv_weekly_parity.py re-runs
it on the backtest panel and must match the cached weekly signals exactly. Only two things are new here: the
weekly resample + market context (build_ctx / weekly_signals) and the summary block the V2 hubs read (tvw()).

Pure numpy/pandas: runs on the GitHub runner (relay/fetch_tail.py) with no vault imports.

CANONICAL COPY lives in the vault at Investing/engine/relay/tv_weekly.py; the deployed copy is relay/tv_weekly.py
in github.com/gavinbarnett2323-cmd/rmdb-prices. Keep them in sync.
"""
import numpy as np
import pandas as pd

try:
    import tv_indicators as ind          # relay/ (runner) and vault Investing/engine/relay/
except ImportError:                       # pragma: no cover
    from . import tv_indicators as ind

# distZ (distance from the 200-bar average, z-scored over 252 bars) is only defined once 200 + 252 bars exist. The
# vault port pads the warmup with zeros; this neutralises distZ (=0, "undefined") for the first DISTZ_WARMUP bars,
# which is what Pine's na does on a chart with less history. With >= 452 weekly bars of history the live bar is real.
DISTZ_WARMUP = 451

SECTORS = ("XLK", "XLY", "XLF", "XLI", "XLB", "XLE", "XLP", "XLU", "XLV", "XLC")
BENCH = {"SPY", "QQQ", "IWM", "DIA", "VOO", "RSP", "MDY", "VTI", "TLT", "IEF", "SHY", "HYG", "LQD", "GLD", "SLV", "USO",
         "XLK", "XLC", "XLE", "XLB", "XLV", "XLP", "XLU", "XLRE", "XLF", "XLI", "XLY", "SMH", "SOXX", "XBI", "IBB", "KRE",
         "XHB", "ITB", "XRT", "XOP", "OIH", "ARKK", "IWF", "IWD", "MTUM", "QUAL", "USMV", "VLUE",
         "BTC-USD", "ETH-USD", "SOL-USD", "^VIX", "^VIX3M", "^VVIX", "^TNX", "^IRX", "^FVX", "^TYX", "^GSPC", "^NDX", "^RUT", "^DJI"}


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
# VERBATIM from Investing/backtest/rules_timing.py (helpers, SYM_P, symbol_core, symbol_signals). Do not edit here:
# change the backtest port, re-grade, then re-copy.
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
SYM_P = dict(
    tierWatch=4, tierStandard=6, tierStrong=8, tierExcept=10, botFireThr=4, topFireThr=6,
    useConflictFilter=True, antiClusterBars=10, useTrigger=True, armWindow=5, useRegimeContext=True,
    useAdxQuiet=True, adxThr=30.0, divLookback=60, divWin=8, rsiOB=70, rsiOS=30, wkRsiOB=70, wkRsiOS=35,
    mtRsiOB=75, mtRsiOS=25, distZThr=2.0, pos52Low=20, botRegPos52=40, topRegPos52=60, botRegDistZ=-1.5,
    mmthFloor=30, mmthCeil=50, mmthWindow=10, vixCrushDrop=20.0, vixSpikeLevel=30.0, vixCompress=13.0,
    volSpikeMult=2.0, atrConsolMult=1.1, springActive=5, climaxVolZ=1.5, climaxRngZ=1.0, climaxClosePos=0.30,
    climaxDecay=12, clusterMin=2, singleCapitVolZ=2.0, churnBars=5, churnMin=4, churnRelVol=1.10, churnBody=0.40,
    churnHiTol=1.005, tdCount=9, tdActive=5, rsi2Thr=10, rsi14Thr=30, ftWindowLow=4, ftWindowHi=7, ftMinPct=1.7,
    ftAltPct=1.0, volRise=1.05, keyRevVolZ=1.0, wkKeyVolMult=1.2, defRotThr=3.0, lowryVolZ=1.5, lowryMin=2,
    lowryPos52=60, ppActive=5, concSpread=2.0, rvMult=1.8, rvFloor=15, stochDLo=30, stochDHi=70,
    rsiTurnLo=45, rsiTurnHi=55, i_huntBot=True, i_huntTop=True,
)


def recent(cond, win):
    """Pine 'bar_index - lastBar <= win' — True for win bars after (and on) each True (vault symbol_layer._recent)."""
    c = np.asarray(cond, dtype=bool); n = len(c)
    idx = np.where(c, np.arange(n), -1)
    last = np.maximum.accumulate(idx)
    return (last >= 0) & ((np.arange(n) - last) <= win)


def edge(sig):
    s = np.asarray(sig, dtype=bool)
    out = s.copy(); out[1:] &= ~s[:-1]
    return out


def shift(a, k=1, fill=np.nan):
    a = np.asarray(a, dtype=float); out = np.full(len(a), fill)
    if k > 0: out[k:] = a[:-k]
    elif k < 0: out[:k] = a[-k:]
    else: out = a.copy()
    return out


def crossover(a, b):
    a = np.asarray(a, float); b = np.asarray(b, float)
    with np.errstate(invalid="ignore"):
        return (a > b) & (shift(a) <= shift(b))


def crossunder(a, b):
    a = np.asarray(a, float); b = np.asarray(b, float)
    with np.errstate(invalid="ignore"):
        return (a < b) & (shift(a) >= shift(b))


def rsum(cond, n):
    """rolling sum over n bars with min_periods=n (NaN -> treated as 0 for the comparison)."""
    c = np.asarray(cond, dtype=float); cs = np.concatenate([[0.0], np.cumsum(c)])
    out = np.full(len(c), np.nan)
    if len(c) >= n:
        out[n - 1:] = cs[n:] - cs[:-n]
    return out


def climax_clusters(sell, buy, decay=12, cluster_min=2):
    """vault symbol_layer._climax_clusters (stateful decay counters)."""
    n = len(sell); cS = cB = cSd = cBd = 0
    cap = np.zeros(n, dtype=bool); dist = np.zeros(n, dtype=bool)
    for i in range(n):
        if sell[i]: cS += 1; cSd = decay
        if buy[i]: cB += 1; cBd = decay
        if cSd > 0: cSd -= 1
        else: cS = max(cS - 1, 0)
        if cBd > 0: cBd -= 1
        else: cB = max(cB - 1, 0)
        cap[i] = (cS >= cluster_min) or sell[i]
        dist[i] = (cB >= cluster_min) or buy[i]
    return cap, dist


def td9(close, count=9, active=5):
    """vault symbol_layer._td9 with the setup count parametrised (default 9)."""
    n = len(close); tdB = tdS = 0; lastB = lastS = None
    rb = np.zeros(n, dtype=bool); rs = np.zeros(n, dtype=bool)
    for i in range(n):
        if i >= 4 and close[i] < close[i - 4]:
            tdB += 1; tdS = 0
        elif i >= 4 and close[i] > close[i - 4]:
            tdS += 1; tdB = 0
        if tdB == count: lastB = i
        if tdS == count: lastS = i
        if lastB is not None and (i - lastB) <= active: rb[i] = True
        if lastS is not None and (i - lastS) <= active: rs[i] = True
    return rb, rs


def pocket_pivot(close, vol, ma50):
    n = len(close); out = np.zeros(n, dtype=bool)
    for i in range(11, n):
        maxDn = 0.0
        for j in range(1, 11):
            if close[i - j] < close[i - j + 1]:
                maxDn = max(maxDn, vol[i - j])
        if close[i] > close[i - 1] and (not np.isnan(ma50[i])) and close[i] > ma50[i] and maxDn > 0 and vol[i] > maxDn:
            out[i] = True
    return out


def fire_machine(botArmed, topArmed, botArmedCond, topArmedCond, trigBot, trigTop, antiCluster, useTrigger=True):
    """vault symbol_layer._fire (anti-cluster gap on each side)."""
    n = len(botArmed); bf = np.zeros(n, dtype=bool); tf = np.zeros(n, dtype=bool); lastB = lastT = None
    for i in range(n):
        gapB = lastB is None or (i - lastB) >= antiCluster
        gapT = lastT is None or (i - lastT) >= antiCluster
        if useTrigger:
            b = botArmed[i] and trigBot[i] and gapB; t = topArmed[i] and trigTop[i] and gapT
        else:
            b = botArmedCond[i] and gapB; t = topArmedCond[i] and gapT
        bf[i] = b; tf[i] = t
        if b: lastB = i
        if t: lastT = i
    return bf, tf


def S(a):
    return pd.Series(np.asarray(a, dtype=float))


def vw(cond, src, occ):
    return ind.valuewhen(pd.Series(cond), pd.Series(src), occ).to_numpy()


def symbol_core(o, h, l, c, v, dates, ctx):
    """o/h/l/c/v: float arrays of the symbol's VALID daily bars; dates: DatetimeIndex; ctx: dict of aligned arrays
    (mmth, vix, xlu, xlp, spy, rsp, sec_above). Returns dict of ndarrays (the vault's intermediate series)."""
    close, high, low, open_, vol = S(c), S(h), S(l), S(o), S(v)
    N = len(close); K = {}
    ma50 = ind.sma(close, 50); ma200 = ind.sma(close, 200)
    atr14 = ind.atr(high, low, close, 14)
    rsi = ind.rsi(close, 14); rsi2 = ind.rsi(close, 2)
    _, _, adxVal = ind.adx(high, low, close, 14, 14)
    _, _, macdHist = ind.macd(close, 12, 26, 9)
    volAvg20 = ind.sma(vol, 20)
    relVol = (vol / volAvg20).where(volAvg20 > 0, 1.0)
    volStdev = ind.stdev(vol, 50)
    volZ = ((vol - volAvg20) / volStdev).where(volStdev > 0, 0.0)
    rng = high - low; bodyAbs = (close - open_).abs()
    upperWick = high - pd.concat([close, open_], axis=1).max(axis=1)
    lowerWick = pd.concat([close, open_], axis=1).min(axis=1) - low
    distFrom200 = ((close - ma200) / ma200 * 100).where(ma200 > 0, 0.0)
    distMu = ind.sma(distFrom200, 252); distSd = ind.stdev(distFrom200, 252)
    distZ = ((distFrom200 - distMu) / distSd).where(distSd > 0, 0.0)
    # DEVIATION (documented): the vault z-scores distFrom200 against a 252-bar window that, for the first 451
    # bars, is padded with warmup zeros (ma200 undefined -> distFrom200 := 0). That is a warmup artefact, not a
    # signal; we neutralise distZ (=0, the vault's own "undefined" value) until the full 200+252 read exists.
    distZ.iloc[:DISTZ_WARMUP] = 0.0
    hi52 = ind.highest(high, 252); lo52 = ind.lowest(low, 252)
    pos52 = ((close - lo52) / (hi52 - lo52) * 100).where((hi52 - lo52) > 0, 50.0)

    # divergences (price pivots 5/5, values confirmed R bars late — vault verbatim)
    L, Rr = 5, 5
    pivH_p = ind.pivothigh(high, L, Rr); pivL_p = ind.pivotlow(low, L, Rr)
    hasH = pivH_p.notna().to_numpy(); hasL = pivL_p.notna().to_numpy()
    hR, rR, mR = high.shift(Rr).to_numpy(), rsi.shift(Rr).to_numpy(), macdHist.shift(Rr).to_numpy()
    lR = low.shift(Rr).to_numpy(); barR = pd.Series(np.arange(N, dtype=float)).shift(Rr).to_numpy()
    K.update(hasH=hasH, hasL=hasL,
             cPH_p=vw(hasH, hR, 0), pPH_p=vw(hasH, hR, 1), cPH_r=vw(hasH, rR, 0), pPH_r=vw(hasH, rR, 1),
             cPH_h=vw(hasH, mR, 0), pPH_h=vw(hasH, mR, 1),
             cPL_p=vw(hasL, lR, 0), pPL_p=vw(hasL, lR, 1), cPL_r=vw(hasL, rR, 0), pPL_r=vw(hasL, rR, 1),
             cPL_h=vw(hasL, mR, 0), pPL_h=vw(hasL, mR, 1),
             cH_bar=vw(hasH, barR, 0), pH_bar=vw(hasH, barR, 1), cL_bar=vw(hasL, barR, 0), pL_bar=vw(hasL, barR, 1))

    # Wyckoff swing levels / ATR consolidation
    K["swLo20"] = ind.lowest(low.shift(1), 20).to_numpy(); K["swHi20"] = ind.highest(high.shift(1), 20).to_numpy()
    K["currAtr"] = ind.sma(atr14, 5).to_numpy(); K["priorAtr"] = ind.sma(atr14, 20).shift(5).to_numpy()
    # range z / churn
    rngSd = ind.stdev(rng, 50)
    K["rngZ"] = ((rng - ind.sma(rng, 50)) / rngSd).where(rngSd > 0, 0.0).to_numpy()
    K["churnHi20Prev"] = ind.highest(high, 20).shift(1).to_numpy()
    # TD9 (default count 9; variants recompute)
    K["tdBuy9"], K["tdSell9"] = td9(c, 9, 5)
    # candles
    K["bullEngulf"] = ((close > open_) & (close.shift(1) < open_.shift(1)) & (close > open_.shift(1)) & (open_ <= close.shift(1)) & (rng > 0)).to_numpy()
    K["bearEngulf"] = ((close < open_) & (close.shift(1) > open_.shift(1)) & (close < open_.shift(1)) & (open_ >= close.shift(1)) & (rng > 0)).to_numpy()
    K["hammer"] = ((rng > 0) & (lowerWick >= bodyAbs * 2) & (upperWick <= bodyAbs * 0.5) & (close > open_)).to_numpy()
    K["morningStar"] = ((rng.shift(2) > 0) & (close.shift(2) < open_.shift(2)) & (bodyAbs.shift(1) <= rng.shift(1) * 0.30) & (close > open_) & (close > (open_.shift(2) + close.shift(2)) / 2)).to_numpy()
    # weekly key reversal + weekly/monthly RSI (symbol's own W-FRI / ME resample, as-of ffill broadcast)
    d = pd.DataFrame({"close": c, "high": h, "low": l, "volume": v}, index=dates)
    wkC = d["close"].resample("W-FRI").last(); wkH = d["high"].resample("W-FRI").max()
    wkL = d["low"].resample("W-FRI").min(); wkV = d["volume"].resample("W-FRI").sum()
    wkVolAvg20 = ind.sma(wkV.reset_index(drop=True), 20); wkVolAvg20.index = wkV.index
    K["wkVolRatio"] = (wkV / wkVolAvg20).reindex(dates, method="ffill").to_numpy()          # for the vol-mult variant
    wkBearShape = (wkC < wkC.shift(1)) & (wkH > wkH.shift(1)) & ((wkH - wkC) > (wkC - wkL))
    wkBullShape = (wkC > wkC.shift(1)) & (wkL < wkL.shift(1)) & ((wkC - wkL) > (wkH - wkC))
    K["wkBearShape"] = wkBearShape.reindex(dates, method="ffill").fillna(False).to_numpy().astype(bool)
    K["wkBullShape"] = wkBullShape.reindex(dates, method="ffill").fillna(False).to_numpy().astype(bool)
    wkRsi_w = ind.rsi(wkC.reset_index(drop=True), 14); wkRsi_w.index = wkC.index
    K["wkRsi"] = wkRsi_w.reindex(dates, method="ffill").to_numpy()
    mC = d["close"].resample("ME").last()
    mtRsi_m = ind.rsi(mC.reset_index(drop=True), 14); mtRsi_m.index = mC.index
    K["mtRsi"] = mtRsi_m.reindex(dates, method="ffill").fillna(50.0).to_numpy()
    # context feeds (already aligned to this symbol's bars)
    mmth = ctx.get("mmth"); vix = ctx.get("vix"); xlu = ctx.get("xlu"); xlp = ctx.get("xlp"); spy = ctx.get("spy")
    rsp = ctx.get("rsp"); sec = ctx.get("sec_above")
    if mmth is not None:
        mm = S(mmth); K["mmth"] = mm.to_numpy(); K["mmthChg20"] = (mm - mm.shift(20)).to_numpy()
        mmthEma = ind.ema(mm, 10); K["mmthEma"] = mmthEma.to_numpy(); K["mmthLow"] = ind.lowest(mmthEma, 10).to_numpy()
    if vix is not None:
        vx = S(vix); K["vix"] = vx.to_numpy()
        K["vixDrop5"] = ((vx.shift(5) - vx) / vx.shift(5) * 100).where(vx.shift(5) > 0, 0.0).to_numpy()
        K["vixHi10"] = ind.highest(vx, 10).to_numpy()
    if xlu is not None and xlp is not None and spy is not None:
        sp = S(spy)
        for nm, s_ in (("xluRoc", S(xlu)), ("xlpRoc", S(xlp))):
            r_ = (s_ / sp).where(sp > 0, 0.0)
            K[nm] = ((r_ - r_.shift(20)) / r_.shift(20) * 100).where(r_.shift(20) > 0, 0.0).to_numpy()
    if spy is not None and rsp is not None:
        sp, rp = S(spy), S(rsp)
        K["spyRoc20"] = ((sp - sp.shift(20)) / sp.shift(20) * 100).where(sp.shift(20) > 0, 0.0).to_numpy()
        K["rspRoc20"] = ((rp - rp.shift(20)) / rp.shift(20) * 100).where(rp.shift(20) > 0, 0.0).to_numpy()
    if sec is not None:
        secEma5 = ind.ema(S(sec) / 10.0, 5); K["secEma5"] = secEma5.to_numpy(); K["secEma5Lo7"] = ind.lowest(secEma5, 7).to_numpy()
    # follow-through helpers
    lowestRecent = ind.lowest(low, 10)
    K["lowBar"] = ind.barssince(low == lowestRecent).to_numpy()
    K["dChgPct"] = ((close - close.shift(1)) / close.shift(1) * 100).where(close.shift(1) > 0, 0.0).to_numpy()
    K["ppRaw"] = pocket_pivot(c, v, ma50.to_numpy())
    K["rv20"] = (ind.stdev(close / close.shift(1) - 1, 20) * np.sqrt(252) * 100).to_numpy()
    ma200Slope = ma200 - ma200.shift(20)
    K["inUptrend"] = ((close > ma200) & (ma200Slope > 0)).to_numpy(); K["inDowntrend"] = ((close < ma200) & (ma200Slope < 0)).to_numpy()
    stochRaw = ind.stoch(rsi, rsi, rsi, 14); stochK = ind.sma(stochRaw, 3); stochD = ind.sma(stochK, 3)
    K["stochK"] = stochK.to_numpy(); K["stochD"] = stochD.to_numpy()
    # raw series
    for nm, s_ in (("close", close), ("open", open_), ("high", high), ("low", low), ("vol", vol), ("ma50", ma50), ("ma200", ma200),
                   ("rsi", rsi), ("rsi2", rsi2), ("adx", adxVal), ("volAvg20", volAvg20), ("relVol", relVol), ("volZ", volZ),
                   ("rng", rng), ("bodyAbs", bodyAbs), ("distFrom200", distFrom200), ("distMu", distMu), ("distSd", distSd),
                   ("distZ", distZ), ("pos52", pos52)):
        K[nm] = s_.to_numpy(dtype=float)
    return K


def symbol_signals(K, p):
    """Components T1..T12 / B1..B12, scores, bonuses, gates, arm/trigger/fire for one parameter set (vault
    symbol_layer.compute verbatim from the core series). Returns dict of bool/int8 arrays."""
    with np.errstate(invalid="ignore"):
        close, open_, high, low, vol = K["close"], K["open"], K["high"], K["low"], K["vol"]
        N = len(close); rng = K["rng"]; volZ = K["volZ"]; rsi = K["rsi"]; ma50 = K["ma50"]
        Lb = p["divLookback"]
        inLbH = ((K["cH_bar"] - K["pH_bar"]) > 0) & ((K["cH_bar"] - K["pH_bar"]) <= Lb)
        inLbL = ((K["cL_bar"] - K["pL_bar"]) > 0) & ((K["cL_bar"] - K["pL_bar"]) <= Lb)
        bearDivRsi = K["hasH"] & inLbH & (K["cPH_p"] > K["pPH_p"]) & (K["cPH_r"] < K["pPH_r"]) & (K["pPH_r"] > p["rsiOB"])
        bullDivRsi = K["hasL"] & inLbL & (K["cPL_p"] < K["pPL_p"]) & (K["cPL_r"] > K["pPL_r"]) & (K["pPL_r"] < p["rsiOS"])
        bearDivMacd = K["hasH"] & inLbH & (K["cPH_p"] > K["pPH_p"]) & (K["cPH_h"] < K["pPH_h"]) & (K["pPH_h"] > 0)
        bullDivMacd = K["hasL"] & inLbL & (K["cPL_p"] < K["pPL_p"]) & (K["cPL_h"] > K["pPL_h"]) & (K["pPL_h"] < 0)
        T1 = recent(bearDivRsi, p["divWin"]); B1 = recent(bullDivRsi, p["divWin"])
        T2 = recent(bearDivMacd, p["divWin"]); B2 = recent(bullDivMacd, p["divWin"])
        # Wyckoff
        volSpikeOk = vol > K["volAvg20"] * p["volSpikeMult"]
        springRaw = (low < K["swLo20"]) & (close > K["swLo20"]) & volSpikeOk & (close > open_)
        upthrustRaw = (high > K["swHi20"]) & (close < K["swHi20"]) & volSpikeOk & (close < open_)
        inConsol = (K["currAtr"] < K["priorAtr"] * p["atrConsolMult"]) & (K["currAtr"] > 0)
        B7 = recent(springRaw & inConsol, p["springActive"]); T7 = recent(upthrustRaw & inConsol, p["springActive"])
        # climax clusters
        cp = p["climaxClosePos"]
        sellingClimax = (volZ > p["climaxVolZ"]) & (K["rngZ"] > p["climaxRngZ"]) & (close < open_) & ((close - low) < rng * cp)
        buyingClimax = (volZ > p["climaxVolZ"]) & (K["rngZ"] > p["climaxRngZ"]) & (close > open_) & ((high - close) < rng * cp)
        capitulationPhase, distributionPhase = climax_clusters(sellingClimax, buyingClimax, p["climaxDecay"], p["clusterMin"])
        churnBar = (K["relVol"] > p["churnRelVol"]) & (K["bodyAbs"] < rng * p["churnBody"])
        churnCount = rsum(churnBar, p["churnBars"])
        churningDistribution = (churnCount >= p["churnMin"]) & (close < K["churnHi20Prev"] * p["churnHiTol"]) & (close > ma50)
        # TD9
        if p["tdCount"] == 9 and p["tdActive"] == 5:
            tdB, tdS = K["tdBuy9"], K["tdSell9"]
        else:
            tdB, tdS = td9(close, p["tdCount"], p["tdActive"])
        # multi-TF RSI
        T3 = (K["wkRsi"] > p["wkRsiOB"]) | (K["mtRsi"] > p["mtRsiOB"]); B3 = (K["wkRsi"] < p["wkRsiOS"]) | (K["mtRsi"] < p["mtRsiOS"])
        T4 = K["distZ"] > p["distZThr"]; B4 = (K["distZ"] < -p["distZThr"]) | (K["pos52"] < p["pos52Low"])
        F_ = np.zeros(N, dtype=bool)
        if "mmth" in K:
            mm = K["mmth"]; priceChg20 = close - shift(close, 20)
            T5 = (mm > 0) & (priceChg20 > 0) & (K["mmthChg20"] < -2) & (mm < 65)
            B5 = (mm > 0) & (K["mmthLow"] < p["mmthFloor"]) & (K["mmthEma"] > p["mmthCeil"])
        else:
            T5 = F_; B5 = F_
        T6 = F_                                              # needs ^VIX9D (not in the panel) -> skipped
        if "vix" in K:
            vx = K["vix"]
            B6 = (vx > 0) & (K["vixHi10"] > p["vixSpikeLevel"]) & (K["vixDrop5"] > p["vixCrushDrop"]) & (vx < p["vixSpikeLevel"])
        else:
            B6 = F_
        T8 = churningDistribution | (distributionPhase & (close > ma50))
        close90Lo = (rng > 0) & ((close - low) <= rng * 0.10)
        B8 = capitulationPhase | ((volZ > p["singleCapitVolZ"]) & close90Lo & (close < shift(close)))
        T9 = tdS; B9 = tdB
        if "xluRoc" in K:
            T10 = (K["xluRoc"] > p["defRotThr"]) | (K["xlpRoc"] > p["defRotThr"])
        else:
            T10 = F_
        T11 = F_                                             # needs ^SKEW / put-call (not in the panel) -> skipped
        B10 = (K["rsi2"] < p["rsi2Thr"]) | ((rsi < p["rsi14Thr"]) & (close > open_) & (close > shift(close)))
        lowBar = K["lowBar"]; dChg = K["dChgPct"]; volRising = vol > shift(vol) * p["volRise"]
        B11 = ((lowBar >= p["ftWindowLow"]) & (lowBar <= p["ftWindowHi"]) & (dChg > p["ftMinPct"]) & volRising) | \
              ((lowBar <= 7) & (close > open_) & (close > shift(close)) & (dChg > p["ftAltPct"]))
        wkBearKey = K["wkBearShape"] & (K["wkVolRatio"] > p["wkKeyVolMult"]); wkBullKey = K["wkBullShape"] & (K["wkVolRatio"] > p["wkKeyVolMult"])
        T12 = wkBearKey | (K["bearEngulf"] & (volZ > p["keyRevVolZ"]) & (K["distFrom200"] > K["distMu"] + K["distSd"]))
        B12 = wkBullKey | ((K["bullEngulf"] | K["hammer"] | K["morningStar"]) & (volZ > p["keyRevVolZ"]) & (K["distFrom200"] < K["distMu"] - K["distSd"]))
        tops = [T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12]; bots = [B1, B2, B3, B4, B5, B6, B7, B8, B9, B10, B11, B12]
        topScore = sum(t.astype(np.int8) for t in tops); botScore = sum(b.astype(np.int8) for b in bots)
        # bonuses
        recentPP = recent(K["ppRaw"], p["ppActive"])
        day90Up = (rng > 0) & ((high - close) <= rng * 0.10) & (volZ > p["lowryVolZ"]) & (close > shift(close))
        day90Dn = (rng > 0) & ((close - low) <= rng * 0.10) & (volZ > p["lowryVolZ"]) & (close < shift(close))
        lowryThrustUp = rsum(day90Up, 5) >= p["lowryMin"]
        lowryClusterDn = (rsum(day90Dn, 5) >= p["lowryMin"]) & (K["pos52"] > p["lowryPos52"])
        fastThrust = (K["secEma5"] > 0.55) & (K["secEma5Lo7"] < 0.45) if "secEma5" in K else F_
        concBear = ((K["spyRoc20"] - K["rspRoc20"]) > p["concSpread"]) & (K["spyRoc20"] > 0) if "spyRoc20" in K else F_
        rv20 = K["rv20"]; volCtrlDelever = (rv20 > shift(rv20, 10) * p["rvMult"]) & (rv20 > p["rvFloor"])
        botBonus = recentPP.astype(np.int8) + lowryThrustUp.astype(np.int8) + fastThrust.astype(np.int8)
        topBonus = concBear.astype(np.int8) + volCtrlDelever.astype(np.int8) + lowryClusterDn.astype(np.int8)
        # gates
        pos52 = K["pos52"]; distZ = K["distZ"]
        topRegimeOk = (not p["useRegimeContext"]) | ((close > ma50) & (K["inUptrend"] | (pos52 > p["topRegPos52"])))
        botRegimeOk = (not p["useRegimeContext"]) | ((close < ma50) | K["inDowntrend"] | (pos52 < p["botRegPos52"]) | (distZ < p["botRegDistZ"]))
        adxQuiet = (not p["useAdxQuiet"]) | (K["adx"] < p["adxThr"])
        conflictOk = (not p["useConflictFilter"]) | (~((topScore > p["tierWatch"]) & (botScore > p["tierWatch"])))
        botArmedCond = bool(p["i_huntBot"]) & (botScore >= p["botFireThr"]) & botRegimeOk & adxQuiet & conflictOk
        topArmedCond = bool(p["i_huntTop"]) & (topScore >= p["topFireThr"]) & topRegimeOk & adxQuiet & conflictOk
        botArmed = recent(botArmedCond, p["armWindow"]); topArmed = recent(topArmedCond, p["armWindow"])
        sK, sD = K["stochK"], K["stochD"]; rsi1, rsi2_ = shift(rsi, 1), shift(rsi, 2)
        trigBotStoch = crossover(sK, sD) & (sD < p["stochDLo"]); trigTopStoch = crossunder(sK, sD) & (sD > p["stochDHi"])
        trigBotRsi = (rsi > rsi1) & (rsi1 <= rsi2_) & (rsi < p["rsiTurnLo"]); trigTopRsi = (rsi < rsi1) & (rsi1 >= rsi2_) & (rsi > p["rsiTurnHi"])
        trigBotPrice = close > shift(high); trigTopPrice = close < shift(low)
        trigBot = trigBotStoch | trigBotRsi | trigBotPrice; trigTop = trigTopStoch | trigTopRsi | trigTopPrice
        botFire, topFire = fire_machine(botArmed, topArmed, botArmedCond, topArmedCond, trigBot, trigTop, p["antiClusterBars"], p["useTrigger"])
        botArmEdge = edge(botArmed) & ~botFire; topArmEdge = edge(topArmed) & ~topFire
    out = dict(T1=T1, T2=T2, T3=T3, T4=T4, T5=T5, T7=T7, T8=T8, T9=T9, T10=T10, T12=T12,
               B1=B1, B2=B2, B3=B3, B4=B4, B5=B5, B6=B6, B7=B7, B8=B8, B9=B9, B10=B10, B11=B11, B12=B12,
               topScore=topScore, botScore=botScore, botBonus=botBonus, topBonus=topBonus,
               pocketPivot=recentPP, lowryThrustUp=lowryThrustUp, fastThrust=fastThrust, concBear=concBear,
               volCtrlDelever=volCtrlDelever, lowryClusterDn=lowryClusterDn,
               topRegimeOk=topRegimeOk, botRegimeOk=botRegimeOk, adxQuiet=adxQuiet, conflictOk=conflictOk,
               botArmedCond=botArmedCond, topArmedCond=topArmedCond, botArmed=botArmed, topArmed=topArmed,
               trigBotStoch=trigBotStoch, trigBotRsi=trigBotRsi, trigBotPrice=trigBotPrice,
               trigTopStoch=trigTopStoch, trigTopRsi=trigTopRsi, trigTopPrice=trigTopPrice,
               botFire=botFire, topFire=topFire, botArm=botArmEdge, topArm=topArmEdge,
               sellingClimax=sellingClimax, buyingClimax=buyingClimax, churningDistribution=churningDistribution,
               wkBearKey=wkBearKey, wkBullKey=wkBullKey, springRaw=springRaw & inConsol, upthrustRaw=upthrustRaw & inConsol)
    return out


# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
# NEW: market context on the daily calendar, the weekly resample, and the per-name summary block
# ═══════════════════════════════════════════════════════════════════════════════════════════════════════════
def sectors_above10(sector_closes):
    """rules_timing.market_layer's sector breadth, verbatim: % of the AVAILABLE sector ETFs above their own 50-bar
    SMA, divided by 10 (a 10-sector-equivalent count for the fastThrust bonus). XLC only exists from 2018-06."""
    secs = [S(s_).reset_index(drop=True) for s_ in sector_closes]
    above = pd.concat([(s_ > ind.sma(s_, 50)).astype(float).where(ind.sma(s_, 50).notna()) for s_ in secs], axis=1)
    breadth50pct = above.mean(axis=1).fillna(0.0) * 100.0
    return breadth50pct / 10.0


def build_ctx(close):
    """rules_timing.build_ctx on a (date x ticker) adjusted-close frame on the SPY session calendar. Returns a dict of
    daily float arrays: vix, spy, rsp, xlu, xlp, sec_above, mmth (% of the non-benchmark universe above its own
    200-session SMA: the MMTH proxy the backtest used)."""
    ctx = {}
    ctx["vix"] = close["^VIX"].ffill().values
    ctx["spy"] = close["SPY"].values
    ctx["rsp"] = close["RSP"].ffill().values
    ctx["xlu"] = close["XLU"].ffill().values
    ctx["xlp"] = close["XLP"].ffill().values
    ctx["sec_above"] = sectors_above10([close[s] if s in close.columns else pd.Series(np.nan, index=close.index) for s in SECTORS]).values
    uni = [t for t in close.columns if t not in BENCH]
    c = close[uni]
    ma = c.rolling(200, min_periods=200).mean()
    above = (c > ma).astype(float).where(ma.notna())
    ctx["mmth"] = (above.mean(axis=1) * 100.0).values
    return ctx


def weekly_frame(dates, o, h, l, c, v, min_daily=1300, min_weeks=230):
    """One name's valid daily bars -> W-FRI weekly bars (open first, high max, low min, close last, volume sum) plus `p`,
    the daily-calendar position of each week's last session (where the market context is sampled). Identical to
    Investing/backtest/weekly_timing2.run_one. None when history is too short (the backtest's own floors)."""
    c = np.asarray(c, dtype=float)
    valid = ~np.isnan(c)
    if valid.sum() < min_daily:
        return None
    pos = np.where(valid)[0]
    df = pd.DataFrame({"o": np.asarray(o, dtype=float)[pos], "h": np.asarray(h, dtype=float)[pos], "l": np.asarray(l, dtype=float)[pos],
                       "c": c[pos], "v": np.nan_to_num(np.asarray(v, dtype=float)[pos]), "p": pos}, index=pd.DatetimeIndex(dates)[pos])
    for k in ("o", "h", "l"):
        df[k] = df[k].where(df[k].notna(), df["c"])
    g = df.groupby(df.index.to_period("W-FRI"))
    W = pd.DataFrame({"o": g["o"].first(), "h": g["h"].max(), "l": g["l"].min(), "c": g["c"].last(), "v": g["v"].sum(), "p": g["p"].last()})
    if len(W) < min_weeks:
        return None
    return W


KEEP = ("botFire", "topFire", "botScore", "topScore", "botRegimeOk", "topRegimeOk", "adxQuiet", "conflictOk")


def weekly_signals(dates, o, h, l, c, v, ctx, min_daily=1300, min_weeks=230):
    """Run the v22 (arm + trigger) and v20 (no trigger) symbol layer on one name's weekly bars. Returns None or a dict:
    wpos (daily position of each weekly bar's last session), period (W-FRI labels), ok (200-week MA defined), c (weekly
    adjusted close), ma200 (200-week SMA), v22|<KEEP>, v20|botFire, v20|topFire."""
    dates = pd.DatetimeIndex(dates)
    W = weekly_frame(dates, o, h, l, c, v, min_daily, min_weeks)
    if W is None:
        return None
    wpos = W["p"].values.astype(int)
    wctx = {k: np.asarray(ctx[k])[wpos] for k in ctx}
    K = symbol_core(W["o"].values, W["h"].values, W["l"].values, W["c"].values, W["v"].values, pd.DatetimeIndex(dates[wpos]), wctx)
    s22 = symbol_signals(K, SYM_P)
    p20 = dict(SYM_P); p20["useTrigger"] = False
    s20 = symbol_signals(K, p20)
    out = {"wpos": wpos, "period": W.index, "ok": np.isfinite(K["ma200"]), "c": W["c"].values, "ma200": K["ma200"]}
    for k in KEEP:
        out["v22|" + k] = np.asarray(s22[k])
    out["v20|botFire"] = np.asarray(s20["botFire"], bool)
    out["v20|topFire"] = np.asarray(s20["topFire"], bool)
    return out


def _since(mask, last):
    """completed weeks since the most recent True at or before index `last` (0 = on `last`), or None."""
    idx = np.nonzero(np.asarray(mask[:last + 1], bool))[0]
    return (int(last - idx[-1]), int(idx[-1])) if len(idx) else (None, None)


def tvw(sig, dates, raw_close=None, complete_last=True, hist_weeks=26):
    """The per-name block the V2 hubs read, from weekly_signals(). Only COMPLETED weeks count: when the newest weekly
    bar is still forming (complete_last=False) it is dropped, so a Monday-Thursday run speaks of last Friday's bar.
      wk        the last completed weekly bar (its last session, ISO date)
      buy_ago   completed weeks since the latest v20-or-v22 bottom fire (0 = fired on `wk`); buy_wk / buy_px / buy_src
      bot, top  the bottom / top scores (0-12) on `wk`; top_reg / adx_q the top-regime and quiet-ADX gates on `wk`
      top5_ago  completed weeks since the latest week that ENTERED topScore >= 5 with the top regime (the trim read)
      sell_ago  completed weeks since the latest v20-or-v22 top fire (SELL v)
      c, ma200w the adjusted weekly close on `wk` and its 200-week average (the blue line on his chart)
      hb, ht    the last `hist_weeks` bottom / top scores as a hex-digit string (oldest first; '-' before the 200-week MA exists)"""
    if sig is None:
        return None
    dates = pd.DatetimeIndex(dates)
    n = len(sig["wpos"])
    last = n - 1 if complete_last else n - 2
    if last < 0 or not bool(sig["ok"][last]):
        return None
    ok = np.asarray(sig["ok"], bool)
    buy = (np.asarray(sig["v20|botFire"], bool) | np.asarray(sig["v22|botFire"], bool)) & ok
    sell = (np.asarray(sig["v20|topFire"], bool) | np.asarray(sig["v22|topFire"], bool)) & ok
    ts = np.asarray(sig["v22|topScore"]).astype(int); bs = np.asarray(sig["v22|botScore"]).astype(int)
    treg = np.asarray(sig["v22|topRegimeOk"], bool)
    t5 = (ts >= 5) & treg & ok
    t5_edge = t5 & ~np.r_[False, t5[:-1]]
    b_ago, b_i = _since(buy, last)
    s_ago, s_i = _since(sell, last)
    t_ago, t_i = _since(t5_edge, last)
    wd = lambda i: dates[sig["wpos"][i]].strftime("%Y-%m-%d")
    rc = (lambda i: (float(raw_close[sig["wpos"][i]]) if raw_close is not None and np.isfinite(raw_close[sig["wpos"][i]]) else None))
    src = None
    if b_i is not None:
        a, b = bool(sig["v20|botFire"][b_i]), bool(sig["v22|botFire"][b_i])
        src = "v20+v22" if (a and b) else ("v20" if a else "v22")
    lo = max(0, last - hist_weeks + 1)
    digit = lambda x, k: (format(min(int(x), 15), "x") if ok[k] else "-")
    out = {
        "wk": wd(last), "complete": bool(complete_last), "n_weeks": int(last + 1),
        "buy_ago": b_ago, "buy_wk": (wd(b_i) if b_i is not None else None), "buy_px": (rc(b_i) if b_i is not None else None), "buy_src": src,
        "bot": int(bs[last]), "top": int(ts[last]), "top_reg": bool(treg[last]), "adx_q": bool(np.asarray(sig["v22|adxQuiet"], bool)[last]),
        "top5_ago": t_ago, "top5_wk": (wd(t_i) if t_i is not None else None),
        "sell_ago": s_ago, "sell_wk": (wd(s_i) if s_i is not None else None),
        "c": float(sig["c"][last]), "ma200w": (float(sig["ma200"][last]) if np.isfinite(sig["ma200"][last]) else None),
        "hb": "".join(digit(bs[k], k) for k in range(lo, last + 1)),
        "ht": "".join(digit(ts[k], k) for k in range(lo, last + 1)),
    }
    return out


def week_complete(last_session, now_et):
    """Is the W-FRI week holding `last_session` finished as of `now_et` (a naive America/New_York datetime)? True once
    that week's Friday close has passed (17:00 ET or any later day). A holiday Friday is caught by the Friday run."""
    ls = pd.Timestamp(last_session)
    fri = (ls.to_period("W-FRI").end_time).normalize()
    now = pd.Timestamp(now_et)
    return bool(now.normalize() > fri or (now.normalize() == fri and now.hour >= 17))
