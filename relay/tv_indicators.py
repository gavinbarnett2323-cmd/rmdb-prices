# -*- coding: utf-8 -*-
"""indicators.py — Timing Engine PHASE 2: Pine-parity primitive library.

Exact-semantics ports of the TradingView Pine `ta.*` / `math.*` functions the APEX v22 + RS Dual-TF v2
scripts use (see Investing/Pine Source/). Everything operates on pandas Series indexed by bar and returns a
Series aligned to the input (NaN where Pine returns `na`). Stateful primitives (pivots, valuewhen, barssince)
use an explicit sequential bar loop — per the build plan we do NOT vectorize state; correctness beats speed at
~60 tickers/day, and a bar loop is diffable against the .pine.

── PARITY DECISIONS (the documented divergence traps — Phase-5 golden-diff verifies these vs a TradingView export) ──
  • Wilder smoothing (rma):   ta.rma seeds with SMA(n) at bar n-1, then x_t = (x_{t-1}*(n-1) + s_t)/n.
                              rsi/atr/adx all build on THIS (the #1 source of silent divergence — plan §4).
  • ema:                      seeded with SMA(n) at bar n-1, then alpha = 2/(n+1). [Pine ta.ema seeding is the
                              classic ambiguity; this is the common Pine-parity convention — FLAGGED for the
                              Phase-5 export check.]
  • stdev / variance:         POPULATION (ddof=0) — matches Pine ta.stdev / ta.variance (NOT numpy default).
  • pivothigh / pivotlow:     the pivot value appears only at the CONFIRMATION bar (right bars AFTER the extreme),
                              NaN elsewhere — the lag is reproduced, not "fixed" (plan §4).
  • valuewhen(cond,src,occ):  the value of src at the occ-th most recent bar where cond was true (0 = most recent).

Not financial advice. Timing grades, it does not guarantee.
"""
import numpy as np
import pandas as pd


# ───────────────────────── helpers ─────────────────────────
def _s(x):
    """Coerce to a float pandas Series (accepts Series / list / ndarray)."""
    if isinstance(x, pd.Series):
        return x.astype(float)
    return pd.Series(np.asarray(x, dtype=float))


def change(src, n=1):
    """ta.change — src - src[n]."""
    s = _s(src)
    return s - s.shift(n)


def sma(src, n):
    """ta.sma — simple moving average over n bars."""
    return _s(src).rolling(n, min_periods=n).mean()


def stdev(src, n):
    """ta.stdev — POPULATION standard deviation (ddof=0), Pine default."""
    return _s(src).rolling(n, min_periods=n).std(ddof=0)


def variance(src, n):
    """ta.variance — POPULATION variance (ddof=0), Pine default."""
    return _s(src).rolling(n, min_periods=n).var(ddof=0)


def highest(src, n):
    """ta.highest — rolling max over n bars."""
    return _s(src).rolling(n, min_periods=n).max()


def lowest(src, n):
    """ta.lowest — rolling min over n bars."""
    return _s(src).rolling(n, min_periods=n).min()


def rolling_sum(src, n):
    """math.sum — rolling sum over n bars."""
    return _s(src).rolling(n, min_periods=n).sum()


# ───────────────────────── Wilder smoothing family ─────────────────────────
def rma(src, n):
    """ta.rma (Wilder). Seed = SMA of the first n bars at index n-1, then x_t = (x_{t-1}*(n-1) + s_t)/n.
    This is the load-bearing parity choice — rsi/atr/adx all inherit it."""
    s = _s(src)
    a = s.to_numpy(dtype=float)
    out = np.full(len(a), np.nan)
    if len(a) < n:
        return pd.Series(out, index=s.index)
    # seed with the SMA over the first n *valid* observations
    seed_slice = a[:n]
    if np.isnan(seed_slice).any():
        # fall back: find the first window of n consecutive non-nan
        start = None
        run = 0
        for i, v in enumerate(a):
            run = run + 1 if not np.isnan(v) else 0
            if run == n:
                start = i - n + 1
                break
        if start is None:
            return pd.Series(out, index=s.index)
        seed_idx = start + n - 1
        out[seed_idx] = np.nanmean(a[start:start + n])
    else:
        seed_idx = n - 1
        out[seed_idx] = seed_slice.mean()
    for i in range(seed_idx + 1, len(a)):
        prev = out[i - 1]
        cur = a[i]
        if np.isnan(cur):
            out[i] = prev
        else:
            out[i] = (prev * (n - 1) + cur) / n
    return pd.Series(out, index=s.index)


def ema(src, n):
    """ta.ema — seeded with SMA(n) at the first FULL window of non-NaN bars, then alpha = 2/(n+1); NaN source
    bars carry the prior value forward (Pine ema over a warmup/`na` prefix — critical for CHAINED indicators
    like ema(zscore(...)) / ema(macd_line), where the input has a leading NaN warmup). [seeding FLAGGED for
    Phase-5 verification]"""
    s = _s(src)
    a = s.to_numpy(dtype=float)
    out = np.full(len(a), np.nan)
    if len(a) < n:
        return pd.Series(out, index=s.index)
    # find the first index where the trailing n-window is fully non-NaN (skip the warmup prefix)
    start = None
    run = 0
    for i, v in enumerate(a):
        run = run + 1 if not np.isnan(v) else 0
        if run == n:
            start = i
            break
    if start is None:
        return pd.Series(out, index=s.index)
    alpha = 2.0 / (n + 1.0)
    out[start] = np.nanmean(a[start - n + 1:start + 1])
    for i in range(start + 1, len(a)):
        cur = a[i]
        out[i] = out[i - 1] if np.isnan(cur) else (alpha * cur + (1 - alpha) * out[i - 1])
    return pd.Series(out, index=s.index)


def rsi(src, n=14):
    """ta.rsi — 100 - 100/(1+rs), rs = rma(gain,n)/rma(loss,n), gain=max(chg,0), loss=max(-chg,0)."""
    s = _s(src)
    chg = s.diff()
    gain = chg.clip(lower=0.0)
    loss = (-chg).clip(lower=0.0)
    avg_gain = rma(gain.fillna(0.0), n)
    avg_loss = rma(loss.fillna(0.0), n)
    rs = avg_gain / avg_loss
    out = 100.0 - 100.0 / (1.0 + rs)
    out = out.where(avg_loss != 0, 100.0)     # all-gains window -> RSI 100 (Pine convention)
    out = out.where(~((avg_gain == 0) & (avg_loss == 0)), 50.0)   # flat -> 50
    # mask the warmup region (before the first rma seed)
    out[avg_gain.isna() | avg_loss.isna()] = np.nan
    return out


def true_range(high, low, close):
    """Pine true range: max(h-l, |h-close[1]|, |l-close[1]|)."""
    h, l, c = _s(high), _s(low), _s(close)
    pc = c.shift(1)
    tr = pd.concat([(h - l), (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    tr.iloc[0] = (h.iloc[0] - l.iloc[0]) if len(h) else np.nan   # first bar: no prev close
    return tr


def atr(high, low, close, n=14):
    """ta.atr — rma of true range."""
    return rma(true_range(high, low, close), n)


def adx(high, low, close, di_len=14, adx_len=14):
    """ta.dmi/ta.adx (Wilder). Returns (plus_di, minus_di, adx)."""
    h, l, c = _s(high), _s(low), _s(close)
    up = h.diff()
    down = -l.diff()
    plus_dm = ((up > down) & (up > 0)) * up
    minus_dm = ((down > up) & (down > 0)) * down
    tr = true_range(h, l, c)
    atr_ = rma(tr, di_len)
    plus_di = 100.0 * rma(plus_dm.fillna(0.0), di_len) / atr_
    minus_di = 100.0 * rma(minus_dm.fillna(0.0), di_len) / atr_
    dx = 100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0.0, np.nan)
    adx_ = rma(dx.fillna(0.0), adx_len)
    return plus_di, minus_di, adx_


def macd(src, fast=12, slow=26, signal=9):
    """ta.macd — returns (macd_line, signal_line, hist)."""
    s = _s(src)
    macd_line = ema(s, fast) - ema(s, slow)
    signal_line = ema(macd_line, signal)
    return macd_line, signal_line, (macd_line - signal_line)


def stoch(close, high, low, n=14):
    """ta.stoch — 100 * (close - lowest(low,n)) / (highest(high,n) - lowest(low,n))."""
    c, h, l = _s(close), _s(high), _s(low)
    ll = lowest(l, n)
    hh = highest(h, n)
    rng = (hh - ll).replace(0.0, np.nan)
    return 100.0 * (c - ll) / rng


# ───────────────────────── crossings ─────────────────────────
def crossover(a, b):
    """ta.crossover(a,b) — a>b now AND a<=b on the prior bar. Boolean Series."""
    a, b = _s(a), _s(b)
    return (a > b) & (a.shift(1) <= b.shift(1))


def crossunder(a, b):
    """ta.crossunder(a,b) — a<b now AND a>=b on the prior bar. Boolean Series."""
    a, b = _s(a), _s(b)
    return (a < b) & (a.shift(1) >= b.shift(1))


# ───────────────────────── stateful (explicit bar loop) ─────────────────────────
def valuewhen(cond, src, occurrence=0):
    """ta.valuewhen — value of src at the occurrence-th most recent bar where cond is true (0 = most recent).
    NaN until that many occurrences exist. Sequential per Pine semantics."""
    cond = _s(cond).fillna(0.0).astype(bool).to_numpy()
    src = _s(src).to_numpy(dtype=float)
    out = np.full(len(src), np.nan)
    hits = []   # indices where cond was true, most-recent last
    for i in range(len(src)):
        if len(hits) > occurrence:
            out[i] = src[hits[-1 - occurrence]]
        if cond[i]:
            hits.append(i)
            if len(hits) > occurrence:      # value becomes available on THIS bar too
                out[i] = src[hits[-1 - occurrence]]
    return pd.Series(out)


def barssince(cond):
    """ta.barssince — number of bars since cond was last true (0 on the bar it's true). NaN before first true."""
    cond = _s(cond).fillna(0.0).astype(bool).to_numpy()
    out = np.full(len(cond), np.nan)
    last = None
    for i in range(len(cond)):
        if cond[i]:
            last = i
        if last is not None:
            out[i] = i - last
    return pd.Series(out)


def pivothigh(src, leftbars, rightbars):
    """ta.pivothigh — the pivot's VALUE placed at its CONFIRMATION bar (rightbars after the extreme), NaN
    elsewhere. A bar j is a pivot high iff src[j] is strictly >= all bars in [j-left, j+right] and strictly >
    at least on each side (Pine treats equal neighbours as non-pivot via strict comparison on both wings)."""
    s = _s(src).to_numpy(dtype=float)
    out = np.full(len(s), np.nan)
    for j in range(leftbars, len(s) - rightbars):
        pivot = s[j]
        if np.isnan(pivot):
            continue
        left = s[j - leftbars:j]
        right = s[j + 1:j + 1 + rightbars]
        if left.size < leftbars or right.size < rightbars:
            continue
        if np.all(pivot > left) and np.all(pivot > right):
            out[j + rightbars] = pivot     # confirmation lag: value shows `rightbars` later
    return pd.Series(out)


def pivotlow(src, leftbars, rightbars):
    """ta.pivotlow — mirror of pivothigh (strict local minimum), value at the confirmation bar."""
    s = _s(src).to_numpy(dtype=float)
    out = np.full(len(s), np.nan)
    for j in range(leftbars, len(s) - rightbars):
        pivot = s[j]
        if np.isnan(pivot):
            continue
        left = s[j - leftbars:j]
        right = s[j + 1:j + 1 + rightbars]
        if left.size < leftbars or right.size < rightbars:
            continue
        if np.all(pivot < left) and np.all(pivot < right):
            out[j + rightbars] = pivot
    return pd.Series(out)


# ───────────────────────── variance-ratio Hurst proxy (APEX §mean_reversion_edge) ─────────────────────────
def hurst_vr(close, k=10, window=50):
    """Variance-ratio Hurst proxy, ported VERBATIM from APEX v22 (lines 787-792):
        lr1  = log(close/close[1]);  lrK = log(close/close[k])
        varK = variance(lrK, window); var1 = variance(lr1, window)
        H = (var1>0 and varK>0) ? log(varK/(k*var1))/log(10)/2 + 0.5 : 0.5
    H > 0.55 => persistent/trending tape (the reversal-fade gate). Returns a Series (0.5 where undefined)."""
    c = _s(close)
    lr1 = np.log(c / c.shift(1))
    lrK = np.log(c / c.shift(k))
    varK = variance(lrK, window)
    var1 = variance(lr1, window)
    with np.errstate(divide="ignore", invalid="ignore"):
        h = np.log(varK / (k * var1)) / np.log(10) / 2.0 + 0.5
    h = h.where((var1 > 0) & (varK > 0), 0.5)
    return h.fillna(0.5)
