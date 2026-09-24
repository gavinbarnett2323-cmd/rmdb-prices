# -*- coding: utf-8 -*-
"""fetch_tail.py — DAILY-CLOSE TAIL emitter for the V2 hubs (2026-09-22).

Runs on a GitHub Actions runner every weekday evening (tail.yml), right after the price relay. Emits
relay/daily_tail.json: the last ~3 years of DAILY adjusted closes (plus the last 130 raw intraday lows and a
20-day average dollar volume) for every ticker in relay/tickers.txt PLUS the benchmark/regime set, aligned to
one shared SPY session calendar so position i means the same date for every name.

WHY (the weekly history.json and the Saturday parquet bars stay untouched): the V2 hubs are wired to the
full-system backtest, whose rules are defined on DAILY features — a 20-session return (r20), the universe
median r20 on the same date, a 63-session realized sigma, the 252-session drawdown, the median rolling
52-week max drawdown over 3 years (the CORRECTION rail depth) and the worst 3-year drawdown (the DEEP rail
depth). Weekly bars approximate those to within a few sessions; this file makes the live engine compute the
SAME numbers the backtest was graded on. Parquet is not readable on the laptop (no pyarrow), JSON is.

Honesty rails (same family as fetch_history.py / fetch_daily.py):
  (1) CHUNKED + RETRIED — never one fragile mega-call.
  (2) EMPTY-WRITE GUARD — if coverage < MIN_FRESH_FRAC, REFUSE to overwrite and exit non-zero (Action RED).
  (3) `gaps` list — names we could NOT price are NAMED, never interpolated, never dropped silently.
  (4) No forward-filling. A missing day is null.

WEEKLY TRADINGVIEW BLOCK (2026-09-24, V2.1): every non-benchmark ticker also carries `tvw`, Gavin's v20/v22
TradingView signals computed on WEEKLY bars (tv_weekly.py: the backtest's weekly port, parity-checked) from the full
download (12 years, so the 200-week average and the 252-week distZ window are real on the live bar, as on his chart).
Only COMPLETED weeks count: a Monday-Thursday run speaks of last Friday's bar. `tvw_meta` stamps the week and the
count. A failure in the weekly layer never blocks the tail (the block is simply absent and `tvw_meta.error` says why).
A run during market hours drops today's partial session (`dropped_partial` names it): the tail only holds finished closes.

CANONICAL COPY lives in the vault at Investing/engine/relay/fetch_tail.py; the deployed copy is
relay/fetch_tail.py in github.com/gavinbarnett2323-cmd/rmdb-prices. Keep them in sync.
"""
import json, os, sys, time, datetime, math
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "daily_tail.json")

YMAP = {"BRKB": "BRK-B", "BFB": "BF-B"}
def ymap(t): return YMAP.get(t, t)

EXTRA = ["SPY", "QQQ", "IWM", "DIA", "VOO", "RSP", "MDY", "VTI", "TLT", "IEF", "SHY", "HYG", "LQD", "GLD", "SLV", "USO",
         "XLK", "XLC", "XLE", "XLB", "XLV", "XLP", "XLU", "XLRE", "XLF", "XLI", "XLY",
         "SMH", "SOXX", "XBI", "IBB", "KRE", "XHB", "ITB", "XRT", "XOP", "OIH", "ARKK",
         "BTC-USD", "ETH-USD", "SOL-USD", "^VIX", "^VIX3M", "^VVIX", "^TNX", "^IRX", "^GSPC", "^NDX", "^RUT"]

BATCH = 100
RETRIES = 3
SLEEP = 2.0
MIN_FRESH_FRAC = 0.60
PERIOD = "12y"         # 12y (was 7y, 2026-09-22): the slow features need 6y; the weekly TradingView layer needs 452 weeks
                       # (200-week average + 252-week distZ window) so the live weekly bar matches his chart (2026-09-24)
HIST_YEARS = 12
N_CLOSE = 780          # ~3.1 years of sessions kept in the file (grading + the fast features)
N_LOW = 130            # raw intraday lows kept (fill checks on resting limits over a 63-session GTC)
# Slow features computed HERE from the full 7y download, exactly as Investing/backtest/features.py defines them
# (they need 6 years of history: rolling 756 of a rolling 756, and a rolling-756 median of a rolling-252 max drawdown):
#   worst_dd_3y    = max over the last 756 sessions of (1 - c / rolling-756 max)
#   med_mdd252_3y  = median over the last 756 sessions of the trailing-252 max drawdown


def _dl(syms, **kw):
    import yfinance as yf
    for attempt in range(1, RETRIES + 1):
        try:
            df = yf.download(syms, progress=False, threads=True, auto_adjust=False, **kw)
            if df is None or getattr(df, "empty", True):
                raise ValueError("empty frame")
            return df
        except Exception as e:
            print("  batch attempt %d/%d failed: %s" % (attempt, RETRIES, e), flush=True)
            time.sleep(SLEEP * attempt)
    return None


def _col(df, field, ys):
    try:
        if isinstance(df.columns, pd.MultiIndex):
            return df[(field, ys)]
        return df[field]
    except KeyError:
        return None


def _r(x):
    if x is None:
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(v):
        return None
    return round(v, 4 if v < 10 else 3 if v < 1000 else 2)


def download(tk):
    """{ticker: {adj, close, low, vol: Series}} from yfinance."""
    ysyms = [ymap(t) for t in tk]
    back = {ymap(t): t for t in tk}
    got = {}
    for i in range(0, len(ysyms), BATCH):
        chunk = ysyms[i:i + BATCH]
        start = (datetime.date.today() - datetime.timedelta(days=int(365.25 * HIST_YEARS))).isoformat()
        df = _dl(chunk, start=start, interval="1d")
        if df is not None:
            for ys in chunk:
                adj = _col(df, "Adj Close", ys)
                if adj is None:
                    continue
                adj = adj.dropna()
                if len(adj) < 60:
                    continue
                got[back.get(ys, ys)] = {"adj": adj, "close": _col(df, "Close", ys), "low": _col(df, "Low", ys), "vol": _col(df, "Volume", ys),
                                         "open": _col(df, "Open", ys), "high": _col(df, "High", ys)}
        print("  tail %d-%d: running %d/%d" % (i, i + len(chunk), len(got), len(tk)), flush=True)
        time.sleep(SLEEP)
    return got


def _slow_features(adj):
    """worst_dd_3y and med_mdd252_3y from the FULL adjusted-close series (pandas, same code path as features.py)."""
    c = adj.dropna().astype("float64")
    if len(c) < 300:
        return None, None, len(c)
    rm3 = c.rolling(756, min_periods=250).max()
    worst = (1 - c / rm3).rolling(756, min_periods=250).max()
    rm = c.rolling(252, min_periods=126).max()
    dd = 1 - c / rm
    mdd252 = dd.rolling(252, min_periods=126).max()
    med = mdd252.rolling(756, min_periods=250).median()
    w = worst.iloc[-1]; m = med.iloc[-1]
    return (float(w) if pd.notna(w) else None), (float(m) if pd.notna(m) else None), len(c)


def _norm(s):
    s = s.copy()
    s.index = pd.to_datetime(s.index).tz_localize(None).normalize()
    return s[~s.index.duplicated(keep="last")].sort_index()


_W = {}


def _weekly_one(t):
    import tv_weekly as TW
    W = _W
    sig = TW.weekly_signals(W["cal"], W["o"][t].values, W["h"][t].values, W["l"][t].values, W["adj"][t].values, W["v"][t].values, W["ctx"])
    if sig is None:
        return t, None
    complete = TW.week_complete(W["cal"][sig["wpos"][-1]], W["now_et"])
    return t, TW.tvw(sig, W["cal"], raw_close=W["raw"][t].values, complete_last=complete)


def weekly_layer(got, now=None):
    """{ticker: tvw block} + meta: Gavin's weekly v20/v22 signals on the full download (tv_weekly.py). OHL are put on the
    adjusted scale by adj/close (the backtest harness's own adjustment); volume raw; the market context (VIX, SPY, RSP,
    XLU, XLP, sector breadth, universe % above the 200-day) on the SPY calendar, sampled at each week's last session."""
    import numpy as np
    import tv_weekly as TW
    now = now or datetime.datetime.now(datetime.timezone.utc)
    try:
        from zoneinfo import ZoneInfo
        now_et = now.astimezone(ZoneInfo("America/New_York")).replace(tzinfo=None)
    except Exception:
        now_et = (now - datetime.timedelta(hours=4)).replace(tzinfo=None)
    cal = _norm(got["SPY"]["adj"]).dropna().index
    names = list(got)
    adj = pd.DataFrame({t: _norm(got[t]["adj"]).reindex(cal) for t in names})
    raw = pd.DataFrame({t: (_norm(got[t]["close"]).reindex(cal) if got[t].get("close") is not None else pd.Series(np.nan, index=cal)) for t in names})
    fac = adj / raw
    fld = lambda k: pd.DataFrame({t: (_norm(got[t][k]).reindex(cal) if got[t].get(k) is not None else pd.Series(np.nan, index=cal)) for t in names})
    o, h, l, v = fld("open") * fac, fld("high") * fac, fld("low") * fac, fld("vol")
    ctx = TW.build_ctx(adj)
    todo = [t for t in names if t not in TW.BENCH]
    _W.update(cal=cal, o=o, h=h, l=l, adj=adj, v=v, raw=raw, ctx=ctx, now_et=now_et)
    try:                                         # fork workers share the frames copy-on-write (Linux runner)
        import multiprocessing as mp
        with mp.get_context("fork").Pool(max(1, min(4, os.cpu_count() or 1))) as pool:
            res = pool.map(_weekly_one, todo, chunksize=16)
    except Exception as e:
        print("  weekly layer: pool unavailable (%s), running serially" % e, flush=True)
        res = [_weekly_one(t) for t in todo]
    out, n_short = {}, 0
    last_complete = None
    for t, blk in res:
        if blk is None:
            n_short += 1
            continue
        out[t] = blk
        last_complete = max(last_complete or blk["wk"], blk["wk"])
    meta = {"wk": last_complete, "n": len(out), "n_short_history": n_short, "hist_start": str(cal[0].date()), "n_sessions": int(len(cal)),
            "distz_warmup": TW.DISTZ_WARMUP, "now_et": now_et.strftime("%Y-%m-%d %H:%M"),
            "rule": "BUY = v20 or v22 botFire on a completed weekly bar; buy_ago = completed weeks since; top5 = first week of topScore>=5 with the top regime"}
    return out, meta


def build(got, tk, now=None):
    """Align every series to SPY's session calendar and produce the JSON document (pure; testable offline)."""
    now = now or datetime.datetime.now(datetime.timezone.utc)
    if "SPY" not in got:
        raise ValueError("SPY missing: no calendar")
    try:
        tvw, tvw_meta = weekly_layer(got, now)
    except Exception as e:                      # the weekly layer never blocks the tail
        import traceback
        traceback.print_exc()
        tvw, tvw_meta = {}, {"error": "%s: %s" % (type(e).__name__, str(e)[:200])}

    cal_all = _norm(got["SPY"]["adj"]).dropna().index
    # A run while the market is open (a manual dispatch, or a push of this file) would carry a PARTIAL last session.
    # Drop it: the tail only ever holds finished closes (the scheduled runs are after the close and never hit this).
    dropped_partial = None
    try:
        from zoneinfo import ZoneInfo
        _et = now.astimezone(ZoneInfo("America/New_York"))
        if _et.weekday() < 5 and (9, 30) <= (_et.hour, _et.minute) < (16, 20) and len(cal_all) and cal_all[-1].date() == _et.date():
            dropped_partial = cal_all[-1].strftime("%Y-%m-%d")
            cal_all = cal_all[:-1]
    except Exception:
        pass
    cal = cal_all[-N_CLOSE:]
    cal_s = [d.strftime("%Y-%m-%d") for d in cal]
    lows_cal = cal[-N_LOW:]
    tickers = {}
    for t, d in got.items():
        adj = _norm(d["adj"]).reindex(cal)
        if adj.notna().sum() < 60:
            continue
        cl = _norm(d["close"]).reindex(cal) if d.get("close") is not None else None
        lo = _norm(d["low"]).reindex(lows_cal) if d.get("low") is not None else None
        vo = _norm(d["vol"]).reindex(cal) if d.get("vol") is not None else None
        dv = (cl * vo) if (cl is not None and vo is not None) else None
        adv20 = float(dv.dropna().tail(20).mean()) if dv is not None and dv.notna().sum() >= 15 else None
        last_valid = adj.dropna()
        w3, m3, n_full = _slow_features(_norm(d["adj"]).loc[:cal[-1]])
        tickers[t] = {
            "last": last_valid.index[-1].strftime("%Y-%m-%d"),
            "last_close": _r(cl.dropna().iloc[-1]) if cl is not None and cl.notna().any() else None,
            "c": [_r(x) for x in adj.values],
            "l": [_r(x) for x in lo.values] if lo is not None else None,
            "adv20_usd": round(adv20) if adv20 else None,
            "worst_dd_3y": (round(w3, 4) if w3 is not None else None),
            "med_mdd252_3y": (round(m3, 4) if m3 is not None else None),
            "n_full": int(n_full),
        }
        if t in tvw:
            tickers[t]["tvw"] = tvw[t]
    return {
        "_doc": "Daily-close tail for the V2 hubs. c = adjusted closes (dividends+splits) aligned to `calendar` (SPY sessions, "
                "oldest first, null = no print that day); l = RAW intraday lows for the last %d sessions (calendar[-%d:]); "
                "adv20_usd = 20-session mean of close*volume; worst_dd_3y / med_mdd252_3y = the two slow backtest features "
                "computed from the full %s download (n_full sessions); tvw = Gavin's weekly TradingView v20/v22 signals on completed weekly bars "
                "(relay/tv_weekly.py; see tvw_meta). Built by relay/fetch_tail.py." % (N_LOW, N_LOW, PERIOD),
        "generated_at": now.strftime("%Y-%m-%d %H:%M UTC"), "as_of": cal_s[-1], "period": PERIOD, "n_sessions": len(cal_s),
        "n_lows": N_LOW, "n_tickers": len(tickers), "gaps": sorted(t for t in tk if t not in tickers), "tvw_meta": tvw_meta, "dropped_partial": dropped_partial,
        "calendar": cal_s, "tickers": tickers,
    }


def main():
    tk = [l.strip().upper() for l in open(os.path.join(HERE, "tickers.txt")) if l.strip() and not l.startswith("#")]
    tk = sorted(set(tk) | set(EXTRA))
    print("daily tail for", len(tk), "tickers", flush=True)
    got = download(tk)
    frac = len(got) / max(1, len(tk))
    if frac < MIN_FRESH_FRAC or "SPY" not in got:
        print("TAIL GUARD TRIPPED: only %d/%d (%.0f%%) priced (SPY %s) — below %.0f%% floor. NOT writing. Exiting non-zero."
              % (len(got), len(tk), 100 * frac, "ok" if "SPY" in got else "MISSING", 100 * MIN_FRESH_FRAC), flush=True)
        sys.exit(1)
    out = build(got, tk)
    tmp = OUT + ".tmp"
    with open(tmp, "w") as f:
        json.dump(out, f, separators=(",", ":"))
    os.replace(tmp, OUT)
    print("tail: %d/%d tickers, as_of %s, %.1f MB, gaps=%d | weekly TradingView block: %s" % (out["n_tickers"], len(tk), out["as_of"], os.path.getsize(OUT) / 1e6, len(out["gaps"]), out.get("tvw_meta")), flush=True)


if __name__ == "__main__":
    main()
