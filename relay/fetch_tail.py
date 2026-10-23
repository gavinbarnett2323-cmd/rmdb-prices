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

BOTTOM HUNTER BLOCK (2026-09-26): `bh` carries the raw inputs of Gavin's Liquidity Bottom Hunter (the vault's
engine/v2/bottom_hunter.py): the last 40 raw closes of the script's ETFs, ^VIX, ^VIX3M, ^MOVE, DX-Y.NYB, and the FRED series
it reads (DFII10, HY OAS, jobless claims, NFCI). See bh_block(). Its failure never blocks the tail.

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
         "BTC-USD", "ETH-USD", "SOL-USD", "^VIX", "^VIX3M", "^VVIX", "^TNX", "^IRX", "^GSPC", "^NDX", "^RUT",
         # 2026-10-01 (Gavin's yes): the 15 international / REIT ETFs of the WHAT x WHEN tranche universe (vault
         # engine/v2/system_book.json), so the monthly tranche ranks all 35 ETFs of its backtest
         "EFA", "EEM", "EWJ", "EWG", "EWU", "EWC", "EWA", "EWW", "EWZ", "EWY", "EWT", "FXI", "INDA", "VNQ", "IYR"]

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


# ------------------------------------------------------------------------------------------------ Bottom Hunter block
# 2026-09-26: the raw inputs of Gavin's Liquidity Bottom Hunter (vault engine/v2/bottom_hunter.py), last BH_N sessions of
# each, through the tail's last FINISHED session. The vault appends them to its TradingView history every build, so the
# script keeps computing on the same prints his chart uses. Kept OUT of `tickers` on purpose: adding ^MOVE / DX-Y.NYB there
# would change the universe median r20 (daily.py) and the weekly layer's breadth context.
#   px    RAW closes (auto_adjust off: split-adjusted, NOT dividend-adjusted, as his chart with ADJ off; adjusted ETF closes
#         move 28 triangles since 2010, measured). The ETFs, ^VIX and ^VIX3M come from the download above (no extra
#         call); ^MOVE and DX-Y.NYB are one small extra download (Yahoo's print of the index TradingView shows as TVC:).
#   fred  DFII10 (10y real yield), BAMLH0A0HYM2 (HY OAS), ICSA (jobless claims), NFCI (Chicago Fed). FRED's public site
#         hangs from GitHub runners (every fred-relay run since 2026-09-16 was cancelled at its 20-minute timeout), so:
#         the FRED API when the repo secret FRED_API_KEY is set (free key), then one short fredgraph try, then the
#         publisher's own file for the two series that have one (DFII10 = Treasury's 10-year par real yield, NFCI =
#         chicagofed.org). One short try per source inside BH_BUDGET_S; a miss is NAMED in bh.errors, never filled.
# A failure here never blocks the tail (main() catches it and writes bh.errors).
BH_N = 40
BH_PX = ["SPY", "RSP", "IWM", "HYG", "LQD", "SHY", "^VIX", "^VIX3M",
         "XLK", "XLY", "XLF", "XLI", "XLB", "XLE", "XLP", "XLU", "XLV", "XLC"]
BH_EXTRA = ["^MOVE", "DX-Y.NYB"]
BH_FRED = ["DFII10", "BAMLH0A0HYM2", "ICSA", "NFCI"]
BH_BUDGET_S = 150
# 2026-09-29: a source whose newest print is older than this (days before the tail's as_of) is STALE: noted, the next source
# is tried, and when every source is stale the freshest rows ship with src '<name> (stale)' and the reason in errors. The
# first live run found chicagofed.org's NFCI file ending 2026-04-24 while the Chicago Fed publishes weekly.
BH_STALE_DAYS = {"DFII10": 10, "BAMLH0A0HYM2": 10, "ICSA": 21, "NFCI": 21}
BH_TIMEOUT_S = 12
_BH_HDR = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
           "Accept": "text/csv,application/json,text/plain,*/*;q=0.8", "Accept-Language": "en-US,en;q=0.9"}


def _bh_get(url):
    import urllib.request
    return urllib.request.urlopen(urllib.request.Request(url, headers=_BH_HDR), timeout=BH_TIMEOUT_S).read().decode("utf-8", "replace")


def _bh_csv(raw):
    import csv, io
    return [r for r in csv.reader(io.StringIO(raw.lstrip("﻿"))) if r]


def _bh_day(x):
    x = str(x).strip().strip('"')[:10]
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.datetime.strptime(x, fmt).date().isoformat()
        except ValueError:
            pass
    return None


def _bh_num(x):
    try:
        v = float(str(x).strip().strip('"'))
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def _bh_rows(pairs):
    """[[YYYY-MM-DD, float]] sorted, one per date, bad rows dropped."""
    d = {}
    for a, b in pairs:
        a, b = _bh_day(a), _bh_num(b)
        if a and b is not None:
            d[a] = b
    return [[k, d[k]] for k in sorted(d)]


def _fred_api(sid, key):
    start = (datetime.date.today() - datetime.timedelta(days=400)).isoformat()
    j = json.loads(_bh_get("https://api.stlouisfed.org/fred/series/observations?series_id=%s&api_key=%s&file_type=json"
                           "&observation_start=%s" % (sid, key, start)))
    return _bh_rows((o.get("date"), o.get("value")) for o in (j.get("observations") or []))


def _fredgraph(sid):
    start = (datetime.date.today() - datetime.timedelta(days=400)).isoformat()
    rows = _bh_csv(_bh_get("https://fred.stlouisfed.org/graph/fredgraph.csv?id=%s&cosd=%s" % (sid, start)))
    return _bh_rows((r[0], r[1]) for r in rows[1:] if len(r) >= 2)


def _treasury_real10():
    """DFII10 = the Treasury's 10-year par real yield (the H.15 republishes it): home.treasury.gov daily real yield curve."""
    y = datetime.date.today().year
    pairs = []
    for yr in (y - 1, y):
        rows = _bh_csv(_bh_get("https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rates.csv/"
                               "%d/all?type=daily_treasury_real_yield_curve&field_tdr_date_value=%d&page&_format=csv" % (yr, yr)))
        if not rows:
            continue
        h = [c.strip().upper() for c in rows[0]]
        j = h.index("10 YR")
        pairs += [(r[0], r[j]) for r in rows[1:] if len(r) > j]
    return _bh_rows(pairs)


def _chicagofed_nfci():
    rows = _bh_csv(_bh_get("https://www.chicagofed.org/-/media/publications/nfci/nfci-data-series-csv.csv"))
    h = [c.strip().upper() for c in rows[0]]
    j = h.index("NFCI")
    return _bh_rows((r[0], r[j]) for r in rows[1:] if len(r) > j)


BH_ALT = {"DFII10": ("treasury.gov", _treasury_real10), "NFCI": ("chicagofed.org", _chicagofed_nfci)}


def bh_block(got, last, n=BH_N, now=None):
    """{px: {ticker: [[date, raw close]]}, fred: {id: [[date, value]]}, src, errors}; px dated after `last` (the tail's last
    finished session) is dropped, so a run during the session never ships a partial bar."""
    t_end = time.time() + BH_BUDGET_S
    key = (os.environ.get("FRED_API_KEY") or "").strip()
    lim = pd.Timestamp(last)
    out = {"as_of": last, "n": n, "px": {}, "fred": {}, "src": {}, "errors": {},
           "rule": "px = RAW daily closes (split-adjusted, not dividend-adjusted) through as_of; fred = FRED observations "
                   "(FRED API, fredgraph, or the publisher's own file, named in src); last n of each; a miss is named in errors"}

    def scrub(m):
        return m.replace(key, "***") if key else m

    def px_rows(s):
        s = _norm(s.dropna())
        s = s[s.index <= lim].tail(n)
        # 4 decimals: yfinance hands float32-derived values (661.8200073242188); the exchange print is 661.82
        return [[d.strftime("%Y-%m-%d"), round(float(v), 4)] for d, v in s.items() if math.isfinite(float(v))]

    for t in BH_PX:
        d = got.get(t)
        if not d or d.get("close") is None:
            out["errors"][t] = "not in the download"
            continue
        out["px"][t] = px_rows(d["close"])
        out["src"][t] = "tail download"
    start = (datetime.date.today() - datetime.timedelta(days=150)).isoformat()
    for s in BH_EXTRA:
        try:
            df = _dl([s], start=start, interval="1d")
            c = _col(df, "Close", s) if df is not None else None
            if c is None or c.dropna().empty:
                out["errors"][s] = "no data"
                continue
            out["px"][s] = px_rows(c)
            out["src"][s] = "yfinance"
        except Exception as e:
            out["errors"][s] = "%s: %s" % (type(e).__name__, str(e)[:160])
    for sid in BH_FRED:
        tries = ([("FRED API", lambda sid=sid: _fred_api(sid, key))] if key else []) + [("fredgraph", lambda sid=sid: _fredgraph(sid))]
        if sid in BH_ALT:
            tries.append(BH_ALT[sid])
        notes = []
        stale = None
        for name, fn in tries:
            if time.time() > t_end:
                notes.append("%s: skipped (time budget)" % name)
                continue
            try:
                rows = fn()
            except Exception as e:
                notes.append("%s: %s: %s" % (name, type(e).__name__, str(e)[:120]))
                continue
            if len(rows) >= 5:
                age = (lim - pd.Timestamp(rows[-1][0])).days
                if age > BH_STALE_DAYS.get(sid, 21):
                    notes.append("%s: newest print %s, %d days before %s (stale)" % (name, rows[-1][0], age, last))
                    if stale is None or rows[-1][0] > stale[1][-1][0]:
                        stale = (name, rows)
                    continue
                out["fred"][sid] = rows[-n:]
                out["src"][sid] = name
                break
            notes.append("%s: %d rows" % (name, len(rows)))
        if sid not in out["fred"]:
            if stale:
                out["fred"][sid] = stale[1][-n:]
                out["src"][sid] = stale[0] + " (stale)"
            out["errors"][sid] = scrub("; ".join(notes))
    return out


# ------------------------------------------------------------------------------------------------ chart block
# 2026-10-12 (Gavin: "make it so my charts html updates on a daily basis"): the vault's chart page
# (Investing/Charts/Charts.html, build_chart.py --daily) appends new daily candles to each ticker's stored history and
# draws v20 on them. It needs, for the last CHART_N finished sessions: full OHLCV per ticker (dividend + split adjusted the
# way TradingView's chart is, i.e. raw OHLC times Adj Close / Close, so the newest bar is the raw print), and the closes of
# the eight feeds v20 reads (CBOE:VIX, VIX9D, SKEW, INDEX:MMTH, AMEX:XLU, XLP, SPY, INDEX:CPC). MMTH and CPC exist only on
# TradingView, so they are read from TradingView's public quote scanner after the close and accumulated in `tv_hist` (this
# file keeps its own history: each run reads the previous file and adds today). A value is stamped with the tail's as_of
# ONLY when the scanner's VIX agrees with Yahoo's ^VIX close for that session (proves the quote is that session's close);
# otherwise it is left out and named in `tv_errors`, never guessed. Written to relay/chart_tail.json, separate from
# daily_tail.json so nothing that reads the tail changes. A failure here never blocks the tail.
CHART_OUT = os.path.join(HERE, "chart_tail.json")
CHART_EXTRA = os.path.join(HERE, "chart_extra.txt")
CHART_N = 40
CHART_YF = ["^VIX9D", "^SKEW"]          # ^VIX, XLU, XLP, SPY are already in the main download
TV_SCAN_URL = "https://scanner.tradingview.com/global/scan"
TV_SCAN = ["INDEX:MMTH", "INDEX:CPC", "CBOE:VIX", "CBOE:SKEW", "CBOE:VIX9D"]
TV_HIST_KEEP = 600


TV_SCAN_MARKETS = ["global", "america", "cfd", "index"]
TV_SCAN_ALT = ["USI:PCC", "USI:MMTH", "TVC:MMTH"]   # recorded for diagnosis only; never stamped into tv_hist


def _tv_scan():
    """{symbol: close} merged over a few scanner markets (indices are not in every one), plus {symbol: market} it came from."""
    import urllib.request
    out, src, errs = {}, {}, {}
    for mkt in TV_SCAN_MARKETS:
        try:
            body = json.dumps({"symbols": {"tickers": TV_SCAN + TV_SCAN_ALT, "query": {"types": []}}, "columns": ["close"]}).encode()
            req = urllib.request.Request(TV_SCAN_URL.replace("/global/", "/%s/" % mkt), data=body,
                                         headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0 rmdb-relay"})
            raw = json.loads(urllib.request.urlopen(req, timeout=30).read().decode("utf-8"))
            for row in raw.get("data") or []:
                d = row.get("d") or []
                if d and d[0] is not None and row.get("s") not in out:
                    out[row.get("s")] = float(d[0]); src[row.get("s")] = mkt
        except Exception as e:
            errs[mkt] = "%s: %s" % (type(e).__name__, str(e)[:100])
    out["_src"] = src; out["_errors"] = errs
    return out


CBOE_DAILY = "https://cdn.cboe.com/data/us/options/market_statistics/daily/%s_daily_options"


def _cboe_total_pc(day):
    """CBOE's own daily market statistics for one session: the TOTAL PUT/CALL RATIO (what TradingView shows as INDEX:CPC).
    Returns (value or None, note)."""
    import urllib.request
    raw = urllib.request.urlopen(urllib.request.Request(CBOE_DAILY % day, headers={"User-Agent": "Mozilla/5.0 rmdb-relay"}), timeout=30).read().decode("utf-8", "replace")
    try:
        doc = json.loads(raw)
    except Exception:
        return None, "not JSON: " + raw[:200]
    hits = []

    def walk(x):
        if isinstance(x, dict):
            nm = str(x.get("name") or x.get("label") or "")
            if "TOTAL PUT/CALL RATIO" in nm.upper():
                hits.append(x.get("value"))
            for k, v in x.items():
                if "TOTAL PUT/CALL RATIO" in str(k).upper() and not isinstance(v, (dict, list)):
                    hits.append(v)
                walk(v)
        elif isinstance(x, list):
            for v in x: walk(v)
    walk(doc)
    for h in hits:
        try:
            v = float(h)
            if 0.2 < v < 3.0:
                return v, "cboe daily market statistics"
        except Exception:
            pass
    return None, "no TOTAL PUT/CALL RATIO in: " + raw[:300]


def chart_extra(got):
    """2026-10-23 (Gavin: "add a significant more amount of etfs into the logging system"): the chart-only tickers in
    relay/chart_extra.txt that the main download does not already carry, downloaded over a short window (the chart block
    keeps the last CHART_N sessions). They go into chart_tail.json, so the vault's chart page logs them every weekday and the
    live relay prices them, and NOT into daily_tail.json, so nothing the engine reads from the daily tail changes."""
    if not os.path.exists(CHART_EXTRA):
        return {}
    names = [l.strip().upper() for l in open(CHART_EXTRA) if l.strip() and not l.startswith("#")]
    need = [t for t in dict.fromkeys(names) if t not in got]
    if not need:
        return {}
    ysyms = [ymap(t) for t in need]
    back = {ymap(t): t for t in need}
    start = (datetime.date.today() - datetime.timedelta(days=120)).isoformat()
    res = {}
    for i in range(0, len(ysyms), BATCH):
        chunk = ysyms[i:i + BATCH]
        df = _dl(chunk, start=start, interval="1d")
        if df is not None:
            for ys in chunk:
                adj = _col(df, "Adj Close", ys)
                if adj is None:
                    continue
                adj = adj.dropna()
                if len(adj) < 20:
                    continue
                res[back.get(ys, ys)] = {"adj": adj, "close": _col(df, "Close", ys), "low": _col(df, "Low", ys), "vol": _col(df, "Volume", ys),
                                         "open": _col(df, "Open", ys), "high": _col(df, "High", ys)}
        time.sleep(SLEEP)
    print("chart extra: %d of %d chart-only tickers priced" % (len(res), len(need)), flush=True)
    return res


def chart_block(got, out, now=None):
    now = now or datetime.datetime.now(datetime.timezone.utc)
    cal = pd.DatetimeIndex(pd.to_datetime(out["calendar"][-CHART_N:]))
    cal_s = [d.strftime("%Y-%m-%d") for d in cal]
    as_of = cal_s[-1]
    tick, errs = {}, {}
    for t, d in got.items():
        try:
            adj = _norm(d["adj"]).reindex(cal); cl = _norm(d["close"]).reindex(cal)
            if adj.notna().sum() < 1:
                continue
            f = adj / cl
            o = _norm(d["open"]).reindex(cal) * f; h = _norm(d["high"]).reindex(cal) * f; l = _norm(d["low"]).reindex(cal) * f
            v = _norm(d["vol"]).reindex(cal)
            tick[t] = {"o": [_r(x) for x in o.values], "h": [_r(x) for x in h.values], "l": [_r(x) for x in l.values],
                       "c": [_r(x) for x in adj.values], "v": [None if not (x == x) else int(x) for x in v.values],
                       "raw_c": [_r(x) for x in cl.values]}
        except Exception as e:
            errs[t] = "%s: %s" % (type(e).__name__, str(e)[:120])
    feeds, fsrc = {}, {}
    for key, t in (("CBOE:VIX", "^VIX"), ("AMEX:XLU", "XLU"), ("AMEX:XLP", "XLP"), ("AMEX:SPY", "SPY")):
        if t in got:
            s = _norm(got[t]["close"]).reindex(cal)
            feeds[key] = [_r(x) for x in s.values]; fsrc[key] = "yahoo %s raw close (TradingView's feed is the split-adjusted close)" % t
    try:
        start = (cal[0] - pd.Timedelta(days=10)).strftime("%Y-%m-%d")
        df = _dl(CHART_YF, start=start, interval="1d")
        for key, t in (("CBOE:VIX9D", "^VIX9D"), ("CBOE:SKEW", "^SKEW")):
            c = _col(df, "Close", t) if df is not None else None
            if c is not None:
                feeds[key] = [_r(x) for x in _norm(c).reindex(cal).values]; fsrc[key] = "yahoo %s close" % t
            else:
                errs[key] = "not in the yahoo download"
    except Exception as e:
        errs["yahoo_feeds"] = "%s: %s" % (type(e).__name__, str(e)[:160])
    prev = {}
    try:
        prev = (json.load(open(CHART_OUT)) or {}).get("tv_hist") or {}
    except Exception:
        prev = {}
    tv_hist = {k: dict(v) for k, v in prev.items() if isinstance(v, dict)}
    tv_errors, tv_now = {}, {"_src": {}}
    try:
        tv_now = _tv_scan()
        tv_src = tv_now.pop("_src", {}); tv_scan_err = tv_now.pop("_errors", {})
        if tv_scan_err: tv_errors["scanner_markets"] = tv_scan_err
        tv_now = {k: v for k, v in tv_now.items()}; tv_now["_src"] = tv_src
        yv = feeds.get("CBOE:VIX", [None])[-1]
        sv = tv_now.get("CBOE:VIX")
        if yv and sv and abs(sv / yv - 1) <= 0.005:
            for k in ("INDEX:MMTH", "INDEX:CPC"):
                if k in tv_now:
                    tv_hist.setdefault(k, {})[as_of] = round(tv_now[k], 4)
                else:
                    tv_errors[k] = "not in the scanner reply"
        else:
            tv_errors["session_check"] = "scanner VIX %s vs yahoo ^VIX close %s for %s: quote is not that session's close, nothing stamped" % (sv, yv, as_of)
    except Exception as e:
        tv_errors["scanner"] = "%s: %s" % (type(e).__name__, str(e)[:160])
    if as_of not in (tv_hist.get("INDEX:CPC") or {}):       # CBOE's own number when the scanner does not carry INDEX:CPC
        try:
            v, why = _cboe_total_pc(as_of)
            if v is not None:
                tv_hist.setdefault("INDEX:CPC", {})[as_of] = round(v, 4); tv_errors.pop("INDEX:CPC", None)
                tv_now["_src"]["INDEX:CPC"] = why
            else:
                tv_errors["INDEX:CPC_cboe"] = why
        except Exception as e:
            tv_errors["INDEX:CPC_cboe"] = "%s: %s" % (type(e).__name__, str(e)[:160])
    for k in list(tv_hist):
        tv_hist[k] = dict(sorted(tv_hist[k].items())[-TV_HIST_KEEP:])
    return {"_doc": "Chart tail for the vault's Investing/Charts (build_chart.py --daily). o/h/l/c = Yahoo OHLC adjusted for dividends and "
                    "splits (raw x Adj Close / Close, the newest bar = the raw print); raw_c = the raw close; v = shares. feeds = v20's "
                    "request.security inputs on the same calendar. tv_hist = INDEX:MMTH and INDEX:CPC closes read from TradingView's quote "
                    "scanner after each close, accumulated run over run, stamped only when the scanner's VIX matched Yahoo's close. "
                    "Built by relay/fetch_tail.py chart_block().",
            "generated_at": now.strftime("%Y-%m-%d %H:%M UTC"), "as_of": as_of, "calendar": cal_s, "n_tickers": len(tick),
            "tickers": tick, "feeds": feeds, "feed_src": fsrc, "tv_hist": tv_hist, "tv_now": tv_now, "tv_errors": tv_errors, "errors": errs}


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
    try:                                         # 2026-09-26: the Bottom Hunter's raw inputs; never blocks the tail
        out["bh"] = bh_block(got, out["as_of"])
    except Exception as e:
        out["bh"] = {"errors": {"bh_block": "%s: %s" % (type(e).__name__, str(e)[:200])}}
    tmp = OUT + ".tmp"
    with open(tmp, "w") as f:
        json.dump(out, f, separators=(",", ":"))
    os.replace(tmp, OUT)
    try:                                         # 2026-10-12: the chart page's daily candles; never blocks the tail
        gch = dict(got)
        try:                                     # 2026-10-23: the chart-only tickers join the chart tail, not the daily tail
            gch.update(chart_extra(got))
        except Exception as e:
            print("chart extra FAILED (the chart tail goes ahead without them): %s" % e, flush=True)
        ch = chart_block(gch, out)
        with open(CHART_OUT + ".tmp", "w") as f:
            json.dump(ch, f, separators=(",", ":"))
        os.replace(CHART_OUT + ".tmp", CHART_OUT)
        print("chart tail: %d tickers, as_of %s, feeds %s, tv_hist %s, tv_errors %s" % (ch["n_tickers"], ch["as_of"], sorted(ch["feeds"]),
              {k: len(v) for k, v in ch["tv_hist"].items()}, ch["tv_errors"] or "none"), flush=True)
    except Exception as e:
        import traceback
        traceback.print_exc()
        print("chart tail FAILED (the daily tail is fine):", e, flush=True)
    print("tail: %d/%d tickers, as_of %s, %.1f MB, gaps=%d | weekly TradingView block: %s" % (out["n_tickers"], len(tk), out["as_of"], os.path.getsize(OUT) / 1e6, len(out["gaps"]), out.get("tvw_meta")), flush=True)
    b = out.get("bh") or {}
    print("bottom hunter block: px %d, fred %d (%s), errors %s" % (len(b.get("px") or {}), len(b.get("fred") or {}),
          ", ".join("%s via %s" % (k, v) for k, v in (b.get("src") or {}).items() if k in BH_FRED), b.get("errors") or "none"), flush=True)


if __name__ == "__main__":
    main()
