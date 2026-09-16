# -*- coding: utf-8 -*-
"""fetch_daily.py — DAILY OHLCV history emitter for the Research Monster backtest layer (2026-09-16).

Runs on a GitHub Actions runner (real egress) alongside fetch_prices.py / fetch_history.py. Emits
relay/daily/*.parquet: ~12 years of DAILY bars (Open/High/Low/Close/Adj Close/Volume) for every ticker
in relay/tickers.txt PLUS the benchmark/regime set below, chunked by ticker so no single file nears
GitHub's 100MB limit. Also emits relay/daily/macro.csv — the FRED regime series (VIX, HY OAS, curve,
net-liquidity legs) pulled from fredgraph.csv (no key needed).

WHY DAILY (the weekly history.json stays untouched): the full-system backtest of every law in the vault —
dislocation entries, NEAR/DEEP/TIMED limit rails, turn detector, timing indicators, regime throttles,
tranche ladders, take-profit levels — needs the DAILY path to know whether a resting limit at price X
would have FILLED (intraday low <= X), how far a rail was touched, and what a Friday close hid. Weekly
bars cannot answer "did the limit fill".

Honesty rails (same family as fetch_history.py):
  (1) CHUNKED + RETRIED — never one fragile mega-call.
  (2) EMPTY-WRITE GUARD — if coverage < MIN_FRESH_FRAC, REFUSE to overwrite and exit non-zero (Action RED).
  (3) `gaps` list — names we could NOT price are NAMED in manifest.json, never interpolated, never dropped
      silently.
  (4) No forward-filling. A missing day is missing.

CANONICAL COPY lives in the vault at Investing/engine/relay/fetch_daily.py; the deployed copy is
relay/fetch_daily.py in github.com/gavinbarnett2323-cmd/rmdb-prices. Keep them in sync.
"""
import json, os, sys, time, datetime, io, urllib.request
import yfinance as yf
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
OUTDIR = os.path.join(HERE, "daily")
os.makedirs(OUTDIR, exist_ok=True)

YMAP = {"BRKB": "BRK-B", "BFB": "BF-B"}
def ymap(t): return YMAP.get(t, t)

# Benchmarks + regime instruments + sector ETFs + crypto proxies. Always fetched, never optional.
EXTRA = ["SPY", "QQQ", "IWM", "DIA", "VOO", "RSP", "MDY", "VTI", "TLT", "IEF", "SHY", "HYG", "LQD", "GLD", "SLV", "USO",
         "XLK", "XLC", "XLE", "XLB", "XLV", "XLP", "XLU", "XLRE", "XLF", "XLI", "XLY",
         "SMH", "SOXX", "XBI", "IBB", "KRE", "XHB", "ITB", "XRT", "XOP", "OIH", "ARKK", "IWF", "IWD", "MTUM", "QUAL", "USMV", "VLUE",
         "BTC-USD", "ETH-USD", "SOL-USD", "^VIX", "^VIX3M", "^VVIX", "^TNX", "^IRX", "^FVX", "^TYX", "^GSPC", "^NDX", "^RUT", "^DJI"]

BATCH = 100
RETRIES = 3
SLEEP = 2.0
MIN_FRESH_FRAC = 0.60
PERIOD = "12y"
CHUNK = 150            # tickers per parquet file

FRED = ["VIXCLS", "BAMLH0A0HYM2", "DGS10", "DGS2", "T10Y2Y", "DFII10", "WALCL", "WTREGEN", "RRPONTSYD", "M2SL",
        "CPIAUCSL", "DTWEXBGS", "BAMLC0A0CM", "FEDFUNDS", "UNRATE", "NFCI", "T10YIE", "DGS3MO", "SP500"]


def _dl(syms, **kw):
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


def _long(df, syms, back):
    """Multi/single-index yfinance frame -> long DataFrame [date,ticker,open,high,low,close,adj_close,volume]."""
    rows = []
    fields = {"Open": "open", "High": "high", "Low": "low", "Close": "close", "Adj Close": "adj_close", "Volume": "volume"}
    if isinstance(df.columns, pd.MultiIndex):
        for ys in syms:
            try:
                sub = df.xs(ys, axis=1, level=1)
            except KeyError:
                continue
            sub = sub.rename(columns=fields)
            sub = sub[[c for c in fields.values() if c in sub.columns]].dropna(subset=["close"])
            if sub.empty:
                continue
            sub = sub.copy()
            sub.insert(0, "ticker", back.get(ys, ys))
            rows.append(sub)
    else:
        sub = df.rename(columns=fields)
        sub = sub[[c for c in fields.values() if c in sub.columns]].dropna(subset=["close"])
        if not sub.empty:
            sub = sub.copy(); sub.insert(0, "ticker", back.get(syms[0], syms[0]))
            rows.append(sub)
    if not rows:
        return pd.DataFrame()
    out = pd.concat(rows)
    out.index.name = "date"
    out = out.reset_index()
    out["date"] = pd.to_datetime(out["date"]).dt.tz_localize(None).dt.normalize()
    for c in ("open", "high", "low", "close", "adj_close"):
        if c in out.columns:
            out[c] = out[c].astype("float32")
    if "volume" in out.columns:
        out["volume"] = out["volume"].fillna(0).astype("int64")
    return out


def fetch_fred():
    frames = []
    for sid in FRED:
        url = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=%s" % sid
        for attempt in range(1, RETRIES + 1):
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (research-monster relay)"})
                raw = urllib.request.urlopen(req, timeout=60).read().decode("utf-8", "replace")
                df = pd.read_csv(io.StringIO(raw))
                df.columns = ["date", sid]
                df["date"] = pd.to_datetime(df["date"])
                df[sid] = pd.to_numeric(df[sid], errors="coerce")
                frames.append(df.set_index("date"))
                print("  fred %s: %d rows" % (sid, len(df)), flush=True)
                break
            except Exception as e:
                print("  fred %s attempt %d failed: %s" % (sid, attempt, e), flush=True)
                time.sleep(SLEEP)
    if not frames:
        return None
    m = pd.concat(frames, axis=1).sort_index()
    m = m[m.index >= "2010-01-01"]
    return m


def main():
    tk = [l.strip().upper() for l in open(os.path.join(HERE, "tickers.txt")) if l.strip() and not l.startswith("#")]
    tk = sorted(set(tk) | set(EXTRA))
    ysyms = [ymap(t) for t in tk]
    back = {ymap(t): t for t in tk}
    now = datetime.datetime.now(datetime.timezone.utc)
    print("yfinance", getattr(yf, "__version__", "?"), "| daily history for", len(tk), "tickers", flush=True)

    got = {}
    for i in range(0, len(ysyms), BATCH):
        chunk = ysyms[i:i + BATCH]
        df = _dl(chunk, period=PERIOD, interval="1d")
        if df is not None:
            lg = _long(df, chunk, back)
            for t, sub in lg.groupby("ticker"):
                if len(sub) >= 60:
                    got[t] = sub
        print("  daily %d-%d: running %d/%d" % (i, i + len(chunk), len(got), len(tk)), flush=True)
        time.sleep(SLEEP)

    frac = len(got) / max(1, len(tk))
    if frac < MIN_FRESH_FRAC:
        print("DAILY GUARD TRIPPED: only %d/%d (%.0f%%) priced — below %.0f%% floor. NOT writing. Exiting non-zero."
              % (len(got), len(tk), 100 * frac, 100 * MIN_FRESH_FRAC), flush=True)
        sys.exit(1)

    # clear old chunks so a shrinking universe never leaves stale files behind
    for f in os.listdir(OUTDIR):
        if f.endswith(".parquet"):
            os.remove(os.path.join(OUTDIR, f))

    names = sorted(got)
    manifest = {"generated_at": now.strftime("%Y-%m-%d %H:%M UTC"), "period": PERIOD, "n_tickers": len(names),
                "gaps": sorted(t for t in tk if t not in got), "chunks": [], "fields": ["date", "ticker", "open", "high", "low", "close", "adj_close", "volume"],
                "adjustment": "adj_close = dividends+splits adjusted (yfinance auto_adjust=False); open/high/low/close are split-adjusted raw prints"}
    total_rows = 0
    for ci in range(0, len(names), CHUNK):
        part = names[ci:ci + CHUNK]
        df = pd.concat([got[t] for t in part]).sort_values(["ticker", "date"]).reset_index(drop=True)
        fn = "bars_%02d.parquet" % (ci // CHUNK)
        df.to_parquet(os.path.join(OUTDIR, fn), index=False, compression="zstd")
        sz = os.path.getsize(os.path.join(OUTDIR, fn))
        manifest["chunks"].append({"file": fn, "tickers": part, "rows": int(len(df)), "bytes": sz,
                                   "first_date": str(df["date"].min().date()), "last_date": str(df["date"].max().date())})
        total_rows += len(df)
        print("  wrote %s: %d tickers, %d rows, %.1f MB" % (fn, len(part), len(df), sz / 1e6), flush=True)
    manifest["rows"] = total_rows

    manifest["macro"] = "see fetch_fred.py / macro_manifest.json (decoupled 2026-09-16)"

    json.dump(manifest, open(os.path.join(OUTDIR, "manifest.json"), "w"), indent=1)
    print("daily: %d/%d tickers, %d rows, gaps=%d" % (len(names), len(tk), total_rows, len(manifest["gaps"])), flush=True)


if __name__ == "__main__":
    main()
