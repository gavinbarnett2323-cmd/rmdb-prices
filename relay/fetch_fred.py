# -*- coding: utf-8 -*-
"""fetch_fred.py — FRED regime-series emitter for the Research Monster backtest layer (2026-09-16).

Runs on a GitHub Actions runner (real egress). Emits relay/daily/macro.csv: the macro series the regime
multiplier, liquidity framework and Flow Engine read — VIX, HY OAS, curve, real rates, net-liquidity legs.
No API key: uses the public fredgraph.csv endpoint with browser-like headers, falling back to the legacy
/data/<ID>.txt endpoint. A series that fails is NAMED in macro_manifest.json, never interpolated.
"""
import json, os, sys, time, io, urllib.request, datetime
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
OUTDIR = os.path.join(HERE, "daily")
os.makedirs(OUTDIR, exist_ok=True)

FRED = ["VIXCLS", "BAMLH0A0HYM2", "DGS10", "DGS2", "T10Y2Y", "DFII10", "WALCL", "WTREGEN", "RRPONTSYD", "M2SL",
        "CPIAUCSL", "DTWEXBGS", "BAMLC0A0CM", "FEDFUNDS", "UNRATE", "NFCI", "T10YIE", "DGS3MO", "SP500", "BAMLH0A0HYM2EY",
        "VXVCLS", "DCOILWTICO", "DEXJPUS", "TOTRESNS", "SOFR", "T5YIE", "USREC", "ICSA", "PERMIT", "INDPRO"]
HDR = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
       "Accept": "text/csv,text/plain,*/*;q=0.8", "Accept-Language": "en-US,en;q=0.9", "Referer": "https://fred.stlouisfed.org/"}


def _get(url):
    req = urllib.request.Request(url, headers=HDR)
    return urllib.request.urlopen(req, timeout=60).read().decode("utf-8", "replace")


def one(sid):
    errs = []
    # 1) fredgraph.csv
    for attempt in range(3):
        try:
            raw = _get("https://fred.stlouisfed.org/graph/fredgraph.csv?id=%s" % sid)
            df = pd.read_csv(io.StringIO(raw))
            if df.shape[1] >= 2:
                df = df.iloc[:, :2]; df.columns = ["date", sid]
                df["date"] = pd.to_datetime(df["date"], errors="coerce")
                df[sid] = pd.to_numeric(df[sid], errors="coerce")
                df = df.dropna(subset=["date"])
                if len(df) > 10:
                    return df.set_index("date"), None
            errs.append("fredgraph: bad shape %s" % (df.shape,))
        except Exception as e:
            errs.append("fredgraph attempt %d: %s" % (attempt, e)); time.sleep(2 + attempt)
    # 2) legacy text endpoint
    try:
        raw = _get("https://fred.stlouisfed.org/data/%s.txt" % sid)
        lines = [l for l in raw.splitlines() if l[:4].isdigit()]
        rows = []
        for l in lines:
            parts = l.split()
            if len(parts) >= 2:
                try:
                    rows.append((pd.Timestamp(parts[0]), float(parts[1]) if parts[1] != "." else float("nan")))
                except Exception:
                    pass
        if len(rows) > 10:
            df = pd.DataFrame(rows, columns=["date", sid]).set_index("date")
            return df, None
        errs.append("data.txt: %d rows" % len(rows))
    except Exception as e:
        errs.append("data.txt: %s" % e)
    return None, "; ".join(errs)


def main():
    frames, fails = [], {}
    for sid in FRED:
        df, err = one(sid)
        if df is not None:
            frames.append(df); print("  fred %s: %d rows (last %s)" % (sid, len(df), df.index.max().date()), flush=True)
        else:
            fails[sid] = err; print("  fred %s FAILED: %s" % (sid, err), flush=True)
        time.sleep(1.0)
    if not frames:
        print("FRED: nothing fetched — exiting non-zero", flush=True); sys.exit(1)
    m = pd.concat(frames, axis=1).sort_index()
    m = m[m.index >= "2005-01-01"]
    m.to_csv(os.path.join(OUTDIR, "macro.csv"))
    json.dump({"generated_at": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
               "series": list(m.columns), "rows": int(len(m)), "last_date": str(m.index.max().date()), "failed": fails},
              open(os.path.join(OUTDIR, "macro_manifest.json"), "w"), indent=1)
    print("macro.csv: %d series, %d rows, %d failed" % (len(m.columns), len(m), len(fails)), flush=True)


if __name__ == "__main__":
    main()
