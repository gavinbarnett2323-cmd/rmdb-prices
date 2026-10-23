#!/usr/bin/env python3
"""research_charts.py (2026-10-23): full daily history for the chart-only tickers in relay/chart_extra.txt.

Gavin, 2026-10-23: "expand the number of etfs we have on the charting, for example include all of the latin america ones and
just way more". The vault's Charts page stores each ticker's whole history and appends new sessions every weekday from
chart_tail.json; a ticker it has never stored needs its history once. This job fetches it with yfinance on the runner (the
vault and Claude's sandbox cannot reach Yahoo) and writes relay/research/charts_history.json.gz:
  {"built", "source", "symbols": {sym: {"first", "last", "n", "d": [YYYY-MM-DD...], "o", "h", "l", "c", "ac", "v": [...]}},
   "errors": {sym: msg}}
c = raw close (split-adjusted only), ac = dividend-adjusted close, o/h/l raw. Also fetches a few tickers the page already
carries (PARITY) so the vault can check its builder against the stored cloud build. Public prices only: nothing about any
script, book or account. Triggered by a push of this file, chart_extra.txt or the workflow, or by hand
(workflow research-charts); never scheduled."""
import datetime, gzip, json, math, os, time

import pandas as pd
import yfinance as yf

HERE = os.path.dirname(os.path.abspath(__file__))
LIST = os.path.join(HERE, "chart_extra.txt")
OUT = os.path.join(HERE, "research", "charts_history.json.gz")
PARITY = ["SPY", "XLE", "SMH", "GLD", "TLT", "QQQ"]


def num(x, nd=6):
    try:
        x = float(x)
    except Exception:
        return None
    return round(x, nd) if math.isfinite(x) else None


def main():
    names = [l.strip().upper() for l in open(LIST) if l.strip() and not l.startswith("#")]
    syms = list(dict.fromkeys(names + PARITY))
    out = {"built": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%MZ"), "source": "yfinance (Yahoo) on GitHub Actions",
           "symbols": {}, "errors": {}}
    for sym in syms:
        df = None
        for attempt in range(3):
            try:
                df = yf.Ticker(sym).history(period="max", interval="1d", auto_adjust=False, actions=False)
                if df is not None and len(df):
                    break
            except Exception as e:
                out["errors"][sym] = "%s: %s" % (type(e).__name__, str(e)[:160])
            time.sleep(2 + 3 * attempt)
        if df is None or not len(df):
            out["errors"].setdefault(sym, "no data")
            continue
        df = df.dropna(subset=["Close"])
        idx = [d.strftime("%Y-%m-%d") for d in pd.DatetimeIndex(df.index).tz_localize(None)]
        rec = {"first": idx[0], "last": idx[-1], "n": len(idx), "d": idx}
        for col, key in (("Open", "o"), ("High", "h"), ("Low", "l"), ("Close", "c"), ("Adj Close", "ac"), ("Volume", "v")):
            rec[key] = [num(x) for x in df[col]] if col in df.columns else None
        out["symbols"][sym] = rec
        out["errors"].pop(sym, None)
        print("%-6s %5d bars %s .. %s" % (sym, len(idx), idx[0], idx[-1]), flush=True)
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    tmp = OUT + ".tmp"
    with gzip.open(tmp, "wt") as fh:
        json.dump(out, fh, separators=(",", ":"))
    os.replace(tmp, OUT)
    print("wrote %s: %d symbols, %d errors %s" % (OUT, len(out["symbols"]), len(out["errors"]), sorted(out["errors"])), flush=True)


if __name__ == "__main__":
    main()
