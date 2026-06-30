# -*- coding: utf-8 -*-
"""Price relay (yfinance) — runs on a GitHub Actions runner (real egress). Batched, FREE, no API key.
Raw Stooq/Yahoo HTTP endpoints 404/401 from datacenter IPs; yfinance handles Yahoo's cookie+crumb and
browser-impersonation. Writes prices.json = [{"symbol","price","asof"}] — the exact shape
sweep_prices.py --from-file consumes. The Claude price-sweep task then just web_fetches the committed
prices.json (one static raw.githubusercontent.com read) — near-zero usage, no flaky calls."""
import json, os, datetime, sys
import yfinance as yf
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
# Yahoo uses '-' where our keys use no separator for share classes (BRK.B -> BRK-B).
YMAP = {"BRKB": "BRK-B", "BFB": "BF-B"}
def ymap(t): return YMAP.get(t, t)

def main():
    tk = [l.strip().upper() for l in open(os.path.join(HERE, "tickers.txt")) if l.strip() and not l.startswith("#")]
    ysyms = [ymap(t) for t in tk]
    asof = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print("yfinance", getattr(yf, "__version__", "?"), "| fetching", len(ysyms), "tickers", flush=True)
    df = yf.download(ysyms, period="5d", interval="1d", progress=False, threads=True, auto_adjust=False)
    print("download df: shape=%s empty=%s multiindex=%s" % (
        getattr(df, "shape", None), getattr(df, "empty", None), isinstance(getattr(df, "columns", None), pd.MultiIndex)), flush=True)
    # Multi-ticker -> columns are a MultiIndex (field, ticker): df["Close"] selects the closes sub-frame.
    # Single-ticker -> simple columns including "Close".
    if isinstance(df.columns, pd.MultiIndex):
        closes = df["Close"]
    elif "Close" in df.columns:
        closes = df[["Close"]].rename(columns={"Close": ysyms[0]})
    else:
        closes = df
    last = closes.ffill().iloc[-1]                          # Series indexed by yahoo symbol
    rows = []
    for t in tk:
        try:
            v = last.get(ymap(t))
            if v is not None and pd.notna(v):
                rows.append({"symbol": t, "price": round(float(v), 2), "asof": asof})
        except Exception:
            pass
    json.dump(rows, open(os.path.join(HERE, "prices.json"), "w"), indent=0)
    got = {r["symbol"] for r in rows}
    missing = [t for t in tk if t not in got]
    print("relay(yfinance): %d/%d priced as of %s | missing(%d): %s"
          % (len(rows), len(tk), asof, len(missing), ",".join(missing[:30]) + ("..." if len(missing) > 30 else "")), flush=True)

if __name__ == "__main__":
    main()
