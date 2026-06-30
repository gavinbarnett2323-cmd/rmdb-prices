# -*- coding: utf-8 -*-
"""Price relay (yfinance) — runs on a GitHub Actions runner (real egress). Batched, FREE, no API key.
Raw Stooq/Yahoo HTTP endpoints 404/401 from datacenter IPs; yfinance handles Yahoo's cookie+crumb and
browser-impersonation, so it works from the runner. Writes prices.json = [{"symbol","price","asof"}] —
the exact shape sweep_prices.py --from-file consumes. The Claude price-sweep task then just web_fetches
the committed prices.json (one static raw.githubusercontent.com read) — near-zero usage, no flaky calls."""
import json, os, datetime
import yfinance as yf
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))

# Yahoo uses '-' where our keys use no separator for share classes (BRK.B -> BRK-B). Map the known ones.
YMAP = {"BRKB": "BRK-B", "BFB": "BF-B"}
def ymap(t): return YMAP.get(t, t)

def main():
    tk = [l.strip().upper() for l in open(os.path.join(HERE, "tickers.txt")) if l.strip() and not l.startswith("#")]
    ysyms = [ymap(t) for t in tk]
    asof = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    df = yf.download(ysyms, period="5d", interval="1d", progress=False, threads=True, auto_adjust=False)
    closes = df["Close"] if ("Close" in getattr(df, "columns", [])) else df
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
    print("relay(yfinance): %d/%d priced as of %s | missing: %s"
          % (len(rows), len(tk), asof, ",".join(missing) or "none"))

if __name__ == "__main__":
    main()
