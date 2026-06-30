# -*- coding: utf-8 -*-
"""Price relay — runs on a GitHub Actions runner (which HAS real internet egress, unlike the Claude sandbox).
Fetches EOD closes for the trigger set in tickers.txt from FREE, no-key, BATCHED sources (Stooq primary,
Yahoo fallback) and writes prices.json as a bare list [{"symbol","price","asof"}] — the exact shape
`sweep_prices.py --from-file` consumes. Zero API keys. The Claude price-sweep task then just web_fetches the
committed prices.json (one static raw.githubusercontent.com read) — near-zero usage, no flaky per-ticker calls.
Run locally too if you have egress: python3 fetch_prices.py"""
import json, os, sys, time, urllib.request, datetime, csv, io

HERE = os.path.dirname(os.path.abspath(__file__))
UA = {"User-Agent": "Mozilla/5.0 (price-relay; +github-actions)"}

def _get(url, timeout=25):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")

def stooq_sym(t):  # AAPL->aapl.us ; BRK.B->brk-b.us
    return t.replace(".", "-").lower() + ".us"

def fetch_stooq(tickers):
    """Batch CSV: one request per ~40 tickers. Returns {TICKER: close}."""
    out = {}
    for i in range(0, len(tickers), 40):
        chunk = tickers[i:i+40]
        s = ",".join(stooq_sym(t) for t in chunk)
        url = "https://stooq.com/q/l/?s=%s&f=sd2t2ohlcv&h&e=csv" % s
        try:
            txt = _get(url)
            for row in csv.DictReader(io.StringIO(txt)):
                sym = (row.get("Symbol") or "").upper().replace(".US", "").replace("-", ".")
                close = row.get("Close")
                if sym and close not in (None, "", "N/D"):
                    try: out[sym] = float(close)
                    except ValueError: pass
        except Exception as e:
            print("stooq chunk %d err: %s" % (i, e), file=sys.stderr)
        time.sleep(0.6)
    return out

def fetch_yahoo(tickers):
    """Fallback for names Stooq missed. Batched quote endpoint. Returns {TICKER: price}."""
    out = {}
    for i in range(0, len(tickers), 40):
        chunk = tickers[i:i+40]
        url = "https://query1.finance.yahoo.com/v7/finance/quote?symbols=" + ",".join(chunk)
        try:
            j = json.loads(_get(url))
            for q in (j.get("quoteResponse", {}).get("result") or []):
                p = q.get("regularMarketPrice") or q.get("regularMarketPreviousClose")
                if q.get("symbol") and p is not None:
                    out[q["symbol"].upper()] = float(p)
        except Exception as e:
            print("yahoo chunk %d err: %s" % (i, e), file=sys.stderr)
        time.sleep(0.6)
    return out

def main():
    tk = [l.strip().upper() for l in open(os.path.join(HERE, "tickers.txt")) if l.strip() and not l.startswith("#")]
    px = fetch_stooq(tk)
    missing = [t for t in tk if t not in px]
    if missing:
        px.update({k: v for k, v in fetch_yahoo(missing).items() if k in set(tk)})
    asof = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    rows = [{"symbol": t, "price": round(px[t], 2), "asof": asof} for t in tk if t in px]
    json.dump(rows, open(os.path.join(HERE, "prices.json"), "w"), indent=0)
    still = [t for t in tk if t not in px]
    print("relay: %d/%d priced (stooq+yahoo) as of %s | missing: %s"
          % (len(rows), len(tk), asof, ",".join(still) or "none"))

if __name__ == "__main__":
    main()
