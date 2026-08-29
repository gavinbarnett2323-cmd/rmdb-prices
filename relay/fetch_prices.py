# -*- coding: utf-8 -*-
"""Price relay (yfinance) — runs on a GitHub Actions runner (real egress). Batched, FREE, no API key.
Raw Stooq/Yahoo HTTP endpoints 404/401 from datacenter IPs; yfinance handles Yahoo's cookie+crumb and
browser-impersonation. Writes prices.json = [{"symbol","price","low","high","asof"}] — the exact shape
sweep_prices.py --from-file consumes. The Claude price-sweep task then just web_fetches the committed
prices.json (one static raw.githubusercontent.com read) — near-zero usage, no flaky calls.

HARDENED 2026-07-06 (Gavin's standing order: "every single one gets run through"). Root cause of the
3-day staleness: a single 1,046-ticker yf.download returned an empty frame (Yahoo throttling / yfinance
break), the old code wrote rows=[] UNCONDITIONALLY, overwrote the last-good prices.json with [], committed
it, and every daily sweep since merged nothing while the header still stamped "now". Fixes:
  (1) CHUNKED + RETRIED fetch (batches, N retries) instead of one fragile mega-call — the actual coverage fix.
  (2) CARRY-FORWARD last-good per ticker — any name that fails THIS run keeps its prior price + honest asof,
      so every ticker always carries a value and nothing silently vanishes from the board.
  (3) EMPTY-WRITE GUARD — if fresh coverage < MIN_FRESH_FRAC (or zero), REFUSE to overwrite the good file
      and sys.exit(1) so the Actions run goes RED and Gavin is notified, instead of silently shipping [].

LOW/HIGH ADDED 2026-08-29 (Gavin caught it: a NOW GTC limit at $124.00 filled, but the board never knew —
it only ever stored ONE close-ish price per ticker per day, so it could not tell that the session's LOW
touched the resting limit, only that the last price it saw was above it). yfinance's same 5d/1d download
already returns Low/High columns at zero extra API cost — this just keeps them instead of discarding them.
Every ticker now carries {price, low, high, asof} so sweep_prices.py can answer "did today's range cross
a resting limit or buy_trigger" retroactively, without any additional fetch or usage.
"""
import json, os, datetime, sys, time
import yfinance as yf
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
PRICES = os.path.join(HERE, "prices.json")

# Yahoo uses '-' where our keys use no separator for share classes (BRK.B -> BRK-B).
YMAP = {"BRKB": "BRK-B", "BFB": "BF-B"}
def ymap(t): return YMAP.get(t, t)

BATCH = 150            # tickers per yf.download call (a 1,046 mega-call is what broke)
RETRIES = 3            # attempts per batch
SLEEP = 2.0            # seconds between retries / batches (be gentle on Yahoo)
MIN_FRESH_FRAC = 0.60  # refuse to publish if fewer than this frac of tickers priced FRESH this run


def _load_last_good():
    """Prior prices.json as {symbol: {"price","low","high","asof"}} for carry-forward. Missing/empty -> {}.
    Back-compat: a prices.json written before 2026-08-29 has no low/high — those come back as None, which
    the touch-detection in sweep_prices.py treats as "no range known" rather than a false touch."""
    try:
        data = json.load(open(PRICES))
        return {r["symbol"]: {"price": r["price"], "low": r.get("low"), "high": r.get("high"),
                               "asof": r.get("asof")}
                for r in data if r.get("price") is not None}
    except Exception:
        return {}


def _fetch_batch(ysyms):
    """Return {yahoo_symbol: {"price","low","high"}} for one batch, with retries. {} on total failure.
    Close/Low/High all come off the SAME yf.download call — pulling all three costs nothing extra."""
    for attempt in range(1, RETRIES + 1):
        try:
            df = yf.download(ysyms, period="5d", interval="1d", progress=False,
                             threads=True, auto_adjust=False)
            if df is None or getattr(df, "empty", True):
                raise ValueError("empty frame")
            if isinstance(df.columns, pd.MultiIndex):
                closes, lows, highs = df["Close"], df["Low"], df["High"]
            elif "Close" in df.columns:
                closes = df[["Close"]].rename(columns={"Close": ysyms[0]})
                lows = df[["Low"]].rename(columns={"Low": ysyms[0]})
                highs = df[["High"]].rename(columns={"High": ysyms[0]})
            else:
                closes = df; lows = df; highs = df
            last_c = closes.ffill().iloc[-1]
            # Low/High are NOT ffilled — a forward-filled range would fabricate a touch that never happened
            # on a day the feed actually had data. Only the SAME (freshest) row's low/high is trusted; a
            # missing row for that symbol just means no range this run (falls through to carry-forward).
            last_l = lows.iloc[-1] if not lows.empty else None
            last_h = highs.iloc[-1] if not highs.empty else None
            out = {}
            for ys in ysyms:
                try:
                    v = last_c.get(ys)
                    if v is None or not pd.notna(v):
                        continue
                    lo = last_l.get(ys) if last_l is not None else None
                    hi = last_h.get(ys) if last_h is not None else None
                    out[ys] = {
                        "price": float(v),
                        "low": float(lo) if lo is not None and pd.notna(lo) else None,
                        "high": float(hi) if hi is not None and pd.notna(hi) else None,
                    }
                except Exception:
                    pass
            if out:
                return out
            raise ValueError("no non-NaN closes")
        except Exception as e:
            print("  batch attempt %d/%d failed: %s" % (attempt, RETRIES, e), flush=True)
            time.sleep(SLEEP)
    return {}


def main():
    tk = [l.strip().upper() for l in open(os.path.join(HERE, "tickers.txt"))
          if l.strip() and not l.startswith("#")]
    asof = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    last_good = _load_last_good()
    print("yfinance", getattr(yf, "__version__", "?"), "| fetching", len(tk),
          "tickers in batches of", BATCH, "| last-good has", len(last_good), "names", flush=True)

    fresh = {}   # our-ticker -> {"price","low","high"} (priced THIS run)
    ysyms_all = [ymap(t) for t in tk]
    for i in range(0, len(ysyms_all), BATCH):
        chunk = ysyms_all[i:i + BATCH]
        got = _fetch_batch(chunk)
        # map yahoo symbols back to our ticker keys
        for t in tk:
            if ymap(t) in got:
                g = got[ymap(t)]
                fresh[t] = {"price": round(g["price"], 2),
                            "low": round(g["low"], 2) if g.get("low") is not None else None,
                            "high": round(g["high"], 2) if g.get("high") is not None else None}
        print("  batch %d-%d: +%d priced (running %d/%d)"
              % (i, i + len(chunk), len(got), len(fresh), len(tk)), flush=True)
        time.sleep(SLEEP)

    frac = len(fresh) / max(1, len(tk))
    # GUARD: never overwrite good data with a bad run. Go RED so Gavin is notified.
    if frac < MIN_FRESH_FRAC:
        print("RELAY GUARD TRIPPED: only %d/%d (%.0f%%) priced fresh — below %.0f%% floor. "
              "NOT overwriting prices.json (keeping last-good). Exiting non-zero."
              % (len(fresh), len(tk), 100 * frac, 100 * MIN_FRESH_FRAC), flush=True)
        sys.exit(1)

    # Build full output: fresh where we have it, carry-forward last-good (with its OLD asof) elsewhere,
    # so EVERY ticker is present. Honest per-ticker asof = never fake a "now" onto a carried price, and a
    # carried row's low/high is the OLD session's range, not today's — still useful for "when did this last
    # touch X", never mistaken for today's touch since asof makes the date explicit.
    rows, carried, no_range = [], 0, 0
    for t in tk:
        if t in fresh:
            f = fresh[t]
            rows.append({"symbol": t, "price": f["price"], "low": f["low"], "high": f["high"], "asof": asof})
            if f["low"] is None or f["high"] is None:
                no_range += 1
        elif t in last_good:
            lg = last_good[t]
            rows.append({"symbol": t, "price": lg["price"], "low": lg.get("low"), "high": lg.get("high"),
                         "asof": lg.get("asof")})
            carried += 1
    json.dump(rows, open(PRICES, "w"), indent=0)
    if no_range:
        print("  note: %d fresh-priced ticker(s) had no low/high this run (price-only fallback)" % no_range,
              flush=True)

    never = [t for t in tk if t not in fresh and t not in last_good]
    print("relay(yfinance): %d fresh + %d carried = %d/%d published as of %s | never-priced(%d): %s"
          % (len(fresh), carried, len(rows), len(tk), asof, len(never),
             ",".join(never[:30]) + ("..." if len(never) > 30 else "")), flush=True)


if __name__ == "__main__":
    main()
