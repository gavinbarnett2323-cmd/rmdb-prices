# -*- coding: utf-8 -*-
"""fetch_options.py — OPTIONS relay. Runs on a GitHub Actions runner (real egress), free, no key.

Same architecture as fetch_prices.py: the sandbox has no egress, so the fetch happens where egress
exists and the vault reads ONE static committed file.

WHY IT MATTERS THAT THIS RUNS EVERY DAY
---------------------------------------
There is no free archive of historical option quotes. None. So this file's daily commits ARE the
archive — the [sourced] tier in the vault's options lane literally does not exist for any date before
this workflow's first successful run. A day skipped is a day permanently missing. That is why the
empty-write guard below refuses to overwrite a good file with a thin one: a silent partial write would
put a hole in a record that can never be backfilled from anywhere.

WHAT IT WRITES  ->  relay/options.json
  {"as_of","generated_at","n_tickers","tickers":{T:{"spot","expiries":[...],"chains":{exp:{"calls":[
     {"strike","bid","ask","mid","last","iv","volume","oi"}]}}}},"gaps":[...]}

Only CALLS, and only the expiries the vault's three standard contracts need (~6mo and ~12mo), because
a full chain for 25 names is tens of MB per day and 99% of it is never read. Puts are deliberately out
of scope until something asks for them.
"""
import json, os, datetime, sys, time

try:
    from zoneinfo import ZoneInfo
    _ET = ZoneInfo("America/New_York")
except Exception:
    _ET = None

import yfinance as yf


def _market_today():
    # as_of must be the US MARKET DAY, not the runner's UTC day. The runner is UTC; after ~20:00 ET the UTC
    # date rolls to tomorrow while the trading day is still today, which stamps a "future" date the vault's
    # freshness guard then rejects. Anchor to America/New_York so the quote's date is the day it was quoted.
    if _ET is not None:
        return datetime.datetime.now(_ET).date()
    return datetime.date.today()

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "options.json")
WATCH = os.path.join(HERE, "options_tickers.txt")

TARGET_DAYS = [182, 365]      # the two tenors the vault's standard contracts live on
NEAR_SPOT = 0.55              # keep strikes within +/-55% of spot; beyond that nothing trades
MIN_FRESH_FRAC = 0.60         # empty-write guard: below this we refuse to overwrite last-good
YMAP = {"BRKB": "BRK-B", "BFB": "BF-B"}


def ymap(t):
    return YMAP.get(t, t)


def pick_expiries(expiries, today):
    """The listed expiry nearest each target tenor. Chains list what they list; we do not invent dates."""
    out = []
    for want in TARGET_DAYS:
        best, bd = None, None
        for e in expiries:
            try:
                d = (datetime.date.fromisoformat(e) - today).days
            except Exception:
                continue
            if d < 30:                       # too short to be either of our contracts
                continue
            if bd is None or abs(d - want) < bd:
                best, bd = e, abs(d - want)
        if best and best not in out:
            out.append(best)
    return out


def main():
    today = _market_today()
    tk = [l.strip().upper() for l in open(WATCH) if l.strip() and not l.startswith("#")]
    asof = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print("options relay | yfinance %s | %d tickers" % (getattr(yf, "__version__", "?"), len(tk)), flush=True)

    out, gaps = {}, []
    for t in tk:
        try:
            y = yf.Ticker(ymap(t))
            spot = None
            try:
                spot = float(y.fast_info["last_price"])
            except Exception:
                h = y.history(period="5d")
                if len(h):
                    spot = float(h["Close"].iloc[-1])
            if not spot or spot <= 0:
                gaps.append([t, "no spot"]); continue
            exps = pick_expiries(list(y.options or []), today)
            if not exps:
                gaps.append([t, "no usable expiry"]); continue
            chains = {}
            for e in exps:
                try:
                    c = y.option_chain(e).calls
                except Exception as ex:
                    gaps.append([t, "chain %s: %s" % (e, str(ex)[:60])]); continue
                rows = []
                for _, r in c.iterrows():
                    k = float(r["strike"])
                    if not (spot * (1 - NEAR_SPOT) <= k <= spot * (1 + NEAR_SPOT)):
                        continue
                    bid = float(r["bid"]) if r["bid"] == r["bid"] else None
                    ask = float(r["ask"]) if r["ask"] == r["ask"] else None
                    mid = round((bid + ask) / 2, 2) if (bid and ask and ask > 0) else None
                    rows.append({"strike": k, "bid": bid, "ask": ask, "mid": mid,
                                 "last": (float(r["lastPrice"]) if r["lastPrice"] == r["lastPrice"] else None),
                                 "iv": (round(float(r["impliedVolatility"]), 4)
                                        if r["impliedVolatility"] == r["impliedVolatility"] else None),
                                 "volume": (int(r["volume"]) if r["volume"] == r["volume"] else 0),
                                 "oi": (int(r["openInterest"]) if r["openInterest"] == r["openInterest"] else 0)})
                if rows:
                    chains[e] = {"calls": rows}
            if not chains:
                gaps.append([t, "no rows near spot"]); continue
            out[t] = {"spot": round(spot, 2), "expiries": list(chains), "chains": chains}
            print("  %-6s spot %.2f | %s | %d strikes" % (t, spot, ",".join(chains),
                  sum(len(v["calls"]) for v in chains.values())), flush=True)
            time.sleep(0.4)                  # be a polite client; the runner has all the time it needs
        except Exception as ex:
            gaps.append([t, str(ex)[:80]])

    frac = len(out) / float(len(tk) or 1)
    doc = {"as_of": today.isoformat(), "generated_at": asof, "n_tickers": len(out),
           "coverage_frac": round(frac, 3), "target_days": TARGET_DAYS,
           "tickers": out, "gaps": gaps,
           "_doc": ("Daily CALL chains for the vault's BUY-flagged + held names, at the two listed "
                    "expiries nearest 182d and 365d, strikes within +/-55% of spot. THIS FILE'S GIT "
                    "HISTORY IS THE ONLY ARCHIVE OF SOURCED OPTION QUOTES THE SYSTEM WILL EVER HAVE - "
                    "a missed day cannot be backfilled from any free source.")}

    # EMPTY-WRITE GUARD (same rail as fetch_history.py): never let a thin run replace a good file.
    if frac < MIN_FRESH_FRAC and os.path.exists(OUT):
        print("REFUSING TO WRITE: coverage %.0f%% < %.0f%% floor. Last-good options.json left intact. "
              "gaps=%s" % (frac * 100, MIN_FRESH_FRAC * 100, gaps[:10]), flush=True)
        sys.exit(1)
    json.dump(doc, open(OUT, "w"), separators=(",", ":"))
    print("options relay: %d/%d tickers (%.0f%%), %d gaps, as of %s"
          % (len(out), len(tk), frac * 100, len(gaps), asof), flush=True)


if __name__ == "__main__":
    main()
