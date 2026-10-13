# -*- coding: utf-8 -*-
"""fetch_live.py - INTRADAY LIVE PRICES for the vault's Charts page (2026-10-13).

Gavin, 2026-10-13: "build it out so we can be pulling live data ... we dont need 1 min". So this is not a
minute feed. Every ~15 minutes while the US market is open (live.yml), it takes today's session so far for
every ticker the chart relay carries (relay/chart_tail.json) plus ^VIX, and writes ONE small file:

    live.json = {"as_of": "...Z", "session": "YYYY-MM-DD", "state": "open|closed",
                 "q": {"AAPL": [o, h, l, c, v, prev_close], ...}, "gaps": [...]}

published to the orphan branch `live` (force-pushed, one commit, so main's history never grows). The page reads
https://raw.githubusercontent.com/gavinbarnett2323-cmd/rmdb-prices/live/live.json (CORS open, ~5 min CDN cache).

Honesty rails (same family as fetch_tail.py):
  (1) Batched + retried, never one fragile mega-call.
  (2) Market-hours gate: outside 09:25-16:20 New York on a weekday, or when Yahoo's newest bar is not today
      (a holiday), it writes NOTHING and prints why (the silent-skip law). --force bypasses the clock only.
  (3) Empty-write guard: under MIN_FRAC coverage it refuses to write and exits non-zero (Action goes red).
  (4) Names it could not price are listed in `gaps`, never filled in.
  (5) Display only. Today's bar is provisional: nothing on the decision path reads this file.

Source: Yahoo via yfinance, the same source as every other relay file. Quotes reach the page roughly 15 to 30
minutes after the trade (cron lag + CDN cache). Raw prices, not dividend-adjusted; for the newest session the two
are the same number.
CANONICAL COPY: vault Investing/engine/relay/fetch_live.py. Keep the two in sync.
"""
import json, os, sys, time, datetime, math

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(os.path.dirname(HERE), "live_out", "live.json")
YMAP = {"BRKB": "BRK-B", "BFB": "BF-B", "VIX": "^VIX"}
EXTRA = ["VIX", "SPY", "QQQ", "IWM"]
BATCH, RETRIES, SLEEP, MIN_FRAC = 150, 3, 2.0, 0.6
FORCE = "--force" in sys.argv


def ny_now():
    from zoneinfo import ZoneInfo
    return datetime.datetime.now(ZoneInfo("America/New_York"))


def tickers():
    try:
        tk = list(json.load(open(os.path.join(HERE, "chart_tail.json")))["tickers"].keys())
    except Exception as e:
        print("live: chart_tail.json unreadable (%s), falling back to tickers.txt" % e, flush=True)
        tk = [l.strip().upper() for l in open(os.path.join(HERE, "tickers.txt")) if l.strip() and not l.startswith("#")]
    for x in EXTRA:
        if x not in tk: tk.append(x)
    return tk


def num(x):
    try:
        x = float(x)
    except Exception:
        return None
    if not math.isfinite(x): return None
    a = abs(x)
    return round(x, 2 if a >= 10 else 3 if a >= 1 else 4)


def dl(syms):
    import yfinance as yf
    for attempt in range(1, RETRIES + 1):
        try:
            df = yf.download(syms, period="5d", interval="1d", progress=False, threads=True, auto_adjust=False, group_by="ticker")
            if df is not None and not df.empty: return df
            raise ValueError("empty frame")
        except Exception as e:
            print("  batch attempt %d/%d failed: %s" % (attempt, RETRIES, e), flush=True)
            time.sleep(SLEEP * attempt)
    return None


def main():
    now = ny_now(); today = now.strftime("%Y-%m-%d"); hm = now.hour * 60 + now.minute
    in_hours = now.weekday() < 5 and 9 * 60 + 25 <= hm <= 16 * 60 + 20
    if not in_hours and not FORCE:
        print("live: SKIP, market closed (%s New York); nothing written" % now.strftime("%a %H:%M"), flush=True)
        return 0
    tk = tickers(); q, gaps, sessions = {}, [], {}
    for i in range(0, len(tk), BATCH):
        chunk = tk[i:i + BATCH]; ys = [YMAP.get(t, t) for t in chunk]
        df = dl(ys)
        for t, y in zip(chunk, ys):
            try:
                if df is None: gaps.append(t); continue
                try:
                    d = df[y]
                except KeyError:
                    d = df
                d = d.dropna(subset=["Close"])
                if d.empty: gaps.append(t); continue
                last = d.iloc[-1]; day = d.index[-1].strftime("%Y-%m-%d")
                prev = num(d["Close"].iloc[-2]) if len(d) > 1 else None
                sessions[day] = sessions.get(day, 0) + 1
                q[t] = [day, num(last["Open"]), num(last["High"]), num(last["Low"]), num(last["Close"]),
                        None if not math.isfinite(float(last["Volume"])) else int(last["Volume"]), prev]
            except Exception:
                gaps.append(t)
    session = max(sessions, key=sessions.get) if sessions else None
    if session != today and not FORCE:
        print("live: SKIP, newest Yahoo session is %s, not today %s (holiday?); nothing written" % (session, today), flush=True)
        return 0
    stale = [t for t, v in q.items() if v[0] != session]
    for t in stale: gaps.append(t); q.pop(t)
    frac = len(q) / max(1, len(tk))
    print("live: priced %d of %d (%.0f%%), session %s, %d gaps" % (len(q), len(tk), 100 * frac, session, len(gaps)), flush=True)
    if frac < MIN_FRAC:
        print("live: REFUSING to write, coverage under %.0f%%" % (100 * MIN_FRAC), flush=True)
        return 1
    state = "open" if (now.weekday() < 5 and session == today and 9 * 60 + 30 <= hm < 16 * 60) else "closed"
    out = {"_doc": "Intraday live prices for the vault Charts page. q[T] = [o, h, l, c, v, prev_close] for the session. "
                   "Yahoo via yfinance, 15 to 30 min behind, display only. Written by relay/fetch_live.py.",
           "as_of": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "ny_time": now.strftime("%Y-%m-%d %H:%M"), "session": session, "state": state,
           "n": len(q), "q": {t: v[1:] for t, v in sorted(q.items())}, "gaps": sorted(set(gaps))}
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    json.dump(out, open(OUT, "w"), separators=(",", ":"))
    print("live: wrote %s (%d bytes)" % (OUT, os.path.getsize(OUT)), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
