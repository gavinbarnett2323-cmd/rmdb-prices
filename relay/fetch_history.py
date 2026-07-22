# -*- coding: utf-8 -*-
"""fetch_history.py — WEEKLY history emitter for the Emergence Scanner (Research Monster, W1.1).

Runs on a GitHub Actions runner (real egress) alongside fetch_prices.py. Emits relay/history.json:
per ticker 26 weekly closes (plus an extended series used ONLY for backruns), 52-week high AND low,
20d average dollar volume, and a 20d-vs-trailing volume ratio.

Why weekly: the scanner is a rotation/basing detector on a multi-week horizon. Daily noise produces
false flags; weekly closes are the honest resolution for "is this group setting up".

Why 26 (not 13): the oversold/basing detector needs downtrend CONTEXT — you cannot tell "washed out
and stabilizing" from "still falling" with one quarter of bars.

52w high/low are computed from WEEKLY CLOSES, not intraday extremes. That is deliberate: "% of members
within 3% of the 52w high" should not be satisfied by a single intraday spike print. Documented so the
consumer never mistakes it for a Yahoo 52w-high field.

Honesty rails (same family as fetch_prices.py):
  (1) CHUNKED + RETRIED — never one fragile mega-call.
  (2) EMPTY-WRITE GUARD — if coverage < MIN_FRESH_FRAC, REFUSE to overwrite the last-good file and
      exit non-zero so the Action goes RED. A thin history silently replacing a good one would make
      every downstream detector wrong while looking fine.
  (3) `gaps` array — names we could NOT price are NAMED in the output, never interpolated, never dropped
      silently. The scanner reports coverage from this.

CANONICAL COPY lives in the vault at Investing/engine/relay/fetch_history.py; the deployed copy is
relay/fetch_history.py in github.com/gavinbarnett2323-cmd/rmdb-prices. Keep them in sync.
"""
import json, os, sys, time, datetime
import yfinance as yf
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "history.json")

YMAP = {"BRKB": "BRK-B", "BFB": "BF-B"}
def ymap(t): return YMAP.get(t, t)

BATCH = 120
RETRIES = 3
SLEEP = 2.0
MIN_FRESH_FRAC = 0.60
WEEKLY_KEEP = 26        # the contract: what the detectors read
WEEKLY_EXT_KEEP = 160   # backrun-only extended series (~3y)
MIN_BARS = 30           # a ticker needs at least this many weekly bars to be usable at all


def _dl(syms, **kw):
    for attempt in range(1, RETRIES + 1):
        try:
            df = yf.download(syms, progress=False, threads=True, auto_adjust=False, **kw)
            if df is None or getattr(df, "empty", True):
                raise ValueError("empty frame")
            return df
        except Exception as e:
            print("  batch attempt %d/%d failed: %s" % (attempt, RETRIES, e), flush=True)
            time.sleep(SLEEP)
    return None


def _complete_weeks(series, today):
    """Drop any trailing bar whose week has NOT finished.

    yfinance labels a weekly bar with the week's MONDAY. A run on Tuesday therefore returns a final
    "week" containing one day of trading. Feeding that to the detectors would read a single day's
    move as a full weekly bar — a silent, systematic error in every RS and stabilization test. A week
    is complete once its Friday (Monday+4) is strictly in the past.
    """
    out = list(series)
    while out:
        try:
            wk = datetime.date.fromisoformat(out[-1][0])
        except Exception:
            break
        if wk + datetime.timedelta(days=4) < today:
            break
        out.pop()
    return out


def _col(df, field, syms):
    """Extract a {symbol: Series} map for one OHLCV field from a single- or multi-index frame."""
    if isinstance(df.columns, pd.MultiIndex):
        try:
            sub = df[field]
        except KeyError:
            return {}
        return {s: sub[s] for s in sub.columns}
    if field in df.columns:
        return {syms[0]: df[field]}
    return {}


def main():
    tk = [l.strip().upper() for l in open(os.path.join(HERE, "tickers.txt"))
          if l.strip() and not l.startswith("#")]
    ysyms = [ymap(t) for t in tk]
    back = {ymap(t): t for t in tk}
    now = datetime.datetime.now(datetime.timezone.utc)
    print("yfinance", getattr(yf, "__version__", "?"), "| history for", len(tk), "tickers", flush=True)

    weekly = {}    # our-ticker -> list[(date, close)]
    wvol = {}      # our-ticker -> list[weekly volume] aligned to weekly[t]
    for i in range(0, len(ysyms), BATCH):
        chunk = ysyms[i:i + BATCH]
        df = _dl(chunk, period="3y", interval="1wk")
        if df is not None:
            closes = _col(df, "Close", chunk)
            vols = _col(df, "Volume", chunk)
            for ys, s in closes.items():
                t = back.get(ys)
                if not t:
                    continue
                s = s.dropna()
                bars = _complete_weeks(
                    [(idx.strftime("%Y-%m-%d"), round(float(v), 4)) for idx, v in s.items()],
                    now.date())
                if len(bars) >= MIN_BARS:
                    weekly[t] = bars
                    # WEEKLY volume aligned to the same bars. This exists so a BACKRUN can evaluate the
                    # same volume gate the live run does. Without it the backrun would have to skip the
                    # volume condition (looser than live) or invent history (never). Aligning by date
                    # keeps the two paths honest and identical.
                    sv = vols.get(ys)
                    if sv is not None:
                        m = {idx.strftime("%Y-%m-%d"): (None if pd.isna(v) else float(v))
                             for idx, v in sv.items()}
                        wvol[t] = [m.get(d) for d, _ in bars]
        print("  weekly %d-%d: running %d/%d" % (i, i + len(chunk), len(weekly), len(tk)), flush=True)
        time.sleep(SLEEP)

    vol = {}       # our-ticker -> (adv20_usd, vol_ratio)
    for i in range(0, len(ysyms), BATCH):
        chunk = ysyms[i:i + BATCH]
        df = _dl(chunk, period="1y", interval="1d")
        if df is not None:
            vv = _col(df, "Volume", chunk)
            cc = _col(df, "Close", chunk)
            for ys, sv in vv.items():
                t = back.get(ys)
                if not t or ys not in cc:
                    continue
                sv = sv.dropna(); sc = cc[ys].dropna()
                if len(sv) < 40:
                    continue
                last20 = sv.iloc[-20:]
                prior = sv.iloc[-120:-20] if len(sv) > 40 else sv.iloc[:-20]
                try:
                    adv = float((sc.iloc[-20:] * last20).mean())
                    ratio = float(last20.mean() / prior.mean()) if float(prior.mean()) > 0 else None
                except Exception:
                    adv, ratio = None, None
                vol[t] = (None if adv is None else round(adv, 0),
                          None if ratio is None else round(ratio, 3))
        print("  volume %d-%d: running %d/%d" % (i, i + len(chunk), len(vol), len(tk)), flush=True)
        time.sleep(SLEEP)

    frac = len(weekly) / max(1, len(tk))
    if frac < MIN_FRESH_FRAC:
        print("HISTORY GUARD TRIPPED: only %d/%d (%.0f%%) have usable weekly bars — below %.0f%% floor. "
              "NOT overwriting history.json. Exiting non-zero."
              % (len(weekly), len(tk), 100 * frac, 100 * MIN_FRESH_FRAC), flush=True)
        sys.exit(1)

    # as_of = the newest weekly bar date shared by the bulk of the universe (the last settled week).
    last_dates = [v[-1][0] for v in weekly.values() if v]
    as_of = max(set(last_dates), key=last_dates.count) if last_dates else None

    out = {"as_of": as_of,
           "generated_at": now.strftime("%Y-%m-%d %H:%M UTC"),
           "bars_weekly": WEEKLY_KEEP,
           "hi_lo_basis": "weekly closes (NOT intraday extremes)",
           "n_tickers": len(weekly),
           "tickers": {},
           "gaps": sorted(t for t in tk if t not in weekly)}

    for t, series in weekly.items():
        closes = [c for _, c in series]
        last52 = closes[-52:] if len(closes) >= 52 else closes
        adv, ratio = vol.get(t, (None, None))
        wv = wvol.get(t) or []
        out["tickers"][t] = {
            "last_bar": series[-1][0],
            "w": closes[-WEEKLY_KEEP:],
            "w_ext": closes[-WEEKLY_EXT_KEEP:],
            "d_ext": [d for d, _ in series[-WEEKLY_EXT_KEEP:]],
            "v_ext": [None if v is None else round(v, 0) for v in wv[-WEEKLY_EXT_KEEP:]],
            "hi52": round(max(last52), 4) if last52 else None,
            "lo52": round(min(last52), 4) if last52 else None,
            "adv20_usd": adv,
            "vol_ratio": ratio,
        }

    json.dump(out, open(OUT, "w"), separators=(",", ":"))
    print("history: %d/%d tickers, as_of=%s, gaps=%d, bytes=%d"
          % (len(weekly), len(tk), as_of, len(out["gaps"]), os.path.getsize(OUT)), flush=True)


if __name__ == "__main__":
    main()
