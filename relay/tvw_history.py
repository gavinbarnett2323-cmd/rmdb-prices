# -*- coding: utf-8 -*-
"""tvw_history.py — ONE-OFF research run (2026-09-24): Gavin's weekly TradingView signals over 2018-2026 computed the
way his chart computes them, with the FULL price history behind every bar.

Why: the backtest panel starts 2014-09, so on weekly bars the distance-from-200-week z-score (distZ, a v20/v22
component and a bottom-regime route) was undefined until ~2023-05 for every name. TradingView has decades of history,
so on his chart distZ is live. This computes the weekly layer twice from a fresh yfinance pull:
  real   daily history from 2005-01-01 (distZ live from ~2014 on: what his chart shows)
  panel  daily history from 2014-09-16 (the backtest panel's start: must reproduce the backtest's weekly cache)
and writes relay/tvw_history.json.gz (weeks from 2018-01 on) so the vault backtest can grade the chart-faithful version
against the one it already graded. Deployed via .github/workflows/tvw_history.yml (manual / on push of this file).
"""
import os, sys, json, gzip, time, datetime
import numpy as np, pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import tv_weekly as TW
import fetch_tail as FT

OUT = os.path.join(HERE, "tvw_history.json.gz")
FROM_WEEK = pd.Period("2018-01-05", freq="W-FRI")


def download(tk, start):
    ys = [FT.ymap(t) for t in tk]; back = {FT.ymap(t): t for t in tk}
    fr = {k: {} for k in ("adj", "close", "open", "high", "low", "vol")}
    for i in range(0, len(ys), FT.BATCH):
        chunk = ys[i:i + FT.BATCH]
        df = FT._dl(chunk, start=start, interval="1d")
        if df is not None:
            for y in chunk:
                a = FT._col(df, "Adj Close", y)
                if a is None or a.dropna().shape[0] < 60:
                    continue
                t = back.get(y, y)
                for k, f in (("adj", "Adj Close"), ("close", "Close"), ("open", "Open"), ("high", "High"), ("low", "Low"), ("vol", "Volume")):
                    s = FT._col(df, f, y)
                    if s is not None:
                        s = s.copy(); s.index = pd.to_datetime(s.index).tz_localize(None).normalize()
                        fr[k][t] = s[~s.index.duplicated(keep="last")]
        print("  %s %d-%d: %d priced" % (start, i, i + len(chunk), len(fr["adj"])), flush=True)
        time.sleep(FT.SLEEP)
    return fr


def run(fr, start_label):
    adj = pd.DataFrame(fr["adj"]).sort_index()
    cal = adj.index[adj["SPY"].notna()]
    adj = adj.loc[cal]
    raw = pd.DataFrame(fr["close"]).reindex(index=cal, columns=adj.columns)
    f = adj / raw
    o = pd.DataFrame(fr["open"]).reindex(index=cal, columns=adj.columns) * f
    h = pd.DataFrame(fr["high"]).reindex(index=cal, columns=adj.columns) * f
    l = pd.DataFrame(fr["low"]).reindex(index=cal, columns=adj.columns) * f
    v = pd.DataFrame(fr["vol"]).reindex(index=cal, columns=adj.columns)
    ctx = TW.build_ctx(adj)
    names = [t for t in adj.columns if t not in TW.BENCH]
    weeks = pd.period_range(FROM_WEEK, cal[-1].to_period("W-FRI"), freq="W-FRI")
    widx = {p: i for i, p in enumerate(weeks)}
    out = {}
    t0 = time.time()
    for n_, t in enumerate(names):
        sig = TW.weekly_signals(cal, o[t].values, h[t].values, l[t].values, adj[t].values, v[t].values, ctx)
        if sig is None:
            continue
        per = sig["period"]
        keep = np.array([p >= FROM_WEEK for p in per])
        if not keep.any():
            continue
        k0 = int(np.argmax(keep))
        i0 = widx[per[k0]]
        sl = slice(k0, len(per))
        ok = np.asarray(sig["ok"][sl], bool)
        rec = {"i0": i0, "n": int(len(per) - k0),
               "ok": "".join("1" if x else "0" for x in ok),
               "bs": "".join(format(min(int(x), 15), "x") for x in np.asarray(sig["v22|botScore"][sl]).astype(int)),
               "ts": "".join(format(min(int(x), 15), "x") for x in np.asarray(sig["v22|topScore"][sl]).astype(int)),
               "br": "".join("1" if x else "0" for x in np.asarray(sig["v22|botRegimeOk"][sl], bool)),
               "tr": "".join("1" if x else "0" for x in np.asarray(sig["v22|topRegimeOk"][sl], bool)),
               "aq": "".join("1" if x else "0" for x in np.asarray(sig["v22|adxQuiet"][sl], bool))}
        for key, nm in (("v20|botFire", "b20"), ("v22|botFire", "b22"), ("v20|topFire", "t20"), ("v22|topFire", "t22")):
            rec[nm] = [int(x) for x in np.nonzero(np.asarray(sig[key][sl], bool))[0]]
        out[t] = rec
        if n_ % 200 == 0:
            print("  [%s] %d/%d names, %.0fs" % (start_label, n_, len(names), time.time() - t0), flush=True)
    return {"start": start_label, "cal0": str(cal[0].date()), "cal1": str(cal[-1].date()), "n_sessions": int(len(cal)),
            "weeks": [str(p.end_time.date()) for p in weeks], "tickers": out}


def main():
    tk = [x.strip().upper() for x in open(os.path.join(HERE, "tickers.txt")) if x.strip() and not x.startswith("#")]
    tk = sorted(set(tk) | set(FT.EXTRA))
    print("tvw history for", len(tk), "tickers", flush=True)
    doc = {"_doc": __doc__, "generated_at": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
           "distz_warmup": TW.DISTZ_WARMUP}
    for label, start in (("real", "2005-01-01"), ("panel", "2014-09-16")):
        fr = download(tk, start)
        if "SPY" not in fr["adj"] or len(fr["adj"]) < 0.6 * len(tk):
            print("GUARD: only %d/%d priced for %s: not writing" % (len(fr["adj"]), len(tk), label), flush=True)
            sys.exit(1)
        doc[label] = run(fr, label)
        doc[label]["gaps"] = sorted(t for t in tk if t not in fr["adj"])
        del fr
    with gzip.open(OUT, "wt", encoding="utf-8") as fh:
        json.dump(doc, fh, separators=(",", ":"))
    print("wrote %s %.1f MB" % (OUT, os.path.getsize(OUT) / 1e6), flush=True)


if __name__ == "__main__":
    main()
