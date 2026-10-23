# -*- coding: utf-8 -*-
"""chart_factors.py (2026-10-14): dividend factors for the Charts page, so it can draw prices the way TradingView does
with ADJ off (split-adjusted, NOT dividend-adjusted).

Gavin, 2026-10-14: "your emas are different than the ones on tradingview". The chart bundle is dividend-adjusted; his
TradingView is not. For every ticker in relay/chart_tickers.txt this emits factor(t) = Close / Adj Close from Yahoo
(auto_adjust=False), stored only where it changes (ex-dividend dates), so the page can multiply each row's prices by the
factor in force on that day. Writes relay/chart_factors.json.gz. Refuses to overwrite a good file if coverage < 80%.
CANONICAL COPY: Investing/engine/relay/chart_factors.py in the vault; deployed copy relay/chart_factors.py in rmdb-prices."""
import gzip, json, os, sys, time, datetime
import pandas as pd
import yfinance as yf

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "chart_factors.json.gz")
YH = {"BRKB": "BRK-B", "HEIA": "HEI-A", "RBKB": "RBKB"}


def main():
    names = [l.strip() for l in open(os.path.join(HERE, "chart_tickers.txt")) if l.strip() and not l.startswith("#")]
    ex = os.path.join(HERE, "chart_extra.txt")          # 2026-10-23: the chart-only tickers get factors too
    if os.path.exists(ex):
        names += [t for t in dict.fromkeys(l.strip().upper() for l in open(ex) if l.strip() and not l.startswith("#")) if t not in names]
    out, gaps = {}, []
    for k in range(0, len(names), 40):
        chunk = names[k:k + 40]; ys = [YH.get(t, t) for t in chunk]
        df = None
        for attempt in range(3):
            try:
                df = yf.download(ys, period="max", interval="1d", auto_adjust=False, actions=False, group_by="ticker",
                                 progress=False, threads=True)
                break
            except Exception as e:
                print("retry", k, e, flush=True); time.sleep(5)
        for t, y in zip(chunk, ys):
            try:
                d = df[y] if len(ys) > 1 else df
                f = (d["Close"] / d["Adj Close"]).dropna()
                f = f[(f > 0) & (f < 50)]
                if len(f) < 20:
                    gaps.append(t); continue
                pts, last = [], None
                for dt, v in f.items():
                    v = round(float(v), 6)
                    if last is None or abs(v / last - 1) > 2e-6:
                        pts.append([dt.strftime("%Y-%m-%d"), v]); last = v
                out[t] = pts
            except Exception:
                gaps.append(t)
        print("chunk", k, "ok", len(out), "gaps", len(gaps), flush=True)
    if len(out) < 0.8 * len(names):
        print("coverage %d/%d below 80%%, refusing to write" % (len(out), len(names))); sys.exit(2)
    doc = {"as_of": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%MZ"), "n": len(out), "gaps": gaps,
           "_doc": "factor = Yahoo Close / Adj Close (split-adjusted raw over dividend+split adjusted). Each ticker: [[date, factor], ...] "
                   "listed only where the factor changes; the factor holds from that date until the next entry. Multiply an adjusted price "
                   "on date t by factor(t) to get the price TradingView shows with ADJ off.",
           "f": out}
    with gzip.open(OUT, "wt") as fh:
        json.dump(doc, fh, separators=(",", ":"))
    print("wrote", OUT, len(out), "tickers, gaps:", gaps)


if __name__ == "__main__":
    main()
