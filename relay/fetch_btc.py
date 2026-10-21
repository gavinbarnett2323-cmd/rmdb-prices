"""BTC daily relay (2026-10-20) for the Stock System dashboard's Bitcoin chart.

Gavin, 2026-10-20: "graph out btc full price history into the charts section". The dashboard refreshes
every weekday from this repo. This job gives it Bitcoin's whole daily close history from Yahoo (BTC-USD,
which starts 2014-09-17), every calendar day, because Bitcoin trades on weekends and the equity relays
only keep stock-market sessions.

Writes relay/btc_daily.json: {_doc, generated_at, source, first, last, n, rows: [[date, close], ...],
ohlc: [[date, open, high, low, close, volume], ...]} (ohlc added 2026-10-21 for the Charts.html BTC candles).
Rails: today's UTC bar is still forming, so it is dropped. Under 4,000 days, or a newest bar more than
4 days old, it writes nothing and exits non-zero so the Action goes red. Display only: nothing on the
decision path reads this file.
"""
import datetime
import json
import os
import time

import pandas as pd
import yfinance as yf

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "btc_daily.json")


def fetch():
    err = None
    for attempt in range(4):
        try:
            df = yf.Ticker("BTC-USD").history(period="max", interval="1d", auto_adjust=False, actions=False)
            if df is not None and len(df) > 4000:
                return df
            err = "only %d rows" % (0 if df is None else len(df))
        except Exception as e:
            err = "%s: %s" % (type(e).__name__, str(e)[:200])
        time.sleep(3 + 5 * attempt)
    raise SystemExit("btc relay: no usable data (%s)" % err)


def main():
    df = fetch().dropna(subset=["Close"])
    idx = pd.DatetimeIndex(df.index)
    if idx.tz is not None:
        idx = idx.tz_convert("UTC").tz_localize(None)
    today = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    by_day, ohlc = {}, {}
    vol = df["Volume"] if "Volume" in df.columns else [0] * len(df)
    for d, o, h, l, c, v in zip(idx, df["Open"], df["High"], df["Low"], df["Close"], vol):
        day = d.strftime("%Y-%m-%d")
        if day < today:
            by_day[day] = round(float(c), 2)
            if o == o and h == h and l == l:       # 2026-10-21: full daily bars for the Charts.html BTC candles + v20
                ohlc[day] = [round(float(o), 2), round(float(h), 2), round(float(l), 2), round(float(c), 2), int(v) if v == v else 0]
    rows = [[d, by_day[d]] for d in sorted(by_day)]
    if len(rows) < 4000:
        raise SystemExit("btc relay: refusing to write %d rows" % len(rows))
    if (datetime.date.fromisoformat(today) - datetime.date.fromisoformat(rows[-1][0])).days > 4:
        raise SystemExit("btc relay: newest bar %s is stale, writing nothing" % rows[-1][0])
    out = {
        "_doc": "BTC/USD daily close, every calendar day, from Yahoo BTC-USD. Today's forming bar is left out. "
                "Written by relay/fetch_btc.py for the Stock System dashboard. Display only.",
        "generated_at": datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        "source": "Yahoo BTC-USD daily close via yfinance %s on GitHub Actions" % getattr(yf, "__version__", "?"),
        "first": rows[0][0], "last": rows[-1][0], "n": len(rows), "rows": rows,
        "ohlc_cols": ["date", "open", "high", "low", "close", "volume"], "ohlc": [[k] + ohlc[k] for k in sorted(ohlc)],
    }
    tmp = OUT + ".tmp"
    with open(tmp, "w") as f:
        json.dump(out, f, separators=(",", ":"))
    os.replace(tmp, OUT)
    print("btc relay: %d days %s .. %s, last close %.2f" % (len(rows), rows[0][0], rows[-1][0], rows[-1][1]))


if __name__ == "__main__":
    main()
