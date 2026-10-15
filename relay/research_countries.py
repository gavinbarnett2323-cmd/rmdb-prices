#!/usr/bin/env python3
"""research_countries.py: one-off research history for the ETF strength study's fresh-universe check (2026-10-15, overnight).

Gavin, 2026-10-14 22:19: "optimize my system run test and variations on everything about this etf hold system". The one
rule change that passed (one pick per tranche) was confirmed on Ken French's 49 US industries; this job adds a universe the
study has never seen: the iShares single-country ETFs that are NOT in the V3 35 (Sweden, Hong Kong, Italy, Belgium,
Switzerland, Malaysia, Netherlands, Austria, Spain, France, Singapore from 1996; South Africa from 2003; the later launches
as they list). yfinance on the runner. Writes relay/research/countries_history.json.gz in the same format as
research_assets.py: {"built", "symbols": {sym: {"name","first","last","n","d","o","h","l","c","ac","v"}}, "errors"}.
c = raw close (split-adjusted only), ac = dividend-adjusted close. Public prices only. Triggered by a push of this file or
by hand (workflow research-countries); never scheduled. Nothing on the decision path reads it."""
import datetime, gzip, json, math, os, sys, time

import pandas as pd
import yfinance as yf

SYMBOLS = {
    "EWD": "Sweden", "EWH": "Hong Kong", "EWI": "Italy", "EWK": "Belgium", "EWL": "Switzerland", "EWM": "Malaysia",
    "EWN": "Netherlands", "EWO": "Austria", "EWP": "Spain", "EWQ": "France", "EWS": "Singapore", "EZA": "South Africa",
    "ECH": "Chile", "TUR": "Turkey", "THD": "Thailand", "EIS": "Israel", "EPU": "Peru", "EIDO": "Indonesia",
    "EPHE": "Philippines", "ENZL": "New Zealand", "EPOL": "Poland", "EZU": "Eurozone", "ILF": "Latin America 40",
    "RSX": "Russia (VanEck, halted 2022)", "ERUS": "Russia (iShares, halted 2022)",
    # reference series, same as research_assets.py
    "SPY": "S&P 500 ETF", "EFA": "MSCI EAFE", "EEM": "MSCI Emerging", "^IRX": "13w T-bill yield",
}
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "research", "countries_history.json.gz")


def num(x, nd=6):
    try:
        x = float(x)
    except Exception:
        return None
    return round(x, nd) if math.isfinite(x) else None


def main():
    out = {"built": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%MZ"), "source": "yfinance (Yahoo) on GitHub Actions",
           "symbols": {}, "errors": {}}
    for sym, name in SYMBOLS.items():
        df = None
        for attempt in range(3):
            try:
                df = yf.Ticker(sym).history(period="max", interval="1d", auto_adjust=False, actions=False)
                if df is not None and len(df):
                    break
            except Exception as e:
                out["errors"][sym] = "%s: %s" % (type(e).__name__, str(e)[:160])
            time.sleep(2 + 3 * attempt)
        if df is None or not len(df):
            out["errors"].setdefault(sym, "no data")
            continue
        df = df.dropna(subset=["Close"])
        idx = [d.strftime("%Y-%m-%d") for d in pd.DatetimeIndex(df.index).tz_localize(None)]
        rec = {"name": name, "first": idx[0], "last": idx[-1], "n": len(idx), "d": idx}
        for col, key in (("Open", "o"), ("High", "h"), ("Low", "l"), ("Close", "c"), ("Adj Close", "ac"), ("Volume", "v")):
            rec[key] = [num(x) for x in df[col]] if col in df.columns else None
        out["symbols"][sym] = rec
        out["errors"].pop(sym, None)
        print("%-10s %5d bars %s .. %s" % (sym, len(idx), idx[0], idx[-1]), flush=True)
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with gzip.open(OUT, "wt", encoding="utf-8") as f:
        json.dump(out, f, separators=(",", ":"))
    print("wrote %s: %d symbols, %d errors %s" % (OUT, len(out["symbols"]), len(out["errors"]), sorted(out["errors"])))
    return 0 if out["symbols"] else 1


if __name__ == "__main__":
    sys.exit(main())
