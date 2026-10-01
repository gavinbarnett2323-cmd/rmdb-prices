#!/usr/bin/env python3
"""research_assets.py: one-off research history for the cross-asset Bottom Hunter study (2026-09-30, Gavin's order).

Gavin, 2026-09-30: backtest "how the bottom hunter is performing across asset classes and individual stocks". The vault's
TradingView bundle covers US stocks, sectors, US bonds, gold, silver and oil; this job adds what it lacks: international
equity, REITs, broad commodities, more bonds, the dollar, gold miners, crypto and long index histories. yfinance on the
runner (the vault and Claude's sandbox cannot reach Yahoo). Writes relay/research/assets_history.json.gz:
  {"built": iso, "symbols": {sym: {"name", "first", "last", "n", "d": [YYYY-MM-DD...], "o","h","l","c","ac","v": [...]}},
   "errors": {sym: msg}}
c = raw close (split-adjusted only), ac = dividend-adjusted close (total return). Public prices only: nothing about the
script, the book or any account. Triggered by a push of this file or by hand (workflow research-assets); never scheduled."""
import datetime, gzip, json, math, os, sys, time

import pandas as pd
import yfinance as yf

SYMBOLS = {
    # international equity
    "EFA": "MSCI EAFE", "EEM": "MSCI Emerging", "VEA": "Developed ex-US", "VWO": "Emerging (Vanguard)",
    "EWJ": "Japan", "EWZ": "Brazil", "FXI": "China large-cap", "EWG": "Germany", "EWU": "UK", "INDA": "India",
    "EWY": "South Korea", "EWT": "Taiwan", "EWC": "Canada", "EWA": "Australia", "EWW": "Mexico",
    # long index histories
    "^GSPC": "S&P 500 index", "^IXIC": "Nasdaq Composite", "^RUT": "Russell 2000", "^N225": "Nikkei 225",
    "^FTSE": "FTSE 100", "^GDAXI": "DAX", "^HSI": "Hang Seng", "^STOXX50E": "Euro Stoxx 50",
    # real estate
    "VNQ": "US REITs", "IYR": "US real estate", "RWX": "International REITs",
    # commodities
    "DBC": "Broad commodities", "GSG": "S&P GSCI", "DBA": "Agriculture", "UNG": "Natural gas", "CPER": "Copper",
    "GDX": "Gold miners", "GDXJ": "Junior gold miners", "SLV": "Silver", "GLD": "Gold", "USO": "Oil",
    # bonds
    "AGG": "US aggregate bonds", "BND": "Total bond", "TIP": "TIPS", "EMB": "EM sovereign bonds", "JNK": "HY bonds (SPDR)",
    "MUB": "Munis", "BNDX": "International bonds", "TLT": "20y Treasuries", "IEF": "7-10y Treasuries", "HYG": "HY bonds",
    "LQD": "IG bonds", "SHY": "1-3y Treasuries",
    # dollar / FX
    "UUP": "US dollar bullish", "FXE": "Euro", "FXY": "Yen",
    # crypto
    "BTC-USD": "Bitcoin", "ETH-USD": "Ether", "GBTC": "Grayscale Bitcoin Trust",
    # reference
    "SPY": "S&P 500 ETF", "QQQ": "Nasdaq 100 ETF", "IWM": "Russell 2000 ETF",
}
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "research", "assets_history.json.gz")


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
