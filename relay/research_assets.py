#!/usr/bin/env python3
"""research_assets.py: one-off research history for the cross-asset Bottom Hunter study (2026-09-30, Gavin's order).

Gavin, 2026-09-30: backtest "how the bottom hunter is performing across asset classes and individual stocks". The vault's
TradingView bundle covers US stocks, sectors, US bonds, gold, silver and oil; this job adds what it lacks: international
equity, REITs, broad commodities, more bonds, the dollar, gold miners, crypto and long index histories. yfinance on the
runner (the vault and Claude's sandbox cannot reach Yahoo). Writes relay/research/assets_history.json.gz:
  {"built": iso, "symbols": {sym: {"name", "first", "last", "n", "d": [YYYY-MM-DD...], "o","h","l","c","ac","v": [...]}},
   "errors": {sym: msg}}
c = raw close (split-adjusted only), ac = dividend-adjusted close (total return). Public prices only: nothing about the
script, the book or any account. 2026-10-14: symbols extended for the ETF strength-rework study (Gavin: "completely rework it"). Triggered by a push of this file or by hand (workflow research-assets); never scheduled."""
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
    # 2026-10-14 strength-rework study: the rest of the V3 35-ETF universe + cross-asset signal series
    "MDY": "S&P 400 midcap", "DIA": "Dow 30", "RSP": "S&P 500 equal weight", "XLK": "Tech", "XLY": "Discretionary",
    "XLC": "Comm services", "XLI": "Industrials", "XLB": "Materials", "XLE": "Energy", "XLF": "Financials",
    "XLV": "Health care", "XLP": "Staples", "XLU": "Utilities", "XLRE": "Real estate sector", "SMH": "Semiconductors",
    "XBI": "Biotech", "KRE": "Regional banks", "SOXX": "Semiconductors (iShares)", "IGV": "Software", "ITA": "Defense",
    "^VIX": "VIX", "^TNX": "10y yield", "^IRX": "13w T-bill yield", "^TYX": "30y yield", "^FVX": "5y yield",
    "HG=F": "Copper futures", "GC=F": "Gold futures", "CL=F": "Crude futures", "DX-Y.NYB": "Dollar index",
    "XHB": "Homebuilders", "IYT": "Transports", "XRT": "Retail", "MTUM": "Momentum factor", "USMV": "Min vol factor",
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


# 2026-10-14 strength-rework Round 4: Ken French's 49 industry portfolios (daily, value weighted) and the daily market
# and T-bill from his factor file, as an untouched 1926 to 2000 confirmation set. Public data, no key.
FRENCH = {
    "FF49": "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/49_Industry_Portfolios_daily_CSV.zip",
    "FF3": "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_Factors_daily_CSV.zip",
}


def french_block(url):
    """first table of a French daily CSV zip: (header names, rows of strings)."""
    import io, urllib.request, zipfile
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (research-assets)"})
    raw = urllib.request.urlopen(req, timeout=120).read()
    z = zipfile.ZipFile(io.BytesIO(raw))
    txt = z.read(z.namelist()[0]).decode("latin-1")
    hdr, rows = None, []
    for ln in txt.splitlines():
        s = ln.strip()
        if hdr is None:
            if s.startswith(",") and len(s.split(",")) > 2:
                hdr = [h.strip() for h in s.split(",")[1:]]
            continue
        parts = [x.strip() for x in s.split(",")]
        if not parts or not parts[0].isdigit() or len(parts[0]) != 8:
            if rows:
                break
            continue
        rows.append(parts)
    return hdr, rows


def french_series(dates, rets):
    """daily returns in percent -> total-return index; -99.99 / -999 are missing."""
    idx, out_d, level, started = [], [], 1.0, False
    for d, r in zip(dates, rets):
        try:
            x = float(r)
        except Exception:
            x = None
        if x is None or x <= -99.0:
            if started:
                out_d.append(d); idx.append(None)
            continue
        started = True
        level *= 1 + x / 100.0
        out_d.append(d); idx.append(num(level, 8))
    return out_d, idx


def add_french(out):
    try:
        hdr, rows = french_block(FRENCH["FF49"])
        dates = ["%s-%s-%s" % (r[0][:4], r[0][4:6], r[0][6:]) for r in rows]
        for k, name in enumerate(hdr):
            d, ix = french_series(dates, [r[k + 1] if k + 1 < len(r) else None for r in rows])
            if d:
                out["symbols"]["FF49:" + name] = {"name": "French 49 industries (VW): " + name, "first": d[0], "last": d[-1],
                                                 "n": len(d), "d": d, "o": None, "h": None, "l": None, "c": ix, "ac": ix, "v": None}
        print("FF49: %d industries, %s .. %s" % (len(hdr), dates[0], dates[-1]), flush=True)
    except Exception as e:
        out["errors"]["FF49"] = "%s: %s" % (type(e).__name__, str(e)[:160])
    try:
        hdr, rows = french_block(FRENCH["FF3"])
        dates = ["%s-%s-%s" % (r[0][:4], r[0][4:6], r[0][6:]) for r in rows]
        mi, ri = hdr.index("Mkt-RF"), hdr.index("RF")
        mkt = ["%.6f" % (float(r[mi + 1]) + float(r[ri + 1])) for r in rows]
        for sym, vals in (("FF:MKT", mkt), ("FF:RF", [r[ri + 1] for r in rows])):
            d, ix = french_series(dates, vals)
            out["symbols"][sym] = {"name": "French " + sym, "first": d[0], "last": d[-1], "n": len(d), "d": d,
                                   "o": None, "h": None, "l": None, "c": ix, "ac": ix, "v": None}
        print("FF3: %s .. %s" % (dates[0], dates[-1]), flush=True)
    except Exception as e:
        out["errors"]["FF3"] = "%s: %s" % (type(e).__name__, str(e)[:160])


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
    add_french(out)
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with gzip.open(OUT, "wt", encoding="utf-8") as f:
        json.dump(out, f, separators=(",", ":"))
    print("wrote %s: %d symbols, %d errors %s" % (OUT, len(out["symbols"]), len(out["errors"]), sorted(out["errors"])))
    return 0 if out["symbols"] else 1


if __name__ == "__main__":
    sys.exit(main())
