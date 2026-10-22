#!/usr/bin/env python3
"""research_lt/run.py: data jobs for the quality look-through study (2026-10-21, Gavin: buy the actually good companies inside
the V3 tranche ETFs instead of the ETFs). Public data only, no keys. Each stage writes into OUT, which the workflow publishes to
the orphan branch research-lt/<stage>/. Nothing on the decision path reads any of it.

Stages
  probe : can the runner reach SEC EDGAR (fund tickers, submissions, N-PORT, XBRL), OpenFIGI, Ken French, AQR, the Wayback
          Machine and the issuers' holdings files; which delisted names Yahoo still serves; plus the natural-experiment ETF
          histories (same format as research_assets.py) and the raw French / AQR quality-factor files.
"""
import datetime, gzip, io, json, math, os, re, sys, time, traceback, zipfile

import requests

UA_SEC = "rmdb-prices research bot (github.com/gavinbarnett2323-cmd/rmdb-prices) gavinbarnett2323-cmd@users.noreply.github.com"
UA_WEB = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
S = requests.Session()
_last = [0.0]


def sec(url, stream=False, timeout=60, head=False):
    """SEC fair-access: declared user agent, at most ~8 requests a second."""
    wait = 0.13 - (time.time() - _last[0])
    if wait > 0:
        time.sleep(wait)
    _last[0] = time.time()
    h = {"User-Agent": UA_SEC, "Accept-Encoding": "gzip, deflate"}
    for attempt in range(4):
        try:
            if head:
                r = S.head(url, headers=h, timeout=timeout, allow_redirects=True)
            else:
                r = S.get(url, headers=h, timeout=timeout, stream=stream)
            if r.status_code in (429, 503):
                time.sleep(2 + 4 * attempt)
                continue
            return r
        except Exception as e:  # noqa
            if attempt == 3:
                raise
            time.sleep(2 + 3 * attempt)
    return r


def web(url, timeout=60, **kw):
    h = {"User-Agent": UA_WEB}
    h.update(kw.pop("headers", {}))
    return S.get(url, headers=h, timeout=timeout, **kw)


def num(x, nd=6):
    try:
        x = float(x)
    except Exception:
        return None
    return round(x, nd) if math.isfinite(x) else None


def yf_history(symbols, label=""):
    """period=max daily bars, unadjusted close c and dividend-adjusted close ac, like research_assets.py."""
    import pandas as pd
    import yfinance as yf
    out, errs = {}, {}
    for sym in symbols:
        df = None
        for attempt in range(3):
            try:
                df = yf.Ticker(sym).history(period="max", interval="1d", auto_adjust=False, actions=False)
                if df is not None and len(df):
                    break
            except Exception as e:
                errs[sym] = "%s: %s" % (type(e).__name__, str(e)[:160])
            time.sleep(1 + 2 * attempt)
        if df is None or not len(df):
            errs.setdefault(sym, "no data")
            continue
        df = df.dropna(subset=["Close"])
        idx = [d.strftime("%Y-%m-%d") for d in pd.DatetimeIndex(df.index).tz_localize(None)]
        rec = {"first": idx[0], "last": idx[-1], "n": len(idx), "d": idx}
        for col, key in (("Open", "o"), ("Close", "c"), ("Adj Close", "ac"), ("Volume", "v")):
            rec[key] = [num(x) for x in df[col]] if col in df.columns else None
        out[sym] = rec
        errs.pop(sym, None)
        print("%s %-8s %5d bars %s .. %s" % (label, sym, len(idx), idx[0], idx[-1]), flush=True)
    return out, errs


# ----------------------------------------------------------------------------------------------------------------- probe
TARGET_FUNDS = ["XLK", "XLY", "XLC", "XLI", "XLB", "XLE", "XLF", "XLV", "XLP", "XLU", "XLRE", "SMH", "XBI", "KRE", "IWM",
                "IYR", "VNQ", "RSP", "QQQ", "QQQM", "QQQE", "SPY", "IVV", "VOO", "MDY", "IJH", "DIA", "IJR", "VTI", "ITOT",
                "IWB", "IWR", "QUAL", "SOXX", "IBB", "BBH"]
DELISTED_TEST = ["ATVI", "TWTR", "SIVB", "SIVBQ", "FRC", "FRCB", "SBNY", "XLNX", "MXIM", "CELG", "SGEN", "HZNP", "CTXS",
                 "PXD", "ANSS", "JNPR", "SPLK", "DFS", "MRO", "CTLT", "WRK", "ALXN", "CERN", "DISCA", "PBCT", "ABMD",
                 "NLSN", "DRE", "VMW", "SAVE", "BBBY", "WBA"]
NAT_ETFS = ["IJR", "IWM", "IJH", "IWR", "IVV", "IWB", "VTI", "ITOT", "QUAL", "SPHQ", "XMHQ", "XSHQ", "JQUA", "DGRW",
            "BBH", "IBB", "XBI", "SOXX", "XSD", "SMH", "KBWR", "QABA", "KRE", "KBE", "VGT", "VDE", "VFH", "VHT", "VCR",
            "VDC", "VIS", "VAW", "VPU", "VOX", "VNQ", "XLK", "XLE", "XLF", "XLV", "XLY", "XLP", "XLI", "XLB", "XLU", "XLC",
            "XLRE", "SPY", "RSP", "QQQ", "QQQE", "QQQM", "MDY", "IWO", "IWN", "IJT", "IJS", "SLY", "VB", "IWC", "OEF",
            "SPLV", "USMV", "MTUM", "VLUE", "SIZE", "^RUT", "^SP600", "^SP400", "^GSPC", "^IRX"]


def stage_probe(OUT):
    rep = {"built": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%MZ"), "checks": {}}
    C = rep["checks"]

    def check(name, fn):
        t = time.time()
        try:
            C[name] = {"ok": True, **(fn() or {})}
        except Exception as e:
            C[name] = {"ok": False, "error": "%s: %s" % (type(e).__name__, str(e)[:300]), "tb": traceback.format_exc()[-800:]}
        C[name]["secs"] = round(time.time() - t, 1)
        print("check %-28s %s" % (name, "ok" if C[name]["ok"] else C[name]["error"]), flush=True)

    mf = {}

    def sec_mf():
        r = sec("https://www.sec.gov/files/company_tickers_mf.json")
        d = r.json()
        fields = d["fields"]
        rows = [dict(zip(fields, x)) for x in d["data"]]
        for x in rows:
            if x.get("symbol") in TARGET_FUNDS:
                mf[x["symbol"]] = x
        json.dump({"fields": fields, "matches": mf, "n_rows": len(rows)}, open(os.path.join(OUT, "mf_tickers_subset.json"), "w"), indent=1)
        return {"status": r.status_code, "n_rows": len(rows), "found": sorted(mf), "missing": sorted(set(TARGET_FUNDS) - set(mf))}
    check("sec_company_tickers_mf", sec_mf)

    def sec_tickers():
        r = sec("https://www.sec.gov/files/company_tickers.json")
        d = r.json()
        with gzip.open(os.path.join(OUT, "company_tickers.json.gz"), "wt") as f:
            json.dump(d, f)
        return {"status": r.status_code, "n": len(d)}
    check("sec_company_tickers", sec_tickers)

    subs = {}

    def sec_subs():
        ciks = sorted({int(x["cik"]) for x in mf.values()})
        for c in ciks:
            r = sec("https://data.sec.gov/submissions/CIK%010d.json" % c)
            if r.status_code != 200:
                subs[c] = {"status": r.status_code}
                continue
            d = r.json()
            rec = d.get("filings", {}).get("recent", {})
            forms = rec.get("form", [])
            dates = rec.get("filingDate", [])
            npt = [i for i, f in enumerate(forms) if f == "NPORT-P"]
            subs[c] = {"name": d.get("name"), "n_recent": len(forms), "n_nport_recent": len(npt),
                       "nport_dates": [dates[npt[-1]], dates[npt[0]]] if npt else None,
                       "older_pages": [x.get("name") for x in d.get("filings", {}).get("files", [])][:40],
                       "forms_seen": sorted(set(forms))[:60]}
        return {"trusts": subs}
    check("sec_submissions", sec_subs)

    sample = {}

    def sec_browse_series():
        out = {}
        for sym in ["XLK", "SMH", "XBI", "QQQ", "SPY", "IVV", "QQQM"]:
            x = mf.get(sym)
            if not x:
                out[sym] = "not in mf list"
                continue
            sid = x["seriesId"]
            url = ("https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=%s&type=NPORT-P&dateb=&owner=include"
                   "&count=100&output=atom" % sid)
            r = sec(url)
            accs = re.findall(r"<accession-number>([0-9-]+)</accession-number>", r.text)
            fdates = re.findall(r"<filing-date>([0-9-]+)</filing-date>", r.text)
            out[sym] = {"series": sid, "cik": x["cik"], "status": r.status_code, "n": len(accs),
                        "first": fdates[-1] if fdates else None, "last": fdates[0] if fdates else None,
                        "accs_head": accs[:3]}
            if sym == "XLK" and accs:
                sample["xlk_acc"] = accs[0]; sample["xlk_cik"] = x["cik"]
            if sym == "SMH" and accs:
                sample["smh_acc"] = accs[-1]; sample["smh_cik"] = x["cik"]
        return {"series": out}
    check("sec_browse_by_series", sec_browse_series)

    def sec_nport_doc():
        out = {}
        for key in ("xlk", "smh"):
            if key + "_acc" not in sample:
                continue
            cik = int(sample[key + "_cik"]); acc = sample[key + "_acc"]
            url = "https://www.sec.gov/Archives/edgar/data/%d/%s/primary_doc.xml" % (cik, acc.replace("-", ""))
            r = sec(url)
            t = r.text
            hold = re.findall(r"<invstOrSec>(.*?)</invstOrSec>", t, flags=re.S)
            fields = sorted(set(re.findall(r"<([a-zA-Z]+)[ >]", "".join(hold[:5]))))
            rep_pd = re.findall(r"<repPdDate>([^<]+)</repPdDate>", t)
            rep_end = re.findall(r"<repPdEnd>([^<]+)</repPdEnd>", t)
            sid = re.findall(r"<seriesId>([^<]+)</seriesId>", t)
            out[key] = {"url": url, "status": r.status_code, "bytes": len(t), "n_holdings": len(hold), "fields": fields,
                        "repPdDate": rep_pd[:1], "repPdEnd": rep_end[:1], "seriesId": sid[:1], "first_holding": hold[0][:1500] if hold else None}
        return out
    check("sec_nport_primary_doc", sec_nport_doc)

    def sec_xbrl():
        r = sec("https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json")
        d = r.json()
        tags = d.get("facts", {}).get("us-gaap", {})
        h = sec("https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip", head=True)
        f = sec("https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip", head=True)
        return {"status": r.status_code, "aapl_usgaap_tags": len(tags), "has_ni": "NetIncomeLoss" in tags,
                "bulk_zip_status": h.status_code, "bulk_zip_bytes": h.headers.get("Content-Length"),
                "subs_zip_status": f.status_code, "subs_zip_bytes": f.headers.get("Content-Length")}
    check("sec_xbrl", sec_xbrl)

    def sec_nport_datasets():
        out = {}
        for url in ("https://www.sec.gov/data-research/sec-markets-data/form-n-port-data-sets",
                    "https://www.sec.gov/dera/data/form-n-port-data-sets"):
            r = sec(url)
            links = sorted(set(re.findall(r'href="([^"]+\.zip)"', r.text)))
            out[url] = {"status": r.status_code, "n_zip": len(links), "links": links[:80]}
        return out
    check("sec_nport_datasets", sec_nport_datasets)

    def sec_cik_lookup():
        h = sec("https://www.sec.gov/Archives/edgar/cik-lookup-data.txt", head=True)
        return {"status": h.status_code, "bytes": h.headers.get("Content-Length")}
    check("sec_cik_lookup", sec_cik_lookup)

    def openfigi():
        jobs = [{"idType": "ID_ISIN", "idValue": "US0378331005"}, {"idType": "ID_CUSIP", "idValue": "00507V109"},
                {"idType": "ID_CUSIP", "idValue": "983919101"}, {"idType": "ID_ISIN", "idValue": "US78486Q1013"}]
        r = S.post("https://api.openfigi.com/v3/mapping", json=jobs, headers={"Content-Type": "application/json"}, timeout=60)
        return {"status": r.status_code, "body": r.text[:2500], "ratelimit": {k: v for k, v in r.headers.items() if "limit" in k.lower()}}
    check("openfigi", openfigi)

    def yahoo_delisted():
        got, errs = yf_history(DELISTED_TEST, "delisted")
        return {"served": {k: [v["first"], v["last"], v["n"]] for k, v in got.items()}, "not_served": sorted(errs)}
    check("yahoo_delisted", yahoo_delisted)

    def french():
        out = {}
        base = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/"
        for fn in ("F-F_Research_Data_5_Factors_2x3_daily_CSV.zip", "F-F_Research_Data_5_Factors_2x3_CSV.zip",
                   "Portfolios_Formed_on_OP_CSV.zip", "Portfolios_Formed_on_OP_Daily_CSV.zip", "F-F_Momentum_Factor_daily_CSV.zip",
                   "49_Industry_Portfolios_daily_CSV.zip"):
            r = web(base + fn)
            out[fn] = {"status": r.status_code, "bytes": len(r.content)}
            if r.status_code == 200 and r.content[:2] == b"PK":
                open(os.path.join(OUT, "french_" + fn), "wb").write(r.content)
        return out
    check("french", french)

    def aqr():
        out = {}
        for nm, url in (("qmj_monthly", "https://www.aqr.com/-/media/AQR/Documents/Insights/Data-Sets/Quality-Minus-Junk-Factors-Monthly.xlsx"),
                        ("qmj_daily", "https://www.aqr.com/-/media/AQR/Documents/Insights/Data-Sets/Quality-Minus-Junk-Factors-Daily.xlsx"),
                        ("bab_monthly", "https://www.aqr.com/-/media/AQR/Documents/Insights/Data-Sets/Betting-Against-Beta-Equity-Factors-Monthly.xlsx")):
            r = web(url)
            out[nm] = {"status": r.status_code, "bytes": len(r.content), "ctype": r.headers.get("Content-Type")}
            if r.status_code == 200 and r.content[:2] == b"PK":
                open(os.path.join(OUT, "aqr_%s.xlsx" % nm), "wb").write(r.content)
        return out
    check("aqr", aqr)

    def wayback():
        out = {}
        for u in ("ssga.com/us/en/intermediary/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xbi.xlsx",
                  "www.ishares.com/us/products/239710/ishares-russell-2000-etf/1467271812596.ajax?fileType=csv&fileName=IWM_holdings&dataType=fund"):
            r = web("https://archive.org/wayback/available?url=%s&timestamp=20160101" % u)
            out[u[:60]] = {"status": r.status_code, "body": r.text[:600]}
        r = web("https://web.archive.org/cdx/search/cdx?url=ssga.com/*holdings-daily-us-en-xbi*&output=json&limit=50")
        out["cdx_xbi"] = {"status": r.status_code, "body": r.text[:2000]}
        return out
    check("wayback", wayback)

    def issuers():
        out = {}
        for nm, url in (("ssga_xbi", "https://www.ssga.com/us/en/intermediary/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xbi.xlsx"),
                        ("ishares_iwm", "https://www.ishares.com/us/products/239710/ishares-russell-2000-etf/1467271812596.ajax?fileType=csv&fileName=IWM_holdings&dataType=fund"),
                        ("vaneck_smh", "https://www.vaneck.com/us/en/investments/semiconductor-etf-smh/holdings/")):
            r = web(url)
            out[nm] = {"status": r.status_code, "bytes": len(r.content), "ctype": r.headers.get("Content-Type")}
        return out
    check("issuer_holdings", issuers)

    def nat():
        got, errs = yf_history(NAT_ETFS, "nat")
        with gzip.open(os.path.join(OUT, "nat_etfs.json.gz"), "wt") as f:
            json.dump({"built": rep["built"], "source": "yfinance on GitHub Actions", "symbols": got, "errors": errs}, f, separators=(",", ":"))
        return {"n": len(got), "errors": errs}
    check("natural_experiment_etfs", nat)

    json.dump(rep, open(os.path.join(OUT, "probe.json"), "w"), indent=1)
    print("probe done", sum(1 for v in C.values() if v["ok"]), "of", len(C), "checks ok")
    return 0


# ---------------------------------------------------------------------------------------------------------------- probe2
ISH = {"IVV": 239726, "IJH": 239763, "IJR": 239774, "IWM": 239710, "IWB": 239707, "IWV": 239714, "ITOT": 239724,
       "SOXX": 239705, "IBB": 239699, "IYR": 239520, "IYW": 239522, "IYF": 239508, "IYE": 239507, "IYH": 239511,
       "IAT": 239521, "IGV": 239771, "OEF": 239723, "QUAL": 256101}


def ishares_url(sym, pid, asof=None):
    u = ("https://www.ishares.com/us/products/%d/fund/1467271812596.ajax?fileType=csv&fileName=%s_holdings&dataType=fund"
         % (pid, sym))
    return u + ("&asOfDate=%s" % asof if asof else "")


def stage_probe2(OUT):
    rep = {"built": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%MZ"), "sec": {}, "ishares": {}, "wayback": {}}
    uas = {"plain": "RMDB Research rmdb-prices@users.noreply.github.com",
           "browser": UA_WEB, "curl": "curl/8.5.0", "none": None}
    urls = ["https://www.sec.gov/files/company_tickers_mf.json", "https://data.sec.gov/submissions/CIK0001064641.json",
            "https://efts.sec.gov/LATEST/search-index?q=%22Select%20Sector%22&forms=NPORT-P",
            "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0001064641&type=NPORT-P&count=10&output=atom"]
    for nm, ua in uas.items():
        for u in urls:
            h = {"Accept-Encoding": "gzip, deflate"}
            if ua:
                h["User-Agent"] = ua
            try:
                r = requests.get(u, headers=h, timeout=30)
                rep["sec"]["%s %s" % (nm, u[:70])] = {"status": r.status_code, "head": r.text[:240]}
            except Exception as e:
                rep["sec"]["%s %s" % (nm, u[:70])] = {"error": str(e)[:200]}
            time.sleep(0.5)
    for sym, pid in ISH.items():
        for asof in (None, "20191231", "20150630", "20100630", "20061229"):
            try:
                r = web(ishares_url(sym, pid, asof), timeout=60)
                t = r.text
                lines = t.splitlines()
                rep["ishares"]["%s %s" % (sym, asof)] = {"status": r.status_code, "bytes": len(t), "head": lines[:9],
                                                        "n_lines": len(lines)}
                if r.status_code == 200 and asof == "20191231" and len(lines) > 12:
                    open(os.path.join(OUT, "ishares_%s_%s.csv" % (sym, asof)), "w").write(t)
            except Exception as e:
                rep["ishares"]["%s %s" % (sym, asof)] = {"error": str(e)[:200]}
            time.sleep(0.4)
        print("ishares", sym, rep["ishares"].get("%s 20191231" % sym, {}).get("status"), flush=True)
    cdx = "https://web.archive.org/cdx/search/cdx?output=json&fl=timestamp,original,statuscode,length&limit=400&url="
    pats = ["spdrs.com/site-content/xls/XBI_All_Holdings.xls", "www.spdrs.com/site-content/xls/XBI_All_Holdings.xls",
            "www.spdrs.com/site-content/xls/KRE_All_Holdings.xls", "www.spdrs.com/site-content/xls/XLK_All_Holdings.xls",
            "www.spdrs.com/site-content/xls/&matchType=prefix&collapse=urlkey",
            "www.ssga.com/us/en/intermediary/etfs/library-content/products/fund-data/etfs/us/&matchType=prefix&collapse=urlkey",
            "www.ssga.com/us/en/individual/etfs/library-content/products/fund-data/etfs/us/&matchType=prefix&collapse=urlkey",
            "www.ssga.com/library-content/products/fund-data/etfs/us/&matchType=prefix&collapse=urlkey",
            "www.sectorspdr.com/sectorspdr/IDCO.Client.Spdrs.Holdings/Export/ExportExcel&matchType=prefix",
            "www.sectorspdr.com/sectorspdr/IDCO.Client.Spdrs.Holdings/Export/ExportCsv&matchType=prefix",
            "www.vaneck.com/us/en/investments/semiconductor-etf-smh/holdings/&matchType=prefix",
            "www.vaneck.com/etf/equity/smh/holdings/&matchType=prefix",
            "www.invesco.com/us/financial-products/etfs/holdings/main/holdings/0&matchType=prefix",
            "www.ishares.com/us/products/239710/&matchType=prefix&filter=original:.*ajax.*&collapse=urlkey"]
    for pat in pats:
        try:
            r = web(cdx + pat, timeout=90)
            js = r.json() if r.status_code == 200 and r.text.strip().startswith("[") else None
            rep["wayback"][pat] = {"status": r.status_code, "n": (len(js) - 1) if js else 0,
                                   "sample": (js[1:6] + js[-5:]) if js and len(js) > 1 else r.text[:300]}
        except Exception as e:
            rep["wayback"][pat] = {"error": str(e)[:200]}
        time.sleep(1)
    json.dump(rep, open(os.path.join(OUT, "probe2.json"), "w"), indent=1)
    print("probe2 done")
    return 0


def main():
    stage, OUT = sys.argv[1], sys.argv[2]
    os.makedirs(OUT, exist_ok=True)
    fn = {"probe": stage_probe, "probe2": stage_probe2}.get(stage)
    if fn is None:
        print("unknown stage", stage)
        return 2
    return fn(OUT)


if __name__ == "__main__":
    sys.exit(main())
