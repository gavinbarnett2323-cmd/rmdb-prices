# -*- coding: utf-8 -*-
"""Company crisis detector data (2026-11-19). Runs on a GitHub Actions runner because data.sec.gov
403s from the Research Monster sandbox proxy, same reason as fetch_flow.py.

For every stock in relay/crisis_universe.txt it pulls two public SEC EDGAR sources:
  1. submissions: every 8-K (with its item codes), 10-Q, 10-K, 20-F and 6-K filing date since 1995,
     so a price drop can be matched to "the company said something" or "the company said nothing".
  2. companyfacts (XBRL, 2009 on): quarterly and annual revenue, diluted EPS, net income and
     operating income, each with the period it covers and the date it was FIRST filed, so a drop
     can be read against what was known on that day, and later against what the next quarters showed.

Writes relay/research/fundamentals.json.gz:
  {"as_of":..., "n":..., "tickers": {T: {"cik":..., "name":..., "filings":[[date,form,items],...],
   "facts": {concept: [[start,end,val_first,filed_first,val_last,form,fp],...]}, "status": "ok"|"no cik"|"error: ..."}}}

Honest limits: today's universe (survivors). XBRL facts start around 2009 to 2011; older drops get
filings only. Foreign filers (20-F, 6-K) have filings but no us-gaap quarterly facts.
Rate limit: SEC asks for 10 requests a second at most; this sleeps 0.12s between calls.
"""
import gzip, json, os, re, sys, time, urllib.request, urllib.error

HERE = os.path.dirname(os.path.abspath(__file__))
UNIVERSE = os.path.join(HERE, "crisis_universe.txt")
OUT = os.path.join(HERE, "research", "fundamentals.json.gz")
STATUS = os.path.join(HERE, "research", "fundamentals_status.json")
CIK_CACHE = os.path.join(HERE, "cik_cache.json")
FROM_YEAR = 1995
FORMS = {"8-K", "8-K/A", "10-Q", "10-Q/A", "10-K", "10-K/A", "20-F", "6-K", "10-K405"}
CONCEPTS = ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet",
            "RevenueFromContractWithCustomerIncludingAssessedTax", "EarningsPerShareDiluted",
            "EarningsPerShareBasic", "NetIncomeLoss", "OperatingIncomeLoss"]
SLEEP = 0.12
TIMEOUT = 60


def user_agent():
    """SEC wants a contact in the User-Agent. Gavin sets it as the SEC_UA secret; failing that the
    runner reuses the UA constant already in fetch_flow.py (read as text, never imported)."""
    ua = os.environ.get("SEC_UA", "").strip()
    if ua:
        return ua
    try:
        src = open(os.path.join(HERE, "fetch_flow.py"), encoding="utf-8").read()
        m = re.search(r'^UA\s*=\s*(["\'])(.*?)\1', src, re.M)
        if m:
            return m.group(2)
    except Exception:
        pass
    return "rmdb-prices research relay"


UA = user_agent()


def get(url, retries=4):
    last = None
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept-Encoding": "gzip, deflate", "Host": url.split("/")[2]})
            with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
                raw = r.read()
                if r.headers.get("Content-Encoding") == "gzip":
                    raw = gzip.decompress(raw)
                time.sleep(SLEEP)
                return json.loads(raw.decode("utf-8"))
        except urllib.error.HTTPError as e:
            last = e
            if e.code == 404:
                return None
            time.sleep(2.0 * (i + 1))
        except Exception as e:
            last = e
            time.sleep(2.0 * (i + 1))
    raise RuntimeError("GET failed %s: %s" % (url, last))


def cik_map():
    """ticker -> 10 digit CIK string. SEC's list uses dashes (BRK-B); the vault uses BRKB."""
    out = {}
    try:
        d = get("https://www.sec.gov/files/company_tickers.json")
        for v in (d or {}).values():
            t = str(v.get("ticker", "")).upper()
            out[t] = "%010d" % int(v["cik_str"])
            out[t.replace("-", "")] = out[t]
            out[t.replace("-", ".")] = out[t]
    except Exception as e:
        print("company_tickers.json failed:", e)
    try:
        for t, c in json.load(open(CIK_CACHE)).items():
            out.setdefault(t.upper(), "%010d" % int(c))
    except Exception:
        pass
    return out


def filings_for(cik):
    base = get("https://data.sec.gov/submissions/CIK%s.json" % cik)
    if not base:
        return None, []
    name = base.get("name")
    rows = []

    def take(block):
        forms = block.get("form") or []
        dates = block.get("filingDate") or []
        items = block.get("items") or []
        for i, f in enumerate(forms):
            if f in FORMS and i < len(dates) and dates[i] >= "%d-01-01" % FROM_YEAR:
                rows.append([dates[i], f, (items[i] if i < len(items) else "") or ""])

    take((base.get("filings") or {}).get("recent") or {})
    for f in ((base.get("filings") or {}).get("files") or []):
        # older pages; skip ones that end before FROM_YEAR
        if str(f.get("filingTo", "9999")) < "%d-01-01" % FROM_YEAR:
            continue
        more = get("https://data.sec.gov/submissions/%s" % f["name"])
        if more:
            take(more)
    rows.sort()
    return name, rows


def facts_for(cik):
    d = get("https://data.sec.gov/api/xbrl/companyfacts/CIK%s.json" % cik)
    if not d:
        return {}
    gaap = (d.get("facts") or {}).get("us-gaap") or {}
    out = {}
    for concept in CONCEPTS:
        node = gaap.get(concept)
        if not node:
            continue
        units = node.get("units") or {}
        unit = "USD/shares" if "USD/shares" in units else ("USD" if "USD" in units else (next(iter(units)) if units else None))
        if not unit:
            continue
        by_period = {}
        for e in units[unit]:
            form = e.get("form") or ""
            if not (form.startswith("10-Q") or form.startswith("10-K")):
                continue
            s, en, filed = e.get("start"), e.get("end"), e.get("filed")
            if not (s and en and filed):
                continue
            key = (s, en)
            cur = by_period.get(key)
            val = e.get("val")
            if cur is None:
                by_period[key] = {"first": (filed, val, form, e.get("fp") or ""), "last": (filed, val)}
            else:
                if filed < cur["first"][0]:
                    cur["first"] = (filed, val, form, e.get("fp") or "")
                if filed >= cur["last"][0]:
                    cur["last"] = (filed, val)
        rows = []
        for (s, en), v in by_period.items():
            rows.append([s, en, v["first"][1], v["first"][0], v["last"][1], v["first"][2], v["first"][3]])
        rows.sort(key=lambda r: (r[1], r[0]))
        out[concept] = rows
    return out


def main():
    limit = int(os.environ.get("LIMIT", "0") or 0)
    tickers = [t.strip().upper() for t in open(UNIVERSE, encoding="utf-8") if t.strip() and not t.startswith("#")]
    if limit:
        tickers = tickers[:limit]
    cmap = cik_map()
    print("universe %d tickers, cik map %d entries, UA set: %s" % (len(tickers), len(cmap), "yes" if UA else "no"))
    out = {"as_of": time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime()), "n": len(tickers), "from_year": FROM_YEAR,
           "concepts": CONCEPTS, "tickers": {}}
    ok = 0
    t0 = time.time()
    for i, t in enumerate(tickers):
        rec = {"cik": None, "name": None, "filings": [], "facts": {}, "status": "no cik"}
        cik = cmap.get(t) or cmap.get(t.replace(".", "-")) or (cmap.get(t[:-1] + "-" + t[-1]) if len(t) >= 4 else None)
        if cik:
            rec["cik"] = cik
            try:
                name, rows = filings_for(cik)
                rec["name"] = name
                rec["filings"] = rows
                rec["facts"] = facts_for(cik)
                rec["status"] = "ok" if rows or rec["facts"] else "empty"
                ok += 1
            except Exception as e:
                rec["status"] = "error: %s" % str(e)[:200]
        out["tickers"][t] = rec
        if (i + 1) % 25 == 0 or i + 1 == len(tickers):
            print("  %d/%d done, %d ok, %.0fs" % (i + 1, len(tickers), ok, time.time() - t0), flush=True)
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with gzip.open(OUT, "wt", encoding="utf-8") as f:
        json.dump(out, f, separators=(",", ":"))
    status = {"as_of": out["as_of"], "tickers": len(tickers), "ok": ok,
              "no_cik": sorted(t for t, r in out["tickers"].items() if r["status"] == "no cik"),
              "errors": {t: r["status"] for t, r in out["tickers"].items() if r["status"].startswith("error")},
              "bytes": os.path.getsize(OUT), "seconds": round(time.time() - t0)}
    json.dump(status, open(STATUS, "w"), indent=1)
    print(json.dumps({k: v for k, v in status.items() if k != "errors"}, indent=1))
    print("errors:", len(status["errors"]))


if __name__ == "__main__":
    main()
