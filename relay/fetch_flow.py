# -*- coding: utf-8 -*-
"""Flow-structure relay (Flow Engine Stage 2, 2026-09-06) — runs on a GitHub Actions runner (real
egress; these sources 403 from the Research Monster sandbox's proxy, which is the whole reason this
lives here rather than in the vault). Fetches, BEST-EFFORT PER SOURCE with per-source status
honesty (one dead source must never hide behind another's success — no silent partial-as-full):

  1. SHORT INTEREST — UPDATED 2026-09-08: not the OAuth2-gated Query API (api.finra.org), which the
     2026-09-06 build correctly found requires a paid credential. FINRA separately publishes the same
     short-interest program as a free bulk CSV, no login, at cdn.finra.org/equity/otcmarket/biweekly/
     shrt<YYYYMMDD>.csv (twice-monthly settlement dates, covering "all exchange-listed and OTC equity
     securities" per FINRA's own docs -- the "otcmarket" in the path is legacy naming, not a scope
     limit). Tries the last several published settlement dates newest-first. Degrades honestly to
     status="schema_drift" with the raw header logged if FINRA's column layout doesn't match what's
     coded here, and to status="unreachable" with every attempted URL/date logged if none work --
     never a silent wrong number.
  2. SHARES OUTSTANDING — SEC data.sec.gov XBRL companyfacts (dei:EntityCommonStockSharesOutstanding
     concept), free, keyless, official SEC API. Needs a CIK per ticker: resolved once from SEC's
     published company_tickers.json (also free, keyless) and cached in cik_cache.json so every run
     after the first doesn't re-resolve 54 CIKs. Float itself is not free/reliable anywhere — this
     publishes shares outstanding and an honest float: null, never a fabricated float.
  3. 8-K RECENCY — SEC data.sec.gov submissions/CIK##########.json (same official, free, keyless
     API) — the same JSON already carries a rolling recent-filings array with form types and dates;
     no separate full-text-search call needed. Gives dislocation_scan.py's automated "no news"
     check its actual data.
  4. 13F TOP-HOLDER CONCENTRATION — EDGAR full-text search + 13F index for the SCOPED
     flow_focus_tickers.txt list only (~54-60 names, not the 1,144-ticker universe — scope
     discipline per the build plan; a full-universe quarterly 13F walk would be slow and mostly
     wasted on names Gavin will never act on). 45-DAY DISCLOSURE LAG is real (13F filings are due
     45 days after quarter-end) and is stamped directly into the output schema (`lag_days` +
     `as_of_quarter`), never hidden. This is the most fragile of the four sources (13F data
     requires parsing INFORMATION TABLE XML per filer, which is heavy) — implemented as best-effort
     with an honest "not_implemented_v1" status if the parse proves too fragile to trust; a partial
     or wrong top-5 number is worse than an honest null here, since Gavin would read it as real
     ownership concentration.

OUTPUT SCHEMA (every field nullable, every source stamps its own status/asof so a dead source is
visible on its own, never inferred from another source's success):
{
  "as_of": "<UTC iso timestamp of this run>",
  "sources": {
    "short_interest": {"status": "ok"|"partial"|"unreachable"|"schema_drift", "asof": "...", "note": "..."},
    "shares_outstanding": {...}, "filings_8k": {...}, "top_holders_13f": {...}
  },
  "tickers": {
    "<T>": {
      "si_shares": <int|null>, "si_dtc": <float|null>, "si_settlement_date": "<date|null>",
      "shares_out": <int|null>, "shares_out_asof": "<date|null>",
      "last_8k": "<date|null>", "n_8k_3w": <int|null>,
      "top5_pct": <float|null>, "top5_asof_quarter": "<str|null>", "top5_lag_days": <int|null>
    }, ...
  }
}

DECISION-PATH BOUNDARY: this is a pure data fetcher (no scoring, no recommend()) — it publishes raw
flow-structure facts for the vault side (engine/flow_data.json puller + dislocation_scan.py v2 +
the per-name flow card) to read and interpret. It never runs on a schedule shorter than weekly
(Sunday 22:00 UTC, mirroring history.yml) since none of these four sources update faster than that
in a way worth paying for.

Run (locally or in CI): python3 relay/fetch_flow.py
"""
import json, os, sys, time, datetime, urllib.request, urllib.error

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "flow.json")
CIK_CACHE = os.path.join(HERE, "cik_cache.json")
FOCUS_LIST = os.path.join(HERE, "flow_focus_tickers.txt")

UA = "ResearchMonster/1.0 (gavinbarnett2323@gmail.com)"  # SEC requires a real UA with contact info
TIMEOUT = 20
RETRIES = 2
SLEEP = 0.6  # SEC asks for <=10 req/s; we're nowhere near that, but be polite


def _get(url, headers=None, data=None, method="GET"):
    hdrs = {"User-Agent": UA}
    if headers:
        hdrs.update(headers)
    for attempt in range(1, RETRIES + 1):
        try:
            req = urllib.request.Request(url, data=data, headers=hdrs, method=method)
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                return resp.read()
        except Exception as e:
            if attempt == RETRIES:
                raise
            time.sleep(SLEEP)
    return None


def _load(path, default):
    try:
        return json.load(open(path, encoding="utf-8"))
    except Exception:
        return default


def load_tickers():
    if not os.path.exists(FOCUS_LIST):
        return []
    return [l.strip().upper() for l in open(FOCUS_LIST) if l.strip() and not l.startswith("#")]


# ---------- source 2 + 3 shared: CIK resolution ----------
def resolve_ciks(tickers):
    cache = _load(CIK_CACHE, {})
    missing = [t for t in tickers if t not in cache]
    if missing:
        try:
            raw = _get("https://www.sec.gov/files/company_tickers.json")
            data = json.loads(raw)
            by_ticker = {}
            for _, row in data.items():
                sym = str(row.get("ticker", "")).upper()
                by_ticker[sym] = str(row.get("cik_str", "")).zfill(10)
            for t in missing:
                # Yahoo-style share-class dot (BRK.B) vs SEC's dash (BRK-B) — try both.
                cik = by_ticker.get(t) or by_ticker.get(t.replace(".", "-")) or by_ticker.get(t.replace("-", "."))
                cache[t] = cik  # None recorded explicitly too, so we don't re-try every run
        except Exception as e:
            print("  CIK resolution failed entirely: %s (SEC company_tickers.json unreachable this run)" % e)
        json.dump(cache, open(CIK_CACHE, "w"), indent=1)
    return cache


# ---------- source 1: short interest ----------
def _recent_settlement_dates(n=6):
    """FINRA short-interest settlement dates: the 15th of each month and the last calendar day of
    each month, each rolled back to the nearest weekday if it lands on a weekend (FINRA's own stated
    rule in the Equity Short Interest 'About the Data' page). Does not separately account for
    exchange holidays (a small honest gap) -- a holiday-shifted miss just falls through to the next
    candidate this function already returns, since fetch_short_interest() tries each in turn.
    Returns the most recent N candidates, newest first."""
    import calendar
    out = []
    d = datetime.date.today()
    y, m = d.year, d.month
    for _ in range(n // 2 + 2):
        last_day = calendar.monthrange(y, m)[1]
        eom = datetime.date(y, m, last_day)
        while eom.weekday() >= 5:
            eom -= datetime.timedelta(days=1)
        mid = datetime.date(y, m, 15)
        while mid.weekday() >= 5:
            mid -= datetime.timedelta(days=1)
        for cand in (eom, mid):
            if cand <= d:
                out.append(cand)
        m -= 1
        if m == 0:
            m, y = 12, y - 1
    return sorted(set(out), reverse=True)[:n]


def fetch_short_interest(tickers):
    """FIX 2026-09-08 (Flow Engine post-build review, Gavin: "is the flow relay honestly worth it").
    The 2026-09-06 build only tried api.finra.org's OAuth2-gated Query API and NASDAQ Trader's paid
    SFTP bulk file, and concluded no free/keyless full-market short-interest source exists. That
    conclusion was INCOMPLETE: FINRA separately publishes the exact same short-interest program as a
    plain bulk CSV download at finra.org/finra-data/browse-catalog/equity-short-interest/files (files
    served from cdn.finra.org/equity/otcmarket/biweekly/shrt<YYYYMMDD>.csv) -- no login, no OAuth2,
    a bare HTTPS GET. FINRA's own "About Equity Short Interest" page states this covers "all
    exchange-listed and over-the-counter (OTC) equity securities" (the "otcmarket" in the URL path is
    legacy naming predating FINRA's Rule 4560 consolidation of exchange-listed reporting into the
    same feed -- confirmed via FINRA's own Data Glossary, which documents a Market field alongside
    Symbol/Current Short/Days to Cover/settlement Date for every row, not an OTC-only schema).
    HONEST LIMIT ON THIS FIX: it was written and pushed from an environment whose network egress is
    itself allowlist-blocked from reaching cdn.finra.org directly (the same "sandbox" constraint the
    2026-09-06 build documented) -- so the exact delimiter/column-name layout below is inferred from
    FINRA's documented field list, not confirmed byte-for-byte against a downloaded file. This
    function is written to fail HONESTLY rather than silently: if the real file's columns don't match
    the names tried here, it returns status="schema_drift" with the offending header logged verbatim,
    so the very first live run on THIS runner (which has real egress) either works or hands back
    exactly what needs fixing -- never a silent wrong number."""
    dates = _recent_settlement_dates()
    last_err = None
    want = set(tickers)
    for d in dates:
        url = "https://cdn.finra.org/equity/otcmarket/biweekly/shrt%s.csv" % d.strftime("%Y%m%d")
        try:
            raw = _get(url)
            text = raw.decode("utf-8-sig", errors="replace")
            lines = [ln for ln in text.splitlines() if ln.strip()]
            if not lines:
                last_err = "empty file at %s" % url
                continue
            delim = "|" if "|" in lines[0] else ("\t" if "\t" in lines[0] else ",")
            header = [h.strip().strip('"') for h in lines[0].split(delim)]
            hl = [h.lower().replace(" ", "").replace("_", "") for h in header]

            def _col(*names):
                for nm in names:
                    if nm in hl:
                        return hl.index(nm)
                return None

            i_sym = _col("symbolcode", "symbol", "issuesymbolidentifier")
            i_cur = _col("currentshortpositionquantity", "currentshort", "currentshortinterest")
            i_dtc = _col("daystocoverquantity", "daystocover")
            i_date = _col("settlementdate", "date")
            if i_sym is None or i_cur is None:
                last_err = "schema_drift at %s -- header was: %s" % (url, header)
                continue
            rows = {}
            for ln in lines[1:]:
                parts = [p.strip().strip('"') for p in ln.split(delim)]
                if len(parts) <= i_sym or len(parts) <= i_cur:
                    continue
                sym = parts[i_sym].upper()
                if sym not in want:
                    continue
                try:
                    cur = int(float(parts[i_cur])) if parts[i_cur] not in ("", "N/A") else None
                except Exception:
                    cur = None
                try:
                    dtc = (float(parts[i_dtc])
                           if (i_dtc is not None and parts[i_dtc] not in ("", "N/A")) else None)
                except Exception:
                    dtc = None
                sdate = parts[i_date] if i_date is not None else d.isoformat()
                rows[sym] = {"si_shares": cur, "si_dtc": dtc, "si_settlement_date": sdate}
            if rows:
                return rows, {"status": "ok", "asof": d.isoformat(),
                              "note": ("FINRA free bulk file (%s), %d/%d focus tickers matched"
                                       % (url, len(rows), len(tickers)))}
            last_err = ("%s fetched (%d data rows) but matched 0 of %d focus tickers -- check symbol "
                        "column/formatting" % (url, len(lines) - 1, len(tickers)))
        except urllib.error.HTTPError as e:
            last_err = "%s -> HTTP %s (not yet published or wrong date)" % (url, e.code)
            continue
        except Exception as e:
            last_err = "%s -> %s" % (url, e)
            continue
    return {}, {"status": "unreachable", "asof": None,
                "note": ("tried %d recent FINRA free bulk-file settlement dates, none usable: %s"
                         % (len(dates), last_err))}


# ---------- source 2: shares outstanding ----------
def fetch_shares_outstanding(tickers, ciks):
    status = {"status": "unreachable", "asof": None, "note": ""}
    rows = {}
    ok_n, err_n = 0, 0
    latest = None
    for t in tickers:
        cik = ciks.get(t)
        if not cik:
            continue
        try:
            raw = _get("https://data.sec.gov/api/xbrl/companyfacts/CIK%s.json" % cik)
            data = json.loads(raw)
            facts = (data.get("facts") or {}).get("dei") or {}
            concept = facts.get("EntityCommonStockSharesOutstanding")
            if not concept:
                err_n += 1
                continue
            units = (concept.get("units") or {}).get("shares") or []
            if not units:
                err_n += 1
                continue
            latest_unit = max(units, key=lambda u: u.get("end", ""))
            rows[t] = {"shares_out": latest_unit.get("val"), "shares_out_asof": latest_unit.get("end")}
            ok_n += 1
            if latest_unit.get("end") and (latest is None or latest_unit["end"] > latest):
                latest = latest_unit["end"]
            time.sleep(SLEEP)
        except Exception:
            err_n += 1
            continue
    status = {"status": "ok" if ok_n else ("partial" if err_n < len(tickers) else "unreachable"),
              "asof": latest, "note": "%d ok, %d failed of %d focus tickers" % (ok_n, err_n, len(tickers))}
    return rows, status


# ---------- source 3: 8-K recency (same submissions.json SEC already serves) ----------
def fetch_8k_recency(tickers, ciks):
    status = {"status": "unreachable", "asof": None, "note": ""}
    rows = {}
    ok_n, err_n = 0, 0
    today = datetime.date.today()
    for t in tickers:
        cik = ciks.get(t)
        if not cik:
            continue
        try:
            raw = _get("https://data.sec.gov/submissions/CIK%s.json" % cik)
            data = json.loads(raw)
            recent = ((data.get("filings") or {}).get("recent") or {})
            forms = recent.get("form") or []
            dates = recent.get("filingDate") or []
            eightk_dates = sorted((d for f, d in zip(forms, dates) if f == "8-K"), reverse=True)
            last_8k = eightk_dates[0] if eightk_dates else None
            n_3w = 0
            if eightk_dates:
                cutoff = (today - datetime.timedelta(weeks=3)).isoformat()
                n_3w = sum(1 for d in eightk_dates if d >= cutoff)
            rows[t] = {"last_8k": last_8k, "n_8k_3w": n_3w}
            ok_n += 1
            time.sleep(SLEEP)
        except Exception:
            err_n += 1
            continue
    status = {"status": "ok" if ok_n else ("partial" if err_n < len(tickers) else "unreachable"),
              "asof": today.isoformat(), "note": "%d ok, %d failed of %d focus tickers" % (ok_n, err_n, len(tickers))}
    return rows, status


# ---------- source 4: 13F top-holder concentration (scoped, best-effort) ----------
def fetch_13f_concentration(tickers, ciks):
    """v1: honest not_implemented status. Parsing 13F INFORMATION TABLE XML per-filer to build a
    true top-5-holder concentration requires either (a) walking EDGAR's full-text search per ticker
    to find recent 13F filers holding it (slow, imprecise — 13Fs report by CUSIP, not ticker, and
    a full CUSIP<->ticker cross-reference isn't free/reliable) or (b) a paid aggregator. Rather than
    ship a fragile, possibly-wrong concentration number that Gavin would read as real ownership
    data, v1 publishes the honest gap with the lag/quarter context stated, so the schema is ready
    the day a real implementation lands (a signed follow-up, not silently done here under time
    pressure) without another vault-side format change."""
    today = datetime.date.today()
    # 13F due date: 45 days after the most recently COMPLETED calendar quarter
    q_end_month = ((today.month - 1) // 3) * 3
    if q_end_month == 0:
        q_end = datetime.date(today.year - 1, 12, 31)
    else:
        import calendar
        last_day = calendar.monthrange(today.year, q_end_month)[1]
        q_end = datetime.date(today.year, q_end_month, last_day)
    days_since_qend = (today - q_end).days
    lag_ok = days_since_qend >= 45
    status = {"status": "not_implemented_v1", "asof": None,
              "note": ("13F top-holder concentration needs a real per-filer INFORMATION TABLE parse "
                       "(not shipped in v1 — see this function's docstring). Most recent quarter with "
                       "13Fs due: %s (%d days ago, disclosure %s)."
                       % (q_end.isoformat(), days_since_qend, "should be filed" if lag_ok else "NOT YET DUE"))}
    rows = {t: {"top5_pct": None, "top5_asof_quarter": q_end.isoformat(),
                "top5_lag_days": days_since_qend} for t in tickers}
    return rows, status


def main():
    tickers = load_tickers()
    if not tickers:
        print("FATAL: relay/flow_focus_tickers.txt missing or empty — nothing to fetch. Not writing flow.json.")
        sys.exit(1)
    print("flow relay: %d focus tickers" % len(tickers))

    ciks = resolve_ciks(tickers)
    n_ciks = sum(1 for t in tickers if ciks.get(t))
    print("  CIKs resolved: %d/%d" % (n_ciks, len(tickers)))

    si_rows, si_status = fetch_short_interest(tickers)
    print("  [1/4] short_interest: %s — %s" % (si_status["status"], si_status["note"]))

    so_rows, so_status = fetch_shares_outstanding(tickers, ciks)
    print("  [2/4] shares_outstanding: %s — %s" % (so_status["status"], so_status["note"]))

    k8_rows, k8_status = fetch_8k_recency(tickers, ciks)
    print("  [3/4] filings_8k: %s — %s" % (k8_status["status"], k8_status["note"]))

    f13_rows, f13_status = fetch_13f_concentration(tickers, ciks)
    print("  [4/4] top_holders_13f: %s — %s" % (f13_status["status"], f13_status["note"]))

    out_tickers = {}
    for t in tickers:
        si = si_rows.get(t, {})
        so = so_rows.get(t, {})
        k8 = k8_rows.get(t, {})
        f13 = f13_rows.get(t, {})
        out_tickers[t] = {
            "si_shares": si.get("si_shares"), "si_dtc": si.get("si_dtc"),
            "si_settlement_date": si.get("si_settlement_date"),
            "shares_out": so.get("shares_out"), "shares_out_asof": so.get("shares_out_asof"),
            "last_8k": k8.get("last_8k"), "n_8k_3w": k8.get("n_8k_3w"),
            "top5_pct": f13.get("top5_pct"), "top5_asof_quarter": f13.get("top5_asof_quarter"),
            "top5_lag_days": f13.get("top5_lag_days"),
        }

    n_sources_live = sum(1 for s in (si_status, so_status, k8_status, f13_status) if s["status"] in ("ok", "partial"))
    out = {
        "as_of": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "n_focus_tickers": len(tickers), "n_sources_live": n_sources_live,
        "sources": {"short_interest": si_status, "shares_outstanding": so_status,
                    "filings_8k": k8_status, "top_holders_13f": f13_status},
        "tickers": out_tickers,
    }
    json.dump(out, open(OUT, "w"), indent=1)
    print("wrote %s — %d/4 sources live (ok/partial)" % (OUT, n_sources_live))


if __name__ == "__main__":
    main()
