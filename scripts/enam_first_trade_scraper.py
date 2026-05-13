#!/usr/bin/env python3.10
"""
enam_first_trade_scraper.py

Finds the first recorded eNAM transaction date (month precision) for each
mandi via parallel 2-phase binary search on the eNAM portal commodity_list API.

API:  POST https://enam.gov.in/web/Ajax_ctrl/commodity_list
      Params: language, stateName (UPPERCASE), apmcName (UPPERCASE),
              fromDate / toDate (YYYY-MM-DD)
      Returns status=200 if trades exist in range; status=500 if none.

Binary search (2 phases, ~8 API calls per mandi with data):
  Phase 1 — bisect 6-month windows [eNAM launch → today]  (~5 calls)
  Phase 2 — bisect months within that window               (~3 calls)
  Reports first_enam_trade_date as the 1st of the target month.

Runtime estimate: ~5s per API call × 8 calls × 1,361 mandis / 4 workers
                  ≈ 4-5 hours. Resume safely with --resume.

Limitations:
  The portal database appears to start ~October 2018 for Phase-1 mandis
  (original April 2016 launch states). Dates reported for those mandis
  reflect the earliest record in the current portal, not the actual
  trading start date.

Output:
  enam_adoption_dates.csv  (written to the working directory)
  Columns: state_id, state_name, mandi_id, mandi_name, first_enam_trade_date
"""

import collections
import csv
import json
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import requests

# ── Configuration ────────────────────────────────────────────────────────────
ENAM_LAUNCH     = date(2016, 4, 14)
TODAY           = date.today()
BASE_URL        = "https://enam.gov.in/web/"
OUTPUT          = Path("enam_adoption_dates.csv")
WORKERS         = 4          # parallel worker threads
CALL_TIMEOUT    = 30         # seconds per API call
CALL_SLEEP      = 0.1        # polite delay after each call (within a worker)

# Per-worker rate limit: at most 1 call every RATE_FLOOR seconds.
# Keeps combined throughput ≤ WORKERS / RATE_FLOOR requests/s.
RATE_FLOOR      = 0.2        # minimum seconds between consecutive calls per worker

AJAX_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "X-Requested-With": "XMLHttpRequest",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Origin": "https://enam.gov.in",
    "Referer": "https://enam.gov.in/web/dashboard/Historical",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "close",
}

# ── Thread-local rate limiter (no session needed — API works without cookies) ─
_local = threading.local()
_print_lock = threading.Lock()
_csv_lock = threading.Lock()


def tprint(*args, **kwargs):
    """Thread-safe print."""
    with _print_lock:
        print(*args, **kwargs, flush=True)


# ── API primitive ─────────────────────────────────────────────────────────────
def has_data(state: str, apmc: str, from_d: date, to_d: date) -> bool:
    """Return True if any trade data exists for this mandi in [from_d, to_d]."""
    if not hasattr(_local, "last_call"):
        _local.last_call = 0.0
    elapsed = time.time() - _local.last_call
    if elapsed < RATE_FLOOR:
        time.sleep(RATE_FLOOR - elapsed)

    for attempt in range(3):
        try:
            t0 = time.time()
            r = requests.post(
                BASE_URL + "Ajax_ctrl/commodity_list",
                headers=AJAX_HEADERS,
                data={
                    "language": "en",
                    "stateName": state,
                    "apmcName": apmc,
                    "fromDate": from_d.strftime("%Y-%m-%d"),
                    "toDate": to_d.strftime("%Y-%m-%d"),
                },
                timeout=CALL_TIMEOUT,
            )
            _local.last_call = time.time()
            time.sleep(CALL_SLEEP)
            d = r.json()
            result = d.get("status") == 200 and len(d.get("data", [])) > 0
            tprint(f"  · {state}/{apmc} {from_d}→{to_d}: {'Y' if result else 'N'} ({time.time()-t0:.1f}s)")
            return result
        except Exception as exc:
            _local.last_call = time.time()
            if attempt == 2:
                tprint(f"  WARNING: {exc} ({state}/{apmc} {from_d}–{to_d})",
                       file=sys.stderr)
                return False
            time.sleep(2 ** attempt)  # backoff: 1s, 2s
    return False


# ── Mandi list ───────────────────────────────────────────────────────────────
def get_all_mandis() -> list:
    """Fetch (state_id, state_name, mandi_id, mandi_name) from portal API."""
    r = requests.post(BASE_URL + "ajax_ctrl/states_name", headers=AJAX_HEADERS,
                      data={}, timeout=CALL_TIMEOUT)
    states = r.json()["data"]
    time.sleep(CALL_SLEEP)

    mandis = []
    for state in states:
        r = requests.post(BASE_URL + "Ajax_ctrl/apmc_list", headers=AJAX_HEADERS,
                          data={"state_id": state["state_id"]}, timeout=CALL_TIMEOUT)
        time.sleep(CALL_SLEEP)
        for a in r.json().get("data", []):
            mandis.append({
                "state_id":   state["state_id"],
                "state_name": state["state_name"],
                "mandi_id":   a["apmc_id"],
                "mandi_name": a["apmc_name"],
            })
    return mandis


# ── Date helpers ──────────────────────────────────────────────────────────────
def month_end(d: date) -> date:
    if d.month == 12:
        return date(d.year, 12, 31)
    return date(d.year, d.month + 1, 1) - timedelta(days=1)


def add_months(d: date, n: int) -> date:
    m = d.month + n
    y = d.year + (m - 1) // 12
    m = ((m - 1) % 12) + 1
    return date(y, m, 1)


def half_year_windows(start: date = ENAM_LAUNCH) -> list:
    """6-month (start, end) pairs from start → TODAY."""
    windows = []
    s = date(start.year, start.month, 1)
    while s <= TODAY:
        e = min(month_end(add_months(s, 5)), TODAY)
        windows.append((s, e))
        s = add_months(date(e.year, e.month, 1), 1)  # first of month after e's month
    return windows


def months_in(start: date, end: date) -> list:
    months = []
    m = date(start.year, start.month, 1)
    while m <= end:
        months.append(m)
        m = add_months(m, 1)
    return months


# ── Binary search (Phase 1 + Phase 2) ────────────────────────────────────────
def find_first_trade_month(state: str, apmc: str) -> Optional[date]:
    """
    2-phase binary search.
    Phase 1: find 6-month window with first data.
    Phase 2: find month within that window.
    Returns the 1st of the target month, or None if no data at all.
    """
    # Quick all-time check
    if not has_data(state, apmc, ENAM_LAUNCH, TODAY):
        return None

    # Phase 1: 6-month windows
    windows = half_year_windows()
    lo, hi = 0, len(windows) - 1
    while lo < hi:
        mid = (lo + hi) // 2
        if has_data(state, apmc, ENAM_LAUNCH, windows[mid][1]):
            hi = mid
        else:
            lo = mid + 1
    win_start, win_end = windows[lo]

    # Phase 2: months within window
    month_list = months_in(win_start, win_end)
    lo_m, hi_m = 0, len(month_list) - 1
    while lo_m < hi_m:
        mid_m = (lo_m + hi_m) // 2
        if has_data(state, apmc, ENAM_LAUNCH, month_end(month_list[mid_m])):
            hi_m = mid_m
        else:
            lo_m = mid_m + 1

    return month_list[lo_m]


# ── Worker function ───────────────────────────────────────────────────────────
def process_mandi(mandi: dict, idx: int, total: int) -> dict:
    """Find first trade month for one mandi. Called from a thread pool worker."""
    state = mandi["state_name"]
    apmc  = mandi["mandi_name"]
    tprint(f"[{idx}/{total}] {state} / {apmc} ...", end=" ")

    first_month = find_first_trade_month(state, apmc)

    result = {
        "state_id":               mandi["state_id"],
        "state_name":             state,
        "mandi_id":               mandi["mandi_id"],
        "mandi_name":             apmc,
        "first_enam_trade_date":  first_month.isoformat() if first_month else "",
    }
    tprint(str(first_month) if first_month else "no data")
    return result


# ── CSV helpers ───────────────────────────────────────────────────────────────
FIELDNAMES = ["state_id", "state_name", "mandi_id", "mandi_name",
              "first_enam_trade_date"]


def load_done(path: Path) -> tuple:
    """Return (done_ids set, results list) from an existing CSV."""
    done_ids, results = set(), []
    if path.exists():
        with open(path, newline="") as f:
            for row in csv.DictReader(f):
                done_ids.add(row["mandi_id"])
                results.append(row)
    return done_ids, results


def append_result(path: Path, row: dict, all_results: list) -> None:
    """Thread-safe incremental CSV write."""
    with _csv_lock:
        all_results.append(row)
        with open(path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=FIELDNAMES)
            w.writeheader()
            w.writerows(all_results)


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    tprint("Fetching mandi list from portal API...")
    mandis = get_all_mandis()
    tprint(f"Found {len(mandis)} mandis across "
           f"{len({m['state_id'] for m in mandis})} states/UTs.")

    done_ids, results = load_done(OUTPUT)
    if done_ids:
        tprint(f"Resuming: {len(done_ids)} mandis already processed.")

    pending = [m for m in mandis if m["mandi_id"] not in done_ids]
    tprint(f"{len(pending)} mandis to process with {WORKERS} parallel workers.\n")

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {
            pool.submit(process_mandi, m, i + 1 + len(done_ids), len(mandis)): m
            for i, m in enumerate(pending)
        }
        for future in as_completed(futures):
            row = future.result()
            append_result(OUTPUT, row, results)

    # ── Summary ───────────────────────────────────────────────────────────
    tprint(f"\n{'='*60}")
    tprint(f"Done. {len(results)} mandis saved to {OUTPUT}")

    quarters: collections.Counter = collections.Counter()
    no_data = 0
    for row in results:
        if row["first_enam_trade_date"]:
            d = date.fromisoformat(row["first_enam_trade_date"])
            quarters[f"Q{(d.month - 1) // 3 + 1} {d.year}"] += 1
        else:
            no_data += 1

    tprint(f"\nMandis with no recorded trade data : {no_data}")
    tprint("Mandis by quarter of first eNAM trade:")
    for q in sorted(quarters, key=lambda x: (int(x.split()[1]), int(x.split()[0][1]))):
        tprint(f"  {q}: {quarters[q]:>4d}")


if __name__ == "__main__":
    main()
