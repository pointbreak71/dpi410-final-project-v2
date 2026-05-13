#!/usr/bin/env python3
"""
Agmarknet Price Range Scraper
Computes mandi-level monthly price range (max - min) from Agmarknet data.

Reads from existing raw JSON.gz files produced by agmarknet_scraper.py where
available; downloads any missing months from the API.

Output:
  data/raw/prices/{crop}/{state_id}_{year}_{month:02d}.json.gz  — raw (shared with modal scraper)
  data/clean/prices/{crop}_monthly_range.csv                    — monthly range per mandi
  data/clean/prices/{crop}_yearly_range.csv                     — yearly range per mandi

Usage:
  python scripts/agmarknet_range_scraper.py [--start-year 2010] [--end-year 2025] [--workers 8]
  python scripts/agmarknet_range_scraper.py --crops wheat onion --start-year 2018
"""

import argparse
import csv
import gzip
import json
import os
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Configuration (mirrors agmarknet_scraper.py)
# ---------------------------------------------------------------------------

BASE_URL = "https://api.agmarknet.gov.in/v1"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
}

CROPS = {
    "wheat":   1,
    "paddy":   2,
    "rice":    3,
    "maize":   4,
    "onion":   23,
    "tomato":  65,
    "potato":  24,
    "mustard": 12,
    "soybean": 13,
    "cotton":  15,
    "chana":   6,
}

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR   = PROJECT_ROOT / "data" / "raw"   / "prices"
CLEAN_DIR = PROJECT_ROOT / "data" / "clean" / "prices"

STATE_NAME_MAP = {
    "Chattisgarh":       "Chhattisgarh",
    "Pondicherry":       "Puducherry",
    "NCT of Delhi":      "Delhi",
    "Uttrakhand":        "Uttarakhand",
    "Jammu and Kashmir": "Jammu & Kashmir",
}

# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def fetch_state_list():
    states = {}
    for page in range(1, 10):
        resp = requests.get(
            f"{BASE_URL}/location/state",
            params={"page": page},
            headers=HEADERS,
            timeout=30,
        )
        if resp.status_code != 200:
            break
        data = resp.json().get("states", [])
        if not data:
            break
        for s in data:
            states[s["id"]] = s["state_name"]
    return states


def fetch_month(session, state_id, commodity_id, year, month, retries=3):
    params = {
        "year": year,
        "month": month,
        "stateId": state_id,
        "commodityId": commodity_id,
    }
    for attempt in range(retries):
        try:
            resp = session.get(
                f"{BASE_URL}/prices-and-arrivals/date-wise/specific-commodity",
                params=params,
                headers=HEADERS,
                timeout=45,
            )
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429:
                wait = 10 * (attempt + 1)
                print(f"  Rate limited, waiting {wait}s …")
                time.sleep(wait)
            else:
                print(f"  HTTP {resp.status_code} for state={state_id} "
                      f"cmdt={commodity_id} {year}-{month:02d}")
                return None
        except requests.exceptions.Timeout:
            print(f"  Timeout (attempt {attempt+1}) for state={state_id} "
                  f"cmdt={commodity_id} {year}-{month:02d}")
            time.sleep(5 * (attempt + 1))
        except requests.exceptions.RequestException as exc:
            print(f"  Request error: {exc}")
            time.sleep(5)
    return None


# ---------------------------------------------------------------------------
# Record flattening
# ---------------------------------------------------------------------------

def flatten_response(data, state_id, state_name, commodity_name):
    state_name = STATE_NAME_MAP.get(state_name, state_name)
    rows = []
    for mkt in data.get("markets", []):
        market_name = mkt.get("marketName", "")
        for day_entry in mkt.get("dates", []):
            arrival_date = day_entry.get("arrivalDate", "")
            for obs in day_entry.get("data", []):
                rows.append({
                    "state_id":    state_id,
                    "state_name":  state_name,
                    "market_name": market_name,
                    "commodity":   commodity_name,
                    "arrival_date": arrival_date,
                    "arrivals_mt": obs.get("arrivals"),
                    "min_price":   obs.get("minimumPrice"),
                    "max_price":   obs.get("maximumPrice"),
                })
    return rows


# ---------------------------------------------------------------------------
# Aggregation: monthly range
# ---------------------------------------------------------------------------

def build_monthly_range(rows):
    """
    Group raw daily rows into monthly market-level range aggregates.

    For each (market, commodity, year, month):
      max_price_avg  — arrival-weighted average of daily maximum prices
      min_price_avg  — arrival-weighted average of daily minimum prices
      range_avg      — max_price_avg minus min_price_avg
      n_obs          — count of daily observations with valid max AND min prices
    """
    groups = defaultdict(lambda: {
        "arrivals": 0.0,
        "max_sum":  0.0,
        "min_sum":  0.0,
        "n":        0,
    })

    for r in rows:
        try:
            day, mon, yr = r["arrival_date"].split("/")
            m, y = int(mon), int(yr)
        except Exception:
            continue

        max_p = r["max_price"]
        min_p = r["min_price"]
        if max_p is None or min_p is None:
            continue

        key = (r["state_id"], r["state_name"], r["market_name"], r["commodity"], y, m)
        arr = r["arrivals_mt"] or 0.0
        groups[key]["max_sum"]  += max_p * arr
        groups[key]["min_sum"]  += min_p * arr
        groups[key]["arrivals"] += arr
        groups[key]["n"]        += 1

    monthly = []
    for (sid, sname, mkt, cmdt, y, m), v in groups.items():
        if v["arrivals"] > 0:
            max_avg   = v["max_sum"]  / v["arrivals"]
            min_avg   = v["min_sum"]  / v["arrivals"]
            range_avg = max_avg - min_avg
        else:
            max_avg = min_avg = range_avg = None

        monthly.append({
            "state_id":      sid,
            "state_name":    sname,
            "market_name":   mkt,
            "commodity":     cmdt,
            "year":          y,
            "month":         m,
            "arrivals_mt":   round(v["arrivals"], 2),
            "max_price_avg": round(max_avg,   2) if max_avg   is not None else None,
            "min_price_avg": round(min_avg,   2) if min_avg   is not None else None,
            "range_avg":     round(range_avg, 2) if range_avg is not None else None,
            "n_obs":         v["n"],
        })
    return monthly


# ---------------------------------------------------------------------------
# CSV writers
# ---------------------------------------------------------------------------

MONTHLY_FIELDS = [
    "state_id", "state_name", "market_name", "commodity",
    "year", "month", "arrivals_mt",
    "max_price_avg", "min_price_avg", "range_avg", "n_obs",
]
YEARLY_FIELDS = [
    "state_id", "state_name", "market_name", "commodity",
    "year", "arrivals_mt",
    "max_price_avg", "min_price_avg", "range_avg", "months_with_data",
]


def append_rows_to_csv(path, rows, fieldnames):
    is_new = not path.exists()
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if is_new:
            writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# Main scraping / processing loop
# ---------------------------------------------------------------------------

def infer_states_from_raw(crops):
    """
    Build {state_id: ''} from filenames in the raw directory.
    Avoids an API call when raw files already exist.
    """
    states = {}
    for crop_name in crops:
        crop_dir = RAW_DIR / crop_name
        if not crop_dir.exists():
            continue
        for p in crop_dir.glob("*.json.gz"):
            sid = int(p.stem.split("_")[0])
            states[sid] = states.get(sid, "")
    return states


def run_scraper(start_year, end_year, workers, crops_filter=None):
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)

    crops = {k: v for k, v in CROPS.items()
             if crops_filter is None or k in crops_filter}

    # Use existing raw files to infer state IDs — no API call needed
    states = infer_states_from_raw(crops)
    if states:
        print(f"Inferred {len(states)} states from existing raw files")
    else:
        print("No raw files found — fetching state list from API …")
        states = fetch_state_list()
    print(f"  Found {len(states)} states")

    months = [(y, m) for y in range(start_year, end_year + 1)
              for m in range(1, 13)]

    # Clear existing range CSVs so we don't accumulate duplicates across runs
    print("\nClearing existing range CSVs …")
    for crop_name in crops:
        for suffix in ("_monthly_range.csv", "_yearly_range.csv"):
            p = CLEAN_DIR / f"{crop_name}{suffix}"
            if p.exists():
                p.unlink()
                print(f"  Removed {p.name}")

    # Build job list: all (state, crop, year, month) combinations
    # Raw files from the modal scraper are reused — we only download what's missing.
    jobs = []
    for sid, sname in states.items():
        for crop_name, cmdt_id in crops.items():
            for year, month in months:
                jobs.append((sid, sname, crop_name, cmdt_id, year, month))

    total = len(jobs)
    done = skipped_empty = errors = 0
    crop_monthly_rows = defaultdict(list)

    print(f"\nProcessing {total:,} combinations "
          f"({start_year}–{end_year}, {len(crops)} crops, {len(states)} states)")
    print(f"  Raw files will be reused from {RAW_DIR} where available")
    print(f"  Clean output → {CLEAN_DIR}\n")

    t0 = time.time()

    def process_job(job):
        sid, sname, crop_name, cmdt_id, year, month = job
        raw_path = RAW_DIR / crop_name / f"{sid}_{year}_{month:02d}.json.gz"

        if raw_path.exists():
            # Reuse existing raw file — no download needed
            try:
                with gzip.open(raw_path, "rt", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception as exc:
                return ("error", crop_name, sid, sname, year, month, [])
        else:
            # Download and cache
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            session = requests.Session()
            data = fetch_month(session, sid, cmdt_id, year, month)
            if data is None:
                return ("error", crop_name, sid, sname, year, month, [])
            with gzip.open(raw_path, "wt", encoding="utf-8") as f:
                json.dump(data, f)

        if not data.get("markets"):
            return ("empty", crop_name, sid, sname, year, month, [])

        rows    = flatten_response(data, sid, sname, crop_name)
        monthly = build_monthly_range(rows)
        return ("ok", crop_name, sid, sname, year, month, monthly)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(process_job, job): job for job in jobs}
        for future in as_completed(futures):
            result = future.result()
            status, crop_name, sid, sname, year, month = result[:6]
            monthly = result[6]

            done += 1
            if status == "empty":
                skipped_empty += 1
            elif status == "error":
                errors += 1
            else:
                crop_monthly_rows[crop_name].extend(monthly)

            # Flush to CSV every 500 results
            if done % 500 == 0 or done == total:
                _flush_csvs(crop_monthly_rows)
                crop_monthly_rows.clear()

            elapsed = time.time() - t0
            rate = done / elapsed if elapsed > 0 else 0
            remaining = (total - done) / rate if rate > 0 else 0
            print(
                f"  [{done}/{total}] {crop_name} | {sname} {year}-{month:02d} "
                f"| {status} | {elapsed/60:.1f}m elapsed | ETA {remaining/60:.1f}m",
                end="\r",
            )

    print(f"\n\nProcessing complete in {(time.time()-t0)/60:.1f} minutes")
    print(f"  OK: {done - skipped_empty - errors}  Empty: {skipped_empty}  Errors: {errors}")

    _build_yearly_csvs(crops)
    _print_summary(start_year, end_year, crops)


def _flush_csvs(crop_monthly_rows):
    for crop_name, rows in crop_monthly_rows.items():
        if not rows:
            continue
        path = CLEAN_DIR / f"{crop_name}_monthly_range.csv"
        append_rows_to_csv(path, rows, MONTHLY_FIELDS)


def _build_yearly_csvs(crops):
    print("\nBuilding yearly range aggregates …")
    for crop_name in crops:
        monthly_path = CLEAN_DIR / f"{crop_name}_monthly_range.csv"
        yearly_path  = CLEAN_DIR / f"{crop_name}_yearly_range.csv"
        if not monthly_path.exists():
            continue
        try:
            df = pd.read_csv(monthly_path)

            # Arrival-weighted yearly max and min; then yearly range = max_avg - min_avg
            df["_max_wp"] = df["max_price_avg"].fillna(0) * df["arrivals_mt"].fillna(0)
            df["_min_wp"] = df["min_price_avg"].fillna(0) * df["arrivals_mt"].fillna(0)

            grp = df.groupby(
                ["state_id", "state_name", "market_name", "commodity", "year"],
                dropna=False,
            ).agg(
                arrivals_mt      = ("arrivals_mt", "sum"),
                _max_wp_sum      = ("_max_wp",     "sum"),
                _min_wp_sum      = ("_min_wp",     "sum"),
                _arr_sum         = ("arrivals_mt", "sum"),
                months_with_data = ("month",       "count"),
            ).reset_index()

            grp["max_price_avg"] = (
                grp["_max_wp_sum"] / grp["_arr_sum"].replace(0, float("nan"))
            ).round(2)
            grp["min_price_avg"] = (
                grp["_min_wp_sum"] / grp["_arr_sum"].replace(0, float("nan"))
            ).round(2)
            grp["range_avg"] = (grp["max_price_avg"] - grp["min_price_avg"]).round(2)
            grp["arrivals_mt"] = grp["arrivals_mt"].round(2)

            grp.drop(columns=["_max_wp_sum", "_min_wp_sum", "_arr_sum"], inplace=True)
            grp[YEARLY_FIELDS].to_csv(yearly_path, index=False)
            print(f"  {crop_name}: {len(grp):,} yearly rows → {yearly_path.name}")
        except Exception as exc:
            print(f"  {crop_name}: yearly aggregation failed — {exc}")


def _print_summary(start_year, end_year, crops):
    print("\n" + "=" * 60)
    print("SUMMARY REPORT — PRICE RANGE")
    print("=" * 60)
    print(f"Date range: {start_year}–{end_year}")
    print(f"Crops: {', '.join(crops.keys())}")
    print()
    for crop_name in crops:
        monthly_path = CLEAN_DIR / f"{crop_name}_monthly_range.csv"
        if not monthly_path.exists():
            print(f"  {crop_name:10s}: no data")
            continue
        try:
            df = pd.read_csv(monthly_path)
            n_obs     = len(df)
            n_markets = df["market_name"].nunique()
            n_states  = df["state_name"].nunique()
            yr_range  = f"{int(df['year'].min())}–{int(df['year'].max())}" if n_obs > 0 else "n/a"
            avg_range = df["range_avg"].mean()
            print(f"  {crop_name:10s}: {n_obs:>8,} monthly obs | "
                  f"{n_markets:>5} markets | {n_states:>2} states | "
                  f"mean range: ₹{avg_range:.0f} | years: {yr_range}")
        except Exception as e:
            print(f"  {crop_name}: (error reading CSV: {e})")
    print()
    print(f"Clean CSVs: {CLEAN_DIR}")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Agmarknet price range scraper (max - min per mandi)"
    )
    parser.add_argument("--start-year", type=int, default=2010)
    parser.add_argument("--end-year",   type=int, default=2025)
    parser.add_argument("--workers",    type=int, default=8)
    parser.add_argument("--crops", nargs="+", default=None,
                        choices=list(CROPS.keys()))
    args = parser.parse_args()

    run_scraper(
        start_year=args.start_year,
        end_year=args.end_year,
        workers=args.workers,
        crops_filter=set(args.crops) if args.crops else None,
    )
