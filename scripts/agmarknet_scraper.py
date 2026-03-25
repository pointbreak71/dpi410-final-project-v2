#!/usr/bin/env python3
"""
Agmarknet Price Data Scraper
Downloads mandi-level daily price data from the Agmarknet 2.0 API
(https://api.agmarknet.gov.in/v1)

Output:
  data/raw/prices/{crop}/{state_id}_{year}_{month:02d}.json.gz  — raw API responses (compressed)
  data/clean/prices/{crop}_monthly.csv                          — monthly aggregates per mandi
  data/clean/prices/{crop}_yearly.csv                           — yearly aggregates per mandi
  data/clean/match_report.csv                                   — fuzzy mandi match report

Usage:
  python scripts/agmarknet_scraper.py [--start-year 2010] [--end-year 2025] [--workers 8]
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
from rapidfuzz import fuzz, process

# ---------------------------------------------------------------------------
# Configuration
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

# Priority crops: name -> Agmarknet commodity ID
CROPS = {
    "wheat":   1,
    "paddy":   2,   # Paddy(Common)
    "rice":    3,
    "maize":   4,
    "onion":   23,
    "tomato":  65,
    "potato":  24,
    "mustard": 12,
    "soybean": 13,  # Soyabean
    "cotton":  15,
    "chana":   6,   # Bengal Gram(Gram)(Whole)
}

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "prices"
CLEAN_DIR = PROJECT_ROOT / "data" / "clean" / "prices"
ENAM_CSV = PROJECT_ROOT / "data" / "clean" / "enam_adoption.csv"

FUZZY_THRESHOLD = 85  # minimum score (0-100) for a match to be accepted

# Normalise Agmarknet API state names → enam_adoption.csv state names
STATE_NAME_MAP = {
    "Chattisgarh":         "Chhattisgarh",
    "Pondicherry":         "Puducherry",
    "NCT of Delhi":        "Delhi",
    "Uttrakhand":          "Uttarakhand",
    "Jammu and Kashmir":   "Jammu & Kashmir",
}

# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def fetch_state_list():
    """Return dict of {state_id: state_name} from Agmarknet API."""
    states = {}
    for page in range(1, 10):
        resp = requests.get(
            f"{BASE_URL}/location/state",
            params={"page": page},
            headers=HEADERS,
            timeout=30,
        )
        if resp.status_code != 200:
            break  # no more pages
        data = resp.json().get("states", [])
        if not data:
            break
        for s in data:
            states[s["id"]] = s["state_name"]
    return states


def fetch_month(session, state_id, commodity_id, year, month, retries=3):
    """
    Fetch one state × commodity × year-month combination.
    Returns the parsed JSON dict, or None on failure.
    """
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

def flatten_response(data, state_id, state_name, commodity_name, year, month):
    """
    Convert one API response dict into a list of flat row dicts.
    Each row is one (market, date, variety) observation.
    """
    state_name = STATE_NAME_MAP.get(state_name, state_name)
    rows = []
    markets = data.get("markets", [])
    for mkt in markets:
        market_name = mkt.get("marketName", "")
        for day_entry in mkt.get("dates", []):
            arrival_date = day_entry.get("arrivalDate", "")
            for obs in day_entry.get("data", []):
                rows.append({
                    "state_id":       state_id,
                    "state_name":     state_name,
                    "market_name":    market_name,
                    "commodity":      commodity_name,
                    "arrival_date":   arrival_date,
                    "variety":        obs.get("variety", ""),
                    "arrivals_mt":    obs.get("arrivals"),
                    "min_price":      obs.get("minimumPrice"),
                    "max_price":      obs.get("maximumPrice"),
                    "modal_price":    obs.get("modalPrice"),
                })
    return rows


# ---------------------------------------------------------------------------
# Aggregation helpers
# ---------------------------------------------------------------------------

def build_monthly(rows):
    """
    Group raw daily rows into monthly market-level aggregates.
    Arrival-weighted average of modal price; summed arrivals.
    """
    groups = defaultdict(lambda: {"arrivals": 0.0, "price_sum": 0.0, "n": 0})
    for r in rows:
        try:
            date_parts = r["arrival_date"].split("/")
            m = int(date_parts[1])
            y = int(date_parts[2])
        except Exception:
            continue
        key = (r["state_id"], r["state_name"], r["market_name"],
               r["commodity"], y, m)
        arr = r["arrivals_mt"] or 0.0
        price = r["modal_price"]
        if price is not None:
            groups[key]["price_sum"] += price * arr
            groups[key]["arrivals"] += arr
            groups[key]["n"] += 1

    monthly = []
    for (sid, sname, mkt, cmdt, y, m), v in groups.items():
        avg_price = (v["price_sum"] / v["arrivals"]) if v["arrivals"] > 0 else None
        monthly.append({
            "state_id":    sid,
            "state_name":  sname,
            "market_name": mkt,
            "commodity":   cmdt,
            "year":        y,
            "month":       m,
            "arrivals_mt": round(v["arrivals"], 2),
            "modal_price_avg": round(avg_price, 2) if avg_price is not None else None,
            "n_obs":       v["n"],
        })
    return monthly


def build_yearly(monthly_rows):
    """Aggregate monthly records to yearly."""
    groups = defaultdict(lambda: {"arrivals": 0.0, "price_sum": 0.0, "months": 0})
    for r in monthly_rows:
        key = (r["state_id"], r["state_name"], r["market_name"],
               r["commodity"], r["year"])
        arr = r["arrivals_mt"] or 0.0
        price = r["modal_price_avg"]
        if price is not None:
            groups[key]["price_sum"] += price * arr
            groups[key]["arrivals"] += arr
            groups[key]["months"] += 1

    yearly = []
    for (sid, sname, mkt, cmdt, y), v in groups.items():
        avg_price = (v["price_sum"] / v["arrivals"]) if v["arrivals"] > 0 else None
        yearly.append({
            "state_id":    sid,
            "state_name":  sname,
            "market_name": mkt,
            "commodity":   cmdt,
            "year":        y,
            "arrivals_mt": round(v["arrivals"], 2),
            "modal_price_avg": round(avg_price, 2) if avg_price is not None else None,
            "months_with_data": v["months"],
        })
    return yearly


# ---------------------------------------------------------------------------
# Mandi matching
# ---------------------------------------------------------------------------

def load_enam_mandis():
    """Return DataFrame from enam_adoption.csv."""
    if not ENAM_CSV.exists():
        print(f"WARNING: {ENAM_CSV} not found — skipping mandi matching.")
        return None
    return pd.read_csv(ENAM_CSV)


def build_match_key(market_name, state_name):
    """Normalised key for matching: lowercase, strip common suffixes."""
    s = market_name.lower().strip()
    for suffix in [" apmc", " mandi", " market", " agri market"]:
        if s.endswith(suffix):
            s = s[: -len(suffix)].strip()
    return s


def match_markets_to_enam(all_markets, enam_df):
    """
    Two-stage match:
      1. Exact match on normalised name + state
      2. Fuzzy match (RapidFuzz token_sort_ratio) on name within same state
    Returns a dict: (market_name, state_name) -> {mandi_id, match_type, score}
    """
    enam_df = enam_df.copy()
    enam_df["_key"] = enam_df.apply(
        lambda r: build_match_key(str(r["mandi_name"]), str(r["state"])), axis=1
    )
    enam_df["_state_norm"] = enam_df["state"].str.lower().str.strip()

    results = {}

    for (market_name, state_name) in all_markets:
        mkt_key = build_match_key(market_name, state_name)
        state_norm = state_name.lower().strip()

        # Filter to same state (flexible: substring match for UTs)
        state_mask = enam_df["_state_norm"].apply(
            lambda s: state_norm in s or s in state_norm
        )
        candidates = enam_df[state_mask]

        if candidates.empty:
            results[(market_name, state_name)] = {
                "mandi_id": None,
                "matched_mandi": None,
                "match_type": "no_state_match",
                "score": 0,
            }
            continue

        # Stage 1: exact
        exact = candidates[candidates["_key"] == mkt_key]
        if not exact.empty:
            row = exact.iloc[0]
            results[(market_name, state_name)] = {
                "mandi_id": int(row["mandi_id"]),
                "matched_mandi": row["mandi_name"],
                "match_type": "exact",
                "score": 100,
            }
            continue

        # Stage 2: fuzzy
        choices = candidates["_key"].tolist()
        best = process.extractOne(
            mkt_key, choices, scorer=fuzz.token_sort_ratio
        )
        if best and best[1] >= FUZZY_THRESHOLD:
            matched_idx = candidates[candidates["_key"] == best[0]].index[0]
            row = enam_df.loc[matched_idx]
            results[(market_name, state_name)] = {
                "mandi_id": int(row["mandi_id"]),
                "matched_mandi": row["mandi_name"],
                "match_type": "fuzzy",
                "score": best[1],
            }
        else:
            best_score = best[1] if best else 0
            results[(market_name, state_name)] = {
                "mandi_id": None,
                "matched_mandi": best[0] if best else None,
                "match_type": "unmatched",
                "score": best_score,
            }

    return results


# ---------------------------------------------------------------------------
# CSV writers
# ---------------------------------------------------------------------------

MONTHLY_FIELDS = [
    "state_id", "state_name", "market_name", "mandi_id", "match_type",
    "match_score", "commodity", "year", "month", "arrivals_mt",
    "modal_price_avg", "n_obs",
]
YEARLY_FIELDS = [
    "state_id", "state_name", "market_name", "mandi_id", "match_type",
    "match_score", "commodity", "year", "arrivals_mt", "modal_price_avg",
    "months_with_data",
]
MATCH_REPORT_FIELDS = [
    "market_name", "state_name", "mandi_id", "matched_mandi",
    "match_type", "score", "n_crops",
]


def append_rows_to_csv(path, rows, fieldnames):
    """Append rows to a CSV, writing header only if file is new."""
    is_new = not path.exists()
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if is_new:
            writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# Main scraping loop
# ---------------------------------------------------------------------------

def run_scraper(start_year, end_year, workers, crops_filter=None):
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)

    print("Fetching state list from Agmarknet API …")
    states = fetch_state_list()
    print(f"  Found {len(states)} states")

    crops = {k: v for k, v in CROPS.items()
             if crops_filter is None or k in crops_filter}

    # Build full job list: (state_id, state_name, crop_name, commodity_id, year, month)
    months = [(y, m) for y in range(start_year, end_year + 1)
              for m in range(1, 13)]
    jobs = []
    for sid, sname in states.items():
        for crop_name, cmdt_id in crops.items():
            for year, month in months:
                raw_path = RAW_DIR / crop_name / f"{sid}_{year}_{month:02d}.json.gz"
                if raw_path.exists():
                    continue  # already downloaded
                jobs.append((sid, sname, crop_name, cmdt_id, year, month))

    total = len(jobs)
    done = 0
    skipped_empty = 0
    errors = 0

    # Load eNAM mandi list for matching
    enam_df = load_enam_mandis()

    # Track all unique (market_name, state_name) pairs for match report
    all_market_keys = set()

    # Per-crop accumulator for monthly rows (flushed periodically to CSV)
    crop_monthly_rows = defaultdict(list)

    print(f"\nStarting download: {total} combinations to fetch")
    print(f"  Start year: {start_year}, End year: {end_year}")
    print(f"  Crops: {', '.join(crops.keys())}")
    print(f"  Workers: {workers}")
    print(f"  Raw output: {RAW_DIR}")
    print(f"  Clean output: {CLEAN_DIR}\n")

    t0 = time.time()

    def process_job(job):
        sid, sname, crop_name, cmdt_id, year, month = job
        raw_path = RAW_DIR / crop_name / f"{sid}_{year}_{month:02d}.json.gz"
        raw_path.parent.mkdir(parents=True, exist_ok=True)

        session = requests.Session()
        data = fetch_month(session, sid, cmdt_id, year, month)
        if data is None:
            return ("error", crop_name, sid, sname, year, month, [], [])

        markets = data.get("markets", [])
        if not markets:
            # Save tiny sentinel so we don't retry empty combos
            with gzip.open(raw_path, "wt", encoding="utf-8") as f:
                json.dump({"markets": []}, f)
            return ("empty", crop_name, sid, sname, year, month, [], [])

        # Save raw compressed JSON
        with gzip.open(raw_path, "wt", encoding="utf-8") as f:
            json.dump(data, f)

        # Flatten and aggregate to monthly
        rows = flatten_response(data, sid, sname, crop_name, year, month)
        monthly = build_monthly(rows)
        return ("ok", crop_name, sid, sname, year, month, monthly)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(process_job, job): job for job in jobs}
        for future in as_completed(futures):
            result = future.result()
            status, crop_name, sid, sname, year, month = result[:6]
            monthly = result[6] if len(result) > 6 else []

            done += 1
            if status == "empty":
                skipped_empty += 1
            elif status == "error":
                errors += 1
            else:
                crop_monthly_rows[crop_name].extend(monthly)
                for r in monthly:
                    all_market_keys.add((r["market_name"], r["state_name"]))

            # Flush monthly rows to CSV every 500 results to limit memory
            if done % 500 == 0 or done == total:
                _flush_csvs(crop_monthly_rows, enam_df)
                crop_monthly_rows.clear()

            elapsed = time.time() - t0
            rate = done / elapsed if elapsed > 0 else 0
            remaining = (total - done) / rate if rate > 0 else 0
            print(
                f"  [{done}/{total}] {crop_name} | {sname} {year}-{month:02d} "
                f"| {status} | {elapsed/60:.1f}m elapsed "
                f"| ETA {remaining/60:.1f}m",
                end="\r",
            )

    print(f"\n\nDownload complete in {(time.time()-t0)/60:.1f} minutes")
    print(f"  OK: {done - skipped_empty - errors}  Empty: {skipped_empty}  Errors: {errors}")

    # Build yearly CSVs by aggregating each completed monthly CSV
    print("\nBuilding yearly aggregates from monthly CSVs …")
    for crop_name in crops:
        monthly_path = CLEAN_DIR / f"{crop_name}_monthly.csv"
        yearly_path  = CLEAN_DIR / f"{crop_name}_yearly.csv"
        if not monthly_path.exists():
            continue
        try:
            df = pd.read_csv(monthly_path)
            df["_wp"] = df["modal_price_avg"].fillna(0) * df["arrivals_mt"].fillna(0)
            grp = df.groupby(
                ["state_id", "state_name", "market_name", "mandi_id",
                 "match_type", "match_score", "commodity", "year"],
                dropna=False,
            ).agg(
                arrivals_mt=("arrivals_mt", "sum"),
                _wp_sum=("_wp", "sum"),
                _arr_sum=("arrivals_mt", "sum"),
                months_with_data=("month", "count"),
            ).reset_index()
            grp["modal_price_avg"] = (
                grp["_wp_sum"] / grp["_arr_sum"].replace(0, float("nan"))
            ).round(2)
            grp.drop(columns=["_wp_sum", "_arr_sum"], inplace=True)
            grp["arrivals_mt"] = grp["arrivals_mt"].round(2)
            grp[YEARLY_FIELDS].to_csv(yearly_path, index=False)
            print(f"  {crop_name}: {len(grp):,} yearly rows")
        except Exception as exc:
            print(f"  {crop_name}: yearly aggregation failed — {exc}")

    # Build match report
    if enam_df is not None and all_market_keys:
        _write_match_report(all_market_keys, enam_df)

    _print_summary(start_year, end_year, crops)


def _flush_csvs(crop_monthly_rows, enam_df):
    """Write accumulated monthly rows to per-crop CSVs, attaching mandi_id."""
    match_cache = {}

    for crop_name, rows in crop_monthly_rows.items():
        if not rows:
            continue
        # Attach mandi IDs via exact/fuzzy matching
        if enam_df is not None:
            keys = {(r["market_name"], r["state_name"]) for r in rows}
            new_keys = keys - set(match_cache.keys())
            if new_keys:
                new_matches = match_markets_to_enam(new_keys, enam_df)
                match_cache.update(new_matches)
        for r in rows:
            m = match_cache.get((r["market_name"], r["state_name"]), {})
            r["mandi_id"] = m.get("mandi_id")
            r["match_type"] = m.get("match_type", "no_enam_df")
            r["match_score"] = m.get("score", 0)

        path = CLEAN_DIR / f"{crop_name}_monthly.csv"
        append_rows_to_csv(path, rows, MONTHLY_FIELDS)


def _write_match_report(all_market_keys, enam_df):
    """Write match_report.csv for all unique (market, state) pairs."""
    print("\nBuilding mandi match report …")
    matches = match_markets_to_enam(all_market_keys, enam_df)

    # Count how many crops each market appears in
    # (not tracked here, so default to unknown)
    report_path = PROJECT_ROOT / "data" / "clean" / "match_report.csv"
    rows = []
    for (mkt, state), m in matches.items():
        rows.append({
            "market_name":    mkt,
            "state_name":     state,
            "mandi_id":       m.get("mandi_id"),
            "matched_mandi":  m.get("matched_mandi"),
            "match_type":     m.get("match_type"),
            "score":          m.get("score"),
            "n_crops":        "",
        })
    rows.sort(key=lambda r: (r["match_type"], r["state_name"], r["market_name"]))
    with open(report_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=MATCH_REPORT_FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    n_exact   = sum(1 for r in rows if r["match_type"] == "exact")
    n_fuzzy   = sum(1 for r in rows if r["match_type"] == "fuzzy")
    n_unmatched = sum(1 for r in rows if r["match_type"] == "unmatched")
    print(f"  Match report written: {report_path}")
    print(f"  Exact: {n_exact}  Fuzzy: {n_fuzzy}  Unmatched: {n_unmatched}")


def _print_summary(start_year, end_year, crops):
    print("\n" + "="*60)
    print("SUMMARY REPORT")
    print("="*60)
    print(f"Date range collected: {start_year}–{end_year}")
    print(f"Crops: {', '.join(crops.keys())}")
    print()
    for crop_name in crops:
        monthly_path = CLEAN_DIR / f"{crop_name}_monthly.csv"
        yearly_path  = CLEAN_DIR / f"{crop_name}_yearly.csv"
        if monthly_path.exists():
            try:
                df = pd.read_csv(monthly_path)
                n_markets  = df["market_name"].nunique()
                n_states   = df["state_name"].nunique()
                n_obs      = len(df)
                matched    = df["mandi_id"].notna().sum()
                match_pct  = 100 * matched / n_obs if n_obs > 0 else 0
                year_range = f"{int(df['year'].min())}–{int(df['year'].max())}" if n_obs > 0 else "n/a"
                print(f"  {crop_name:10s}: {n_obs:>8,} monthly obs | "
                      f"{n_markets:>5} markets | {n_states:>2} states | "
                      f"eNAM match: {match_pct:.0f}% | years: {year_range}")
            except Exception as e:
                print(f"  {crop_name}: (error reading CSV: {e})")
        else:
            print(f"  {crop_name}: no data")
    print()
    print(f"Clean CSVs: {CLEAN_DIR}")
    match_report = PROJECT_ROOT / "data" / "clean" / "match_report.csv"
    if match_report.exists():
        print(f"Match report: {match_report}")
    print("="*60)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Agmarknet price data scraper")
    parser.add_argument("--start-year", type=int, default=2010,
                        help="First year to collect (default: 2010)")
    parser.add_argument("--end-year",   type=int, default=2025,
                        help="Last year to collect (default: 2025)")
    parser.add_argument("--workers",    type=int, default=8,
                        help="Parallel download workers (default: 8)")
    parser.add_argument("--crops",      nargs="+", default=None,
                        choices=list(CROPS.keys()),
                        help="Limit to specific crops (default: all)")
    args = parser.parse_args()

    run_scraper(
        start_year=args.start_year,
        end_year=args.end_year,
        workers=args.workers,
        crops_filter=set(args.crops) if args.crops else None,
    )
