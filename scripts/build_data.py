"""
build_data.py
-------------
Constructs the three clean analysis datasets from raw inputs.
Run once; outputs saved to data/clean/did_inputs/.

Cleaning steps documented in README_data.md.
"""

import warnings
warnings.filterwarnings("ignore")

import re
import numpy as np
import pandas as pd
from pathlib import Path

ROOT    = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "data" / "clean" / "did_inputs"
OUT_DIR.mkdir(parents=True, exist_ok=True)

SEP = "=" * 70

CROPS_ALL = ["wheat", "paddy", "rice", "maize", "onion", "tomato",
             "potato", "mustard", "soybean", "cotton", "chana"]

# Sample B: drop crops with < 50 mandis in range files (low within-mandi coverage)
# rice=32, soybean=32, chana=34, mustard=39 → dropped
CROPS_BALANCED = ["wheat", "paddy", "maize", "onion", "tomato", "potato", "cotton"]


# ── helpers ───────────────────────────────────────────────────────────────────

def norm_name(s: str) -> str:
    """Normalise a mandi/market name for fuzzy key matching."""
    s = str(s).upper().strip()
    s = re.sub(r"\b(APMC|MANDI|MARKET|KRISHI|UPAJ)\b", "", s)
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    return re.sub(r"\s+", " ", s).strip()

def norm_state(s: str) -> str:
    """Normalise a state name (handles & vs AND, case)."""
    s = str(s).upper().strip()
    s = s.replace("&", "AND").replace("-", " ")
    return re.sub(r"\s+", " ", s).strip()


# ── 1. LOAD RAW FILES ─────────────────────────────────────────────────────────

print(SEP)
print("STEP 1: LOADING RAW FILES")
print(SEP)

# 1a. First-trade dates (mandi-level, portal apmc_id)
adopt_dates = pd.read_csv(ROOT / "enam_adoption_dates.csv")
adopt_dates["first_enam_trade_date"] = pd.to_datetime(
    adopt_dates["first_enam_trade_date"], errors="coerce"
)
print(f"[adopt_dates]   {adopt_dates.shape}  | cols: {adopt_dates.columns.tolist()}")

# 1b. eNAM metadata (sequential mandi_id, has district)
meta = pd.read_csv(ROOT / "data/clean/enam_adoption.csv")
print(f"[meta]          {meta.shape}  | cols: {meta.columns.tolist()}")

# 1c. Market-name → sequential mandi_id bridge
match = pd.read_csv(ROOT / "data/clean/match_report.csv")
match = match[match["mandi_id"].notna()].copy()
match["mandi_id"] = match["mandi_id"].astype(int)
print(f"[match_report]  {match.shape}  | cols: {match.columns.tolist()}")

# 1d–e. Price files loaded lazily in later steps


# ── 2. FIX THE MANDI ADOPTION JOIN ───────────────────────────────────────────
#
# Problem: adopt_dates uses the portal's apmc_id (hundreds–thousands);
#          meta uses a sequential internal mandi_id (1–1388).
#          Joining on mandi_id produces nonsense district assignments.
#
# Fix: match on normalised (mandi_name, state_name) instead.

print(f"\n{SEP}")
print("STEP 2: FIX MANDI → DISTRICT JOIN (name-matching)")
print(SEP)

# Build lookup from meta: norm_key → (district, state, meta_mandi_id)
meta["_key"] = meta["mandi_name"].apply(norm_name) + "||" + meta["state"].apply(norm_state)
meta_lookup  = meta.set_index("_key")[["district", "state", "mandi_id"]].copy()
meta_lookup.columns = ["district", "state_clean", "meta_mandi_id"]

# Build keys for adopt_dates
adopt_dates["_key"] = (adopt_dates["mandi_name"].apply(norm_name) + "||" +
                       adopt_dates["state_name"].apply(norm_state))

# Exact match
adopt = adopt_dates.merge(
    meta_lookup.reset_index(), on="_key", how="left"
)
n_exact = adopt["district"].notna().sum()
print(f"\nExact name-match: {n_exact} / {len(adopt)} mandis ({100*n_exact/len(adopt):.1f}%)")

# For unmatched: try matching on mandi name only (state-agnostic)
unmatched_mask = adopt["district"].isna()
meta_name_only = meta_lookup.reset_index().copy()
meta_name_only["_mandi_key"] = meta_name_only["_key"].str.split("||").str[0]
meta_name_only = meta_name_only.drop_duplicates("_mandi_key").set_index("_mandi_key")

adopt.loc[unmatched_mask, "_mandi_key"] = adopt.loc[unmatched_mask, "_key"].str.split("||").str[0]
fallback = adopt.loc[unmatched_mask, "_mandi_key"].map(meta_name_only["district"])
adopt.loc[unmatched_mask, "district"] = fallback
state_fb  = adopt.loc[unmatched_mask, "_mandi_key"].map(meta_name_only["state_clean"])
adopt.loc[unmatched_mask, "state_clean"] = state_fb
id_fb     = adopt.loc[unmatched_mask, "_mandi_key"].map(meta_name_only["meta_mandi_id"])
adopt.loc[unmatched_mask, "meta_mandi_id"] = id_fb

n_total_matched = adopt["district"].notna().sum()
print(f"After name-only fallback: {n_total_matched} / {len(adopt)} mandis ({100*n_total_matched/len(adopt):.1f}%)")

# Clean up
adopt = adopt.drop(columns=["_key", "_mandi_key"], errors="ignore")
adopt["adopt_period"] = (adopt["first_enam_trade_date"].dt.year * 12 +
                         adopt["first_enam_trade_date"].dt.month)
adopt["adopt_year"]   = adopt["first_enam_trade_date"].dt.year

# Verify: spot-check state assignments
print("\nSpot-check (Andhra Pradesh mandis should no longer map to Haryana):")
ap = adopt[adopt["state_name"].str.contains("ANDHRA", na=False)].head(5)
print(ap[["mandi_name","state_name","district","state_clean"]].to_string(index=False))

print(f"\nAdoption year distribution (post-fix):")
print(adopt["adopt_year"].value_counts().sort_index().to_string())

adopt.to_csv(OUT_DIR / "adoption_clean.csv", index=False)
print(f"\nSaved: data/clean/did_inputs/adoption_clean.csv")


# ── 3. BUILD MODAL PRICE LOOKUP (market_name + state_id → meta_mandi_id) ─────

print(f"\n{SEP}")
print("STEP 3: BUILD MANDI_ID BRIDGE FOR RANGE FILES")
print(SEP)

# Use modal files (which have both market_name and mandi_id) as bridge.
# mandi_id in modal files = same sequential ID as meta.
sample_modal = pd.read_csv(ROOT / "data/clean/prices/wheat_monthly.csv",
                            usecols=["state_id","state_name","market_name","mandi_id"])
for crop in CROPS_ALL[1:]:
    tmp = pd.read_csv(ROOT / f"data/clean/prices/{crop}_monthly.csv",
                      usecols=["state_id","state_name","market_name","mandi_id"])
    sample_modal = pd.concat([sample_modal, tmp], ignore_index=True)

bridge = (sample_modal[sample_modal["mandi_id"].notna()]
          .drop_duplicates(subset=["market_name","state_id"])
          [["market_name","state_id","state_name","mandi_id"]]
          .copy())
bridge["mandi_id"] = bridge["mandi_id"].astype(int)

# Add district and adopt_period from adopt table via meta_mandi_id
adopt_lookup = adopt[["meta_mandi_id","district","state_clean","adopt_period"]].dropna(subset=["meta_mandi_id"]).copy()
adopt_lookup["meta_mandi_id"] = adopt_lookup["meta_mandi_id"].astype(int)

bridge = bridge.merge(adopt_lookup, left_on="mandi_id", right_on="meta_mandi_id", how="left")
n_bridge_treated = bridge["adopt_period"].notna().sum()
print(f"Bridge mandis with adoption date : {n_bridge_treated} / {len(bridge)}")
bridge.to_csv(OUT_DIR / "mandi_bridge.csv", index=False)
print(f"Saved: data/clean/did_inputs/mandi_bridge.csv")


# ── 4. CONSTRUCT VARIABLE 1: WITHIN-MANDI PRICE RANGE ─────────────────────────

print(f"\n{SEP}")
print("STEP 4: WITHIN-MANDI PRICE RANGE")
print(SEP)

range_frames = []
for crop in CROPS_ALL:
    df = pd.read_csv(ROOT / f"data/clean/prices/{crop}_monthly_range.csv")
    df["crop"] = crop
    range_frames.append(df)
range_raw = pd.concat(range_frames, ignore_index=True)

# Join mandi_id + district + adopt_period via bridge
range_df = range_raw.merge(
    bridge[["market_name","state_id","mandi_id","district","state_clean","adopt_period"]],
    on=["market_name","state_id"], how="left"
)
range_df["time_period"] = range_df["year"] * 12 + range_df["month"]

n_total = len(range_df)
n_neg   = (range_df["range_avg"] < 0).sum()
n_zero  = (range_df["range_avg"] == 0).sum()
n_pos   = (range_df["range_avg"] > 0).sum()
n_linked = range_df["mandi_id"].notna().sum()
print(f"Total rows          : {n_total:>10,}")
print(f"Rows with mandi_id  : {n_linked:>10,}  ({100*n_linked/n_total:.1f}%)")
print(f"range_avg < 0 (drop): {n_neg:>10,}  ({100*n_neg/n_total:.1f}%)")
print(f"range_avg = 0 (drop): {n_zero:>10,}  ({100*n_zero/n_total:.1f}%)")
print(f"range_avg > 0 (keep): {n_pos:>10,}  ({100*n_pos/n_total:.1f}%)")

range_df["log_range"] = np.where(range_df["range_avg"] > 0,
                                  np.log(range_df["range_avg"]), np.nan)
range_df.to_csv(OUT_DIR / "within_mandi_range.csv", index=False)
print(f"Saved: data/clean/did_inputs/within_mandi_range.csv")


# ── 5. CONSTRUCT VARIABLE 2: ACROSS-MANDI SD — DISTRICT LEVEL ────────────────

print(f"\n{SEP}")
print("STEP 5: ACROSS-MANDI PRICE SD — DISTRICT LEVEL")
print(SEP)

modal_frames = []
for crop in CROPS_ALL:
    df = pd.read_csv(ROOT / f"data/clean/prices/{crop}_monthly.csv")
    df["crop"] = crop
    modal_frames.append(df)
modal_raw = pd.concat(modal_frames, ignore_index=True)
modal_raw = modal_raw[modal_raw["match_type"].isin(["exact","fuzzy"])].copy()
modal_raw["mandi_id"] = modal_raw["mandi_id"].astype("Int64")

# Join district + adopt_period
modal_df = modal_raw.merge(
    adopt_lookup.rename(columns={"meta_mandi_id":"mandi_id"}),
    on="mandi_id", how="left"
)
modal_df["time_period"] = modal_df["year"] * 12 + modal_df["month"]

# District-level SD (require ≥4 mandis per district-crop-month)
dist_raw = (
    modal_df[modal_df["district"].notna()]
    .groupby(["district","state_clean","crop","year","month","time_period"])
    .agg(price_sd=("modal_price_avg","std"),
         n_mandis=("mandi_id","nunique"),
         price_mean=("modal_price_avg","mean"))
    .reset_index()
)

# District treatment: earliest adoption among mandis in district
dist_treat = (
    adopt[adopt["adopt_period"].notna() & adopt["district"].notna()]
    .groupby("district")["adopt_period"]
    .min().reset_index()
    .rename(columns={"adopt_period":"district_adopt_period"})
)
dist_df = dist_raw.merge(dist_treat, on="district", how="left")

# Apply ≥4 mandi threshold
dist_df_4 = dist_df[dist_df["n_mandis"] >= 4].copy()

n_zero_d = (dist_df_4["price_sd"] == 0).sum()
n_nan_d  = dist_df_4["price_sd"].isna().sum()
n_pos_d  = (dist_df_4["price_sd"] > 0).sum()
print(f"District-month obs (>=4 mandis): {len(dist_df_4):>10,}")
print(f"price_sd = 0 (drop)            : {n_zero_d:>10,}")
print(f"price_sd = NaN (drop)          : {n_nan_d:>10,}")
print(f"price_sd > 0 (keep)            : {n_pos_d:>10,}")
dist_df_4["log_price_sd"] = np.where(dist_df_4["price_sd"] > 0,
                                      np.log(dist_df_4["price_sd"]), np.nan)
dist_df_4.to_csv(OUT_DIR / "across_mandi_district.csv", index=False)
print(f"Saved: data/clean/did_inputs/across_mandi_district.csv")


# ── 6. CONSTRUCT VARIABLE 3: ACROSS-MANDI SD — STATE LEVEL ───────────────────

print(f"\n{SEP}")
print("STEP 6: ACROSS-MANDI PRICE SD — STATE LEVEL")
print(SEP)

# Join state via mandi_id → adopt_lookup
modal_state = modal_raw.merge(
    adopt_lookup[["meta_mandi_id","state_clean","adopt_period"]]
    .rename(columns={"meta_mandi_id":"mandi_id"}),
    on="mandi_id", how="left"
)
# Fill from state_name column where adopt_lookup didn't match
modal_state["state_final"] = modal_state["state_clean"].fillna(modal_state["state_name"])
modal_state["time_period"] = modal_state["year"] * 12 + modal_state["month"]

state_raw = (
    modal_state[modal_state["state_final"].notna()]
    .groupby(["state_final","crop","year","month","time_period"])
    .agg(price_sd=("modal_price_avg","std"),
         n_mandis=("mandi_id","nunique"),
         price_mean=("modal_price_avg","mean"))
    .reset_index()
)

# State treatment: earliest adoption among mandis in state
state_treat = (
    adopt[adopt["adopt_period"].notna() & adopt["state_clean"].notna()]
    .groupby("state_clean")["adopt_period"]
    .min().reset_index()
    .rename(columns={"adopt_period":"state_adopt_period","state_clean":"state_final"})
)
state_df = state_raw.merge(state_treat, on="state_final", how="left")

n_zero_s = (state_df["price_sd"] == 0).sum()
n_pos_s  = (state_df["price_sd"] > 0).sum()
print(f"State-month obs : {len(state_df):>10,}")
print(f"price_sd = 0 (drop): {n_zero_s:>10,}")
print(f"price_sd > 0 (keep): {n_pos_s:>10,}")
state_df["log_price_sd"] = np.where(state_df["price_sd"] > 0,
                                     np.log(state_df["price_sd"]), np.nan)
state_df.to_csv(OUT_DIR / "across_mandi_state.csv", index=False)
print(f"Saved: data/clean/did_inputs/across_mandi_state.csv")


# ── 7. SUMMARY ────────────────────────────────────────────────────────────────

print(f"\n{SEP}")
print("DATASET SUMMARY")
print(SEP)
print(f"\nSpec 1 — Within-mandi range")
print(f"  All crops, rows with log_range    : {range_df['log_range'].notna().sum():>10,}")
print(f"  Linked to treatment (mandi_id)    : {range_df[range_df['log_range'].notna() & range_df['adopt_period'].notna()].shape[0]:>10,}")

print(f"\nSpec 2 — Across-mandi SD (district, >=4 mandis)")
print(f"  Rows with log_price_sd            : {dist_df_4['log_price_sd'].notna().sum():>10,}")
print(f"  Unique districts                  : {dist_df_4['district'].nunique():>10,}")
print(f"  Districts with treatment          : {dist_df_4[dist_df_4['district_adopt_period'].notna()]['district'].nunique():>10,}")

print(f"\nSpec 3 — Across-mandi SD (state)")
print(f"  Rows with log_price_sd            : {state_df['log_price_sd'].notna().sum():>10,}")
print(f"  Unique states                     : {state_df['state_final'].nunique():>10,}")
print(f"  States with treatment             : {state_df[state_df['state_adopt_period'].notna()]['state_final'].nunique():>10,}")

print(f"\nSample B crops (mandis_w_data >= 50): {CROPS_BALANCED}")
print(f"Dropped crops                       : {[c for c in CROPS_ALL if c not in CROPS_BALANCED]}")
print(f"\nData build complete.")
