"""
diagnostics.py
Step 0 + Step 1: Load all data, construct the three analysis variables,
print file heads, confirm mappings, and produce diagnostic tables.
Pauses before any regression.
"""

import warnings
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SEP  = "=" * 70

CROPS = ["wheat", "paddy", "rice", "maize", "onion", "tomato",
         "potato", "mustard", "soybean", "cotton", "chana"]


# ── 1. LOAD RAW FILES ─────────────────────────────────────────────────────────

print(SEP)
print("LOADING FILES")
print(SEP)

# --- Adoption dates (mandi-level) ---
adoption = pd.read_csv(ROOT / "enam_adoption_dates.csv")
adoption["first_enam_trade_date"] = pd.to_datetime(
    adoption["first_enam_trade_date"], errors="coerce"
)
print("\n[1] enam_adoption_dates.csv")
print(f"    Columns : {adoption.columns.tolist()}")
print(f"    Shape   : {adoption.shape}")
print(adoption.head(3).to_string(index=False))

# --- eNAM adoption metadata (has district) ---
meta = pd.read_csv(ROOT / "data/clean/enam_adoption.csv")
print("\n[2] data/clean/enam_adoption.csv")
print(f"    Columns : {meta.columns.tolist()}")
print(f"    Shape   : {meta.shape}")
print(meta.head(3).to_string(index=False))

# --- Match report (market_name + state_name → mandi_id) ---
match = pd.read_csv(ROOT / "data/clean/match_report.csv")
match = match[match["mandi_id"].notna()].copy()
match["mandi_id"] = match["mandi_id"].astype(int)
print("\n[3] data/clean/match_report.csv")
print(f"    Columns : {match.columns.tolist()}")
print(f"    Shape   : {match.shape}")
print(match.head(3).to_string(index=False))

# --- Price range files (within-mandi dispersion) ---
print("\n[4] *_monthly_range.csv (11 crops)")
range_frames = []
for crop in CROPS:
    path = ROOT / f"data/clean/prices/{crop}_monthly_range.csv"
    df = pd.read_csv(path)
    df["crop"] = crop
    range_frames.append(df)
range_raw = pd.concat(range_frames, ignore_index=True)
print(f"    Columns : {range_raw.columns.tolist()}")
print(f"    Shape   : {range_raw.shape}")
print(range_raw.head(3).to_string(index=False))

# --- Modal price files (across-mandi dispersion) ---
print("\n[5] *_monthly.csv (11 crops)")
modal_frames = []
for crop in CROPS:
    path = ROOT / f"data/clean/prices/{crop}_monthly.csv"
    df = pd.read_csv(path)
    df["crop"] = crop
    modal_frames.append(df)
modal_raw = pd.concat(modal_frames, ignore_index=True)
modal_raw = modal_raw[modal_raw["match_type"].isin(["exact", "fuzzy"])].copy()
print(f"    Columns : {modal_raw.columns.tolist()}")
print(f"    Shape   : {modal_raw.shape}")
print(modal_raw.head(3).to_string(index=False))


# ── 2. CONSTRUCT MANDI ADOPTION TABLE ────────────────────────────────────────

print(f"\n{SEP}")
print("CONSTRUCTING VARIABLE 1: MANDI ADOPTION DATES")
print(SEP)

# Join first_enam_trade_date → district via meta
adopt = adoption.merge(
    meta[["mandi_id", "district", "state"]],
    on="mandi_id", how="left"
)
# Create numeric time period: year*12 + month (for regression)
adopt["adopt_year"]  = adopt["first_enam_trade_date"].dt.year
adopt["adopt_month"] = adopt["first_enam_trade_date"].dt.month
adopt["adopt_period"] = (
    adopt["adopt_year"] * 12 + adopt["adopt_month"]
)

print(f"\nadopt table shape : {adopt.shape}")
print(f"mandis with date  : {adopt['first_enam_trade_date'].notna().sum()} / {len(adopt)}")
print(f"mandis with district: {adopt['district'].notna().sum()} / {len(adopt)}")
print(f"\nAdoption period distribution (year):")
print(adopt["adopt_year"].value_counts().sort_index().to_string())
print(adopt[["mandi_id","mandi_name","district","state",
             "first_enam_trade_date","adopt_period"]].head(5).to_string(index=False))


# ── 3. CONSTRUCT WITHIN-MANDI DISPERSION ─────────────────────────────────────

print(f"\n{SEP}")
print("CONSTRUCTING VARIABLE 2: WITHIN-MANDI PRICE RANGE")
print(SEP)

# Build mandi_id bridge: (market_name, state_name) → mandi_id from match_report
# range_raw has state_id but state_name is null; get state_name from modal_raw
state_map = (modal_raw[["state_id","state_name"]]
             .dropna(subset=["state_name"])
             .drop_duplicates("state_id")
             .set_index("state_id")["state_name"])
range_raw["state_name_filled"] = range_raw["state_id"].map(state_map)

# Join to match_report on market_name + state_name
range_mid = range_raw.merge(
    match[["market_name","state_name","mandi_id"]],
    left_on=["market_name","state_name_filled"],
    right_on=["market_name","state_name"],
    how="left"
)
n_pre  = len(range_mid)
n_matched = range_mid["mandi_id"].notna().sum()
print(f"\nRange rows before join : {n_pre:,}")
print(f"Rows with mandi_id     : {n_matched:,}  ({100*n_matched/n_pre:.1f}%)")

# Join to adopt to get district + adoption date
range_df = range_mid.merge(
    adopt[["mandi_id","district","state","first_enam_trade_date","adopt_period"]],
    on="mandi_id", how="left"
)
range_df["time_period"] = range_df["year"] * 12 + range_df["month"]

# Report zeros/negatives
n_total    = len(range_df)
n_neg      = (range_df["range_avg"] < 0).sum()
n_zero     = (range_df["range_avg"] == 0).sum()
n_pos      = (range_df["range_avg"] > 0).sum()
print(f"\nrange_avg breakdown:")
print(f"  negative : {n_neg:>8,}  ({100*n_neg/n_total:.1f}%) — will DROP")
print(f"  zero     : {n_zero:>8,}  ({100*n_zero/n_total:.1f}%) — will DROP")
print(f"  positive : {n_pos:>8,}  ({100*n_pos/n_total:.1f}%) — KEPT")

range_df["log_range"] = np.where(
    range_df["range_avg"] > 0, np.log(range_df["range_avg"]), np.nan
)
print(f"\nFinal within-mandi table shape : {range_df.shape}")
print(f"Columns: {range_df.columns.tolist()}")


# ── 4. CONSTRUCT ACROSS-MANDI DISPERSION ─────────────────────────────────────

print(f"\n{SEP}")
print("CONSTRUCTING VARIABLE 3: ACROSS-MANDI PRICE SD (DISTRICT LEVEL)")
print(SEP)

# Join modal prices to district via mandi_id → adopt table
modal_df = modal_raw.merge(
    adopt[["mandi_id","district","state","first_enam_trade_date","adopt_period"]],
    on="mandi_id", how="left"
)
modal_df["time_period"] = modal_df["year"] * 12 + modal_df["month"]

n_with_dist = modal_df["district"].notna().sum()
print(f"\nModal rows with district : {n_with_dist:,} / {len(modal_df):,}")

# Compute SD of modal prices across mandis within district × crop × time
district_df = (
    modal_df[modal_df["district"].notna()]
    .groupby(["district","state","crop","year","month","time_period"])
    .agg(
        price_sd   = ("modal_price_avg", "std"),
        n_mandis   = ("modal_price_avg", "count"),
        price_mean = ("modal_price_avg", "mean"),
    )
    .reset_index()
)

# Treatment timing at district level: first adoption among mandis in district
district_treat = (
    adopt[adopt["first_enam_trade_date"].notna()]
    .groupby("district")["adopt_period"]
    .min()
    .reset_index()
    .rename(columns={"adopt_period": "district_adopt_period"})
)
district_df = district_df.merge(district_treat, on="district", how="left")

n_total_d  = len(district_df)
n_neg_d    = (district_df["price_sd"] < 0).sum()   # shouldn't happen
n_zero_d   = (district_df["price_sd"] == 0).sum()
n_nan_d    = district_df["price_sd"].isna().sum()   # single-mandi districts
n_pos_d    = (district_df["price_sd"] > 0).sum()
print(f"\nprice_sd breakdown:")
print(f"  NaN (1 mandi in district that month) : {n_nan_d:>8,}")
print(f"  zero                                  : {n_zero_d:>8,}  — will DROP")
print(f"  positive                              : {n_pos_d:>8,}  — KEPT")

district_df["log_price_sd"] = np.where(
    district_df["price_sd"] > 0, np.log(district_df["price_sd"]), np.nan
)
print(f"\nFinal district-level table shape : {district_df.shape}")
print(f"Columns: {district_df.columns.tolist()}")


# ── 5. DIAGNOSTIC TABLE A — CROP COVERAGE ────────────────────────────────────

print(f"\n{SEP}")
print("DIAGNOSTIC TABLE 1: CROP COVERAGE")
print(SEP)

rows = []
for crop in CROPS:
    rd = range_df[range_df["crop"] == crop]
    md = modal_df[modal_df["crop"] == crop]

    # Mandis with any price data in range file
    n_mandis_range = rd["mandi_id"].dropna().nunique()
    n_mandis_modal = md["mandi_id"].dropna().nunique()

    # Share of mandi×time cells non-missing (positive range)
    cells_possible = rd["mandi_id"].dropna().nunique() * rd["time_period"].nunique()
    cells_nonmiss  = (rd["range_avg"] > 0).sum()
    share_nonmiss  = cells_nonmiss / cells_possible if cells_possible > 0 else np.nan

    # Mandis ever adopting eNAM
    n_treated = rd[rd["first_enam_trade_date"].notna()]["mandi_id"].dropna().nunique()

    # Date range
    t_min = rd["time_period"].min()
    t_max = rd["time_period"].max()
    yr_min = t_min // 12
    mo_min = t_min % 12 or 12
    yr_max = t_max // 12
    mo_max = t_max % 12 or 12

    rows.append({
        "crop":           crop,
        "mandis_w_data":  n_mandis_range,
        "mandis_modal":   n_mandis_modal,
        "ever_treated":   n_treated,
        "pct_treated":    round(100*n_treated/n_mandis_range, 1) if n_mandis_range else 0,
        "fill_rate":      round(100*share_nonmiss, 1),
        "earliest":       f"{yr_min}-{mo_min:02d}",
        "latest":         f"{yr_max}-{mo_max:02d}",
        "obs_positive":   cells_nonmiss,
    })

crop_table = pd.DataFrame(rows)
print(f"\n{crop_table.to_string(index=False)}")


# ── 6. DIAGNOSTIC TABLE B — DISTRICT MANDI COUNT ─────────────────────────────

print(f"\n{SEP}")
print("DIAGNOSTIC TABLE 2: DISTRICT MANDI COUNT (across-mandi spec)")
print(SEP)

# Mandis per district with any price data (pooled across crops)
mandi_per_district = (
    modal_df[modal_df["district"].notna()]
    .groupby("district")["mandi_id"]
    .nunique()
    .reset_index()
    .rename(columns={"mandi_id": "n_mandis_price"})
)
treated_per_district = (
    adopt[adopt["first_enam_trade_date"].notna() & adopt["district"].notna()]
    .groupby("district")["mandi_id"]
    .nunique()
    .reset_index()
    .rename(columns={"mandi_id": "n_treated"})
)
dist_table = mandi_per_district.merge(
    treated_per_district, on="district", how="left"
)
dist_table["n_treated"] = dist_table["n_treated"].fillna(0).astype(int)
dist_table["flag_lt4"]  = dist_table["n_mandis_price"] < 4

print(f"\nTotal districts with any price data : {len(dist_table):,}")
print(f"Districts with <4 mandis            : {dist_table['flag_lt4'].sum():,}  "
      f"({100*dist_table['flag_lt4'].mean():.1f}%)")
print(f"Districts with >=4 mandis           : {(~dist_table['flag_lt4']).sum():,}")
print(f"Districts with >=2 mandis           : {(dist_table['n_mandis_price']>=2).sum():,}")

print(f"\nMandis-per-district summary stats:")
desc = dist_table["n_mandis_price"].describe(percentiles=[.25,.5,.75,.9])
print(desc.round(1).to_string())

print(f"\nDistribution:")
for n in [1,2,3,4,5,6,7,8,10,15]:
    cnt = (dist_table["n_mandis_price"] == n).sum()
    gte = (dist_table["n_mandis_price"] >= n).sum()
    if n <= 8 or cnt > 0:
        print(f"  = {n} mandis : {cnt:>4d} districts   |   >= {n} mandis : {gte:>4d} districts")

# Top 20 districts by mandi count
print(f"\nTop 20 districts by mandi count:")
print(dist_table.sort_values("n_mandis_price", ascending=False)
      .head(20).to_string(index=False))


# ── 7. SAVE DIAGNOSTIC TABLES ─────────────────────────────────────────────────

out = ROOT / "output" / "diagnostics"
out.mkdir(parents=True, exist_ok=True)
crop_table.to_csv(out / "crop_coverage.csv", index=False)
dist_table.sort_values("n_mandis_price", ascending=False).to_csv(
    out / "district_mandi_count.csv", index=False
)
print(f"\n\nDiagnostic tables saved to output/diagnostics/")

print(f"\n{SEP}")
print("READY FOR THRESHOLD DECISIONS")
print(SEP)
print("""
Please specify:
  (a) Crop coverage threshold for Sample B (balanced subsample)
      Suggested: drop crops with fill_rate < X% or mandis_w_data < N
      → Look at the crop coverage table above and tell me the cutoff

  (b) Minimum mandi count per district for Spec 2 (across-district)
      Suggested candidates: >=2, >=3, or >=4
      → Look at the district table above and tell me the cutoff

Do NOT proceed until these are confirmed.
""")
