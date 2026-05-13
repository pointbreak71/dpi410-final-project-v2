# Data Cleaning Documentation

This document describes every transformation applied to raw data before running the difference-in-differences analysis. All cleaning is implemented in `scripts/build_data.py` and the DiD estimation in `scripts/run_did.py`.

---

## 1. Raw Data Sources

| File / directory | Content | Rows | Key columns |
|---|---|---|---|
| `enam_adoption_dates.csv` (project root) | eNAM portal adoption dates scraped via binary search | 1,361 | `state_id`, `mandi_name`, `mandi_id` (portal apmc_id), `first_enam_trade_date` |
| `data/clean/enam_adoption.csv` | Manually compiled eNAM adoption registry | 1,388 | `mandi_id` (sequential 1–1388), `mandi_name`, `district`, `state`, `year_joined_enam`, `month_joined_enam`, `enam_phase` |
| `data/raw/prices/<crop>/` | Monthly Agmarknet mandi-level price data (11 crops, 2000–2023) | ~1.4M rows | `state_id`, `state_name`, `market_name`, `commodity`, `year`, `month`, `arrivals_mt`, `max_price`, `min_price`, `modal_price` |
| `data/raw/enam_mandi_scraped_raw.json` | eNAM portal mandi directory (scraped) | 1,139 mandis | `state_id`, `market_name`, `apmc_id` |

---

## 2. Critical Data Issue: Dual mandi_id Systems

### Problem discovered

`enam_adoption_dates.csv` uses the **portal apmc_id** (hundreds to thousands, e.g. apmc_id = 1247), while `enam_adoption.csv` uses a **sequential internal ID** (1–1388). A naive join on `mandi_id` silently mis-assigned mandis across states — e.g., Andhra Pradesh mandis were mapped to Haryana districts — because both files happened to share numeric IDs that referred to completely different markets.

### Fix: name-based matching with state constraint

Since both datasets contain `mandi_name` and state identifiers, matching was performed via normalized name strings within the same state. The normalization function:

```python
def norm_name(s):
    s = str(s).upper().strip()
    s = re.sub(r"\b(APMC|MANDI|MARKET|KRISHI|UPAJ)\b", "", s)
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    return re.sub(r"\s+", " ", s).strip()
```

This strips common institutional suffixes (APMC, MANDI, etc.), removes non-alphanumeric characters, and collapses whitespace. Matching was done in two passes:

1. **Exact match**: normalized `(state, mandi_name)` pair matches exactly across both datasets
2. **Fuzzy fallback**: for unmatched rows, fuzzy string matching (RapidFuzz `token_sort_ratio`) within the same state, requiring score ≥ 80

**Match results** (from `data/clean/match_report.csv`):

| Match type | Count |
|---|---|
| Exact | 944 |
| Fuzzy | 192 |
| Unmatched (no eNAM entry) | 2,442 |
| No state match | 48 |

Of 3,626 unique market-state combinations in Agmarknet, 1,136 (31.3%) were matched to eNAM adoption records. The remaining 2,442 are markets not on eNAM — used as never-treated controls in Spec 1.

The bridge table is saved as `data/clean/did_inputs/mandi_bridge.csv`.

---

## 3. eNAM Adoption Date Construction

### Portal truncation issue

The eNAM portal database begins in approximately **October 2018**. Phase 1 mandis that joined eNAM in **April 2016** therefore show a `first_enam_trade_date` of late 2018 — not their true adoption date. This means the `first_enam_trade_date` field represents the first date of recorded trade activity on the portal, which is systematically right-shifted for early adopters.

**Consequence for identification**: For Phase 1 mandis, what appears as a "pre-adoption" period in the DiD (2018–2019) is actually **post-adoption** by 2+ years. Event-study pre-trends for these cohorts mix true pre-adoption and post-adoption periods.

### Adoption date coding

From `enam_adoption_dates.csv`, the `first_enam_trade_date` was converted to a monthly integer period (YYYYMM format):

```python
adopt_period = year * 12 + month   # e.g., 2016 × 12 + 4 = 24196
```

This format allows comparison of calendar months via integer arithmetic. Units with no adoption record receive `adopt_period = 0` (never-treated sentinel).

`data/clean/did_inputs/adoption_clean.csv` contains the cleaned adoption records.

---

## 4. Outcome Variable Construction

### Spec 1: Within-mandi price range — `within_mandi_range.csv`

**Outcome**: `log_range = log(max_price_avg − min_price_avg)`, where max and min are the monthly average maximum and minimum prices at a given market for a given crop.

**Construction steps**:
1. Load all crop-level price CSVs; retain observations where monthly arrivals > 0
2. Compute `range_avg = max_price_avg − min_price_avg` for each market-crop-month cell
3. Keep only observations with `range_avg > 0` (zero range is typically a data recording artifact where max = min)
4. Compute `log_range = log(range_avg)`
5. Join treatment timing via the mandi bridge (name-based match described above)
6. Rows without a mandi match (`mandi_id = NaN`) have **unknown treatment status** and are excluded from the DiD

**Coverage**: 58,033 observations with valid `log_range` and known treatment status across 1,073 mandi-crop units.

### Spec 2: Across-mandi price dispersion — district level — `across_mandi_district.csv`

**Outcome**: `log_price_sd = log(SD of modal prices across mandis in the same district-crop-month cell)`.

**Construction steps**:
1. Aggregate Agmarknet modal prices to the district-crop-month level by computing the standard deviation across all mandis in that district
2. Retain district-crop-month cells with **≥ 4 mandis** (to ensure SD is meaningful; cells with fewer mandis are dropped)
3. Assign district-level treatment timing: the treatment date for a district is the earliest `adopt_period` of any eNAM mandi in that district
4. Districts where no mandi appears in the eNAM adoption data receive `district_adopt_period = 0` (never-treated)
5. Compute `log_price_sd = log(price_sd)`

**Coverage**: 21,138 observations across 83 districts. All districts are eventually treated (no never-treated units); later-adopting districts serve as not-yet-treated controls for earlier cohorts.

**Sample restriction**: Applied 4-mandi minimum per district-crop-month.

### Spec 3: Across-mandi price dispersion — state level — `across_mandi_state.csv`

**Outcome**: Same as Spec 2 but aggregated at the state level.

**Construction steps**:
1. Aggregate modal prices to the state-crop-month level; compute SD across all mandis in the state
2. Assign state-level treatment timing as the earliest `adopt_period` of any eNAM mandi in the state
3. No minimum-mandi filter at state level (states have enough mandis by construction)
4. Compute `log_price_sd = log(price_sd)`

**Coverage**: 22,278 observations across 23 states. All states eventually treated.

---

## 5. Sample Restrictions

Two analysis samples are used:

| Sample | Crops | Rationale |
|---|---|---|
| **A — Full** | wheat, paddy, rice, maize, onion, tomato, potato, mustard, soybean, cotton, chana | All available crops |
| **B — Balanced** | wheat, paddy, maize, onion, tomato, potato, cotton | Drops crops with < 50 mandis in the within-mandi range data (rice, mustard, soybean, chana) |

The balanced sample guards against results being driven by thinly-traded crops where the price range measure may be unreliable.

---

## 6. DiD Panel Structure

For all three specs, the panel is constructed as follows:

```
unit_id  = factorize(unit_cols)   # e.g., (mandi_id, crop) for Spec 1
time     = time_period            # YYYYMM integer
gname    = adopt_period           # 0 = never-treated, >0 = adoption month×year
rel_year = (obs_year − adopt_year) for treated, NaN for never-treated
```

Relative year is computed in **calendar years** (monthly periods divided by 12), so each event-study coefficient represents a full year bin. The reference period is `rel_year = −1` (one year before adoption). Event-study dummies span `[−5, +8]` years relative to adoption.

Treatment indicator: `post_treat = 1` if `gname > 0` and `time_period ≥ gname`.

---

## 7. Estimator

**Two-Way Fixed Effects (TWFE)** with unit and time fixed effects, estimated via `pyfixest.feols()`:

```
y ~ post_treat | unit_id + time_period         # pooled ATT
y ~ Dm5+Dm4+Dm3+Dm2+Dp0+...+Dp8 | unit_id + time_period  # event study
```

where `Dm{k}` and `Dp{k}` denote relative-year dummies at k years before and after adoption.

**Standard errors**: clustered at the state level (CRV1) for Specs 1 and 2 where the number of clusters ≥ 10. HC1 heteroskedasticity-robust SEs are used for Spec 3 (only 23 state clusters, below the conventional CRV1 threshold).

**Note on Callaway–Sant'Anna / DID2S**: These estimators were tested but could not be applied. Specs 2 and 3 have no never-treated units (all districts/states eventually adopt eNAM), which breaks the CS2021 identification assumption. The pyfixest `did2s` estimator also encountered a shape mismatch bug with unbalanced panels at this sample size. Standard TWFE with explicit relative-year dummies is the fallback, using later-adopting units as not-yet-treated controls — a defensible approach given the staggered rollout.

---

## 8. Output Files

```
data/clean/did_inputs/
    adoption_clean.csv          eNAM adoption dates, cleaned
    mandi_bridge.csv            Agmarknet ↔ eNAM name-match bridge
    within_mandi_range.csv      Spec 1 panel input
    across_mandi_district.csv   Spec 2 panel input (≥4 mandis filter applied)
    across_mandi_state.csv      Spec 3 panel input

results/
    sample_A_full/
        spec1_within_mandi/     main_results_pooled.{csv,tex}
                                event_study.{png,pdf}
                                event_study_coefs.csv
                                crop_heterogeneity.{png,pdf}
                                crop_heterogeneity_table.{csv,tex}
        spec2_across_district/  (same structure)
        spec3_across_state/     (same structure)
    sample_B_balanced/          (same structure)
    summary_comparison.{csv,tex}
```

---

## 9. Known Limitations

1. **Portal truncation (Phase 1)**: `first_enam_trade_date` understates adoption by ~2 years for Phase 1 mandis (adopted April 2016, first portal record ~late 2018). The ATT for early-adopting cohorts absorbs some post-treatment effects into the "pre-period."

2. **Low match rate for Spec 1**: Only 31.3% of Agmarknet markets could be matched to eNAM adoption records. The remaining 68.7% are excluded (unknown treatment status). Results may not generalize to unmatched markets.

3. **No never-treated units in Specs 2–3**: All districts and states eventually adopt eNAM. Identification relies entirely on variation in adoption timing (earlier vs. later adopters as controls), which is valid under parallel trends but cannot be verified against a true control group.

4. **Collinearity in state-level crop regressions (Spec 3)**: Crops grown in only a few states (wheat, soybean, cotton) yield collinear TWFE designs when all units in the crop sample adopt in the same period. Affected crop-spec combinations are skipped with a warning.
