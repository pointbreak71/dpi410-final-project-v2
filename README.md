# DPI-410 Final Project — eNAM and Agricultural Price Dispersion in India

This repository contains all data collection, cleaning, and analysis code for a staggered difference-in-differences study of India's **eNAM (Electronic National Agriculture Market)** platform and its effect on agricultural price dispersion across mandis (wholesale markets).

**Research question:** Did digitisation of agricultural markets through eNAM reduce price dispersion within and across mandis?

**Key finding:** eNAM adoption is associated with a significant *increase* in within-mandi price dispersion (+15.7 log points, p<0.01), driven primarily by the minimum (floor) price falling while the maximum (ceiling) price remains stable. Effects on across-mandi dispersion are small and statistically insignificant.

---

## Repository structure

```
scripts/          All data collection and analysis scripts (see below)
data/
  raw/            Raw scraped data (price JSON.gz files gitignored; eNAM directory PDFs)
  clean/
    enam_adoption.csv          Manually compiled eNAM adoption registry
    did_inputs/                Clean DiD panel datasets (built by build_data.py)
enam_adoption_dates.csv        Portal-scraped first trade dates (enam_first_trade_scraper.py)
results/                       Regression outputs (CSVs + LaTeX)
output/
  paper_outputs/               Publication-ready figures and tables
    figures/                   Figure 1–4 (PDF + PNG)
    tables/                    LaTeX and CSV regression tables
    table_images/              PNG versions of tables for direct document use
    README_outputs.md          Interpretation guide for every output
  diagnostics/                 Crop coverage and data quality tables
  price_components/            Max/min decomposition event-study coefficients
README.md                      This file
README_Prices.md               Agmarknet price dataset documentation
README_eNAM.md                 eNAM adoption dataset documentation
README_data_cleaning.md        Full data cleaning pipeline documentation
```

---

## Scripts — in order of execution

### Step 1 — Collect price data

**`scripts/agmarknet_scraper.py`**
Scrapes monthly mandi-level **modal prices** from the Agmarknet 2.0 API for 11 crops (2010–present). Saves raw responses as compressed JSON files at `data/raw/prices/{crop}/`. See `README_Prices.md` for full methodology.

```bash
python scripts/agmarknet_scraper.py --crops wheat onion --start-year 2010 --end-year 2025
```

**`scripts/agmarknet_range_scraper.py`**
Reads the same raw JSON files and extracts **max and min prices** per mandi-crop-month. Computes `range = max − min` and saves clean monthly CSVs at `data/clean/prices/`. Shares raw files with the modal scraper so no duplicate API calls are made.

```bash
python scripts/agmarknet_range_scraper.py --start-year 2010 --end-year 2025 --workers 8
```

---

### Step 2 — Collect eNAM adoption dates

**`scripts/enam_first_trade_scraper.py`**
For each of the 1,361 mandis in the eNAM directory, finds the **first month any trade was recorded on the eNAM portal** via a binary search on the portal's `commodity_list` API endpoint. Runs ~8 API calls per mandi (~4–5 hours total with 4 parallel workers). Output: `enam_adoption_dates.csv`.

> **Important limitation:** The eNAM portal database starts ~October 2018, even for mandis that joined in April 2016. The scraped date is therefore the earliest *portal record*, not the true adoption date, for Phase 1 mandis.

```bash
python scripts/enam_first_trade_scraper.py --resume   # safe to restart if interrupted
```

---

### Step 3 — Build clean analysis datasets

**`scripts/build_data.py`**
Combines all raw inputs into three clean DiD panel datasets saved to `data/clean/did_inputs/`. Key steps:
- Fixes a mandi ID collision between data sources using name-based fuzzy matching
- Constructs `log(price range)` as the within-mandi outcome
- Aggregates price SD across mandis at district and state level (≥4 mandis filter for districts)
- Assigns treatment timing from the scraped adoption dates

Output files: `adoption_clean.csv`, `mandi_bridge.csv`, `within_mandi_range.csv`, `across_mandi_district.csv`, `across_mandi_state.csv`. See `README_data_cleaning.md` for full documentation of every cleaning decision.

```bash
python scripts/build_data.py
```

---

### Step 4 — Diagnostics (optional)

**`scripts/diagnostics.py`**
Produces crop coverage tables, data quality checks, and district-level mandi counts. Useful for verifying the build step and for the data section of the paper. Output: `output/diagnostics/`.

```bash
python scripts/diagnostics.py
```

---

### Step 5 — Run DiD analysis

**`scripts/run_did.py`**
Runs all DiD regressions across 3 specifications × 2 samples. For each combination it estimates:
- **Pooled ATT** via TWFE: `y ~ post_treat | unit + time`
- **Event study** via relative-year dummies: `y ~ Dm5+...+Dp8 | unit + time`
- **Per-crop ATT** separately for each commodity

Specifications:
| Spec | Outcome | Unit | Notes |
|---|---|---|---|
| 1 — within-mandi | log(max − min price) | mandi × crop | Main result |
| 2 — across district | log(SD of prices) | district × crop | ≥4 mandis filter |
| 3 — across state | log(SD of prices) | state × crop | HC1 SEs |

Samples: A = all 11 crops; B = 7 crops with ≥50 mandis in range data.

Output: `results/{sample}/{spec}/` with CSVs, LaTeX tables, and PNG/PDF event-study and heterogeneity plots. Also writes `results/summary_comparison.csv`.

```bash
python scripts/run_did.py
```

---

### Step 6 — Mechanism figure

**`scripts/plot_price_components.py`**
Decomposes the within-mandi range effect into its components by running parallel event studies on `log(max price)` and `log(min price)` separately. Shows the range increase is driven by the **floor (min) falling** rather than the ceiling (max) rising. Also produces raw-trend and calendar-time plots. Output: `output/price_components/`.

```bash
python scripts/plot_price_components.py
```

---

### Step 7 — Generate paper outputs

**`scripts/make_paper_outputs.py`**
Produces publication-ready figures and LaTeX tables from the saved regression results. No regressions are re-run — it reads the CSV outputs from Step 5 and Step 6. Output: `output/paper_outputs/figures/` and `output/paper_outputs/tables/`.

```bash
python scripts/make_paper_outputs.py
```

**`scripts/make_table_images.py`**
Renders all tables as standalone PNG images with interpretation notes below each one — ready to paste directly into a Word document or slide deck. Output: `output/paper_outputs/table_images/`.

```bash
python scripts/make_table_images.py
```

---

## Documentation

| File | Contents |
|---|---|
| `README.md` | This file — project overview and script guide |
| `README_Prices.md` | Agmarknet scraping methodology, API details, data structure |
| `README_eNAM.md` | eNAM adoption dataset: phases, sources, confidence levels |
| `README_data_cleaning.md` | Every data cleaning decision: mandi ID fix, adoption date coding, outcome construction, sample restrictions |
| `output/paper_outputs/README_outputs.md` | Interpretation guide for every table and figure: what was estimated, how to read the coefficients |

---

## Estimator

All regressions use **Two-Way Fixed Effects (TWFE)** estimated via [`pyfixest`](https://github.com/py-econometrics/pyfixest). The treatment variable is a binary post-adoption indicator; the event study uses explicit relative-year dummies spanning −5 to +8 years. Standard errors are clustered at the state level (CRV1) for within-mandi and district-level specs; HC1 for state-level specs. See `README_data_cleaning.md` §7 for full estimator details.

---

## Requirements

```bash
pip install pyfixest pandas numpy matplotlib scipy
```

Python 3.11+. Raw price data files (~8 GB) are gitignored; contact the author for access or re-scrape using the scripts above.
