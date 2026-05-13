# DPI-410 Final Project — eNAM and Agricultural Price Dispersion in India

**Research question:** Did digitisation of agricultural markets through eNAM reduce price dispersion within and across mandis (wholesale markets)?

**Key finding:** eNAM adoption is associated with a significant *increase* in within-mandi price dispersion (+15.7 log points, p<0.01). The mechanism is a falling price floor: the minimum price recorded at treated mandis declines post-adoption while the maximum price stays flat. Effects on across-mandi dispersion are small and statistically insignificant.

---

## What this repo does

The project builds a staggered difference-in-differences (DiD) study of India's **eNAM (Electronic National Agriculture Market)** platform using two main data sources:

1. **Agmarknet price data** — monthly modal, maximum, and minimum prices scraped from the Agmarknet 2.0 API for 11 crops across ~1,000 mandis, 2010–2025.
2. **eNAM adoption dates** — the first month each mandi recorded a trade on the eNAM portal, scraped via binary search on the portal's own API.

These are combined into clean panel datasets and analysed using TWFE regressions estimated with `pyfixest`. The pipeline produces publication-ready tables (LaTeX + PNG) and figures (PDF + PNG).

---

## Repo structure

```
scripts/                      All runnable Python scripts (details below)
data/
  raw/
    prices/                   Raw JSON.gz price files from Agmarknet (gitignored, ~8 GB)
    eNAM_Directory_*.pdf      Official eNAM mandi registry (source document)
    enam_mandi_scraped_raw.json  Mandis scraped from eNAM portal
  clean/
    enam_adoption.csv         Manually compiled eNAM adoption registry with mandi IDs
    did_inputs/               Clean panel datasets ready for regression (built by build_data.py)
      adoption_clean.csv        Treatment timing: one row per mandi, first portal trade date
      mandi_bridge.csv          Crosswalk between Agmarknet market names and eNAM mandi IDs
      within_mandi_range.csv    Main DiD panel: mandi × crop × month, log(max−min price)
      across_mandi_district.csv District × crop × month, log(SD of modal prices)
      across_mandi_state.csv    State × crop × month, log(SD of modal prices)
enam_adoption_dates.csv       Portal-scraped first trade dates (output of enam_first_trade_scraper.py)
results/
  sample_A_full/              Regression outputs for all 11 crops
    spec1_within_mandi/         Pooled ATT, event study, per-crop ATTs
    spec2_across_district/
    spec3_across_state/
  sample_B_balanced/          Same specs, 7 crops with ≥50 mandis only
  summary_comparison.csv      All pooled ATTs in one file
output/
  paper_outputs/
    figures/                  Figure 1–4 (PDF + PNG) — ready to include in paper
    tables/                   Table 1–3 (LaTeX .tex + .csv)
    table_images/             Table 0–3 as PNG images with interpretation notes
    README_outputs.md         Interpretation guide for every table and figure
  price_components/           Max/min decomposition event-study outputs (Figure 3 source)
  diagnostics/                Crop coverage and district mandi count tables
  enam_adoption/              Descriptive charts: cumulative adoption, state map
README.md                     This file
README_Prices.md              Agmarknet data: scraping method, API details, field definitions
README_eNAM.md                eNAM adoption data: phases, sources, confidence levels
README_data_cleaning.md       Every cleaning decision: mandi ID fix, outcome construction, sample rules
```

---

## Scripts — in order of execution

### Step 1 — Scrape price data

**`scripts/agmarknet_scraper.py`** — scrapes monthly **modal prices** from the Agmarknet 2.0 API for 11 crops (2010–present). Saves compressed JSON to `data/raw/prices/{crop}/`.

```bash
python scripts/agmarknet_scraper.py --crops wheat onion --start-year 2010 --end-year 2025
```

**`scripts/agmarknet_range_scraper.py`** — reads the same raw JSON files and extracts **max and min prices** per mandi-crop-month. Saves monthly CSVs to `data/clean/prices/`. Shares raw files with the modal scraper — no duplicate API calls.

```bash
python scripts/agmarknet_range_scraper.py --start-year 2010 --end-year 2025 --workers 8
```

---

### Step 2 — Scrape eNAM adoption dates

**`scripts/enam_first_trade_scraper.py`** — for each of the 1,361 mandis in the eNAM directory, finds the **first month any trade was recorded on the eNAM portal** via binary search on the portal's `commodity_list` API endpoint (~8 calls per mandi, ~4–5 hours total). Output: `enam_adoption_dates.csv`.

> **Limitation:** The portal database starts ~October 2018, even for mandis that joined in April 2016. The scraped date is the first *portal record*, not the true adoption date, for Phase 1 mandis.

```bash
python scripts/enam_first_trade_scraper.py --resume   # safe to restart if interrupted
```

---

### Step 3 — Build clean analysis datasets

**`scripts/build_data.py`** — combines all raw inputs into the five clean panel datasets in `data/clean/did_inputs/`. Key steps: resolves a mandi ID collision between Agmarknet and eNAM using fuzzy name matching; constructs `log(price range)` as the within-mandi outcome; aggregates price SD across mandis at district and state level; assigns treatment timing from the scraped adoption dates.

```bash
python scripts/build_data.py
```

---

### Step 4 — Diagnostics (optional)

**`scripts/diagnostics.py`** — produces crop coverage tables and district-level mandi counts. Useful for the data section of the paper. Output: `output/diagnostics/`.

```bash
python scripts/diagnostics.py
```

---

### Step 5 — Run DiD analysis

**`scripts/run_did.py`** — runs all DiD regressions. For each of 3 specifications × 2 samples it estimates a pooled ATT, an event study, and per-crop ATTs.

| Spec | Outcome | Unit | SE |
|---|---|---|---|
| 1 — within-mandi | log(max − min price) | mandi × crop | CRV1, state |
| 2 — across district | log(SD of modal prices) | district × crop | CRV1, state |
| 3 — across state | log(SD of modal prices) | state × crop | HC1 |

Samples: A = all 11 crops; B = 7 crops with ≥50 mandis in range data.

Output: `results/{sample}/{spec}/` — CSVs, LaTeX tables, event-study plots.

```bash
python scripts/run_did.py
```

---

### Step 6 — Mechanism figure

**`scripts/plot_price_components.py`** — decomposes the within-mandi range effect by running parallel event studies on `log(max price)`, `log(min price)`, and `log(range)`. Shows the range increase is driven by the **floor falling**, not the ceiling rising. Output: `output/price_components/`.

```bash
python scripts/plot_price_components.py
```

---

### Step 7 — Generate paper outputs

**`scripts/make_paper_outputs.py`** — reads saved regression CSVs (no re-estimation) and produces publication-ready figures and LaTeX tables. Output: `output/paper_outputs/figures/` and `output/paper_outputs/tables/`.

```bash
python scripts/make_paper_outputs.py
```

**`scripts/make_table_images.py`** — renders all tables as styled PNG images with interpretation notes — ready to paste into a Word document or slide deck. Output: `output/paper_outputs/table_images/`.

```bash
python scripts/make_table_images.py
```

---

## Paper outputs — quick reference

| Output | File | What it shows |
|---|---|---|
| Table 0 | `table_images/Table0_summary_stats.png` | Mean modal, max, min, range prices by crop; eNAM treatment rates |
| Table 1 | `table_images/Table1_within_mandi.png` | Main result: eNAM → +15.7 log-pt increase in within-mandi spread |
| Table 2 | `table_images/Table2_across_mandi.png` | Null result: no significant effect on across-mandi dispersion |
| Table 3 | `table_images/Table3_crop_heterogeneity.png` | Per-crop ATTs: storables > perishables |
| Figure 1 | `figures/Figure1_event_study_within.png` | Event study for within-mandi dispersion (pre-trends flat, post rising) |
| Figure 2 | `figures/Figure2_event_study_across.png` | Event study for across-mandi dispersion (long-run convergence pattern) |
| Figure 3 | `figures/Figure3_mechanism.png` | Max vs min decomposition — floor falls, ceiling stays flat |
| Figure 4 | `figures/Figure4_crop_heterogeneity.png` | Crop ATTs as horizontal bar chart |

For detailed interpretation of each output, see **`output/paper_outputs/README_outputs.md`**.

---

## Documentation index

| File | Contents |
|---|---|
| `README.md` | This file — project overview, repo map, execution order |
| `README_Prices.md` | Agmarknet scraping methodology, API field definitions, data structure |
| `README_eNAM.md` | eNAM adoption dataset: phases, administrative sources, confidence levels |
| `README_data_cleaning.md` | Every data cleaning decision: mandi ID collision fix, adoption date coding, outcome construction, sample restrictions, estimator details |
| `output/paper_outputs/README_outputs.md` | How every table and figure was created; how to interpret each coefficient |

---

## Estimator

All regressions use **Two-Way Fixed Effects (TWFE)** estimated via [`pyfixest`](https://github.com/py-econometrics/pyfixest). Treatment is a binary post-adoption indicator. Event studies use explicit relative-year dummies spanning −5 to +8 years, with k = −1 as the reference year. Standard errors are clustered at the state level (CRV1) for within-mandi and district-level specs; HC1 for state-level specs (only 23 clusters). Never-treated mandis and not-yet-treated mandis from later cohorts both serve as controls in the within-mandi analysis.

---

## Requirements

```bash
pip install pyfixest pandas numpy matplotlib scipy
```

Python 3.11+. Raw price files (~8 GB) are gitignored; contact the author or re-scrape with `agmarknet_scraper.py` and `agmarknet_range_scraper.py`.
