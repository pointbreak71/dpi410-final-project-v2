# Agmarknet Price Dataset

**Dataset:** `data/clean/prices/`
**Coverage:** 11 crops × 36 states × 2010–2025 | 1,119,787 monthly observations across ~16,386 unique markets
**Purpose:** Step 2 of a staggered difference-in-differences (DiD) analysis of the welfare effects of India's eNAM agricultural digitalisation program

---

## Background

[Agmarknet](https://agmarknet.gov.in) (Agricultural Marketing Information Network) is the Government of India's official repository of daily mandi-level agricultural commodity prices. It records minimum, maximum, and modal prices along with arrival quantities for hundreds of commodities across thousands of APMC mandis, with data going back to the early 2000s.

This dataset was collected from the **Agmarknet 2.0 API** (`https://api.agmarknet.gov.in/v1/`) — the backend powering the redesigned Agmarknet website — which exposes a clean JSON API for querying price data by state, commodity, year, and month.

---

## Methodology

### 1. API Discovery

Agmarknet 2.0 is a React single-page application. The underlying API endpoints were identified by inspecting the JavaScript bundle (`/static/js/main.3fa6332c.js`). The key endpoint for historical data is:

```
GET https://api.agmarknet.gov.in/v1/prices-and-arrivals/date-wise/specific-commodity
    ?year=<YYYY>&month=<M>&stateId=<ID>&commodityId=<ID>
```

This returns all markets within the specified state for a given commodity-month-year, with daily price entries per market (min, max, modal price and arrivals).

Supporting endpoints used:
- `GET /v1/location/state?page=<N>` — returns all 36 states with their IDs
- `GET /v1/commodities?page_size=500` — returns all commodities with their IDs

### 2. Scraping

The scraper (`scripts/agmarknet_scraper.py`) iterates over all combinations of:
- **36 states** (all states and UTs in the Agmarknet system)
- **11 crops** (see crop table below)
- **192 year-month combinations** (January 2010 – December 2025)

Total combinations: 74,929 (after deduplication). Combinations returning empty responses (no data for that state-crop-month) are skipped. The scraper uses 8 parallel workers with resumability — it checks for an existing raw file before making an API call, so it can be interrupted and restarted without duplicating work.

Raw responses are saved as gzip-compressed JSON to `data/raw/prices/{crop}/{stateId}_{year}_{month:02d}.json.gz`.

### 3. Flattening

Each raw JSON response has the structure:
```json
{
  "markets": [
    {
      "marketName": "Achalda APMC",
      "dates": [
        {
          "arrivalDate": "01/01/2020",
          "total_arrivals": 6.0,
          "data": [
            {"variety": "Dara", "arrivals": 6.0,
             "minimumPrice": 2000.0, "maximumPrice": 2060.0, "modalPrice": 2040.0}
          ]
        }
      ]
    }
  ]
}
```

Records are flattened to one row per market × date × variety, then aggregated to monthly and yearly summaries (arrival-weighted average modal price, total arrivals, observation count).

### 4. Mandi Matching

Each Agmarknet market is matched to `data/clean/enam_adoption.csv` using a two-stage approach:

1. **Exact match** — strip " APMC", " Mandi", etc. from both names; match on normalised name + state
2. **Fuzzy match** — use RapidFuzz token-sort ratio; accept matches ≥ 85 score

Matched records carry the `mandi_id` from `enam_adoption.csv`, enabling a direct join between price data and eNAM adoption timing for DiD analysis.

---

## Output Files

### `data/clean/prices/{crop}_monthly.csv`

One row per market × year × month. Prices are the arrival-weighted average modal price across all days and varieties that month.

| Column | Type | Description |
|--------|------|-------------|
| `state_id` | Integer | Agmarknet state ID |
| `state_name` | String | State name |
| `market_name` | String | Mandi name as listed on Agmarknet |
| `mandi_id` | Integer (nullable) | Matched ID from `enam_adoption.csv`; blank if unmatched |
| `match_type` | String | `exact`, `fuzzy`, or `unmatched` |
| `match_score` | Float | Fuzzy match score (100 = exact) |
| `commodity` | String | Crop name (lowercase) |
| `year` | Integer | Year |
| `month` | Integer | Month (1–12) |
| `arrivals_mt` | Float | Total arrivals in metric tonnes |
| `modal_price_avg` | Float | Arrival-weighted average modal price (Rs/quintal) |
| `n_obs` | Integer | Number of daily observations in that month |

### `data/clean/prices/{crop}_yearly.csv`

One row per market × year. Aggregated from monthly data.

| Column | Type | Description |
|--------|------|-------------|
| `state_id` | Integer | Agmarknet state ID |
| `state_name` | String | State name |
| `market_name` | String | Mandi name |
| `mandi_id` | Integer (nullable) | Matched ID from `enam_adoption.csv` |
| `match_type` | String | `exact`, `fuzzy`, or `unmatched` |
| `match_score` | Float | Fuzzy match score |
| `commodity` | String | Crop name |
| `year` | Integer | Year |
| `arrivals_mt` | Float | Total annual arrivals (metric tonnes) |
| `modal_price_avg` | Float | Arrival-weighted average modal price (Rs/quintal) |
| `months_with_data` | Integer | Number of months with at least one observation |

### `data/clean/match_report.csv`

All unique Agmarknet market × state combinations, with their match outcome against `enam_adoption.csv`. Use this to manually review and resolve unmatched mandis.

| Column | Description |
|--------|-------------|
| `market_name` | Agmarknet market name |
| `state_name` | State |
| `mandi_id` | Matched eNAM mandi ID (blank if unmatched) |
| `matched_mandi` | Name of the matched eNAM mandi |
| `match_type` | `exact`, `fuzzy`, `no_state_match`, or `unmatched` |
| `score` | Match score |
| `n_crops` | Number of crops this market appears in |

---

## Coverage Summary

| Crop | Commodity ID | Monthly rows | Unique markets | Match rate (exact) |
|------|-------------|-------------|---------------|-------------------|
| Wheat | 1 | 153,273 | 1,889 | 45% |
| Paddy (Common) | 2 | 112,377 | 2,005 | 29% |
| Rice | 3 | 63,025 | 964 | 30% |
| Maize | 4 | 92,627 | 1,681 | 41% |
| Onion | 23 | 142,433 | 1,904 | 38% |
| Tomato | 65 | 131,264 | 1,850 | 36% |
| Potato | 24 | 142,023 | 1,783 | 35% |
| Mustard | 12 | 81,365 | 1,134 | 53% |
| Soybean | 13 | 68,239 | 887 | 46% |
| Cotton | 15 | 40,830 | 911 | 46% |
| Chana (Bengal Gram) | 6 | 91,331 | 1,378 | 47% |

**Match report summary (3,626 unique market-state pairs):**

| Match type | Count |
|-----------|-------|
| Exact match | 944 |
| Fuzzy match (≥85) | 192 |
| No state overlap with eNAM | 48 |
| Unmatched | 2,442 |

The high unmatched rate (~67%) reflects that Agmarknet covers all APMC mandis in India (~7,000+), while eNAM covers only 1,388. Most unmatched markets are simply non-eNAM mandis — this is expected and not an error.

---

## Limitations and Caveats

1. **Monthly aggregation loses daily variation.** The raw JSON contains daily price observations. The clean CSVs aggregate to monthly averages. If daily price volatility is of interest, re-run the flattening step without aggregation.

2. **Match rate varies by crop.** Mustard (53%) and soybean (46%) have higher exact match rates than paddy (29%) because eNAM is concentrated in wheat/oilseed belt states where Agmarknet naming conventions are more standardised.

3. **Prices are in Rs/quintal.** All modal, min, and max prices are in Indian Rupees per quintal (100 kg). Arrivals are in metric tonnes.

4. **Some mandis appear in prices but not eNAM.** This is correct — Agmarknet pre-dates eNAM and includes all APMCs, not just eNAM-integrated ones. For DiD analysis, filter to rows where `mandi_id` is non-null.

5. **VPN required for re-scraping.** The Agmarknet 2.0 API (`api.agmarknet.gov.in`) blocks requests from non-Indian IP addresses. An India-based VPN is required to run the scraper.

6. **Date range is 2010–2025.** Data from 2003 is available on Agmarknet but sparsely populated before 2010. The scraper was configured to start from 2010 to balance coverage and runtime (~3 hours with 8 workers).

---

## Reproducing the Dataset

```bash
# Requires: India VPN active
# Install dependencies
pip install requests rapidfuzz

# Run scraper (resumable — safe to interrupt and restart)
python scripts/agmarknet_scraper.py --start-year 2010 --end-year 2025 --workers 8

# Monitor progress
tail -f /tmp/agmarknet_full_run.log
```

---

## Recommended Citation

> Agmarknet Price Dataset, compiled March 2026. Source: Agmarknet 2.0 API (agmarknet.gov.in), Ministry of Agriculture and Farmers Welfare, Government of India.
