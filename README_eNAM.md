# eNAM Mandi Adoption Dataset

**Dataset:** `data/clean/enam_adoption.csv`
**Coverage:** 1,388 mandis across 27 states and union territories
**Purpose:** Step 1 of a staggered difference-in-differences (DiD) analysis of the welfare effects of India's eNAM agricultural digitalisation program

---

## Background

The Electronic National Agriculture Market (eNAM) is a pan-India online trading portal launched on **April 14, 2016** by the Government of India. It links existing APMC (Agricultural Produce Market Committee) mandis through a common electronic platform to create a unified national market for agricultural commodities. Mandis were integrated in phases:

- **Phase 1 (FY 2016–18):** 585 mandis across 14 states. Original 8 states launched April 14, 2016; remaining Phase 1 states joined through FY 2017–18.
- **Phase 2 (FY 2019–20):** 415 additional mandis, reaching a total of 1,000 mandis across 18 states and 3 UTs by May 2020.
- **Phase 3 (FY 2021+):** Continued expansion to new states; portal currently shows ~1,388+ mandis.

---

## Methodology

### 1. Mandi List: Live Portal Scrape

The mandi list was scraped directly from the eNAM portal's backend API. After inspecting the JavaScript on [enam.gov.in/web/apmc-contact-details](https://enam.gov.in/web/apmc-contact-details), three chained AJAX endpoints were identified:

1. `POST /Ajax_ctrl/state_namedetail` — returns all 27 states/UTs
2. `POST /Ajax_ctrl/district_name_detail` (param: `state_id`) — returns districts per state
3. `POST /Ajax_ctrl/mandi_namedetail` (params: `state_code`, `district`) — returns mandi names per district

All three endpoints were called sequentially with a session cookie to build the full mandi × district × state list. This approach yielded **1,388 mandis**, which is more complete and current than the official PDF directory (which listed 1,000 mandis as of July 2021).

The raw JSON output from the scrape is saved at `data/raw/enam_mandi_scraped_raw.json`.

> **Note:** The portal's mandi detail endpoint (`/Ajax_ctrl/mandi_name`) returns address and commodity fields but no date-of-integration field. Adoption dates are not exposed through any public eNAM API endpoint.

### 2. Adoption Dates: Phased Rollout Documentation

Since per-mandi adoption dates are not publicly available, adoption years were assigned at the **state level** using the following sources:

| Source | Description |
|--------|-------------|
| PIB Press Release PRID=1622906 (May 11, 2020) | Confirmed states in Phase 2 May 2020 batch: GJ, HR, J&K, KL, MH, OD, PB, RJ, TN, WB |
| SFAC NAM Booklet (`data/raw/SFAC_NAM_Booklet.pdf`) | Phase 1 timeline: 21 mandis in 8 states (Apr 2016), 250 in 10 states (Sep 2016), 417 in 13 states (Mar 2017), 585 in 15 states + 1 UT (Mar 2018) |
| Tamil Nadu Agri Marketing Dept (`agrimark.tn.gov.in`) | TN first mandi (Ammoor) launched 24 October 2017 |
| News reports (Swarajya, Business Standard, Inc42) | Karnataka added May 1, 2020; original 8 Phase 1 states confirmed |
| eNAM Directory PDF (`data/raw/eNAM_Directory_20210720.pdf`) | Official list of 1,000 mandis as of March 2021, state-wise counts |

**State-year mapping applied:**

| Year | States | Confidence |
|------|--------|-----------|
| 2016 | Andhra Pradesh, Telangana, Gujarat, Haryana, Rajasthan, Madhya Pradesh, Uttar Pradesh, Himachal Pradesh | High — original 8 launch states |
| 2016 | Chhattisgarh, Jharkhand | Medium — joined by Sep 2016 (in first 10 states per SFAC booklet) |
| 2017 | Maharashtra, Tamil Nadu, Odisha, Uttarakhand | Medium–High — Phase 1 FY 2017–18; TN date confirmed Oct 2017 |
| 2020 | Punjab, Kerala, West Bengal, Chandigarh, Jammu & Kashmir, Karnataka, Puducherry | High — confirmed in Phase 2 May 2020 PIB press release |
| 2022 | Assam, Bihar, Goa, Nagaland, Tripura, Andaman & Nicobar Islands | Low — not present in July 2021 PDF; exact year unconfirmed |

### 3. Raw Sources

All raw files are saved in `data/raw/`:

| File | Description |
|------|-------------|
| `enam_mandi_scraped_raw.json` | Raw portal scrape output (mandi name, district, state) |
| `eNAM_Directory_20210720.pdf` | Official eNAM Directory (July 2021, 1,000 mandis, 188 pages) |
| `SFAC_NAM_Booklet.pdf` | SFAC Phase 1 rollout booklet with timeline milestones |
| `NAARM_eNAM_Report_2020.pdf` | NAARM 2020 policy report on strengthening eNAM |

---

## Column Definitions

| Column | Type | Description |
|--------|------|-------------|
| `mandi_id` | Integer | Sequential unique identifier assigned during scrape |
| `mandi_name` | String | Name of the APMC mandi as listed on the eNAM portal |
| `district` | String | District in which the mandi is located |
| `state` | String | State or Union Territory |
| `year_joined_enam` | Integer | Year the state first integrated with eNAM (see methodology above) |
| `month_joined_enam` | Integer (nullable) | Month of first integration, where documented. Missing for ~23% of mandis. |
| `adoption_year_confidence` | String | `high`: confirmed from government press release or official record; `medium`: inferred from phase documentation; `low`: post-2021 states, exact year unconfirmed |
| `enam_phase` | Integer | `1` = Phase 1 (2016–2018); `2` = Phase 2 (2020); `3` = Phase 3+ (2022+) |

---

## Summary Statistics

| Metric | Value |
|--------|-------|
| Total mandis | 1,388 |
| States / UTs covered | 27 |
| Mandis with confirmed year | 1,388 (100%) |
| Mandis with confirmed month | 1,071 (77%) |
| Phase 1 mandis (2016–2018) | 1,204 |
| Phase 2 mandis (2020) | 127 |
| Phase 3+ mandis (2022+) | 57 |

---

## Limitations and Caveats

1. **State-level adoption year, not mandi-level.** All mandis within a state receive the same adoption year. Individual mandis within a state went live at different points within the fiscal year, but this within-state variation is not captured.

2. **Phase 3 states are approximate.** For Assam, Bihar, Goa, Nagaland, Tripura, and Andaman & Nicobar Islands (57 mandis), the adoption year is estimated as 2022 based on their absence from the July 2021 PDF. Their exact year should be verified before use in causal inference.

3. **Portal reflects current state, not historical.** The mandi count (1,388) exceeds the July 2021 PDF count (1,000) because the portal was scraped in March 2026 and includes mandis added after the PDF was published.

4. **For staggered DiD:** The adoption year variable is suitable for state-level treatment timing. For mandi-level treatment timing, a preferred approach is to use first-transaction dates from AGMARKNET data as a proxy for when each mandi began active eNAM trading.

---

## Recommended Citation

> eNAM Mandi Adoption Dataset, compiled March 2026. Sources: eNAM portal (enam.gov.in), PIB Press Release PRID=1622906, SFAC NAM Booklet (2018), Tamil Nadu Directorate of Agricultural Marketing.
