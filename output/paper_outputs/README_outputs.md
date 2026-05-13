# Paper Outputs — Tables and Figures

This document explains how each table and figure was produced and how to interpret it.

All outputs live in two subfolders:
- `figures/` — PDF and PNG versions of all charts
- `tables/` — LaTeX (`.tex`) and CSV versions of all tables
- `table_images/` — PNG images of tables ready to paste into a document

Scripts: `scripts/make_paper_outputs.py` (figures + LaTeX tables), `scripts/make_table_images.py` (PNG table images), `scripts/run_did.py` (underlying regressions), `scripts/plot_price_components.py` (mechanism figure).

---

## Tables

---

### Table 0 — Summary Statistics by Crop
**File:** `table_images/Table0_summary_stats.png`

**How it was created:**
- Mean max and mean min prices are computed from `data/clean/did_inputs/within_mandi_range.csv`, which contains monthly averages of the highest and lowest prices recorded at each mandi-crop-month cell (sourced from Agmarknet).
- Mean modal price is computed from `data/clean/did_inputs/across_mandi_district.csv` — specifically the `price_mean` column, which is the district-level average of the prevailing modal (auction) price across mandis.
- Price range = mean max − mean min, averaged across all observations for that crop.
- Mandis in data = number of distinct market names in the Agmarknet dataset for that crop.
- eNAM treated (%) = the share of mandis successfully matched to the eNAM adoption registry that have a recorded first trade date on the platform.

**Interpretation:**
Prices are in Indian rupees per quintal (100 kg). There is wide variation across crops: cotton (~Rs 5,100/qtl) is more than six times the price of potato (~Rs 860/qtl). The mean range column shows how much within-mandi price dispersion there is on average before and after eNAM adoption — this is the outcome variable for the main DiD analysis. Crops with high eNAM treatment rates (paddy 90%, cotton 93%) have more treated mandis in the sample; crops like rice and soybean have fewer matched mandis and should be interpreted with more caution.

---

### Table 1 — Effect of eNAM on Within-Mandi Price Dispersion
**Files:** `tables/Table1_within_mandi.tex`, `tables/Table1_within_mandi.csv`, `table_images/Table1_within_mandi.png`

**How it was created:**
This is the main DiD result. The regression is:

```
log(max_price − min_price)_{i,c,t} = β · PostAdopt_{i,t} + α_i + γ_t + ε_{i,c,t}
```

where the unit of observation is a mandi × crop × month cell. `PostAdopt` equals 1 for a mandi from its first recorded eNAM trade date onwards. `α_i` are mandi×crop fixed effects (absorbing all time-invariant differences between markets and commodities). `γ_t` are time fixed effects (absorbing common price trends). Standard errors are clustered at the state level to account for correlated shocks within states.

Column (1) uses all 11 crops. Column (2) drops rice, mustard, soybean, and chana — crops with fewer than 50 mandis in the range data — as a robustness check to confirm the result is not driven by thin-market crops.

The regression is estimated using `pyfixest.feols()` in Python. The adoption date used is the `first_enam_trade_date` scraped from the eNAM portal (see `README_data_cleaning.md` for detail on how this was constructed).

**Interpretation:**
The coefficient of +0.157 (column 1) means that eNAM adoption is associated with a **15.7 log-point increase** in the within-mandi price range — approximately a 17% increase in the spread between the highest and lowest prices recorded at the same market in the same month. This effect is statistically significant at the 1% level and robust to dropping thin-market crops (column 2: +0.133***). The effect is counterintuitive if you expected eNAM to reduce price dispersion — see Figure 3 for the mechanism decomposition, which shows the increase is driven by the minimum price falling (the floor dropping) rather than the maximum rising.

---

### Table 2 — Effect of eNAM on Across-Mandi Price Dispersion
**Files:** `tables/Table2_across_mandi.tex`, `tables/Table2_across_mandi.csv`, `table_images/Table2_across_mandi.png`

**How it was created:**
This uses two alternative outcome variables that measure how much prices vary *across* markets rather than within a single market:

- **District-level (columns 1–2):** The outcome is `log(SD of modal prices across mandis within a district-crop-month cell)`. Only cells with ≥4 mandis are included to ensure the standard deviation is meaningful. Treatment timing is assigned at the district level as the earliest adoption date of any eNAM mandi in that district.
- **State-level (columns 3–4):** Same outcome aggregated at the state level. Treatment timing is the earliest adoption date in the state.

The regression specification is identical to Table 1 (TWFE with unit and time FEs), but the unit is now district×crop or state×crop. Because all districts and states eventually adopt eNAM, there are no never-treated control units — identification comes entirely from variation in *when* adoption occurred, with later adopters serving as not-yet-treated controls.

Standard errors for state-level columns use HC1 (heteroskedasticity-robust) rather than clustered SEs because there are only 23 state-level clusters, which is below the conventional threshold for reliable cluster-robust inference.

**Interpretation:**
None of the four estimates are statistically significant. The district-level effect is near zero (+0.018 for full sample), and the state-level effect is negative but very imprecise (−0.098, SE = 0.188). This means we cannot detect a significant effect of eNAM on cross-market price convergence at the district or state level. Two caveats: (1) with no never-treated units, the identification assumptions are stronger and harder to verify; (2) the event study for these specs (Figure 2) shows the district-level effect becomes increasingly negative over 8 years, suggesting a potential long-run convergence effect that the pooled ATT averages away.

---

### Table 3 — Crop-Level Treatment Effects
**Files:** `tables/Table3_crop_heterogeneity.tex`, `tables/Table3_crop_heterogeneity.csv`, `table_images/Table3_crop_heterogeneity.png`

**How it was created:**
Each row is a separate TWFE regression run on one crop's data only, using the same within-mandi specification as Table 1 (Spec 1, full sample). The crop subset is filtered before running, so the ATT for wheat is estimated using only wheat observations, the ATT for onion using only onion observations, and so on. This allows the treatment effect to differ freely across commodities rather than imposing a common slope.

Crops are sorted from smallest to largest ATT. The "Type" column classifies crops as perishable (onion, tomato, potato) or storable (all others) — a distinction that matters for market structure and storage behavior.

**Interpretation:**
The positive effect on price dispersion holds for nearly all crops and is statistically significant for 7 of 11. The effect is largest for storables — soybean (+0.832***), rice (+0.696***), paddy (+0.309***), wheat (+0.371***) — and smallest or insignificant for perishables — tomato (+0.085***), potato (+0.105***), onion (+0.133***). Chana is the only crop with a negative estimate (−0.195), though this is not significant. The pattern for storables vs. perishables is consistent with the idea that eNAM's competitive bidding has larger effects on crops where storage means buyers have more time to shop for low prices and sellers have more time to wait for high ones.

Note that mustard (+1.021*) and soybean (+0.832***) have wide confidence intervals because they have fewer observations and fewer state clusters.

---

## Figures

---

### Figure 1 — Event Study: Within-Mandi Price Dispersion
**Files:** `figures/Figure1_event_study_within.pdf`, `figures/Figure1_event_study_within.png`

**How it was created:**
Instead of a single pooled ATT, this regression includes a separate dummy variable for each year relative to adoption:

```
log(range)_{i,c,t} = Σ_k β_k · D_k_{i,t} + α_i + γ_t + ε_{i,c,t}
```

where `D_k = 1` if a treated mandi is exactly k years before/after its adoption year, and 0 otherwise. Never-treated mandis have all `D_k = 0` (they contribute to the fixed effects but not the event-study coefficients). The reference year is k = −1 (one year before adoption), so all coefficients are relative to that baseline. Pre-adoption dummies span k = −5 to −2; post-adoption span k = 0 to +8. The dashed gray line shows pre-adoption coefficients; the solid navy line shows post-adoption.

**Interpretation:**
- **Pre-adoption (dashed, k = −5 to −2):** Coefficients are close to zero and statistically indistinguishable from zero. This is the parallel trends test — it shows that treated and control mandis had similar trends in price dispersion *before* eNAM adoption, lending credibility to the DiD design.
- **Post-adoption (solid, k = 0 to +8):** The effect jumps immediately at k = 0 (+0.20 log points) and grows over time, reaching roughly +0.55 log points by year 6–7 before stabilising. This suggests eNAM's effect on within-market price dispersion builds gradually over time, rather than being an immediate one-off shock.
- **Key caveat:** Phase 1 mandis (adopted April 2016) have portal records only from ~October 2018 due to database truncation. Their "pre-period" in this plot is therefore effectively post-adoption, which could cause the pre-trend to look artificially flat and the immediate post-adoption jump to be understated.

---

### Figure 2 — Event Study: Across-Mandi Price Dispersion
**Files:** `figures/Figure2_event_study_across.pdf`, `figures/Figure2_event_study_across.png`

**How it was created:**
Same event-study specification as Figure 1, applied separately to the district-level (panel a) and state-level (panel b) across-mandi outcomes. The outcome in both panels is `log(SD of modal prices)`. Unit fixed effects are district×crop or state×crop. The teal line is district-level; the brown line is state-level.

**Interpretation:**
- **District level (panel a):** The pre-trends are reasonably flat. After adoption, the coefficient trends downward — reaching around −0.8 log points by year 7–8. This suggests that over the long run, eNAM may reduce price divergence *across* districts (i.e., prices in different markets become more similar). However, the pooled ATT (Table 2) averages this to near zero because early post-adoption years show little effect; the convergence takes several years to materialise.
- **State level (panel b):** Very wide confidence intervals make interpretation difficult. The direction is also negative but uncertain.
- **Important caveat:** With no never-treated units, the control group at long horizons (k = +6, +7, +8) shrinks to only the latest-adopting districts/states. Results at long horizons should be treated cautiously.

---

### Figure 3 — Mechanism: Decomposing Max vs. Min Price
**Files:** `figures/Figure3_mechanism.pdf`, `figures/Figure3_mechanism.png`

**How it was created:**
Three separate event-study regressions are run in parallel using the same specification and sample as Figure 1, but with three different outcome variables:
1. `log(max_price_avg)` — the monthly average maximum price at the mandi
2. `log(min_price_avg)` — the monthly average minimum price at the mandi
3. `log(range_avg)` — the price range (same as Figure 1)

The three sets of coefficients are overlaid on the same plot: blue for max (ceiling), red for min (floor), green dashed for range (spread).

**Interpretation:**
This is the key mechanism figure. The large increase in the price range (green dashed, +0.5 log points by year 6) is primarily driven by the **floor falling** — the minimum price recorded at treated mandis declines by 5–15 log points post-adoption (red line dipping below zero). The ceiling (blue) barely moves, staying near zero throughout with wide confidence intervals.

In plain language: after joining eNAM, the highest prices at a mandi stay roughly the same, but the lowest prices fall. One interpretation is that eNAM's transparent bidding platform allows buyers to identify and purchase lower-quality or lower-priced lots that previously went unrecorded or unsold — pulling the observed minimum price down. Another is that eNAM increases buyer competition at the top end while also enabling harder bargaining at the bottom. Either way, the result is a wider recorded spread, driven from below.

---

### Figure 4 — Crop Heterogeneity Chart
**Files:** `figures/Figure4_crop_heterogeneity.pdf`, `figures/Figure4_crop_heterogeneity.png`

**How it was created:**
A horizontal bar chart of the crop-level ATTs from Table 3. Each bar represents the point estimate from a separate crop-level TWFE regression. Error bars show the 95% confidence interval (±1.96 × SE). Bars are sorted from smallest to largest ATT. Red bars are perishable crops (onion, tomato, potato); green bars are storables.

**Interpretation:**
The chart makes the heterogeneity pattern immediately visible. Nearly all crops are to the right of zero, confirming the positive effect is not driven by a single commodity. Storables (green) cluster at larger effect sizes, while perishables (red) cluster near the bottom of the positive range. Chana is the one crop with a negative estimate, and its confidence interval is wide. Mustard's bar extends far to the right with a very large CI, reflecting high uncertainty from a small sample (659 obs, few state clusters).

---

## A note on the estimator

All regressions use **Two-Way Fixed Effects (TWFE)** with unit and time fixed effects, estimated via `pyfixest.feols()`. The pooled treatment effect ("post × treated") is a single weighted average of the treatment effect across all treated units and all post-treatment periods. The event study separates this into year-by-year effects.

For the within-mandi analysis, never-treated mandis (matched to the eNAM registry but without an adoption date) serve as the control group alongside not-yet-treated mandis from later cohorts. For the across-mandi analysis, there are no never-treated units — all districts and states eventually join eNAM — so identification relies entirely on timing variation.

The treatment date used throughout is the **first recorded trade date on the eNAM portal** (scraped via binary search), not the official administrative enrollment date. These differ by up to 2.5 years for Phase 1 mandis due to portal database truncation. See `README_data_cleaning.md` for full details.
