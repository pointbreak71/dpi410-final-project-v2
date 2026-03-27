"""
Price Dispersion Over Time — State-Level
Within-state SD of modal prices for 3 Phase 1 states × 3 crops, 2010-2025
Layout: 3 rows (states) × 3 cols (crops)
Output: data/clean/figures/dispersion_over_time.png
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from pathlib import Path

ROOT    = Path(__file__).resolve().parent.parent
FIG_DIR = ROOT / "data/clean/figures"
FIG_DIR.mkdir(parents=True, exist_ok=True)

CROPS  = ["wheat", "onion", "potato"]
STATES = ["Uttar Pradesh", "Madhya Pradesh", "Rajasthan"]   # Phase 1, high coverage
COLOR  = "#1a6b3c"

STATE_MAP = {
    "Andaman and Nicobar": "Andaman & Nicobar Islands",
    "Jammu And Kashmir":   "Jammu & Kashmir",
    "Jammu and Kashmir":   "Jammu & Kashmir",
    "Pondicherry":         "Puducherry",
    "Delhi":               None,
    "Manipur":             None,
}

def state_annual_sd(crop, state):
    df = pd.read_csv(ROOT / f"data/clean/prices/{crop}_monthly.csv")
    df = df[df["match_type"].isin(["exact", "fuzzy"])].copy()
    df["state_name"] = df["state_name"].replace(STATE_MAP)
    df = df[df["state_name"] == state]

    # Monthly IQR (min 3 mandis)
    def iqr(x): return x.quantile(0.75) - x.quantile(0.25)
    sm = (df.groupby(["year", "month"])
            .agg(iqr_val=("modal_price_avg", iqr), n=("modal_price_avg", "count"))
            .reset_index())
    sm = sm[sm["n"] >= 3]

    # Annual weighted mean IQR
    ann = (sm.groupby("year")
             .apply(lambda x: np.average(x["iqr_val"], weights=x["n"]), include_groups=False)
             .reset_index(name="sd"))
    return ann[(ann["year"] >= 2010) & (ann["year"] <= 2025)].sort_values("year")

# ── Plot 3×3 panel ─────────────────────────────────────────────────────────────
fig, axes = plt.subplots(3, 3, figsize=(14, 10), facecolor="white",
                         constrained_layout=True)

for row, state in enumerate(STATES):
    for col, crop in enumerate(CROPS):
        ax  = axes[row][col]
        ann = state_annual_sd(crop, state)

        ax.plot(ann["year"], ann["sd"],
                color=COLOR, linewidth=2.0, marker="o",
                markersize=4, markerfacecolor="white",
                markeredgewidth=1.6, markeredgecolor=COLOR)

        ax.axvline(2016, color="#888888", linewidth=1.1, linestyle="--")
        ylim = ax.get_ylim()
        ax.text(2016.2, ylim[1] * 0.97, "eNAM", fontsize=7.5,
                color="#888888", va="top")

        # Column titles (crop) on top row only
        if row == 0:
            ax.set_title(crop.capitalize(), fontsize=11, fontweight="bold", pad=6)
        # Row labels (state) on left col only
        if col == 0:
            ax.set_ylabel(f"{state}\n\nIQR (Rs/quintal)", fontsize=9)
        # X label on bottom row only
        if row == 2:
            ax.set_xlabel("Year", fontsize=9)

        ax.set_xlim(2009.5, 2025.5)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.grid(axis="y", linestyle="--", alpha=0.35)
        ax.xaxis.set_major_locator(ticker.MultipleLocator(4))
        ax.set_facecolor("white")
        ax.tick_params(labelsize=8)

fig.suptitle(
    "Within-State Price Dispersion Over Time (IQR)\n"
    "Phase 1 eNAM States (adopted 2016)",
    fontsize=13, fontweight="bold"
)

out = FIG_DIR / "dispersion_over_time_iqr.png"
plt.savefig(out, dpi=300, bbox_inches="tight", facecolor="white")
plt.close()
print(f"Saved: {out}")
