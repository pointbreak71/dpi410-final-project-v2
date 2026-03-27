"""
Parallel Trends Analysis — Price Dispersion (SD) across eNAM Adoption Groups
Outcome: SD of modal prices across mandis within a state-crop-month
Groups:  Early adopters (year_joined_enam <= 2016) vs Late adopters (>= 2017)
Output:  data/clean/figures/parallel_trends_{crop}.png + panel figure
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from pathlib import Path

ROOT     = Path(__file__).resolve().parent.parent
FIG_DIR  = ROOT / "data/clean/figures"
FIG_DIR.mkdir(parents=True, exist_ok=True)

CROPS    = ["wheat", "paddy", "rice", "maize", "onion", "tomato",
            "potato", "mustard", "soybean", "cotton", "chana"]
TOP3     = ["wheat", "onion", "potato"]

# State name harmonisation (price data → enam_adoption names)
STATE_MAP = {
    "Andaman and Nicobar": "Andaman & Nicobar Islands",
    "Jammu And Kashmir":   "Jammu & Kashmir",
    "Jammu and Kashmir":   "Jammu & Kashmir",
    "Pondicherry":         "Puducherry",
    "Delhi":               None,   # not in eNAM
    "Manipur":             None,   # not in eNAM
}

# ── Load eNAM adoption ────────────────────────────────────────────────────────
enam = (pd.read_csv(ROOT / "data/clean/enam_adoption.csv")
        [["state", "year_joined_enam"]]
        .drop_duplicates("state"))

enam["group"] = enam["year_joined_enam"].apply(
    lambda y: "Early (≤2016)" if y <= 2016 else "Late (≥2017)"
)

# ── Build SD panel for all crops ──────────────────────────────────────────────
def build_panel(crop):
    df = pd.read_csv(ROOT / f"data/clean/prices/{crop}_monthly.csv")
    df = df[df["match_type"].isin(["exact", "fuzzy"])].copy()

    # Harmonise state names
    df["state_name"] = df["state_name"].replace(STATE_MAP)
    df = df[df["state_name"].notna()]

    # Merge adoption group
    df = df.merge(enam, left_on="state_name", right_on="state", how="inner")

    # State-month SD (min 3 mandis)
    grp = df.groupby(["state", "group", "year", "month"])["modal_price_avg"]
    sd  = grp.agg(sd=("std"), n=("count")).reset_index()
    sd  = sd[sd["n"] >= 3].copy()

    # Annual mean SD by group
    annual = (sd.groupby(["group", "year"])["sd"]
                .mean()
                .reset_index()
                .rename(columns={"sd": "mean_sd"}))
    annual["crop"] = crop
    return annual, sd

panels = {}
for crop in CROPS:
    ann, _ = build_panel(crop)
    panels[crop] = ann

# ── Pre-period summary table ───────────────────────────────────────────────────
print("Pre-period mean SD (2010–2015) by crop and group")
print("=" * 60)
header = f"{'Crop':<12} | {'Early (≤2016)':>14} | {'Late (≥2017)':>13} | {'Diff':>8}"
print(header)
print("-" * len(header))
for crop in TOP3:
    pre = panels[crop][panels[crop]["year"] <= 2015]
    e   = pre[pre["group"]=="Early (≤2016)"]["mean_sd"].mean()
    l   = pre[pre["group"]=="Late (≥2017)"]["mean_sd"].mean()
    d   = e - l
    print(f"{crop.capitalize():<12} | {e:>14.1f} | {l:>13.1f} | {d:>+8.1f}")

print()

# ── Plotting ──────────────────────────────────────────────────────────────────
STYLE = {
    "Early (≤2016)": {"color": "#1a6b3c", "lw": 2.2, "ls": "-",  "marker": "o"},
    "Late (≥2017)":  {"color": "#c0392b", "lw": 2.2, "ls": "--", "marker": "s"},
}

def plot_crop(ax, crop, show_ylabel=True):
    data = panels[crop]
    years = sorted(data["year"].unique())

    for group, style in STYLE.items():
        sub = data[data["group"] == group].sort_values("year")
        ax.plot(sub["year"], sub["mean_sd"],
                color=style["color"], linewidth=style["lw"],
                linestyle=style["ls"], marker=style["marker"],
                markersize=4, markerfacecolor="white",
                markeredgewidth=1.5)

        # Direct label at last year
        last = sub[sub["year"] == sub["year"].max()]
        if not last.empty:
            ax.annotate(
                group,
                xy=(last["year"].values[0], last["mean_sd"].values[0]),
                xytext=(8, 0), textcoords="offset points",
                fontsize=8, color=style["color"], va="center",
            )

    # eNAM launch line
    ax.axvline(2016, color="#555555", linewidth=1.2, linestyle=":", alpha=0.8)
    ax.text(2016.15, ax.get_ylim()[1] * 0.97, "eNAM\n2016",
            fontsize=7.5, color="#555555", va="top")

    ax.set_title(crop.capitalize(), fontsize=11, fontweight="bold")
    ax.set_xlim(2009.5, 2026)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis="y", linestyle="--", alpha=0.35)
    ax.set_xlabel("Year", fontsize=9)
    if show_ylabel:
        ax.set_ylabel("Mean SD of Modal Price\n(Rs/quintal)", fontsize=9)
    ax.xaxis.set_major_locator(ticker.MultipleLocator(3))

# Individual plots
for crop in TOP3:
    fig, ax = plt.subplots(figsize=(8, 4.5), facecolor="white")
    ax.set_facecolor("white")
    plot_crop(ax, crop)
    fig.suptitle(
        "Pre-trends in Price Dispersion: Early vs Late eNAM Adopters",
        fontsize=12, fontweight="bold", y=1.01
    )
    plt.tight_layout()
    out = FIG_DIR / f"parallel_trends_{crop}.png"
    plt.savefig(out, dpi=300, bbox_inches="tight", facecolor="white")
    plt.close()
    print(f"Saved: {out}")

# Panel figure (1 row × 3 cols)
fig, axes = plt.subplots(1, 3, figsize=(16, 5), facecolor="white",
                         sharey=False, constrained_layout=True)
for i, (ax, crop) in enumerate(zip(axes, TOP3)):
    ax.set_facecolor("white")
    plot_crop(ax, crop, show_ylabel=(i == 0))

    # After plot is drawn, add eNAM line label now y-lim is set
    ax.get_lines()  # force render

fig.suptitle(
    "Pre-trends in Price Dispersion: Early vs Late eNAM Adopters",
    fontsize=13, fontweight="bold"
)
out_panel = FIG_DIR / "parallel_trends_panel.png"
plt.savefig(out_panel, dpi=300, bbox_inches="tight", facecolor="white")
plt.close()
print(f"Saved: {out_panel}")
