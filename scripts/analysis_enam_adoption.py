"""
Analysis: eNAM Mandi Adoption — Summary Statistics and Cumulative Adoption Chart
Output saved to: output/enam_adoption/
"""

import os
import csv
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from collections import Counter

# ── Paths ────────────────────────────────────────────────────────────────────
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "data/clean/enam_adoption.csv")
OUT  = os.path.join(ROOT, "output/enam_adoption")
os.makedirs(OUT, exist_ok=True)

# ── Load data ────────────────────────────────────────────────────────────────
df = pd.read_csv(DATA)
df["year_joined_enam"] = df["year_joined_enam"].astype(int)

# ── 1. Summary statistics table ──────────────────────────────────────────────
by_year = (
    df.groupby("year_joined_enam")
    .agg(new_mandis=("mandi_id", "count"), new_states=("state", "nunique"))
    .reset_index()
)
by_year["cumulative_mandis"] = by_year["new_mandis"].cumsum()
by_year["cumulative_states"] = by_year["new_states"].cumsum()
by_year.columns = ["Year", "New Mandis", "New States", "Cumulative Mandis", "Cumulative States"]

# Save summary table as CSV
table_path = os.path.join(OUT, "adoption_by_year.csv")
by_year.to_csv(table_path, index=False)
print("Saved:", table_path)
print()
print(by_year.to_string(index=False))
print()

# States per year
state_year = (
    df.groupby(["state", "year_joined_enam"])
    .size()
    .reset_index(name="mandis")
    .sort_values(["year_joined_enam", "mandis"], ascending=[True, False])
)
state_table_path = os.path.join(OUT, "mandis_by_state.csv")
state_year.to_csv(state_table_path, index=False)
print("Saved:", state_table_path)

# ── 2. High-level summary table ─────────────────────────────────────────────
summary = pd.DataFrame([
    ("Total mandis on eNAM",            f"{len(df):,}"),
    ("States and Union Territories",    f"{df['state'].nunique()}"),
    ("Districts covered",               f"{df['district'].nunique()}"),
    ("Date coverage",                   "April 2016 – 2022"),
    ("Phase 1 mandis (2016–2018)",      f"{len(df[df['enam_phase']==1]):,}"),
    ("Phase 2 mandis (2020)",           f"{len(df[df['enam_phase']==2]):,}"),
    ("Phase 3 mandis (2022+)",          f"{len(df[df['enam_phase']==3]):,}"),
    ("Mandis with confirmed month",     f"{df['month_joined_enam'].notna().sum():,} ({df['month_joined_enam'].notna().mean()*100:.0f}%)"),
    ("Adoption year confidence: high",  f"{(df['adoption_year_confidence']=='high').sum():,} mandis"),
    ("Adoption year confidence: medium",f"{(df['adoption_year_confidence']=='medium').sum():,} mandis"),
    ("Adoption year confidence: low",   f"{(df['adoption_year_confidence']=='low').sum():,} mandis"),
], columns=["Metric", "Value"])

summary_path = os.path.join(OUT, "summary_table.csv")
summary.to_csv(summary_path, index=False)
print("Saved:", summary_path)
print()
print(summary.to_string(index=False))
print()

# ── 3. Cumulative adoption line chart ────────────────────────────────────────
# Build a year-by-year series from 2015 (pre-eNAM) through 2023
years_full = list(range(2015, 2024))
cumulative = []
total = 0
for y in years_full:
    total += len(df[df["year_joined_enam"] == y])
    cumulative.append(total)

fig, ax = plt.subplots(figsize=(8, 5))

# Main line
ax.plot(years_full, cumulative, color="#1a6b3c", linewidth=2.5, marker="o",
        markersize=7, markerfacecolor="white", markeredgewidth=2, markeredgecolor="#1a6b3c")

# Annotate each data point
annotations = {
    2016: (828,  "Phase 1\n8 states, 828 mandis"),
    2017: (1204, "Phase 1 cont.\n+376 mandis"),
    2020: (1331, "Phase 2\n+127 mandis"),
    2022: (1388, "Phase 3\n+57 mandis"),
}
offsets = {
    2016: (-0.05,  60),
    2017: ( 0.05,  60),
    2020: (-0.05, -80),
    2022: ( 0.05,  60),
}
for year, (count, label) in annotations.items():
    dx, dy = offsets[year]
    ax.annotate(
        label,
        xy=(year, count),
        xytext=(year + dx, count + dy),
        fontsize=8.5,
        color="#333333",
        ha="center",
        arrowprops=dict(arrowstyle="-", color="#aaaaaa", lw=1),
    )

# Phase shading
ax.axvspan(2015.5, 2018.5, alpha=0.06, color="#2196F3", label="Phase 1 (2016–2018)")
ax.axvspan(2019.5, 2020.5, alpha=0.06, color="#FF9800", label="Phase 2 (2020)")
ax.axvspan(2021.5, 2022.5, alpha=0.06, color="#9C27B0", label="Phase 3 (2022)")

# Formatting
ax.set_xlim(2014.5, 2023.5)
ax.set_ylim(0, 1550)
ax.set_xticks(years_full)
ax.set_xlabel("Year", fontsize=11)
ax.set_ylabel("Cumulative Mandis on eNAM", fontsize=11)
ax.set_title("Cumulative eNAM Mandi Adoption, 2016–2022", fontsize=13, fontweight="bold", pad=12)
ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.grid(axis="y", linestyle="--", alpha=0.4)
ax.legend(fontsize=9, loc="lower right")

plt.tight_layout()
chart_path = os.path.join(OUT, "cumulative_adoption.png")
plt.savefig(chart_path, dpi=300, bbox_inches="tight")
plt.close()
print("Saved:", chart_path)

# ── 4. Print final summary ───────────────────────────────────────────────────
print()
print("=" * 50)
print("SUMMARY")
print("=" * 50)
print(f"Total mandis on eNAM:    {len(df):,}")
print(f"States / UTs covered:    {df['state'].nunique()}")
print(f"Phase 1 (2016–2018):     {len(df[df['enam_phase']==1]):,} mandis")
print(f"Phase 2 (2020):          {len(df[df['enam_phase']==2]):,} mandis")
print(f"Phase 3 (2022+):         {len(df[df['enam_phase']==3]):,} mandis")
print(f"High confidence dates:   {(df['adoption_year_confidence']=='high').sum():,} mandis")
print(f"Medium confidence:       {(df['adoption_year_confidence']=='medium').sum():,} mandis")
print(f"Low confidence:          {(df['adoption_year_confidence']=='low').sum():,} mandis")
