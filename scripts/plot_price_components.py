"""
plot_price_components.py
------------------------
Decomposes the within-mandi price range effect into its two components:
  log(max_price)  — does eNAM push the maximum up?
  log(min_price)  — does eNAM pull the minimum down?

Produces three plots:
  1. Event-study coefficients for log_max, log_min, and log_range on one chart
     (regression-based, controlling for unit and time FEs)
  2. Raw group-mean trajectories for treated mandis: log_max and log_min
     normalized to 0 at rel_year=-1
  3. Calendar-time trends for treatment vs control group (min and max),
     normalized to their pre-2018 mean
"""

import warnings
warnings.filterwarnings("ignore")

import re
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from pathlib import Path

import pyfixest as pf

ROOT = Path(__file__).resolve().parent.parent
DAT  = ROOT / "data" / "clean" / "did_inputs"
OUT  = ROOT / "output" / "price_components"
OUT.mkdir(parents=True, exist_ok=True)

# ── constants (match run_did.py) ───────────────────────────────────────────────
K_PRE  = 5
K_POST = 8
REF    = -1

BLUE  = "#1f6fb5"
RED   = "#c0392b"
GREEN = "#1a6b3c"
GRAY  = "#555555"
LGRAY = "#cccccc"

plt.rcParams.update({
    "font.family":      "serif",
    "axes.spines.top":  False,
    "axes.spines.right":False,
    "axes.grid":        False,
    "figure.dpi":       150,
    "font.size":        11,
})


# ── load & prepare data ────────────────────────────────────────────────────────

def load_data() -> pd.DataFrame:
    df = pd.read_csv(DAT / "within_mandi_range.csv")
    df = df[df["mandi_id"].notna() & df["log_range"].notna()].copy()

    # Outcomes
    df["log_max"] = np.log(df["max_price_avg"].clip(lower=1e-6))
    df["log_min"] = np.log(df["min_price_avg"].clip(lower=1e-6))

    df["adopt_period"] = df["adopt_period"].fillna(0).astype(int)
    df["time_period"]  = df["time_period"].astype(int)

    # Unit ID
    unit_key = df.apply(lambda r: f"{r['mandi_id']}__{r['crop']}", axis=1)
    df["unit_id"] = pd.factorize(unit_key)[0]

    # Relative year
    df["obs_year"]   = df["time_period"] // 12
    df["adopt_year"] = np.where(df["adopt_period"] > 0, df["adopt_period"] // 12, 0)
    df["rel_year"]   = np.where(df["adopt_period"] > 0,
                                df["obs_year"] - df["adopt_year"],
                                np.nan)
    df["post_treat"] = ((df["adopt_period"] > 0) &
                        (df["time_period"] >= df["adopt_period"])).astype(int)
    return df


def add_dummies(df: pd.DataFrame):
    cols = []
    for k in range(-K_PRE, K_POST + 1):
        if k == REF: continue
        col = f"Dm{abs(k)}" if k < 0 else f"Dp{k}"
        df[col] = (df["rel_year"] == k).astype(float)
        cols.append(col)
    return df, cols


def cluster_arg(df):
    col = "state_clean"
    if col in df.columns and df[col].nunique() >= 10:
        return {"CRV1": col}
    return "HC1"


# ── event study helper ─────────────────────────────────────────────────────────

def run_es(df, yname, dummy_cols) -> pd.DataFrame | None:
    rhs = " + ".join(dummy_cols)
    fml = f"{yname} ~ {rhs} | unit_id + time_period"
    try:
        fit  = pf.feols(fml, data=df, vcov=cluster_arg(df))
        tidy = fit.tidy().reset_index()

        def parse_k(s):
            m = re.search(r"Dm(\d+)", str(s))
            if m: return -int(m.group(1))
            m = re.search(r"Dp(\d+)", str(s))
            if m: return int(m.group(1))
            return np.nan

        tidy["rel_year"] = tidy["Coefficient"].apply(parse_k)
        tidy = tidy[tidy["rel_year"].notna()].copy()
        tidy["rel_year"] = tidy["rel_year"].astype(int)

        ref = pd.DataFrame([{"rel_year": REF, "Estimate": 0.0,
                              "Std. Error": 0.0, "2.5%": 0.0, "97.5%": 0.0}])
        tidy = pd.concat([tidy, ref], ignore_index=True).sort_values("rel_year")

        if "2.5%" not in tidy.columns or tidy["2.5%"].isna().all():
            tidy["2.5%"]  = tidy["Estimate"] - 1.96 * tidy["Std. Error"]
            tidy["97.5%"] = tidy["Estimate"] + 1.96 * tidy["Std. Error"]

        return tidy[["rel_year", "Estimate", "Std. Error", "2.5%", "97.5%"]]
    except Exception as e:
        print(f"  Event study error ({yname}): {e}")
        return None


# ── plot 1: event-study decomposition ─────────────────────────────────────────

def plot_es_decomposition(es_max, es_min, es_range):
    fig, ax = plt.subplots(figsize=(9, 5), facecolor="white")
    ax.set_facecolor("white")

    series = [
        (es_range, GREEN, "Log(price range)  (spread)",   "--"),
        (es_max,   BLUE,  "Log(max price)    (ceiling)",  "-"),
        (es_min,   RED,   "Log(min price)    (floor)",    "-"),
    ]

    for es, col, lbl, ls in series:
        if es is None or es.empty:
            continue
        ax.fill_between(es["rel_year"], es["2.5%"], es["97.5%"],
                        alpha=0.10, color=col)
        ax.plot(es["rel_year"], es["Estimate"],
                color=col, lw=2.0, ls=ls, marker="o",
                ms=4.5, mfc="white", mew=1.4, mec=col,
                label=lbl, zorder=3)

    ax.axhline(0, color="black", lw=0.8)
    ax.axvline(-0.5, color=GRAY, lw=0.9, ls=":", alpha=0.6)
    ylim = ax.get_ylim()
    ax.text(-0.35, ylim[0] + 0.90*(ylim[1]-ylim[0]),
            "eNAM\nadoption", fontsize=8, color=GRAY, ha="left", va="top")

    ax.set_xlabel("Years relative to eNAM adoption", fontsize=10)
    ax.set_ylabel("Coefficient (log points, relative to year −1)", fontsize=10)
    ax.set_title("eNAM and within-mandi price dispersion:\ndecomposing max vs min price",
                 fontsize=11, fontweight="bold", pad=8)
    ax.xaxis.set_major_locator(ticker.MultipleLocator(2))
    ax.legend(fontsize=9.5, framealpha=0, loc="upper left")
    ax.tick_params(labelsize=9)
    fig.tight_layout()

    for ext in (".png", ".pdf"):
        fig.savefig(str(OUT / f"event_study_decomposition{ext}"),
                    dpi=300, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print("  Saved event_study_decomposition.png/pdf")


# ── plot 2: raw group means by relative year (treated mandis) ─────────────────

def plot_raw_rel_year(df):
    treated = df[df["adopt_period"] > 0].copy()
    treated = treated[treated["rel_year"].between(-K_PRE, K_POST)]

    grp = (treated.groupby("rel_year")
           .agg(log_max=("log_max", "mean"),
                log_min=("log_min", "mean"),
                n=("log_max", "size"))
           .reset_index())

    # Normalize to 0 at rel_year = -1
    ref_max = grp.loc[grp["rel_year"] == REF, "log_max"].values
    ref_min = grp.loc[grp["rel_year"] == REF, "log_min"].values
    if len(ref_max) == 0 or len(ref_min) == 0:
        print("  No ref year data — skipping raw trends plot")
        return

    grp["log_max_norm"] = grp["log_max"] - ref_max[0]
    grp["log_min_norm"] = grp["log_min"] - ref_min[0]

    fig, ax = plt.subplots(figsize=(9, 5), facecolor="white")
    ax.set_facecolor("white")

    pre  = grp[grp["rel_year"] <  0]
    post = grp[grp["rel_year"] >= 0]

    for segment_pre, segment_post, col, lbl in [
        (pre["log_max_norm"], post["log_max_norm"], BLUE, "Log(max price)  — ceiling"),
        (pre["log_min_norm"], post["log_min_norm"], RED,  "Log(min price)  — floor"),
    ]:
        ax.plot(pre["rel_year"],  segment_pre,  color=col, lw=1.8, ls="--",
                marker="o", ms=4.5, mfc="white", mew=1.4, mec=col)
        ax.plot(post["rel_year"], segment_post, color=col, lw=2.2, ls="-",
                marker="o", ms=4.5, mfc="white", mew=1.4, mec=col, label=lbl)

    ax.axhline(0, color="black", lw=0.8)
    ax.axvline(-0.5, color=GRAY, lw=0.9, ls=":", alpha=0.6)
    ylim = ax.get_ylim()
    ax.text(-0.35, ylim[0] + 0.92*(ylim[1]-ylim[0]),
            "eNAM adoption", fontsize=8, color=GRAY, ha="left", va="top")

    ax.fill_between(grp["rel_year"],
                    grp["log_max_norm"], grp["log_min_norm"],
                    alpha=0.07, color="purple",
                    label="Range (max − min gap)")

    ax.set_xlabel("Years relative to eNAM adoption", fontsize=10)
    ax.set_ylabel("Change in log price (relative to year −1)", fontsize=10)
    ax.set_title("Raw price trajectories for eNAM-adopting mandis:\nmaximum vs minimum price (all crops pooled)",
                 fontsize=11, fontweight="bold", pad=8)
    ax.xaxis.set_major_locator(ticker.MultipleLocator(2))
    ax.legend(fontsize=9.5, framealpha=0, loc="upper left")
    ax.tick_params(labelsize=9)
    fig.tight_layout()

    for ext in (".png", ".pdf"):
        fig.savefig(str(OUT / f"raw_max_min_treated{ext}"),
                    dpi=300, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print("  Saved raw_max_min_treated.png/pdf")


# ── plot 3: calendar-time trends, treatment vs control ─────────────────────────

def plot_calendar_trends(df):
    df = df.copy()
    df["treated_group"] = df["adopt_period"] > 0

    # Aggregate by year and group
    grp = (df.groupby(["obs_year", "treated_group"])
           .agg(log_max=("log_max", "mean"),
                log_min=("log_min", "mean"),
                n=("log_max", "size"))
           .reset_index())

    treat = grp[grp["treated_group"]].copy()
    ctrl  = grp[~grp["treated_group"]].copy()

    # Normalize each group to its 2014-2015 mean (pre-eNAM for all)
    def normalize(sub, years=(2014, 2015)):
        base_max = sub.loc[sub["obs_year"].between(*years), "log_max"].mean()
        base_min = sub.loc[sub["obs_year"].between(*years), "log_min"].mean()
        sub = sub.copy()
        sub["log_max_n"] = sub["log_max"] - base_max
        sub["log_min_n"] = sub["log_min"] - base_min
        return sub

    treat = normalize(treat)
    ctrl  = normalize(ctrl)

    fig, axes = plt.subplots(1, 2, figsize=(12, 5), facecolor="white",
                             sharey=True)
    fig.subplots_adjust(wspace=0.12)

    for ax, sub, title, shade_from in [
        (axes[0], treat, "Treated mandis (adopted eNAM)", 2016),
        (axes[1], ctrl,  "Control mandis (never-treated)", None),
    ]:
        ax.set_facecolor("white")
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

        ax.plot(sub["obs_year"], sub["log_max_n"],
                color=BLUE, lw=2.0, marker="o", ms=4, mfc="white",
                mew=1.4, mec=BLUE, label="Log(max price)")
        ax.plot(sub["obs_year"], sub["log_min_n"],
                color=RED, lw=2.0, marker="o", ms=4, mfc="white",
                mew=1.4, mec=RED, label="Log(min price)")
        ax.fill_between(sub["obs_year"], sub["log_max_n"], sub["log_min_n"],
                        alpha=0.07, color="purple")

        if shade_from:
            ax.axvspan(shade_from, sub["obs_year"].max() + 0.5,
                       alpha=0.05, color=GREEN, label="Post first eNAM wave")
            ax.axvline(shade_from, color=GREEN, lw=1.1, ls="--", alpha=0.7)

        ax.axhline(0, color="black", lw=0.7)
        ax.set_xlabel("Calendar year", fontsize=10)
        ax.set_title(title, fontsize=10.5, fontweight="bold", pad=6)
        ax.legend(fontsize=9, framealpha=0, loc="upper left")
        ax.tick_params(labelsize=9)

    axes[0].set_ylabel("Change in log price (relative to 2014–15)", fontsize=10)
    fig.suptitle("Within-mandi price ceilings and floors:\ntreatment vs control, calendar time",
                 fontsize=11.5, fontweight="bold", y=1.02)
    fig.tight_layout()

    for ext in (".png", ".pdf"):
        fig.savefig(str(OUT / f"calendar_trends{ext}"),
                    dpi=300, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print("  Saved calendar_trends.png/pdf")


# ── main ───────────────────────────────────────────────────────────────────────

print("Loading data...")
df = load_data()
df, dummy_cols = add_dummies(df)
print(f"  {len(df):,} obs | {df['unit_id'].nunique():,} units | "
      f"{(df['adopt_period']>0).sum():,} treated obs | "
      f"{(df['adopt_period']==0).sum():,} control obs")

print("\n[Event study — log_max]")
es_max = run_es(df, "log_max", dummy_cols)

print("[Event study — log_min]")
es_min = run_es(df, "log_min", dummy_cols)

print("[Event study — log_range]")
es_range = run_es(df, "log_range", dummy_cols)

print("\n[Plot 1: event-study decomposition]")
plot_es_decomposition(es_max, es_min, es_range)

print("[Plot 2: raw trajectories for treated mandis]")
plot_raw_rel_year(df)

print("[Plot 3: calendar-time trends, treatment vs control]")
plot_calendar_trends(df)

# Save coefficient tables
if es_max is not None:
    es_max.to_csv(OUT / "es_log_max_coefs.csv", index=False)
if es_min is not None:
    es_min.to_csv(OUT / "es_log_min_coefs.csv", index=False)

print(f"\nAll outputs saved to {OUT.relative_to(ROOT)}/")
print("Done.")
