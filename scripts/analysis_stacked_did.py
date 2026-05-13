"""
Stacked DiD: Across-Mandi Price Dispersion
Outcome:  Annual mean SD of modal prices across mandis within each state
Design:   Stacked DiD exploiting staggered eNAM rollout across states.
          For each adoption cohort g, treated states = those adopting in year g;
          clean controls = states not yet treated at time g (adopt later or never).
          Stacks are pooled; cohort×state and cohort×year FE are absorbed.
Outputs:  output/stacked_did/
          - {crop}_event_study.png  (one per crop)
          - {crop}_raw_trends.png   (one per crop)
          - event_study_panel.png   (all 11 crops, 3×4 grid)
          - raw_trends_panel.png    (all 11 crops, 3×4 grid)
          - event_study_coefficients.csv
          - att_summary.csv
"""

import warnings
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from pathlib import Path

ROOT    = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "output" / "stacked_did"
OUT_DIR.mkdir(parents=True, exist_ok=True)

CROPS = ["wheat", "paddy", "rice", "maize", "onion", "tomato",
         "potato", "mustard", "soybean", "cotton", "chana"]

STATE_MAP = {
    "Andaman and Nicobar": "Andaman & Nicobar Islands",
    "Jammu And Kashmir":   "Jammu & Kashmir",
    "Jammu and Kashmir":   "Jammu & Kashmir",
    "Pondicherry":         "Puducherry",
    "Delhi":               None,
    "Manipur":             None,
}

MIN_MANDIS  = 3   # min mandis per state-month for a valid SD observation
WINDOW_PRE  = 5   # years before treatment to include
WINDOW_POST = 8   # years after treatment to include
REF_PERIOD  = -1  # omitted relative-time period

GREEN = "#1a6b3c"
RED   = "#c0392b"
GRAY  = "#888888"
BLUE  = "#2563eb"


# ── 1. Load state-level eNAM adoption ────────────────────────────────────────

enam_raw = pd.read_csv(ROOT / "data/clean/enam_adoption.csv")
state_adoption = (
    enam_raw.groupby("state")["year_joined_enam"]
    .min().reset_index()
    .rename(columns={"year_joined_enam": "treat_year"})
)
print("States by adoption cohort:")
print(state_adoption["treat_year"].value_counts().sort_index().to_string())
print()


# ── 2. Build state × year SD panel for a single crop ─────────────────────────

def build_sd_panel(crop: str) -> pd.DataFrame:
    df = pd.read_csv(ROOT / f"data/clean/prices/{crop}_monthly.csv")
    df = df[df["match_type"].isin(["exact", "fuzzy"])].copy()
    df["state_name"] = df["state_name"].replace(STATE_MAP)
    df = df[df["state_name"].notna()]

    # Monthly SD within state (require at least MIN_MANDIS)
    monthly = (
        df.groupby(["state_name", "year", "month"])["modal_price_avg"]
        .agg(sd="std", n="count")
        .reset_index()
    )
    monthly = monthly[monthly["n"] >= MIN_MANDIS]

    # Annual mean of within-month SDs
    annual = (
        monthly.groupby(["state_name", "year"])["sd"]
        .mean().reset_index()
    )
    annual = annual[(annual["year"] >= 2010) & (annual["year"] <= 2025)]

    # Merge adoption year; never-treated → treat_year = 9999
    annual = annual.merge(
        state_adoption, left_on="state_name", right_on="state", how="left"
    )
    annual["treat_year"] = annual["treat_year"].fillna(9999).astype(int)
    return annual


# ── 3. Construct stacked dataset ─────────────────────────────────────────────

def build_stack(panel: pd.DataFrame) -> pd.DataFrame:
    """
    For each cohort g, create a sub-experiment:
      Treated  : states with treat_year == g
      Controls : states with treat_year > g (not yet treated, or never)
      Window   : [g - WINDOW_PRE, g + WINDOW_POST]
    Tag each row with (cohort, rel_time, treat_indicator).
    """
    cohorts = sorted(panel[panel["treat_year"] != 9999]["treat_year"].unique())
    stacks = []

    for g in cohorts:
        sub = panel[
            (panel["treat_year"] == g) | (panel["treat_year"] > g)
        ].copy()
        sub = sub[
            (sub["year"] >= g - WINDOW_PRE) & (sub["year"] <= g + WINDOW_POST)
        ].copy()

        if sub.empty:
            continue

        # Drop cohorts with no controls
        n_treated  = (sub["treat_year"] == g).sum()
        n_controls = (sub["treat_year"] != g).sum()
        if n_treated == 0 or n_controls == 0:
            continue

        sub["cohort"]      = g
        sub["rel_time"]    = sub["year"] - g
        sub["treat_ind"]   = (sub["treat_year"] == g).astype(int)
        sub["state_cohort"] = sub["state_name"].astype(str) + "__" + str(g)
        sub["year_cohort"]  = sub["year"].astype(str) + "__" + str(g)
        stacks.append(sub)

    if not stacks:
        return pd.DataFrame()
    return pd.concat(stacks, ignore_index=True)


# ── 4. Two-way FE OLS with cluster-robust SEs ────────────────────────────────

def ols_cluster_robust(y: np.ndarray, X: np.ndarray,
                       clusters: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """
    OLS coefficients and cluster-robust SEs.
    Clusters: 1-D array of cluster labels (strings or ints).
    """
    XtX     = X.T @ X
    XtX_inv = np.linalg.pinv(XtX)
    beta    = XtX_inv @ X.T @ y
    resid   = y - X @ beta

    n, k = X.shape
    unique_c = np.unique(clusters)
    G = len(unique_c)

    meat = np.zeros((k, k))
    for g in unique_c:
        mask = clusters == g
        Xg = X[mask]
        eg = resid[mask]
        score = Xg.T @ eg
        meat += np.outer(score, score)

    # Small-sample correction
    correction = (G / (G - 1)) * ((n - 1) / (n - k))
    V  = XtX_inv @ meat @ XtX_inv * correction
    se = np.sqrt(np.abs(np.diag(V)))
    return beta, se


def run_event_study(stack: pd.DataFrame, crop: str
                    ) -> pd.DataFrame | None:
    """
    Stacked DiD event study regression:
      SD = Σ_{k≠-1} β_k · (1[rel_time==k] × treat_ind)
           + state×cohort FE + year×cohort FE + ε
    FE absorbed via get_dummies; SEs clustered at state level.
    Returns a DataFrame with columns: rel_time, coef, se, ci_low, ci_high.
    """
    if stack.empty:
        return None

    # Relative times available (excluding reference)
    rel_times     = sorted(stack["rel_time"].unique())
    rel_times_est = [k for k in rel_times if k != REF_PERIOD]
    if len(rel_times_est) < 2:
        return None

    # Event-study interaction dummies: treat_ind × 1[rel_time == k]
    for k in rel_times_est:
        stack[f"_D{k:+d}"] = (stack["rel_time"] == k).astype(float) * stack["treat_ind"]

    event_cols = [f"_D{k:+d}" for k in rel_times_est]

    # Fixed-effect dummies (drop_first to avoid perfect collinearity)
    sc_dummies = pd.get_dummies(stack["state_cohort"], prefix="sc", drop_first=True, dtype=float)
    yc_dummies = pd.get_dummies(stack["year_cohort"],  prefix="yc", drop_first=True, dtype=float)

    X_df = pd.concat([stack[event_cols], sc_dummies, yc_dummies], axis=1)
    y_s  = stack["sd"]

    # Drop rows with any NaN
    valid = y_s.notna() & X_df.notna().all(axis=1)
    if valid.sum() < len(event_cols) + 5:
        print(f"  [{crop}] Too few obs after dropping NaN — skipping.")
        return None

    y_arr  = y_s[valid].to_numpy(dtype=float)
    X_arr  = X_df[valid].to_numpy(dtype=float)
    cl_arr = stack["state_name"][valid].to_numpy()

    n_clusters = len(np.unique(cl_arr))
    if n_clusters < 5:
        print(f"  [{crop}] Only {n_clusters} clusters — SE estimates unreliable.")

    beta, se = ols_cluster_robust(y_arr, X_arr, cl_arr)

    # Collect event-study coefficients (first len(event_cols) entries)
    rows = []
    for i, k in enumerate(rel_times_est):
        rows.append({
            "rel_time": k,
            "coef":     beta[i],
            "se":       se[i],
            "ci_low":   beta[i] - 1.96 * se[i],
            "ci_high":  beta[i] + 1.96 * se[i],
        })
    # Add reference point at k = -1 (normalised to zero)
    rows.append({"rel_time": REF_PERIOD, "coef": 0.0, "se": 0.0,
                 "ci_low": 0.0, "ci_high": 0.0})

    df_out = pd.DataFrame(rows).sort_values("rel_time").reset_index(drop=True)
    print(f"  [{crop}] Estimated on {valid.sum()} obs, {n_clusters} state-clusters.")
    return df_out


# ── 5. Plotting ───────────────────────────────────────────────────────────────

def _style_ax(ax):
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis="y", linestyle="--", alpha=0.3)


def plot_event_study(coefs: pd.DataFrame | None, crop: str,
                     ax=None, save: bool = True) -> None:
    standalone = ax is None
    if standalone:
        fig, ax = plt.subplots(figsize=(9, 5), facecolor="white")
        ax.set_facecolor("white")

    if coefs is None or coefs.empty:
        ax.text(0.5, 0.5, "Insufficient data", ha="center", va="center",
                transform=ax.transAxes, fontsize=11, color=GRAY)
        ax.set_title(crop.capitalize(), fontsize=12, fontweight="bold")
        if standalone and save:
            fig.tight_layout()
            fig.savefig(OUT_DIR / f"{crop}_event_study.png", dpi=200,
                        bbox_inches="tight", facecolor="white")
            plt.close(fig)
        return

    pre  = coefs[coefs["rel_time"] < 0].sort_values("rel_time")
    post = coefs[coefs["rel_time"] >= 0].sort_values("rel_time")

    # Shaded CI band (all periods)
    ax.fill_between(coefs["rel_time"], coefs["ci_low"], coefs["ci_high"],
                    alpha=0.12, color=GREEN, zorder=1)

    # Pre-period: dashed gray
    ax.plot(pre["rel_time"], pre["coef"], color=GRAY, linewidth=1.8,
            linestyle="--", marker="o", markersize=5,
            markerfacecolor="white", markeredgecolor=GRAY,
            markeredgewidth=1.5, zorder=3, label="Pre-adoption")

    # Post-period: solid green
    ax.plot(post["rel_time"], post["coef"], color=GREEN, linewidth=2.0,
            linestyle="-", marker="o", markersize=5,
            markerfacecolor="white", markeredgecolor=GREEN,
            markeredgewidth=1.5, zorder=3, label="Post-adoption")

    ax.axhline(0, color="black", linewidth=0.8)
    ax.axvline(-0.5, color="#555555", linewidth=1.0, linestyle=":", alpha=0.7)

    ylim = ax.get_ylim()
    mid  = ylim[0] + 0.88 * (ylim[1] - ylim[0])
    ax.text(-0.4, mid, "eNAM\nadopt.", fontsize=7.5, color="#555555",
            ha="left", va="center")

    ax.set_xlabel("Years relative to eNAM adoption", fontsize=9)
    ax.set_ylabel("Δ SD of modal prices\n(Rs/quintal)", fontsize=9)
    ax.set_title(crop.capitalize(), fontsize=12, fontweight="bold")
    ax.xaxis.set_major_locator(ticker.MultipleLocator(2))
    ax.tick_params(labelsize=8)
    _style_ax(ax)

    if standalone and save:
        ax.legend(fontsize=8, loc="upper left")
        fig.suptitle(
            f"Event Study — {crop.capitalize()}\n"
            "Stacked DiD | State×Cohort + Year×Cohort FE | Clustered SE (state)",
            fontsize=9, y=1.02
        )
        fig.tight_layout()
        out = OUT_DIR / f"{crop}_event_study.png"
        fig.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
        plt.close(fig)
        print(f"  Saved: {out.name}")


def plot_raw_trends(panel: pd.DataFrame, crop: str,
                    ax=None, save: bool = True) -> None:
    """
    Raw mean within-state SD by calendar year for:
      - Phase 1 adopters (2016)
      - Phase 2+ adopters (2017, 2020, 2022)
      - Never / not-yet-treated
    Lines show group averages; each individual state shown as thin background line.
    """
    standalone = ax is None
    if standalone:
        fig, ax = plt.subplots(figsize=(9, 5), facecolor="white")
        ax.set_facecolor("white")

    def group_label(ty):
        if ty == 2016: return "Phase 1 (adopted 2016)"
        if ty == 2017: return "Phase 2a (adopted 2017)"
        if ty in [2020, 2022]: return "Phase 2b/3 (adopted 2020+)"
        return "Not yet / never treated"

    panel = panel.copy()
    panel["group"] = panel["treat_year"].apply(group_label)

    # Individual state thin lines
    for state, sdf in panel.groupby("state_name"):
        sdf = sdf.sort_values("year")
        ty  = sdf["treat_year"].iloc[0]
        col = GREEN if ty == 2016 else (BLUE if ty == 2017 else
              (RED if ty in [2020, 2022] else GRAY))
        ax.plot(sdf["year"], sdf["sd"], color=col, linewidth=0.5,
                alpha=0.25, zorder=1)

    # Group mean lines
    style = {
        "Phase 1 (adopted 2016)":        {"color": GREEN, "ls": "-",  "lw": 2.2, "marker": "o"},
        "Phase 2a (adopted 2017)":       {"color": BLUE,  "ls": "-",  "lw": 2.2, "marker": "s"},
        "Phase 2b/3 (adopted 2020+)":    {"color": RED,   "ls": "--", "lw": 2.0, "marker": "^"},
        "Not yet / never treated":       {"color": GRAY,  "ls": ":",  "lw": 1.8, "marker": "D"},
    }
    agg = panel.groupby(["group", "year"])["sd"].mean().reset_index()
    for grp, st in style.items():
        sub = agg[agg["group"] == grp].sort_values("year")
        if sub.empty:
            continue
        ax.plot(sub["year"], sub["sd"], color=st["color"], linewidth=st["lw"],
                linestyle=st["ls"], marker=st["marker"], markersize=4,
                markerfacecolor="white", markeredgewidth=1.4,
                markeredgecolor=st["color"], label=grp, zorder=3)

    # Vertical lines for adoption phases
    for yr, lbl, col in [(2016, "Ph.1", GREEN), (2017, "Ph.2a", BLUE),
                          (2020, "Ph.2b", RED)]:
        ax.axvline(yr, color=col, linewidth=1.0, linestyle=":", alpha=0.6)
        ylim = ax.get_ylim()
        ax.text(yr + 0.15, ylim[0] + 0.88 * (ylim[1] - ylim[0]),
                lbl, fontsize=7, color=col, ha="left")

    ax.set_xlabel("Year", fontsize=9)
    ax.set_ylabel("Mean SD of modal prices\n(Rs/quintal)", fontsize=9)
    ax.set_title(crop.capitalize(), fontsize=12, fontweight="bold")
    ax.xaxis.set_major_locator(ticker.MultipleLocator(3))
    ax.tick_params(labelsize=8)
    _style_ax(ax)

    if standalone and save:
        ax.legend(fontsize=8, loc="upper left", framealpha=0.7)
        fig.suptitle(
            f"Raw Price Dispersion Trends — {crop.capitalize()}\n"
            "Annual mean within-state SD of modal prices by eNAM adoption group",
            fontsize=9, y=1.02
        )
        fig.tight_layout()
        out = OUT_DIR / f"{crop}_raw_trends.png"
        fig.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
        plt.close(fig)
        print(f"  Saved: {out.name}")


# ── 6. Main loop ──────────────────────────────────────────────────────────────

all_coefs = []
att_rows  = []

print("=" * 60)
print("Stacked DiD — Across-Mandi Price Dispersion")
print("=" * 60)

for crop in CROPS:
    print(f"\n── {crop.capitalize()} ──")
    panel = build_sd_panel(crop)
    stack = build_stack(panel)
    coefs = run_event_study(stack, crop)

    # Save individual plots
    plot_event_study(coefs, crop, save=True)
    plot_raw_trends(panel,  crop, save=True)

    if coefs is not None and not coefs.empty:
        coefs["crop"] = crop
        all_coefs.append(coefs)

        # Average post-treatment coefficient (ATT proxy)
        post  = coefs[coefs["rel_time"] >= 0]
        att   = post["coef"].mean()
        pre   = coefs[(coefs["rel_time"] < 0) & (coefs["rel_time"] != REF_PERIOD)]
        pre_f = pre["coef"].abs().mean()  # mean abs pre-trend (should ≈ 0)
        att_rows.append({
            "crop":              crop,
            "att_avg_post":      round(att, 2),
            "pre_trend_abs_avg": round(pre_f, 2),
            "n_post_periods":    len(post),
            "n_pre_periods":     len(pre),
        })


# ── 7. Panel figure — event studies ──────────────────────────────────────────

NCOLS = 4
NROWS = 3   # ceil(11 / 4) = 3 rows, last cell blank

fig, axes = plt.subplots(NROWS, NCOLS, figsize=(22, 14),
                          facecolor="white", constrained_layout=True)
axes_flat = axes.flatten()

for i, crop in enumerate(CROPS):
    ax = axes_flat[i]
    ax.set_facecolor("white")
    panel = build_sd_panel(crop)
    stack = build_stack(panel)
    coefs = run_event_study(stack, crop)
    plot_event_study(coefs, crop, ax=ax, save=False)

# Hide unused subplot
for j in range(len(CROPS), len(axes_flat)):
    axes_flat[j].set_visible(False)

fig.suptitle(
    "Event Study: Effect of eNAM Integration on Across-Mandi Price Dispersion (SD)\n"
    "Stacked DiD | State×Cohort + Year×Cohort FE | Clustered SE (state level)",
    fontsize=13, fontweight="bold"
)
out_panel = OUT_DIR / "event_study_panel.png"
fig.savefig(out_panel, dpi=200, bbox_inches="tight", facecolor="white")
plt.close(fig)
print(f"\nSaved: {out_panel.name}")


# ── 8. Panel figure — raw trends ─────────────────────────────────────────────

fig2, axes2 = plt.subplots(NROWS, NCOLS, figsize=(22, 14),
                            facecolor="white", constrained_layout=True)
axes2_flat = axes2.flatten()

# Build a shared legend from the first subplot
handles, labels = [], []
for i, crop in enumerate(CROPS):
    ax = axes2_flat[i]
    ax.set_facecolor("white")
    panel = build_sd_panel(crop)
    plot_raw_trends(panel, crop, ax=ax, save=False)
    if i == 0:
        handles, labels = ax.get_legend_handles_labels()

for j in range(len(CROPS), len(axes2_flat)):
    axes2_flat[j].set_visible(False)

fig2.suptitle(
    "Raw Price Dispersion Trends: eNAM Adoption Groups\n"
    "Annual mean within-state SD of modal prices across mandis (thin lines = individual states)",
    fontsize=13, fontweight="bold"
)
if handles:
    fig2.legend(handles, labels, loc="lower right", fontsize=9,
                bbox_to_anchor=(0.98, 0.02), framealpha=0.85)

out_raw = OUT_DIR / "raw_trends_panel.png"
fig2.savefig(out_raw, dpi=200, bbox_inches="tight", facecolor="white")
plt.close(fig2)
print(f"Saved: {out_raw.name}")


# ── 9. Save tables ────────────────────────────────────────────────────────────

if all_coefs:
    coef_df = pd.concat(all_coefs, ignore_index=True)
    coef_df = coef_df[["crop", "rel_time", "coef", "se", "ci_low", "ci_high"]].round(3)
    coef_path = OUT_DIR / "event_study_coefficients.csv"
    coef_df.to_csv(coef_path, index=False)
    print(f"Saved: {coef_path.name}")

if att_rows:
    att_df = pd.DataFrame(att_rows)
    att_df = att_df.sort_values("att_avg_post")
    att_path = OUT_DIR / "att_summary.csv"
    att_df.to_csv(att_path, index=False)
    print(f"Saved: {att_path.name}")

    print("\n── ATT Summary (avg post-treatment β by crop) ──")
    print(f"{'Crop':<12}  {'ATT (avg β post)':>18}  {'Pre-trend (|β| avg)':>20}")
    print("-" * 55)
    for _, r in att_df.iterrows():
        arrow = "▼" if r["att_avg_post"] < 0 else "▲"
        print(f"{r['crop']:<12}  {arrow} {r['att_avg_post']:>+15.1f}  "
              f"{r['pre_trend_abs_avg']:>20.1f}")

print("\nDone.")
