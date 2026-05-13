"""
run_did.py
----------
Staggered DiD using TWFE with explicit relative-year dummies via pyfixest feols.

Design
------
* Pooled ATT  : feols("y ~ post_treat | unit + time", ...)
* Event study : feols("y ~ D_-5 + ... + D_-2 + D_0 + ... + D_+8 | unit + time", ...)
  where D_k = 1 if treated unit is k years relative to its adoption year, else 0.
  Never-treated units have all D_k = 0 and contribute only to unit/time FEs.
  Reference period is k = -1 (one year pre-adoption).

Specs
-----
  1  Within-mandi   log(price range)       mandi × crop unit
  2  District-level log(SD modal prices)   district × crop unit, ≥4 mandis/district
  3  State-level    log(SD modal prices)   state × crop unit

Samples
-------
  A  All crops
  B  Balanced: crops with ≥50 mandis in range data
     (drops rice, mustard, soybean, chana)
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
RES  = ROOT / "results"
RES.mkdir(exist_ok=True)

# ── constants ─────────────────────────────────────────────────────────────────

CROPS_ALL      = ["wheat", "paddy", "rice", "maize", "onion",
                  "tomato", "potato", "mustard", "soybean", "cotton", "chana"]
CROPS_BALANCED = ["wheat", "paddy", "maize", "onion",
                  "tomato", "potato", "cotton"]

PERISHABLE = {"onion", "tomato", "potato"}
CROP_COLOR = {c: ("#c0392b" if c in PERISHABLE else "#1a6b3c") for c in CROPS_ALL}

K_PRE  = 5   # years of pre-treatment window
K_POST = 8   # years of post-treatment window
REF    = -1  # omitted relative year

SAMPLES = {
    "sample_A_full":     CROPS_ALL,
    "sample_B_balanced": CROPS_BALANCED,
}

SPECS = {
    "spec1_within_mandi": {
        "file":    "within_mandi_range.csv",
        "yname":   "log_range",
        "unit_cols": ["mandi_id", "crop"],
        "gname":   "adopt_period",
        "tname":   "time_period",
        "cluster": "state_clean",
        "label":   "Within-mandi log(price range)",
        # Drop rows with no mandi_id: treatment status unknown for unmatched markets
        "require_notna": "mandi_id",
    },
    "spec2_across_district": {
        "file":    "across_mandi_district.csv",
        "yname":   "log_price_sd",
        "unit_cols": ["district", "crop"],
        "gname":   "district_adopt_period",
        "tname":   "time_period",
        "cluster": "state",
        "label":   "Across-mandi log(price SD) — District",
    },
    "spec3_across_state": {
        "file":    "across_mandi_state.csv",
        "yname":   "log_price_sd",
        "unit_cols": ["state_final", "crop"],
        "gname":   "state_adopt_period",
        "tname":   "time_period",
        "cluster": "state_final",   # few clusters; HC1 used instead
        "label":   "Across-mandi log(price SD) — State",
    },
}

# ── plot style ────────────────────────────────────────────────────────────────

plt.rcParams.update({
    "font.family":       "serif",
    "axes.spines.top":   False,
    "axes.spines.right": False,
    "axes.grid":         False,
    "figure.dpi":        150,
    "font.size":         11,
})
GREEN = "#1a6b3c"
RED   = "#c0392b"
GRAY  = "#555555"


# ── data preparation ──────────────────────────────────────────────────────────

def prepare_data(spec: dict, crops: list) -> pd.DataFrame:
    df = pd.read_csv(DAT / spec["file"])
    df = df[df["crop"].isin(crops)].copy()

    yname  = spec["yname"]
    gname  = spec["gname"]
    tname  = spec["tname"]

    # Optional pre-filter: drop rows where a required column is NaN
    if "require_notna" in spec:
        df = df[df[spec["require_notna"]].notna()].copy()

    df = df[df[yname].notna()].copy()
    df[gname] = df[gname].fillna(0).astype(int)
    df[tname] = df[tname].astype(int)

    # Integer unit ID (unique per unit_cols combination)
    unit_key = df[spec["unit_cols"]].apply(
        lambda row: "__".join(str(x) for x in row), axis=1
    )
    df["unit_id"] = pd.factorize(unit_key)[0]

    # Relative year: calendar year of obs minus calendar year of adoption
    df["obs_year"]   = df[tname] // 12
    df["adopt_year"] = np.where(df[gname] > 0, df[gname] // 12, 0)
    df["rel_year"]   = np.where(df[gname] > 0,
                                df["obs_year"] - df["adopt_year"],
                                np.nan)   # NaN for never-treated

    # post_treat flag: treated unit, at or after adoption
    df["post_treat"] = ((df[gname] > 0) &
                        (df[tname] >= df[gname])).astype(int)

    return df


def add_event_dummies(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """
    Add relative-year dummies for k in [-K_PRE, K_POST], k ≠ REF.
    Names use Dm{n} for negative k and Dp{n} for non-negative k (valid Python
    identifiers that formulaic won't misparse as operators).
    """
    dummy_cols = []
    for k in range(-K_PRE, K_POST + 1):
        if k == REF:
            continue
        col = f"Dm{abs(k)}" if k < 0 else f"Dp{k}"
        df[col] = (df["rel_year"] == k).astype(float)
        dummy_cols.append(col)
    return df, dummy_cols


# ── regression helpers ────────────────────────────────────────────────────────

def cluster_arg(df: pd.DataFrame, spec: dict) -> dict | str:
    col = spec["cluster"]
    if col not in df.columns or df[col].nunique() < 10:
        return "HC1"
    return {"CRV1": col}


def run_att(df: pd.DataFrame, spec: dict) -> dict | None:
    """Pooled ATT via simple post × treated TWFE."""
    yname  = spec["yname"]
    n_treat = (df["post_treat"] == 1).sum()
    n_ctrl  = (df["post_treat"] == 0).sum()
    if n_treat < 10 or n_ctrl < 10:
        return None
    try:
        fit = pf.feols(
            fml=f"{yname} ~ post_treat | unit_id + {spec['tname']}",
            data=df,
            vcov=cluster_arg(df, spec),
        )
        t = fit.tidy().reset_index()
        row = t[t["Coefficient"] == "post_treat"].iloc[0]
        return {
            "att":       float(row["Estimate"]),
            "se":        float(row["Std. Error"]),
            "ci_low":    float(row.get("2.5%",  row["Estimate"] - 1.96*row["Std. Error"])),
            "ci_high":   float(row.get("97.5%", row["Estimate"] + 1.96*row["Std. Error"])),
            "pval":      float(row.get("Pr(>|t|)", np.nan)),
            "n_obs":     int(df[yname].notna().sum()),
            "n_units":   int(df["unit_id"].nunique()),
            "n_time":    int(df[spec["tname"]].nunique()),
            "n_treated": int(df[df[spec["gname"]] > 0]["unit_id"].nunique()),
        }
    except Exception as e:
        print(f"    ATT error: {e}")
        return None


def run_event_study(df: pd.DataFrame, spec: dict,
                    dummy_cols: list[str]) -> pd.DataFrame | None:
    """Event study via relative-year dummies."""
    yname = spec["yname"]
    if df[yname].notna().sum() < 50:
        return None
    rhs = " + ".join(dummy_cols)
    fml = f"{yname} ~ {rhs} | unit_id + {spec['tname']}"
    try:
        fit  = pf.feols(fml, data=df, vcov=cluster_arg(df, spec))
        tidy = fit.tidy().reset_index()

        # Parse relative year from coefficient names (Dm5 → -5, Dp0 → 0)
        def parse_k(s):
            m = re.search(r"Dm(\d+)", str(s))
            if m: return -int(m.group(1))
            m = re.search(r"Dp(\d+)", str(s))
            if m: return int(m.group(1))
            return np.nan

        tidy["rel_year"] = tidy["Coefficient"].apply(parse_k)
        tidy = tidy[tidy["rel_year"].notna()].copy()
        tidy["rel_year"] = tidy["rel_year"].astype(int)

        # Add reference row
        ref_row = {"rel_year": REF, "Estimate": 0.0,
                   "Std. Error": 0.0, "2.5%": 0.0, "97.5%": 0.0}
        tidy = pd.concat([tidy, pd.DataFrame([ref_row])], ignore_index=True)
        tidy = tidy.sort_values("rel_year").reset_index(drop=True)

        # Ensure CI columns exist
        if "2.5%" not in tidy.columns or tidy["2.5%"].isna().all():
            tidy["2.5%"]  = tidy["Estimate"] - 1.96 * tidy["Std. Error"]
            tidy["97.5%"] = tidy["Estimate"] + 1.96 * tidy["Std. Error"]

        return tidy[["rel_year", "Estimate", "Std. Error", "2.5%", "97.5%"]]
    except Exception as e:
        print(f"    Event study error: {e}")
        return None


# ── plotting ──────────────────────────────────────────────────────────────────

def save_fig(fig, stem: Path):
    fig.savefig(str(stem) + ".png", dpi=300, bbox_inches="tight", facecolor="white")
    fig.savefig(str(stem) + ".pdf", bbox_inches="tight", facecolor="white")
    plt.close(fig)


def plot_event_study(es: pd.DataFrame, title: str, stem: Path):
    if es is None or es.empty:
        return
    fig, ax = plt.subplots(figsize=(8, 4.5), facecolor="white")
    ax.set_facecolor("white")

    pre  = es[es["rel_year"] < 0].sort_values("rel_year")
    post = es[es["rel_year"] >= 0].sort_values("rel_year")

    ax.fill_between(es["rel_year"], es["2.5%"], es["97.5%"],
                    alpha=0.12, color=GREEN)
    ax.plot(pre["rel_year"],  pre["Estimate"],  color=GRAY,  lw=1.8,
            ls="--", marker="o", ms=4.5, mfc="white", mew=1.4, mec=GRAY,
            label="Pre-adoption", zorder=3)
    ax.plot(post["rel_year"], post["Estimate"], color=GREEN, lw=2.0,
            ls="-",  marker="o", ms=4.5, mfc="white", mew=1.4, mec=GREEN,
            label="Post-adoption", zorder=3)

    ax.axhline(0, color="black", lw=0.8)
    ax.axvline(-0.5, color=GRAY, lw=0.9, ls=":", alpha=0.7)
    ylim = ax.get_ylim()
    ax.text(-0.35, ylim[0] + 0.88*(ylim[1]-ylim[0]),
            "Adoption", fontsize=8, color=GRAY, ha="left")

    ax.set_xlabel("Years relative to eNAM adoption", fontsize=10)
    ax.set_ylabel("Coefficient (log points)", fontsize=10)
    ax.set_title(title, fontsize=11, fontweight="bold", pad=8)
    ax.xaxis.set_major_locator(ticker.MultipleLocator(2))
    ax.legend(fontsize=9, framealpha=0, loc="upper left")
    ax.tick_params(labelsize=9)
    fig.tight_layout()
    save_fig(fig, stem)


def plot_crop_het(crop_atts: dict, title: str, stem: Path):
    rows = [(c, r["att"], r["ci_low"], r["ci_high"])
            for c, r in crop_atts.items() if r is not None]
    if not rows:
        return
    rows.sort(key=lambda x: x[1])
    crops, atts, ci_lo, ci_hi = zip(*rows)
    err_lo = [a - lo for a, lo in zip(atts, ci_lo)]
    err_hi = [hi - a  for a, hi in zip(atts, ci_hi)]

    fig, ax = plt.subplots(figsize=(7, max(3, 0.55*len(crops)+1.5)),
                           facecolor="white")
    ax.set_facecolor("white")
    y = np.arange(len(crops))
    colors = [CROP_COLOR.get(c, GREEN) for c in crops]

    ax.barh(y, atts, xerr=[err_lo, err_hi], color=colors, alpha=0.75,
            height=0.55, error_kw={"elinewidth": 1.4, "ecolor": "black",
                                    "capsize": 3})
    ax.axvline(0, color="black", lw=0.9, ls="--", alpha=0.7)
    ax.set_yticks(y)
    ax.set_yticklabels([c.capitalize() for c in crops], fontsize=10)
    ax.set_xlabel("ATT (log points)", fontsize=10)
    ax.set_title(title, fontsize=11, fontweight="bold", pad=8)

    from matplotlib.patches import Patch
    ax.legend(handles=[Patch(facecolor=GREEN, alpha=0.75, label="Storable"),
                        Patch(facecolor=RED,   alpha=0.75, label="Perishable")],
              fontsize=9, framealpha=0, loc="lower right")
    ax.tick_params(labelsize=9)
    fig.tight_layout()
    save_fig(fig, stem)


# ── LaTeX helpers ─────────────────────────────────────────────────────────────

def stars(p):
    if pd.isna(p): return ""
    return "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""


def to_latex(df: pd.DataFrame, caption: str, label: str,
             note: str = "") -> str:
    ncols = len(df.columns)
    col_fmt = "l" + "r" * (ncols - 1)
    lines = ["\\begin{table}[htbp]", "\\centering",
             f"\\caption{{{caption}}}", f"\\label{{{label}}}",
             f"\\begin{{tabular}}{{{col_fmt}}}",
             "\\hline\\hline",
             " & ".join(str(c) for c in df.columns) + " \\\\",
             "\\hline"]
    for _, row in df.iterrows():
        lines.append(" & ".join(str(v) for v in row.values) + " \\\\")
    lines += ["\\hline\\hline"]
    if note:
        lines.append(f"\\multicolumn{{{ncols}}}{{l}}"
                     f"{{\\footnotesize {note}}} \\\\")
    lines += ["\\end{tabular}", "\\end{table}"]
    return "\n".join(lines)


# ── main loop ─────────────────────────────────────────────────────────────────

summary_rows = []

for sample_name, crops in SAMPLES.items():
    for spec_name, spec in SPECS.items():

        tag = f"{sample_name} / {spec_name}"
        print(f"\n{'='*70}")
        print(f"  {tag}")
        print(f"{'='*70}")

        out = RES / sample_name / spec_name
        out.mkdir(parents=True, exist_ok=True)

        # ── load & prep ───────────────────────────────────────────────────────
        try:
            df = prepare_data(spec, crops)
        except Exception as e:
            print(f"  LOAD ERROR: {e}")
            continue

        df, dummy_cols = add_event_dummies(df)

        n_total   = len(df)
        n_treated = (df[spec["gname"]] > 0).sum()
        n_never   = (df[spec["gname"]] == 0).sum()
        n_units   = df["unit_id"].nunique()
        print(f"  Rows {n_total:,} | units {n_units:,} | "
              f"treated obs {n_treated:,} | never-treated obs {n_never:,}")

        # ── pooled ATT ────────────────────────────────────────────────────────
        print(f"\n  [Pooled ATT]")
        att_res = run_att(df, spec)

        if att_res:
            sig = stars(att_res["pval"])
            print(f"    ATT = {att_res['att']:+.4f}{sig}  "
                  f"SE = {att_res['se']:.4f}  "
                  f"N = {att_res['n_obs']:,}")

            rt = pd.DataFrame([{
                "Metric": m, "Value": v
            } for m, v in {
                "ATT":             f"{att_res['att']:+.4f}{sig}",
                "SE":              f"({att_res['se']:.4f})",
                "95% CI":          f"[{att_res['ci_low']:.4f}, {att_res['ci_high']:.4f}]",
                "N observations":  f"{att_res['n_obs']:,}",
                "N units":         f"{att_res['n_units']:,}",
                "N time periods":  f"{att_res['n_time']:,}",
                "N treated units": f"{att_res['n_treated']:,}",
            }.items()])
            rt.to_csv(out / "main_results_pooled.csv", index=False)
            note = ("$^{***}$p$<$0.01, $^{**}$p$<$0.05, $^{*}$p$<$0.10. "
                    "TWFE with unit and time FE. "
                    f"Clustered SE ({spec['cluster']}).")
            with open(out / "main_results_pooled.tex", "w") as f:
                f.write(to_latex(rt, caption=spec["label"],
                                 label=f"tab:{spec_name}_pooled", note=note))
            print(f"    Saved main_results_pooled.csv/tex")

            summary_rows.append({
                "sample": sample_name, "spec": spec_name,
                **{k: att_res[k] for k in ["att","se","ci_low","ci_high","n_obs"]},
                "sig": sig,
            })
        else:
            print(f"    SKIP — insufficient variation for pooled ATT")

        # ── pooled event study ────────────────────────────────────────────────
        print(f"\n  [Pooled event study]")
        es = run_event_study(df, spec, dummy_cols)
        if es is not None:
            es.to_csv(out / "event_study_coefs.csv", index=False)
            plot_event_study(es, spec["label"], out / "event_study")
            print(f"    Saved event_study.png/pdf")
        else:
            print(f"    SKIP — event study failed")

        # ── per-crop ATT ──────────────────────────────────────────────────────
        print(f"\n  [Per-crop]")
        crop_atts = {}
        for crop in crops:
            sub = df[df["crop"] == crop].copy()
            sub, dc = add_event_dummies(sub)
            res = run_att(sub, spec)
            crop_atts[crop] = res
            if res:
                sig = stars(res["pval"])
                print(f"    {crop:<12} ATT={res['att']:+.4f}{sig}  "
                      f"SE={res['se']:.4f}  N={res['n_obs']:,}")
            else:
                print(f"    {crop:<12} SKIP")

        # Crop heterogeneity plot
        if any(v is not None for v in crop_atts.values()):
            plot_crop_het(crop_atts,
                          title=f"{spec['label']} — Crop heterogeneity",
                          stem=out / "crop_heterogeneity")
            print(f"    Saved crop_heterogeneity.png/pdf")

        # Crop heterogeneity table
        het_rows = []
        for crop, res in crop_atts.items():
            if res is None:
                continue
            het_rows.append({
                "Crop":    crop.capitalize(),
                "ATT":     f"{res['att']:+.4f}{stars(res['pval'])}",
                "SE":      f"({res['se']:.4f})",
                "CI low":  f"{res['ci_low']:.4f}",
                "CI high": f"{res['ci_high']:.4f}",
                "N obs":   f"{res['n_obs']:,}",
            })
        if het_rows:
            ht = (pd.DataFrame(het_rows)
                  .sort_values("ATT").reset_index(drop=True))
            ht.to_csv(out / "crop_heterogeneity_table.csv", index=False)
            with open(out / "crop_heterogeneity_table.tex", "w") as f:
                f.write(to_latex(ht,
                    caption=f"Crop heterogeneity — {spec['label']}",
                    label=f"tab:{spec_name}_crops",
                    note=("$^{***}$p$<$0.01, $^{**}$p$<$0.05, $^{*}$p$<$0.10. "
                          "TWFE with unit and time FE.")))
            print(f"    Saved crop_heterogeneity_table.csv/tex")


# ── summary comparison table ──────────────────────────────────────────────────

print(f"\n{'='*70}")
print("  SUMMARY COMPARISON")
print(f"{'='*70}")

if summary_rows:
    sum_df = pd.DataFrame(summary_rows)
    sum_df["col"] = sum_df["sample"] + "\n" + sum_df["spec"]

    # Build readable wide table
    metrics = ["att", "se", "ci_low", "ci_high", "n_obs"]
    metric_labels = ["ATT", "SE", "95% CI lower", "95% CI upper", "N obs"]
    col_order = [f"{s}/{p}" for s in SAMPLES for p in SPECS]

    wide_rows = []
    for met, lbl in zip(metrics, metric_labels):
        row = {"Metric": lbl}
        for r in summary_rows:
            key = f"{r['sample']}/{r['spec']}"
            val = r[met]
            if met == "att":
                val = f"{val:+.4f}{r['sig']}"
            elif met in ["se", "ci_low", "ci_high"]:
                val = f"{val:.4f}"
            else:
                val = f"{val:,}"
            row[key] = val
        wide_rows.append(row)

    wide = pd.DataFrame(wide_rows)
    wide.to_csv(RES / "summary_comparison.csv", index=False)
    with open(RES / "summary_comparison.tex", "w") as f:
        f.write(to_latex(wide,
            caption="Summary of pooled ATT across all sample and specification combinations",
            label="tab:summary_comparison",
            note=("$^{***}$p$<$0.01, $^{**}$p$<$0.05, $^{*}$p$<$0.10. "
                  "TWFE estimator with unit and time FE. "
                  "Outcome: log(price range) for Spec 1, log(price SD) for Specs 2--3.")))
    print("\n" + wide.to_string(index=False))
    print(f"\nSaved results/summary_comparison.csv/tex")

print("\nDone.")
