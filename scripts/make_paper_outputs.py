"""
make_paper_outputs.py
---------------------
Produces all publication-ready tables and figures.

Tables (output/paper_outputs/tables/)
    Table1_within_mandi.{tex,csv}         Pooled ATT, Spec 1 (within-mandi)
    Table2_across_mandi.{tex,csv}         Pooled ATT, Specs 2–3 (across-mandi)
    Table3_crop_heterogeneity.{tex,csv}   Per-crop ATT, Spec 1 Sample A

Figures (output/paper_outputs/figures/)
    Figure1_event_study_within.{pdf,png}  Event study, Spec 1 (Sample A)
    Figure2_event_study_across.{pdf,png}  Event studies, Spec 2 + 3 side-by-side
    Figure3_mechanism.{pdf,png}           log_max / log_min / log_range decomp
    Figure4_crop_heterogeneity.{pdf,png}  Per-crop ATT chart, Spec 1 Sample A
"""

import warnings; warnings.filterwarnings("ignore")
import re, numpy as np, pandas as pd
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.patches as mpatches
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RES  = ROOT / "results"
PC   = ROOT / "output" / "price_components"

TABS = ROOT / "output" / "paper_outputs" / "tables"
FIGS = ROOT / "output" / "paper_outputs" / "figures"
TABS.mkdir(parents=True, exist_ok=True)
FIGS.mkdir(parents=True, exist_ok=True)

# ── style ──────────────────────────────────────────────────────────────────────
NAVY   = "#1f3a5f"
TEAL   = "#1a6b3c"
RED    = "#c0392b"
BLUE   = "#1f6fb5"
GRAY   = "#444444"
LGRAY  = "#bbbbbb"
CBAND  = "#d0dae8"   # CI band fill

plt.rcParams.update({
    "font.family":       "serif",
    "axes.spines.top":   False,
    "axes.spines.right": False,
    "axes.grid":         False,
    "figure.dpi":        180,
    "font.size":         11,
    "axes.labelsize":    11,
    "xtick.labelsize":   10,
    "ytick.labelsize":   10,
    "legend.fontsize":   10,
})

CROP_COLOR = {
    "onion":   RED, "tomato":  RED, "potato":  RED,   # perishable
    "wheat":   TEAL, "paddy":  TEAL, "rice":   TEAL,  # storable
    "maize":   TEAL, "mustard":TEAL, "soybean":TEAL,
    "cotton":  TEAL, "chana":  TEAL,
}

PERISHABLE = {"onion", "tomato", "potato"}


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def load_pooled(sample: str, spec: str) -> dict:
    """Parse a main_results_pooled.csv into a dict of clean values."""
    path = RES / sample / spec / "main_results_pooled.csv"
    if not path.exists():
        return {}
    df = pd.read_csv(path).set_index("Metric")["Value"]
    raw_att = str(df.get("ATT", ""))
    # extract numeric part and stars
    m = re.match(r"([+-]?\d+\.\d+)(\*+)?", raw_att)
    att_num = float(m.group(1)) if m else np.nan
    stars   = m.group(2) if m and m.group(2) else ""
    se_raw  = str(df.get("SE", ""))
    se_m    = re.search(r"(\d+\.\d+)", se_raw)
    se_num  = float(se_m.group(1)) if se_m else np.nan
    def clean_int(s):
        return str(s).replace(",", "").replace('"', "")
    return {
        "att":        att_num,
        "stars":      stars,
        "se":         se_num,
        "ci":         str(df.get("95% CI", "")),
        "n_obs":      clean_int(df.get("N observations", "")),
        "n_units":    clean_int(df.get("N units", "")),
        "n_treated":  clean_int(df.get("N treated units", "")),
    }


def load_es(sample: str, spec: str) -> pd.DataFrame | None:
    path = RES / sample / spec / "event_study_coefs.csv"
    if not path.exists():
        return None
    df = pd.read_csv(path)
    df["rel_year"] = df["rel_year"].astype(int)
    return df.sort_values("rel_year").reset_index(drop=True)


def stars_str(p_or_stars):
    """Return stars from a p-value or pass through existing stars string."""
    if isinstance(p_or_stars, str):
        return p_or_stars
    p = float(p_or_stars)
    if np.isnan(p): return ""
    return "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""


def fmt_num(x, dp=4):
    if np.isnan(x): return "---"
    return f"{x:+.{dp}f}"


def save_fig(fig, stem: Path):
    for ext in (".pdf", ".png"):
        fig.savefig(str(stem) + ext, dpi=300, bbox_inches="tight",
                    facecolor="white")
    plt.close(fig)
    print(f"  Saved {stem.name}.pdf/.png")


# ══════════════════════════════════════════════════════════════════════════════
# TABLE 1 — Within-mandi pooled ATT
# ══════════════════════════════════════════════════════════════════════════════

def make_table1():
    # (sample, spec, col_header)
    specs = [
        ("sample_A_full",     "spec1_within_mandi", "Full sample"),
        ("sample_B_balanced", "spec1_within_mandi", "Balanced crops"),
    ]
    dicts = [load_pooled(sample, spec) for sample, spec, _ in specs]

    def att_cell(d):
        sup = f"^{{{d['stars']}}}" if d["stars"] else ""
        return f"${d['att']:+.4f}{sup}$"

    def tex_row(label, vals):
        return label + " & " + " & ".join(vals) + " \\\\"

    # CSV
    csv_rows = [
        {"": hdr,
         "Post-adoption (ATT)": f"{d['att']:+.4f}{d['stars']}",
         "SE":                   f"({d['se']:.4f})",
         "N observations":       f"{int(d['n_obs']):,}",
         "N units":              f"{int(d['n_units']):,}",
         "N treated units":      f"{int(d['n_treated']):,}"}
        for (_, __, hdr), d in zip(specs, dicts)
    ]
    pd.DataFrame(csv_rows).to_csv(TABS / "Table1_within_mandi.csv", index=False)

    # LaTeX
    nc = len(specs)
    lines = [
        "\\begin{table}[htbp]", "\\centering",
        "\\caption{Effect of eNAM on Within-Mandi Price Dispersion}",
        "\\label{tab:within_mandi_main}",
        f"\\begin{{tabular}}{{l{'c'*nc}}}",
        "\\hline\\hline",
        " & " + " & ".join(f"({i+1})" for i in range(nc)) + " \\\\",
        " & " + " & ".join(hdr for _, __, hdr in specs) + " \\\\",
        "\\hline",
        tex_row("Post-adoption",     [att_cell(d) for d in dicts]),
        tex_row("",                  [f"$({d['se']:.4f})$" for d in dicts]),
        "\\hline",
        tex_row("Observations",      [f"${int(d['n_obs']):,}$" for d in dicts]),
        tex_row("Units (mandi$\\times$crop)", [f"${int(d['n_units']):,}$" for d in dicts]),
        tex_row("Treated units",     [f"${int(d['n_treated']):,}$" for d in dicts]),
        tex_row("Unit FE",           ["Yes"] * nc),
        tex_row("Time FE",           ["Yes"] * nc),
        tex_row("Clustered SE",      ["State"] * nc),
        "\\hline\\hline",
        (f"\\multicolumn{{{nc+1}}}{{l}}{{\\footnotesize \\textit{{Notes:}} "
         "Outcome is log(max price $-$ min price) within a mandi-crop-month cell. "
         "Post-adoption $= 1$ at or after the mandi's first eNAM trade date. "
         "Balanced crops drops rice, mustard, soybean, chana ($<$50 mandis in range data). "
         "Standard errors clustered at state level. "
         "$^{{***}}$p$<$0.01, $^{{**}}$p$<$0.05, $^{{*}}$p$<$0.10.}} \\\\"),
        "\\end{tabular}", "\\end{table}",
    ]
    (TABS / "Table1_within_mandi.tex").write_text("\n".join(lines))
    print("  Saved Table1_within_mandi.tex/.csv")


# ══════════════════════════════════════════════════════════════════════════════
# TABLE 2 — Across-mandi pooled ATT (Specs 2 & 3)
# ══════════════════════════════════════════════════════════════════════════════

def make_table2():
    # (sample, spec, col_header)
    specs = [
        ("sample_A_full",     "spec2_across_district", "District — Full"),
        ("sample_B_balanced", "spec2_across_district", "District — Balanced"),
        ("sample_A_full",     "spec3_across_state",    "State — Full"),
        ("sample_B_balanced", "spec3_across_state",    "State — Balanced"),
    ]
    dicts    = [load_pooled(sample, spec) for sample, spec, _ in specs]
    se_notes = ["CRV1 (State)", "CRV1 (State)", "HC1", "HC1"]
    nc       = len(specs)

    def att_cell(d):
        sup = f"^{{{d['stars']}}}" if d["stars"] else ""
        return f"${d['att']:+.4f}{sup}$"

    # CSV
    csv_rows = [
        {"": hdr,
         "Post-adoption (ATT)": f"{d['att']:+.4f}{d['stars']}",
         "SE":                   f"({d['se']:.4f})",
         "N observations":       f"{int(d['n_obs']):,}",
         "N units":              f"{int(d['n_units']):,}",
         "SE type":              se}
        for (_, __, hdr), d, se in zip(specs, dicts, se_notes)
    ]
    pd.DataFrame(csv_rows).to_csv(TABS / "Table2_across_mandi.csv", index=False)

    # LaTeX
    lines = [
        "\\begin{table}[htbp]", "\\centering",
        "\\caption{Effect of eNAM on Across-Mandi Price Dispersion}",
        "\\label{tab:across_mandi_main}",
        f"\\begin{{tabular}}{{l{'c'*nc}}}",
        "\\hline\\hline",
        " & (1) & (2) & (3) & (4) \\\\",
        " & \\multicolumn{2}{c}{District-level} & \\multicolumn{2}{c}{State-level} \\\\",
        "\\cmidrule(lr){2-3}\\cmidrule(lr){4-5}",
        " & Full & Balanced & Full & Balanced \\\\",
        "\\hline",
        "Post-adoption & " + " & ".join(att_cell(d) for d in dicts) + " \\\\",
        " & "             + " & ".join(f"$({d['se']:.4f})$" for d in dicts) + " \\\\",
        "\\hline",
        "Observations & "       + " & ".join(f"${int(d['n_obs']):,}$" for d in dicts) + " \\\\",
        "Units & "              + " & ".join(f"${int(d['n_units']):,}$" for d in dicts) + " \\\\",
        "Unit FE & "            + " & ".join(["Yes"] * nc) + " \\\\",
        "Time FE & "            + " & ".join(["Yes"] * nc) + " \\\\",
        "SE & "                 + " & ".join(se_notes) + " \\\\",
        "Never-treated units & "+ " & ".join(["None"] * nc) + " \\\\",
        "\\hline\\hline",
        (f"\\multicolumn{{{nc+1}}}{{l}}{{\\footnotesize \\textit{{Notes:}} "
         "Outcome is log(SD of modal prices across mandis) within a geography-crop-month cell. "
         "District sample requires $\\geq 4$ mandis per cell. "
         "All districts and states eventually adopt eNAM; "
         "later adopters serve as not-yet-treated controls. "
         "HC1 SEs used for state-level regressions ($n=23$ clusters). "
         "$^{{***}}$p$<$0.01, $^{{**}}$p$<$0.05, $^{{*}}$p$<$0.10.}} \\\\"),
        "\\end{tabular}", "\\end{table}",
    ]
    (TABS / "Table2_across_mandi.tex").write_text("\n".join(lines))
    print("  Saved Table2_across_mandi.tex/.csv")


# ══════════════════════════════════════════════════════════════════════════════
# TABLE 3 — Crop heterogeneity
# ══════════════════════════════════════════════════════════════════════════════

def make_table3():
    path = RES / "sample_A_full" / "spec1_within_mandi" / "crop_heterogeneity_table.csv"
    df = pd.read_csv(path)

    # ATT column already has stars embedded; sort numerically
    def parse_att(s):
        m = re.match(r"([+-]?\d+\.\d+)", str(s))
        return float(m.group(1)) if m else np.nan
    df["_sort"] = df["ATT"].apply(parse_att)
    df = df.sort_values("_sort").drop(columns="_sort").reset_index(drop=True)

    # Add crop type column
    df.insert(1, "Type",
              df["Crop"].str.lower().map(
                  lambda c: "Perishable" if c in PERISHABLE else "Storable"))

    # Save CSV
    df.to_csv(TABS / "Table3_crop_heterogeneity.csv", index=False)

    # LaTeX
    ncols = len(df.columns)
    col_fmt = "ll" + "r" * (ncols - 2)
    lines = [
        "\\begin{table}[htbp]",
        "\\centering",
        "\\caption{Crop-Level Treatment Effects: Within-Mandi Price Dispersion (Full Sample)}",
        "\\label{tab:crop_het}",
        f"\\begin{{tabular}}{{{col_fmt}}}",
        "\\hline\\hline",
        " & ".join(df.columns) + " \\\\",
        "\\hline",
    ]
    for _, row in df.iterrows():
        lines.append(" & ".join(str(v) for v in row.values) + " \\\\")
    lines += [
        "\\hline\\hline",
        (f"\\multicolumn{{{ncols}}}{{l}}"
         "{\\footnotesize \\textit{Notes:} TWFE estimates for each crop separately. "
         "SE clustered at state level. "
         "$^{***}$p$<$0.01, $^{**}$p$<$0.05, $^{*}$p$<$0.10.} \\\\"),
        "\\end{tabular}",
        "\\end{table}",
    ]
    (TABS / "Table3_crop_heterogeneity.tex").write_text("\n".join(lines))
    print("  Saved Table3_crop_heterogeneity.tex/.csv")


# ══════════════════════════════════════════════════════════════════════════════
# FIGURE HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _draw_es(ax, es: pd.DataFrame, color=NAVY, label_adopt=True, ylim=None):
    pre  = es[es["rel_year"] <  0]
    post = es[es["rel_year"] >= 0]

    ax.fill_between(es["rel_year"], es["2.5%"], es["97.5%"],
                    color=CBAND, alpha=0.55, zorder=1)
    ax.plot(pre["rel_year"],  pre["Estimate"],
            color=GRAY, lw=1.8, ls="--", marker="o",
            ms=4.5, mfc="white", mew=1.4, mec=GRAY, zorder=3)
    ax.plot(post["rel_year"], post["Estimate"],
            color=color, lw=2.0, ls="-",  marker="o",
            ms=4.5, mfc="white", mew=1.4, mec=color, zorder=3)

    ax.axhline(0, color="black", lw=0.75, zorder=2)
    ax.axvline(-0.5, color=GRAY, lw=0.9, ls=":", alpha=0.6, zorder=2)

    if label_adopt:
        yl = ylim if ylim else ax.get_ylim()
        ax.text(-0.35, yl[0] + 0.92*(yl[1]-yl[0]),
                "Adoption", fontsize=8.5, color=GRAY,
                ha="left", va="top")

    ax.xaxis.set_major_locator(mticker.MultipleLocator(2))
    ax.tick_params(labelsize=9.5)


# ══════════════════════════════════════════════════════════════════════════════
# FIGURE 1 — Event study, Spec 1 (within-mandi, Sample A)
# ══════════════════════════════════════════════════════════════════════════════

def make_figure1():
    es = load_es("sample_A_full", "spec1_within_mandi")
    if es is None: return

    fig, ax = plt.subplots(figsize=(6.5, 4.0), facecolor="white")
    ax.set_facecolor("white")

    _draw_es(ax, es, color=NAVY)

    # Custom legend
    from matplotlib.lines import Line2D
    handles = [
        Line2D([0], [0], color=GRAY,  lw=1.8, ls="--",
               marker="o", ms=4, mfc="white", mew=1.3, mec=GRAY,
               label="Pre-adoption"),
        Line2D([0], [0], color=NAVY, lw=2.0, ls="-",
               marker="o", ms=4, mfc="white", mew=1.3, mec=NAVY,
               label="Post-adoption"),
        mpatches.Patch(facecolor=CBAND, alpha=0.6, label="95% CI"),
    ]
    ax.legend(handles=handles, fontsize=9.5, framealpha=0, loc="upper left")

    ax.set_xlabel("Years relative to eNAM adoption", fontsize=11)
    ax.set_ylabel("Treatment effect (log points)", fontsize=11)
    fig.tight_layout()
    save_fig(fig, FIGS / "Figure1_event_study_within")


# ══════════════════════════════════════════════════════════════════════════════
# FIGURE 2 — Event studies, Specs 2 & 3 side-by-side
# ══════════════════════════════════════════════════════════════════════════════

def make_figure2():
    es2 = load_es("sample_A_full", "spec2_across_district")
    es3 = load_es("sample_A_full", "spec3_across_state")

    fig, axes = plt.subplots(1, 2, figsize=(11, 4.2), facecolor="white",
                             sharey=False)
    fig.subplots_adjust(wspace=0.28)

    for ax, es, color, panel_label, subtitle in [
        (axes[0], es2, TEAL, "(a)", "District-level\n(≥4 mandis per district-crop-month)"),
        (axes[1], es3, "#7b3f00", "(b)", "State-level"),
    ]:
        if es is None:
            ax.text(0.5, 0.5, "No data", ha="center", va="center",
                    transform=ax.transAxes, fontsize=11, color=GRAY)
            continue
        ax.set_facecolor("white")
        _draw_es(ax, es, color=color, label_adopt=True)
        ax.set_xlabel("Years relative to eNAM adoption", fontsize=11)
        ax.set_ylabel("Treatment effect (log points)", fontsize=11)

        # Panel label
        ax.text(-0.06, 1.04, panel_label, transform=ax.transAxes,
                fontsize=11, fontweight="bold")
        ax.set_title(subtitle, fontsize=10, pad=6, color=GRAY)

    fig.tight_layout()
    save_fig(fig, FIGS / "Figure2_event_study_across")


# ══════════════════════════════════════════════════════════════════════════════
# FIGURE 3 — Mechanism: log_max / log_min / log_range decomposition
# ══════════════════════════════════════════════════════════════════════════════

def make_figure3():
    """Rebuild the mechanism decomposition from the saved coef files."""
    pc_max   = PC / "es_log_max_coefs.csv"
    pc_min   = PC / "es_log_min_coefs.csv"
    es_range = load_es("sample_A_full", "spec1_within_mandi")

    if not pc_max.exists() or not pc_min.exists():
        print("  Mechanism coefs not found — skipping Figure 3")
        return

    es_max = pd.read_csv(pc_max).sort_values("rel_year")
    es_min = pd.read_csv(pc_min).sort_values("rel_year")

    fig, ax = plt.subplots(figsize=(7.5, 4.5), facecolor="white")
    ax.set_facecolor("white")

    series = [
        (es_max,   BLUE,  "Log(max price) — ceiling",  "-",  2.0),
        (es_min,   RED,   "Log(min price) — floor",    "-",  2.0),
        (es_range, TEAL,  "Log(price range) — spread", "--", 1.8),
    ]
    from matplotlib.lines import Line2D

    for es, col, lbl, ls, lw in series:
        ax.fill_between(es["rel_year"], es["2.5%"], es["97.5%"],
                        alpha=0.08, color=col)
        ax.plot(es["rel_year"], es["Estimate"],
                color=col, lw=lw, ls=ls, marker="o",
                ms=4.5, mfc="white", mew=1.4, mec=col,
                label=lbl, zorder=3)

    ax.axhline(0, color="black", lw=0.75)
    ax.axvline(-0.5, color=GRAY, lw=0.9, ls=":", alpha=0.6)
    yl = ax.get_ylim()
    ax.text(-0.35, yl[0] + 0.92*(yl[1]-yl[0]),
            "Adoption", fontsize=8.5, color=GRAY, ha="left", va="top")

    ax.set_xlabel("Years relative to eNAM adoption", fontsize=11)
    ax.set_ylabel("Treatment effect (log points)", fontsize=11)
    ax.xaxis.set_major_locator(mticker.MultipleLocator(2))
    ax.legend(fontsize=9.5, framealpha=0, loc="upper left")
    ax.tick_params(labelsize=9.5)
    fig.tight_layout()
    save_fig(fig, FIGS / "Figure3_mechanism")


# ══════════════════════════════════════════════════════════════════════════════
# FIGURE 4 — Crop heterogeneity (Spec 1, Sample A)
# ══════════════════════════════════════════════════════════════════════════════

def make_figure4():
    path = RES / "sample_A_full" / "spec1_within_mandi" / "crop_heterogeneity_table.csv"
    df = pd.read_csv(path)

    def parse_att(s):
        m = re.match(r"([+-]?\d+\.\d+)", str(s))
        return float(m.group(1)) if m else np.nan
    def parse_se(s):
        m = re.search(r"(\d+\.\d+)", str(s))
        return float(m.group(1)) if m else np.nan

    df["att_num"] = df["ATT"].apply(parse_att)
    df["se_num"]  = df["SE"].apply(parse_se)
    df["ci_lo"]   = df["att_num"] - 1.96 * df["se_num"]
    df["ci_hi"]   = df["att_num"] + 1.96 * df["se_num"]
    df = df.sort_values("att_num").reset_index(drop=True)

    n = len(df)
    fig, ax = plt.subplots(figsize=(6.5, max(3.5, 0.55*n + 1.5)),
                           facecolor="white")
    ax.set_facecolor("white")

    y = np.arange(n)
    colors = [CROP_COLOR.get(c.lower(), TEAL) for c in df["Crop"]]
    err_lo = df["att_num"] - df["ci_lo"]
    err_hi = df["ci_hi"]  - df["att_num"]

    bars = ax.barh(y, df["att_num"],
                   color=colors, alpha=0.80, height=0.55,
                   xerr=[err_lo.tolist(), err_hi.tolist()],
                   error_kw={"elinewidth": 1.4, "ecolor": GRAY,
                              "capsize": 3.5, "capthick": 1.3})
    ax.axvline(0, color="black", lw=0.9, ls="--", alpha=0.6)

    ax.set_yticks(y)
    ax.set_yticklabels([c.capitalize() for c in df["Crop"]], fontsize=10.5)
    ax.set_xlabel("Treatment effect (log points)", fontsize=11)
    ax.tick_params(labelsize=9.5)

    legend_handles = [
        mpatches.Patch(facecolor=RED,  alpha=0.80, label="Perishable"),
        mpatches.Patch(facecolor=TEAL, alpha=0.80, label="Storable"),
    ]
    ax.legend(handles=legend_handles, fontsize=9.5, framealpha=0,
              loc="lower right")
    fig.tight_layout()
    save_fig(fig, FIGS / "Figure4_crop_heterogeneity")


# ══════════════════════════════════════════════════════════════════════════════
# RUN ALL
# ══════════════════════════════════════════════════════════════════════════════

print("─" * 60)
print("Tables")
print("─" * 60)
make_table1()
make_table2()
make_table3()

print()
print("─" * 60)
print("Figures")
print("─" * 60)
make_figure1()
make_figure2()
make_figure3()
make_figure4()

print()
print(f"All outputs → {(ROOT / 'output' / 'paper_outputs').relative_to(ROOT)}/")
print("Done.")
