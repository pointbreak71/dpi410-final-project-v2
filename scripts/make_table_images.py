"""
make_table_images.py
---------------------
Renders all paper tables as standalone PNG images with interpretation notes.
Output: output/paper_outputs/table_images/
"""

import warnings; warnings.filterwarnings("ignore")
import numpy as np, pandas as pd
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DAT  = ROOT / "data" / "clean" / "did_inputs"
RES  = ROOT / "results"
OUT  = ROOT / "output" / "paper_outputs" / "table_images"
OUT.mkdir(parents=True, exist_ok=True)

# ── colour palette ─────────────────────────────────────────────────────────────
HDR_BG   = "#1f3a5f"
HDR_FG   = "white"
ROW_ALT  = "#f0f4f8"
ROW_MAIN = "white"
RULE_COL = "#c8d4e0"
NOTE_COL = "#444444"

plt.rcParams.update({"font.family": "serif"})


# ══════════════════════════════════════════════════════════════════════════════
# Core rendering helper
# ══════════════════════════════════════════════════════════════════════════════

def render_table(
    df: pd.DataFrame,
    filename: str,
    title: str,
    note: str,
    col_widths: list[float] | None = None,
    num_cols: set[int] | None = None,   # 0-indexed columns to right-align
    bold_rows: set[int] | None = None,  # 0-indexed data rows to bold
    merge_header_rows: int = 1,         # rows at top of df treated as sub-headers
    figw: float = 10,
):
    """Render a DataFrame as a publication-quality table image."""
    nrows, ncols = df.shape
    num_cols  = num_cols  or set()
    bold_rows = bold_rows or set()

    ROW_H    = 0.38   # inches per data row
    HDR_H    = 0.44   # header row height
    TITLE_H  = 0.65
    NOTE_H   = 0.20 * (1 + note.count("\n"))
    PAD      = 0.18
    fig_h = TITLE_H + HDR_H + nrows * ROW_H + NOTE_H + 2 * PAD

    fig, ax = plt.subplots(figsize=(figw, fig_h))
    ax.set_axis_off()
    fig.patch.set_facecolor("white")

    if col_widths is None:
        col_widths = [1.0 / ncols] * ncols
    total = sum(col_widths)
    col_widths = [w / total for w in col_widths]

    # coordinate helpers (axes coords 0-1)
    y_top   = 1.0 - PAD / fig_h
    y_title = y_top - TITLE_H / fig_h
    y_hdr   = y_title - HDR_H  / fig_h
    def y_row(i):
        return y_hdr - (i + 1) * ROW_H / fig_h

    def x_left(col):
        return sum(col_widths[:col])
    def x_mid(col):
        return x_left(col) + col_widths[col] / 2

    # ── title ──────────────────────────────────────────────────────────────────
    ax.text(0.5, y_top, title,
            transform=ax.transAxes, fontsize=13, fontweight="bold",
            ha="center", va="top", color="#1a1a1a")

    # ── header row ─────────────────────────────────────────────────────────────
    ax.add_patch(mpatches.FancyBboxPatch(
        (0, y_title), 1.0, HDR_H / fig_h,
        boxstyle="square,pad=0", transform=ax.transAxes,
        facecolor=HDR_BG, edgecolor="none", zorder=2))
    for c, cname in enumerate(df.columns):
        ha = "left" if c == 0 else "right" if c in num_cols else "center"
        xpos = x_left(c) + 0.005 if ha == "left" else \
               x_left(c) + col_widths[c] - 0.005 if ha == "right" else x_mid(c)
        ax.text(xpos, y_title + (HDR_H / fig_h) / 2,
                str(cname), transform=ax.transAxes,
                fontsize=10, fontweight="bold", color=HDR_FG,
                ha=ha, va="center", zorder=3)

    # ── data rows ──────────────────────────────────────────────────────────────
    for r, (_, row) in enumerate(df.iterrows()):
        y0 = y_row(r)
        bg = ROW_ALT if r % 2 == 0 else ROW_MAIN
        ax.add_patch(mpatches.FancyBboxPatch(
            (0, y0), 1.0, ROW_H / fig_h,
            boxstyle="square,pad=0", transform=ax.transAxes,
            facecolor=bg, edgecolor="none", zorder=1))

        for c, val in enumerate(row):
            ha  = "left"  if c == 0 else \
                  "right" if c in num_cols else "center"
            xpos = x_left(c) + 0.007 if ha == "left" else \
                   x_left(c) + col_widths[c] - 0.007 if ha == "right" else \
                   x_mid(c)
            fw = "bold" if r in bold_rows else "normal"
            is_se = str(val).startswith("(") and str(val).endswith(")")
            fs = 9.0 if not is_se else 8.5
            fc = "#555555" if is_se else "#1a1a1a"
            ax.text(xpos, y0 + (ROW_H / fig_h) / 2,
                    str(val), transform=ax.transAxes,
                    fontsize=fs, fontweight=fw, color=fc,
                    ha=ha, va="center", zorder=3)

    # bottom rule
    y_bot = y_row(nrows - 1)
    for y_line in [y_title, y_hdr, y_bot]:
        ax.plot([0, 1], [y_line, y_line], color=RULE_COL, lw=0.8,
                transform=ax.transAxes, zorder=4)

    # ── notes ──────────────────────────────────────────────────────────────────
    ax.text(0.0, y_bot - 0.02,
            note, transform=ax.transAxes,
            fontsize=8.5, color=NOTE_COL, ha="left", va="top",
            style="italic", wrap=True,
            multialignment="left")

    fig.tight_layout(pad=0)
    path = OUT / filename
    fig.savefig(path, dpi=200, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"  Saved {filename}")


# ══════════════════════════════════════════════════════════════════════════════
# TABLE 0 — Summary statistics
# ══════════════════════════════════════════════════════════════════════════════

def make_summary_stats():
    rng = pd.read_csv(DAT / "within_mandi_range.csv")
    dis = pd.read_csv(DAT / "across_mandi_district.csv")
    cov = pd.read_csv(ROOT / "output" / "diagnostics" / "crop_coverage.csv")

    # Mean modal price: aggregate from district-level price_mean
    modal = (dis.groupby("crop")["price_mean"]
               .mean().reset_index()
               .rename(columns={"price_mean": "modal_mean"}))

    # Max / min / range stats from within_mandi_range (all obs, no mandi_id filter)
    price_stats = (rng[rng["range_avg"] > 0]
                   .groupby("crop")
                   .agg(
                       max_mean  =("max_price_avg",  "mean"),
                       min_mean  =("min_price_avg",  "mean"),
                       range_mean=("range_avg",       "mean"),
                       n_mandis  =("market_name",     "nunique"),
                       n_obs     =("range_avg",       "size"),
                   )
                   .reset_index())

    # eNAM treatment coverage
    treat = cov[["crop", "ever_treated", "pct_treated"]].copy()

    df = (price_stats
          .merge(modal,  on="crop", how="left")
          .merge(treat,  on="crop", how="left"))

    # Crop order: storables then perishables
    order = ["wheat","paddy","rice","maize","mustard","soybean","cotton",
             "chana","onion","tomato","potato"]
    df["_o"] = df["crop"].map({c: i for i, c in enumerate(order)})
    df = df.sort_values("_o").drop(columns="_o").reset_index(drop=True)

    PERISHABLE = {"onion","tomato","potato"}

    out = pd.DataFrame({
        "Crop":              df["crop"].str.capitalize(),
        "Type":              df["crop"].map(
                                 lambda c: "Perishable" if c in PERISHABLE
                                           else "Storable"),
        "Mean modal\n(Rs/qtl)":  df["modal_mean"].map(
                                     lambda x: f"{x:,.0f}" if pd.notna(x) else "—"),
        "Mean max\n(Rs/qtl)":    df["max_mean"].map(lambda x: f"{x:,.0f}"),
        "Mean min\n(Rs/qtl)":    df["min_mean"].map(lambda x: f"{x:,.0f}"),
        "Mean range\n(Rs/qtl)":  df["range_mean"].map(lambda x: f"{x:,.0f}"),
        "Mandis\nin data":       df["n_mandis"].map(lambda x: f"{x:,}"),
        "eNAM\ntreated (%)":     df["pct_treated"].map(
                                     lambda x: f"{x:.0f}%" if pd.notna(x) else "—"),
    })

    render_table(
        out,
        filename="Table0_summary_stats.png",
        title="Summary Statistics by Crop",
        col_widths=[1.4, 1.0, 1.2, 1.2, 1.2, 1.2, 1.0, 1.1],
        num_cols={2, 3, 4, 5, 6, 7},
        note=(
            "Notes: Prices are in Indian rupees per quintal (100 kg), averaged to the mandi–crop–month level. "
            "Modal price is the prevailing auction price at which the largest volume traded; "
            "max and min are the highest and lowest prices recorded in that month at the same mandi. "
            "Price range (max − min) is the outcome variable for the within-mandi analysis (Specification 1). "
            "Mandis in data counts distinct market names in the Agmarknet price dataset. "
            "eNAM treated (%) is the share of matched mandis that joined the eNAM platform."
        ),
        figw=11,
    )


# ══════════════════════════════════════════════════════════════════════════════
# TABLE 1 — Within-mandi pooled ATT
# ══════════════════════════════════════════════════════════════════════════════

def make_table1():
    rows = [
        ("Post-adoption (ATT)",   "+0.157***",  "+0.133***"),
        ("",                      "(0.016)",    "(0.016)"),
        ("─" * 22,                "─" * 10,     "─" * 10),
        ("Observations",          "58,033",     "54,517"),
        ("Units (mandi × crop)",  "1,073",      "955"),
        ("Treated units",         "1,004",      "893"),
        ("Unit fixed effects",    "Yes",        "Yes"),
        ("Time fixed effects",    "Yes",        "Yes"),
        ("Clustered SE",          "State",      "State"),
    ]
    df = pd.DataFrame(rows, columns=["", "(1) Full sample", "(2) Balanced crops"])

    render_table(
        df,
        filename="Table1_within_mandi.png",
        title="Effect of eNAM Adoption on Within-Mandi Price Dispersion",
        col_widths=[2.2, 1.3, 1.3],
        num_cols={1, 2},
        bold_rows={0},
        note=(
            "Notes: The outcome is log(max price − min price) within a mandi–crop–month cell, "
            "where max and min are the monthly average highest and lowest prices at the same market. "
            "Post-adoption equals 1 for eNAM-registered mandis from their first recorded portal trade date onwards. "
            "A positive coefficient means adoption is associated with a wider within-market price spread. "
            "Column (2) drops crops with fewer than 50 mandis in the range dataset (rice, mustard, soybean, chana). "
            "Standard errors are clustered at the state level. *** p<0.01."
        ),
        figw=7.5,
    )


# ══════════════════════════════════════════════════════════════════════════════
# TABLE 2 — Across-mandi pooled ATT
# ══════════════════════════════════════════════════════════════════════════════

def make_table2():
    rows = [
        ("Post-adoption (ATT)",    "+0.018",   "−0.039",   "−0.098",   "−0.075"),
        ("",                       "(0.042)",  "(0.056)",  "(0.188)",  "(0.167)"),
        ("─" * 22, "─"*8, "─"*8, "─"*8, "─"*8),
        ("Observations",           "21,138",   "14,763",   "22,278",   "16,848"),
        ("Units (geo × crop)",     "321",      "235",      "178",      "122"),
        ("Unit fixed effects",     "Yes",      "Yes",      "Yes",      "Yes"),
        ("Time fixed effects",     "Yes",      "Yes",      "Yes",      "Yes"),
        ("Standard errors",        "CRV1",     "CRV1",     "HC1",      "HC1"),
        ("Never-treated units",    "None",     "None",     "None",     "None"),
    ]
    df = pd.DataFrame(rows, columns=[
        "", "(1) District\nFull", "(2) District\nBalanced",
        "(3) State\nFull", "(4) State\nBalanced"])

    render_table(
        df,
        filename="Table2_across_mandi.png",
        title="Effect of eNAM Adoption on Across-Mandi Price Dispersion",
        col_widths=[2.0, 1.15, 1.15, 1.15, 1.15],
        num_cols={1, 2, 3, 4},
        bold_rows={0},
        note=(
            "Notes: The outcome is log(standard deviation of modal prices across mandis) "
            "within a geography–crop–month cell — a measure of how much prices vary between markets. "
            "A negative coefficient would indicate that eNAM reduced cross-market price divergence (convergence). "
            "None of these estimates are statistically significant. "
            "District columns require ≥4 mandis per cell. All districts and states eventually adopt eNAM, "
            "so identification relies on variation in timing; later adopters serve as not-yet-treated controls. "
            "HC1 heteroskedasticity-robust SEs are used for state-level columns (only 23 state clusters)."
        ),
        figw=10,
    )


# ══════════════════════════════════════════════════════════════════════════════
# TABLE 3 — Crop heterogeneity
# ══════════════════════════════════════════════════════════════════════════════

def make_table3():
    src = pd.read_csv(
        ROOT / "output" / "paper_outputs" / "tables" / "Table3_crop_heterogeneity.csv")

    # Keep clean columns
    out = src[["Crop", "Type", "ATT", "SE", "N obs"]].copy()
    out.columns = ["Crop", "Type", "ATT", "Std. error", "N obs"]

    render_table(
        out,
        filename="Table3_crop_heterogeneity.png",
        title="Crop-Level Treatment Effects on Within-Mandi Price Dispersion",
        col_widths=[1.2, 1.1, 1.0, 1.0, 1.0],
        num_cols={2, 3, 4},
        note=(
            "Notes: Each row is a separate TWFE regression run on that crop's data only. "
            "The ATT is the pooled post-adoption effect on log(max price − min price). "
            "Storable crops (wheat, paddy, rice, maize, mustard, soybean, cotton, chana) tend to show "
            "larger positive effects than perishables (onion, tomato, potato), "
            "possibly because their prices are more responsive to the increased market participation "
            "that eNAM enables. Chana is the only crop with a negative (insignificant) estimate. "
            "Standard errors clustered at the state level. *** p<0.01, * p<0.10."
        ),
        figw=7.5,
    )


# ══════════════════════════════════════════════════════════════════════════════
# RUN ALL
# ══════════════════════════════════════════════════════════════════════════════

print("Generating table images...")
make_summary_stats()
make_table1()
make_table2()
make_table3()
print(f"\nAll saved to {OUT.relative_to(ROOT)}/")
