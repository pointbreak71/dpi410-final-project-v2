"""
Generate interactive HTML reports showing within-state price dispersion over time.
One HTML for IQR, one for SD.
Each chart has a vertical line at the correct eNAM adoption year for that state.
Charts organised by crop with tab navigation and state filter.
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import io, base64
from pathlib import Path

ROOT    = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "output"
OUT_DIR.mkdir(exist_ok=True)

CROPS = ["wheat","paddy","rice","maize","onion","tomato",
         "potato","mustard","soybean","cotton","chana"]

STATE_MAP = {
    "Andaman and Nicobar": "Andaman & Nicobar Islands",
    "Jammu And Kashmir":   "Jammu & Kashmir",
    "Jammu and Kashmir":   "Jammu & Kashmir",
    "Pondicherry":         "Puducherry",
    "Delhi":               None,
    "Manipur":             None,
}

PHASE_COLORS = {1: "#1a6b3c", 2: "#e07b00", 3: "#c0392b"}

enam = (pd.read_csv(ROOT / "data/clean/enam_adoption.csv")
        [["state", "year_joined_enam", "enam_phase"]]
        .drop_duplicates("state"))
adoption_year = dict(zip(enam["state"], enam["year_joined_enam"]))
adoption_phase = dict(zip(enam["state"], enam["enam_phase"]))

# ── Build dispersion series ───────────────────────────────────────────────────
def get_series(crop, state, metric):
    df = pd.read_csv(ROOT / f"data/clean/prices/{crop}_monthly.csv")
    df = df[df["match_type"].isin(["exact","fuzzy"])].copy()
    df["state_name"] = df["state_name"].replace(STATE_MAP)
    df = df[df["state_name"] == state]
    if df.empty:
        return None

    def iqr(x): return x.quantile(0.75) - x.quantile(0.25)
    agg_fn = "std" if metric == "sd" else iqr

    sm = (df.groupby(["year","month"])
            .agg(val=("modal_price_avg", agg_fn), n=("modal_price_avg","count"))
            .reset_index())
    sm = sm[sm["n"] >= 3]
    if sm.empty:
        return None

    ann = (sm.groupby("year")
             .apply(lambda x: np.average(x["val"], weights=x["n"]), include_groups=False)
             .reset_index(name="val"))
    ann = ann[(ann["year"] >= 2010) & (ann["year"] <= 2025)].sort_values("year")
    return ann if len(ann) >= 3 else None

# ── Render one chart → base64 PNG ─────────────────────────────────────────────
def make_chart(crop, state, metric):
    series = get_series(crop, state, metric)
    if series is None:
        return None

    yr  = adoption_year.get(state)
    ph  = adoption_phase.get(state, 1)
    col = PHASE_COLORS.get(ph, "#1a6b3c")
    ylabel = "IQR (Rs/quintal)" if metric == "iqr" else "SD (Rs/quintal)"

    fig, ax = plt.subplots(figsize=(6, 3.2), facecolor="white")
    ax.set_facecolor("white")
    ax.plot(series["year"], series["val"], color=col, linewidth=2.0,
            marker="o", markersize=4, markerfacecolor="white",
            markeredgewidth=1.5, markeredgecolor=col)

    if yr:
        ax.axvline(yr, color="#555555", linewidth=1.2, linestyle="--", alpha=0.85)
        ylim = ax.get_ylim()
        ax.text(yr + 0.2, ylim[1] * 0.96,
                f"eNAM\n({yr})", fontsize=7.5, color="#555555", va="top")

    ax.set_xlim(2009.5, 2025.5)
    ax.set_xlabel("Year", fontsize=9)
    ax.set_ylabel(ylabel, fontsize=9)
    ax.set_title(f"{crop.capitalize()} — {state}", fontsize=10, fontweight="bold")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis="y", linestyle="--", alpha=0.35)
    ax.xaxis.set_major_locator(ticker.MultipleLocator(3))
    ax.tick_params(labelsize=8)
    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=130, bbox_inches="tight", facecolor="white")
    plt.close()
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")

# ── Build chart data ──────────────────────────────────────────────────────────
print("Generating charts...")
charts = {}   # {metric: {crop: [(state, phase, b64), ...]}}

for metric in ["iqr", "sd"]:
    charts[metric] = {}
    for crop in CROPS:
        df = pd.read_csv(ROOT / f"data/clean/prices/{crop}_monthly.csv")
        df["state_name"] = df["state_name"].replace(STATE_MAP)
        df = df[df["match_type"].isin(["exact","fuzzy"]) & df["state_name"].notna()]

        # States with >=3 mandis in >=12 months
        counts = df.groupby(["state_name","year","month"])["market_name"].count().reset_index(name="n")
        valid_states = (counts[counts["n"]>=3]
                        .groupby("state_name")["n"].count()
                        .reset_index(name="months"))
        valid_states = sorted(valid_states[valid_states["months"]>=6]["state_name"].tolist())

        crop_charts = []
        for state in valid_states:
            b64 = make_chart(crop, state, metric)
            if b64:
                ph = adoption_phase.get(state, 0)
                crop_charts.append((state, ph, b64))
                print(f"  {metric} | {crop} | {state}")

        charts[metric][crop] = crop_charts

# ── HTML template ─────────────────────────────────────────────────────────────
PHASE_LABEL = {1:"Phase 1 (2016)", 2:"Phase 2 (2017–2019)", 3:"Phase 3 (2020+)", 0:"Non-eNAM"}
PHASE_BADGE = {1:"#1a6b3c", 2:"#e07b00", 3:"#c0392b", 0:"#888888"}

def build_html(metric):
    label = "IQR" if metric == "iqr" else "Standard Deviation (SD)"
    title = f"Price Dispersion ({label}) by State and Crop — eNAM Research"

    # Build tab content
    tabs_html  = ""
    panes_html = ""
    all_states = sorted({s for crop_charts in charts[metric].values() for s, _, _ in crop_charts})

    for i, crop in enumerate(CROPS):
        crop_charts = charts[metric].get(crop, [])
        if not crop_charts:
            continue
        active = "active" if i == 0 else ""
        tabs_html += f'<button class="tab {active}" onclick="showTab(\'{crop}\',this)">{crop.capitalize()}</button>\n'

        cards = ""
        for state, ph, b64 in sorted(crop_charts, key=lambda x: (x[1], x[0])):
            badge_color = PHASE_BADGE.get(ph, "#888")
            phase_label = PHASE_LABEL.get(ph, "")
            cards += f"""
            <div class="card" data-state="{state}">
              <div class="badge" style="background:{badge_color}">{phase_label}</div>
              <img src="data:image/png;base64,{b64}" loading="lazy">
            </div>"""

        display = "block" if i == 0 else "none"
        panes_html += f'<div id="pane-{crop}" class="pane" style="display:{display}">\n<div class="grid">{cards}</div>\n</div>\n'

    state_options = "\n".join(f'<option value="{s}">{s}</option>' for s in all_states)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>{title}</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: Georgia, serif; background: #f8f8f8; color: #222; }}
  header {{ background: #1a6b3c; color: white; padding: 18px 28px; }}
  header h1 {{ font-size: 1.35rem; margin-bottom: 4px; }}
  header p  {{ font-size: 0.85rem; opacity: 0.85; }}
  .legend {{ display:flex; gap:16px; padding:10px 28px; background:#fff;
             border-bottom:1px solid #ddd; flex-wrap:wrap; align-items:center; font-size:0.8rem; }}
  .legend-dot {{ width:12px; height:12px; border-radius:2px; display:inline-block; margin-right:4px; }}
  .controls {{ display:flex; gap:12px; padding:12px 28px; background:#fff;
               border-bottom:1px solid #ddd; align-items:center; flex-wrap:wrap; }}
  .tabs {{ display:flex; flex-wrap:wrap; gap:6px; flex:1; }}
  .tab {{ padding:6px 14px; border:1px solid #ccc; border-radius:4px; background:#fff;
          cursor:pointer; font-size:0.85rem; }}
  .tab.active {{ background:#1a6b3c; color:white; border-color:#1a6b3c; }}
  .filter {{ padding:6px 10px; border:1px solid #ccc; border-radius:4px;
             font-size:0.85rem; min-width:180px; }}
  .grid {{ display:grid; grid-template-columns: repeat(auto-fill, minmax(420px, 1fr));
           gap:16px; padding:20px 28px; }}
  .card {{ background:#fff; border:1px solid #e0e0e0; border-radius:6px;
           padding:12px; position:relative; }}
  .card img {{ width:100%; height:auto; display:block; }}
  .badge {{ position:absolute; top:10px; right:10px; font-size:0.7rem; color:white;
            padding:2px 7px; border-radius:3px; font-family: sans-serif; }}
  .pane {{ display:none; }}
</style>
</head>
<body>
<header>
  <h1>{title}</h1>
  <p>Within-state {label} of modal prices across mandis. Dashed line = state's eNAM adoption year.
     Only states with ≥3 mandis reporting in ≥6 months shown.</p>
</header>
<div class="legend">
  <strong>Adoption phase:</strong>
  <span><span class="legend-dot" style="background:#1a6b3c"></span>Phase 1 (2016)</span>
  <span><span class="legend-dot" style="background:#e07b00"></span>Phase 2 (2017–2019)</span>
  <span><span class="legend-dot" style="background:#c0392b"></span>Phase 3 (2020+)</span>
  <span><span class="legend-dot" style="background:#888"></span>Non-eNAM state</span>
</div>
<div class="controls">
  <div class="tabs">{tabs_html}</div>
  <select class="filter" id="stateFilter" onchange="filterState()">
    <option value="">All states</option>
    {state_options}
  </select>
</div>
{panes_html}
<script>
function showTab(crop, btn) {{
  document.querySelectorAll('.pane').forEach(p => p.style.display='none');
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.getElementById('pane-'+crop).style.display='block';
  btn.classList.add('active');
  filterState();
}}
function filterState() {{
  const val = document.getElementById('stateFilter').value;
  document.querySelectorAll('.card').forEach(c => {{
    c.style.display = (!val || c.dataset.state === val) ? 'block' : 'none';
  }});
}}
</script>
</body>
</html>"""

for metric in ["iqr", "sd"]:
    html = build_html(metric)
    out  = OUT_DIR / f"dispersion_{metric}.html"
    out.write_text(html, encoding="utf-8")
    size = len(html) / 1024 / 1024
    print(f"\nSaved: {out}  ({size:.1f} MB)")
