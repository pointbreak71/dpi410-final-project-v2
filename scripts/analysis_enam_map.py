"""
eNAM Mandi Coverage Map
- Geocodes districts via Nominatim (cached)
- Downloads India state boundaries from Natural Earth
- Plots mandis colour-coded by adoption phase
- Output: output/enam_adoption/enam_map.png at 300 DPI
"""

import os, json, time, zipfile, io
import pandas as pd
import requests
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import geopandas as gpd
from pathlib import Path

ROOT    = Path(__file__).resolve().parent.parent
DATA    = ROOT / "data/clean/enam_adoption.csv"
CACHE   = ROOT / "data/raw/geocode_cache.json"
FIG_DIR = ROOT / "output/enam_adoption"
FIG_DIR.mkdir(parents=True, exist_ok=True)

# ── Load adoption data ────────────────────────────────────────────────────────
df = pd.read_csv(DATA)

# ── 1. Geocode districts (with cache) ────────────────────────────────────────
cache = json.loads(CACHE.read_text()) if CACHE.exists() else {}

def geocode(district, state):
    key = f"{district}|{state}"
    if key in cache:
        return cache[key]
    queries = [
        f"{district}, {state}, India",
        f"{district} district, India",
        f"{district}, India",
    ]
    for q in queries:
        try:
            r = requests.get(
                "https://nominatim.openstreetmap.org/search",
                params={"q": q, "format": "json", "limit": 1},
                headers={"User-Agent": "eNAM-research/1.0 (academic)"},
                timeout=10,
            )
            results = r.json()
            if results:
                lat = float(results[0]["lat"])
                lon = float(results[0]["lon"])
                cache[key] = (lat, lon)
                CACHE.write_text(json.dumps(cache, indent=2))
                return (lat, lon)
        except Exception:
            pass
        time.sleep(1)
    cache[key] = None
    CACHE.write_text(json.dumps(cache, indent=2))
    return None

# Get unique district-state pairs
pairs = df[["district", "state"]].drop_duplicates()
print(f"Geocoding {len(pairs)} unique districts (skipping cached)...")

to_fetch = [(d, s) for d, s in zip(pairs["district"], pairs["state"])
            if f"{d}|{s}" not in cache]
print(f"  {len(pairs) - len(to_fetch)} already cached, {len(to_fetch)} to fetch")

for i, (district, state) in enumerate(to_fetch, 1):
    result = geocode(district, state)
    status = f"({result[0]:.2f}, {result[1]:.2f})" if result else "FAILED"
    print(f"  [{i}/{len(to_fetch)}] {district}, {state} → {status}")
    time.sleep(1)

# Merge coords back onto mandis
def get_coords(row):
    return cache.get(f"{row['district']}|{row['state']}")

df["coords"] = df.apply(get_coords, axis=1)
df["lat"] = df["coords"].apply(lambda c: c[0] if c else None)
df["lon"] = df["coords"].apply(lambda c: c[1] if c else None)

ok   = df["lat"].notna().sum()
fail = df["lat"].isna().sum()
print(f"\nGeocode results: {ok} succeeded, {fail} failed ({fail/len(df)*100:.1f}%)")

df_geo = df.dropna(subset=["lat", "lon"]).copy()

# ── 2. Download India shapefile (Natural Earth admin-1 states) ───────────────
shp_path = ROOT / "data/raw/india_states.geojson"
if not shp_path.exists():
    print("\nDownloading India state boundaries from Natural Earth...")
    url = "https://naturalearth.s3.amazonaws.com/10m_cultural/ne_10m_admin_1_states_provinces.zip"
    r = requests.get(url, timeout=60)
    if r.status_code == 200:
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            z.extractall(ROOT / "data/raw/ne_states_tmp")
        world_states = gpd.read_file(ROOT / "data/raw/ne_states_tmp/ne_10m_admin_1_states_provinces.shp")
        india = world_states[world_states["admin"] == "India"]
        india.to_file(shp_path, driver="GeoJSON")
        print(f"  Saved {len(india)} Indian state polygons")
    else:
        raise RuntimeError(f"Failed to download shapefile: HTTP {r.status_code}")

india = gpd.read_file(shp_path)
# Clip to mainland bounding box
india = india.cx[68:98, 6:38]

# ── 3. Plot map ───────────────────────────────────────────────────────────────
PHASE_COLORS = {
    1: ("#1a6b3c", "Phase 1 (2016)"),
    2: ("#e07b00", "Phase 2 (2017–2019)"),
    3: ("#c0392b", "Phase 3 (2020+)"),
}

fig, ax = plt.subplots(figsize=(9, 11), facecolor="white")
ax.set_facecolor("white")

# State boundaries
india.plot(ax=ax, color="#f0f0f0", edgecolor="#aaaaaa", linewidth=0.6)

# Mandis by phase
for phase, (color, label) in PHASE_COLORS.items():
    sub = df_geo[df_geo["enam_phase"] == phase]
    ax.scatter(
        sub["lon"], sub["lat"],
        c=color, s=18, alpha=0.75, linewidths=0.3,
        edgecolors="white", label=label, zorder=3,
    )

# Legend
legend_handles = [
    mpatches.Patch(color=color, label=label)
    for _, (color, label) in PHASE_COLORS.items()
]
ax.legend(
    handles=legend_handles,
    loc="lower left", fontsize=10,
    frameon=True, framealpha=0.9,
    edgecolor="#cccccc",
    title="Adoption Phase", title_fontsize=10,
)

ax.set_title("eNAM Mandi Coverage by Adoption Phase",
             fontsize=14, fontweight="bold", pad=14)
ax.axis("off")
plt.tight_layout()

out = FIG_DIR / "enam_map.png"
plt.savefig(out, dpi=300, bbox_inches="tight", facecolor="white")
plt.close()
print(f"\nSaved: {out}")
print(f"Map shows {len(df_geo):,} of {len(df):,} mandis ({len(df_geo)/len(df)*100:.1f}% geocoded)")
