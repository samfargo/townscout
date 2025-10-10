Goal: Integrate PRISM climate normals into Townscout’s livability data pipeline in a compact, standardized, and developer-transparent way.

Townscout should be able to express “how a place feels year-round” — not just how fast you can drive to Costco. The weather layer gives each hex real physical context (warmth, cold, wetness), so users can blend climate comfort with accessibility when deciding where to live.

In short: make climate a core dimension of livability—accurate, lightweight, and transparent from source raster to user interaction.

0) What we’ll ingest (PRISM “Normals”, 1991–2020)
	•	Variables: monthly tmean, tmin, tmax (°C) and ppt (mm)
	•	Grid: ~4 km resolution, nationwide coverage
	•	Units: download in °C/mm, convert temps to °F, keep mm and add inches
		•	Outputs per hex (store one row per `h3_id` + `res`; `_q` = quantized int fields):
			•	Monthly means: temp_mean_{jan…dec}_f_q  (int16, tenths °F)
			•	temp_max_hot_month_f_q, temp_min_cold_month_f_q, temp_mean_summer_f_q, temp_mean_winter_f_q
			•	ppt_{jan…dec}_mm_q, ppt_ann_mm_q  (uint16, tenths mm)

1) Repo layout (target)

```
data/
  climate/
    prism/
      normals_1991_2020/
        tmean/   # GeoTIFFs per month
        tmin/
        tmax/
        ppt/
out/
  climate/
    hex_climate.parquet     # columns: h3_id, res, climate metrics
src/
  climate/
    prism_normals_fetch.py  # downloads PRISM tiles
    prism_to_hex.py         # raster → H3 parquet
```

2) Download PRISM rasters

Keep the fetcher deterministic so it can be re-run without side effects. Dependencies: `requests`, `tqdm` (optional progress), and room for retries.

**Important: PRISM File Patterns**
The PRISM normals follow a strict naming pattern: `prism_{var}_us_25m_2020{month:02d}_avg_30y.tif`. When matching files:
- Use exact patterns like `*_202001_*.tif` for January, `*_202002_*.tif` for February, etc.
- Avoid ambiguous patterns like `*01*.tif` which could match October (202010) or November (202011)
- Always sort matches for deterministic selection
- Validate that each month maps to its correct file to avoid seasonal pattern issues

```python
# src/climate/prism_normals_fetch.py
from __future__ import annotations
import os
import pathlib
import time
import requests

MONTHS = ["jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"]
BASE_URL = "https://prism.oregonstate.edu/path/to/normals"  # set to the known-good endpoint

def prism_filename(var: str, month: str) -> str:
    return f"PRISM_{var}_30yr_normal_4kmM2_1991-2020_{month}.tif"

def download_normals(out_dir: str, variables=("tmean","tmin","tmax","ppt")):
    root = pathlib.Path(out_dir)
    for var in variables:
        var_dir = root / var
        var_dir.mkdir(parents=True, exist_ok=True)
        for month in MONTHS:
            fn = prism_filename(var, month)
            url = f"{BASE_URL}/{var}/{fn}"
            dst = var_dir / fn
            if dst.exists():
                continue
            resp = requests.get(url, timeout=180)
            resp.raise_for_status()
            dst.write_bytes(resp.content)
            time.sleep(0.25)  # stay polite
```

3) Raster → per-hex climate parquet

Key requirement: reuse the same H3 universe the travel-time pipeline already computes, so every hex in `state_tiles/us_r{7,8}.parquet` gets weather and we don’t waste time on ocean/oob cells.

```
Inputs:
  •  data/minutes/*_drive_t_hex.parquet  (produced in step 03)
  •  data/climate/prism/normals_1991_2020/**.tif
Output:
  •  out/climate/hex_climate.parquet  (h3_id, res, climate fields)
Deps:
  •  rasterio, geopandas, shapely, rasterstats, polars, numpy
```

```python
# src/climate/prism_to_hex.py
from __future__ import annotations
import glob
import os
from functools import lru_cache

import geopandas as gpd
import h3
import numpy as np
import polars as pl
from rasterstats import zonal_stats
from shapely.geometry import Polygon

MONTHS = ["jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"]
PRISM_DIR = "data/climate/prism/normals_1991_2020"
MINUTES_GLOB = "data/minutes/*_drive_t_hex.parquet"
OUT_PARQUET = "out/climate/hex_climate.parquet"

def c_to_f(values: np.ndarray) -> np.ndarray:
    return values * 9.0 / 5.0 + 32.0

@lru_cache
def load_hex_ids(res: int) -> list[str]:
    scan = pl.scan_parquet(MINUTES_GLOB, columns=["h3_id", "res"])
    return (
        scan.filter(pl.col("res") == res)
        .select("h3_id")
        .unique()
        .collect()
        .to_series()
        .to_list()
    )

def build_hex_frame(res: int) -> gpd.GeoDataFrame:
    ids = load_hex_ids(res)
    geoms = [Polygon(h3.h3_to_geo_boundary(h, geo_json=True)) for h in ids]
    return gpd.GeoDataFrame({"h3_id": ids, "res": res}, geometry=geoms, crs="EPSG:4326")

def collect_monthly(var: str, month: str) -> str | None:
    pattern = os.path.join(PRISM_DIR, var, f"*{month}*.tif")
    matches = glob.glob(pattern)
    return matches[0] if matches else None

def zonal_mean(layer: gpd.GeoDataFrame, tif: str, nodata=-9999.0) -> np.ndarray:
    stats = zonal_stats(
        vectors=layer.geometry,
        raster=tif,
        stats=["mean"],
        all_touched=True,
        nodata=nodata,
    )
    return np.array([entry["mean"] if entry["mean"] is not None else np.nan for entry in stats])

def process(resolutions=(7, 8)) -> str:
    frames = []
    for res in resolutions:
        hexes = build_hex_frame(res)
        data = {"h3_id": hexes["h3_id"].values, "res": res}

        for var in ("tmean", "tmin", "tmax"):
            for month in MONTHS:
                tif = collect_monthly(var, month)
                if not tif:
                    continue
                values = zonal_mean(hexes, tif, nodata=-9999)
                data[f"{var}_{month}_c"] = values

        for month in MONTHS:
            tif = collect_monthly("ppt", month)
            if tif:
                data[f"ppt_{month}_mm"] = zonal_mean(hexes, tif, nodata=-9999)

        df = pl.DataFrame(data)

        for month in MONTHS:
            if f"tmean_{month}_c" in df.columns:
                df = df.with_columns(
                    pl.col(f"tmean_{month}_c").map_elements(c_to_f).alias(f"tmean_{month}_f")
                )
            if f"tmin_{month}_c" in df.columns:
                df = df.with_columns(
                    pl.col(f"tmin_{month}_c").map_elements(c_to_f).alias(f"tmin_{month}_f")
                )
            if f"tmax_{month}_c" in df.columns:
                df = df.with_columns(
                    pl.col(f"tmax_{month}_c").map_elements(c_to_f).alias(f"tmax_{month}_f")
                )

        tmean_cols = [f"tmean_{m}_f" for m in MONTHS if f"tmean_{m}_f" in df.columns]
        if tmean_cols:
            df = df.with_columns([
                pl.mean_horizontal(tmean_cols).alias("temp_mean_ann_f"),
                pl.mean_horizontal([f"tmean_{m}_f" for m in ["jun","jul","aug"] if f"tmean_{m}_f" in df.columns]).alias("temp_mean_summer_f"),
                pl.mean_horizontal([f"tmean_{m}_f" for m in ["dec","jan","feb"] if f"tmean_{m}_f" in df.columns]).alias("temp_mean_winter_f"),
            ])

        tmax_cols = [f"tmax_{m}_f" for m in MONTHS if f"tmax_{m}_f" in df.columns]
        if tmax_cols:
            df = df.with_columns(pl.max_horizontal(tmax_cols).alias("temp_max_hot_month_f"))

        tmin_cols = [f"tmin_{m}_f" for m in MONTHS if f"tmin_{m}_f" in df.columns]
        if tmin_cols:
            df = df.with_columns(pl.min_horizontal(tmin_cols).alias("temp_min_cold_month_f"))

        ppt_cols = [f"ppt_{m}_mm" for m in MONTHS if f"ppt_{m}_mm" in df.columns]
        if ppt_cols:
            df = df.with_columns(
                pl.sum_horizontal(ppt_cols).alias("ppt_ann_mm"),
            )

        temp_float_cols = [c for c in df.columns if c.endswith("_f")]
        ppt_float_cols = [c for c in df.columns if c.endswith("_mm")]

        if temp_float_cols:
            df = df.with_columns([
                (pl.col(col) * 10)
                .round()
                .cast(pl.Int16)
                .alias(f"{col}_q")
                for col in temp_float_cols
            ])

        if ppt_float_cols:
            df = df.with_columns([
                (pl.col(col) * 10)
                .round()
                .clip_min(0)
                .cast(pl.UInt16)
                .alias(f"{col}_q")
                for col in ppt_float_cols
            ])

        df = df.drop([col for col in df.columns if col.endswith("_c") or col.endswith("_f") or col.endswith("_mm")])
        frames.append(df)

    climate_df = pl.concat(frames)
    climate_df = climate_df.with_columns([
        pl.col("^.*_f_q$").cast(pl.Int16),
        pl.col("^ppt_.*_q$").cast(pl.UInt16),
    ])
    climate_df.write_parquet(OUT_PARQUET)
    return OUT_PARQUET

if __name__ == "__main__":
    process()
```

Running this once after `make minutes` gives a single parquet with both resolutions. If you ever need to backfill missing coastal cells, fill NaNs by propagating the nearest neighbor via `h3.k_ring`.

4) Quantized storage, decoding, and metadata

Naming and decoding
	•	Every quantized column ends with `_q`.
	•	Temperatures use tenths of °F (`int16`): e.g., `temp_mean_ann_f_q = 655` → 65.5 °F.
	•	Precipitation uses tenths of mm (`uint16`): e.g., `ppt_ann_mm_q = 812` → 81.2 mm.
	•	Keep the ints in the parquet and PMTiles; decode in consumers by dividing by 10.

Frontend helper (`tiles/web/lib/utils/climateDecode.ts`):

```ts
export const TEMP_SCALE = 0.1;  // °F per integer
export const PPT_SCALE = 0.1;   // mm per integer

export function tempF(q?: number | null) {
  return q == null ? NaN : q * TEMP_SCALE;
}

export function pptMm(q?: number | null) {
  return q == null ? NaN : q * PPT_SCALE;
}
```

MapLibre expressions should divide before comparisons, e.g.:

```ts
const comfortExpr = [
  "exp",
  [
    "/",
    ["-", ["/", ["get", "temp_mean_ann_f_q"], 10.0], ["var", "desiredF"]],
    -2 * sigma * sigma
  ]
];
```

`04_merge_states.py` should preserve the compact dtypes when joining:

```python
climate = pd.read_parquet(
    "out/climate/hex_climate.parquet",
    dtype_backend="pyarrow"
)
cast_map = {}
for col in climate.columns:
    if col.endswith("_f_q"):
        cast_map[col] = "int16"
    elif col.endswith("_mm_q"):
        cast_map[col] = "uint16"
if cast_map:
    climate = climate.astype(cast_map, copy=False)
```

`05_h3_to_geojson.py` options:
	•	Default: pass `_q` ints through unchanged (smallest PMTiles). Add a doc note in the tiles README: “Fields ending in `_q` are quantized; divide by 10 to decode.”
	•	Optional flag `CLIMATE_DECODE_AT_EXPORT=true`: decode to floats before writing GeoJSON if we ever want human-friendly tiles. Implement by calling a helper that strips `_q` and multiplies by `0.1`.

Attach scale metadata to the parquet for analytics workflows:

```python
import json
import pyarrow as pa
import pyarrow.parquet as pq

meta = {
    "townscout_prism": json.dumps({
        "source": "PRISM Normals 1991-2020",
        "temp_scale": 0.1,
        "ppt_scale": 0.1,
        "generated_utc": generated_ts,
    })
}

table = climate_df.to_arrow()
existing = table.schema.metadata or {}
table = table.replace_schema_metadata(
    {**existing, **{k: v.encode() for k, v in meta.items()}}
)
pq.write_table(table, OUT_PARQUET)
```

Guardrails:
	•	Add a unit test that reads `out/climate/hex_climate.parquet` and asserts `int16/uint16` dtypes.
	•	Expose the build flag in CI so we catch regressions if someone flips decoding.
	•	Document the `_q` convention in `docs/data_contracts.md`.

5) Merge with the Townscout pipeline

We want the climate fields to ride along with the existing `state_tiles/us_r{res}.parquet` files so the PMTiles automatically include them. That keeps the frontend simple (one source, no client-side joins).

Steps:

1. Add a Makefile target that depends on `minutes` and produces the parquet above:

```
.PHONY: climate
climate: minutes ## Build PRISM climate parquet for r7 + r8
	$(PY) src/climate/prism_to_hex.py
```

2. Update the pipeline so `make tiles` runs the climate step before the merge:

```
tiles: climate state_tiles/us_r7.parquet state_tiles/us_r8.parquet ...
```

3. Modify `src/04_merge_states.py` after `final_wide` is computed:

```python
climate_path = "out/climate/hex_climate.parquet"
if os.path.exists(climate_path):
    climate = pd.read_parquet(climate_path, dtype_backend="pyarrow")
    cast_map = {}
    for col in climate.columns:
        if col.endswith("_f_q"):
            cast_map[col] = "int16"
        elif col.endswith("_mm_q"):
            cast_map[col] = "uint16"
    if cast_map:
        climate = climate.astype(cast_map, copy=False)
    final_wide = final_wide.merge(climate, on=["h3_id", "res"], how="left")
else:
    print("[warn] climate parquet missing; skipping weather merge")
```

4. The downstream steps (`05_h3_to_geojson.py`, `06_build_tiles.py`) already carry all columns through, so no further changes are required for data export.

Result: every hex in both r7 and r8 PMTiles carries weather columns, and we don’t need a second tileset or runtime join.

6) Frontend hooks (to be designed)

We now expose `temp_*` and `ppt_*` fields in the hover payload and when building MapLibre expressions. Decide later how sliders/legends should work, but the data is already in the state store after Step 4 so the UI can read directly from `feature.properties`.

7) Dependencies & operations
	•	Add `rasterio`, `rasterstats`, and `geopandas[speedups]` to `requirements.txt`. These pull in GDAL; document any brew/apt snippets the team needs before running `make init`.
	•	`out/climate/hex_climate.parquet` with both resolutions and monthly columns is roughly 50 columns × ~600k rows ≈ tens of MB — fine to commit to the artifacts cache.
	•	Normals are static, so only rebuild when we refresh tiles for a new Townscout release.
	•	If a PRISM tile is missing for a month, fail fast (raise) so we catch gaps instead of silently shipping partial data.

8) QA checklist
	•	After the merge, confirm `state_tiles/us_r8.parquet` contains `temp_mean_ann_f`, `ppt_ann_mm`, etc.
	•	Compare sample hexes (Phoenix, Boston, Seattle) against trusted climate summaries.
	•	Ensure r7 tiles also include climate columns (spot-check via `tippecanoe-json` or `pmtiles show`).
	•	Verify that missing-data hexes show up as NaN/undefined rather than zeroed.
	•	Run the hover panel in dev after integrating UI work to confirm the new fields surface without breaking travel-time logic.



Give users a quick entry point into “what kind of weather do I want to live in?” without needing numeric sliders.

🧭 TownScout Climate Typology

Structure

Each climate type comes from three measurable dimensions you already have:

Dimension	Derived from columns	What it represents
Heat Index	temp_mean_summer_f_q / 10	How hot summers get
Cold Index	temp_mean_winter_f_q / 10	How cold winters get
Moisture Index	ppt_ann_in_q / 10	How wet or dry overall


🌤️ Final Set of 9 Climate Labels

Label	Criteria (approx.)	Intuitive Meaning	Example Regions
Arctic Cold	summer < 60°F and winter < 30°F	Long, frigid winters; short cool summers	Alaska interior, N Rockies peaks
Cold Seasonal	summer ≥ 65°F and winter < 32°F and ppt > 20"	Hot summers, snowy winters	Upper Midwest, northern New England
Mild Continental	summer 70–80°F, winter 32–45°F, ppt 25–50"	Warm summers, chilly winters, distinct seasons	Midwest, Northeast
Cool Maritime	summer < 70°F, winter > 35°F, ppt > 35"	Mild year-round, gray and damp	Pacific Northwest coast
Warm Humid	summer > 80°F, winter > 45°F, ppt > 40"	Hot, sticky summers and mild winters	Deep South, Southeast
Hot Dry (Desert)	summer > 80°F, ppt < 10"	Extremely hot, parched	Arizona, Nevada, SE California
Warm Semi-Arid (Steppe)	summer 75–85°F, ppt 10–20"	Hot, dry but with short wet season	Texas Panhandle, inland California
Mediterranean Mild	summer > 75°F, ppt < 30" and ppt_winter > ppt_summer	Dry, sunny summers, wet mild winters	Coastal California
Mountain Mixed	summer 60–75°F, winter < 35°F, ppt variable	Wide seasonal swings, cooler year-round	Rockies, Appalachians highlands


Computation Sketch (Polars or Python)

def classify_townscout_climate(row):
    summer = row["temp_mean_summer_f_q"] / 10
    winter = row["temp_mean_winter_f_q"] / 10
    ppt = row["ppt_ann_in_q"] / 10
    ppt_summer = row["ppt_jul_in_q"] / 10
    ppt_winter = row["ppt_dec_in_q"] / 10

    if summer < 60 and winter < 30:
        return "Arctic Cold"
    if summer >= 65 and winter < 32 and ppt > 20:
        return "Cold Seasonal"
    if 70 <= summer <= 80 and 32 <= winter <= 45 and 25 <= ppt <= 50:
        return "Mild Continental"
    if summer < 70 and winter > 35 and ppt > 35:
        return "Cool Maritime"
    if summer > 80 and winter > 45 and ppt > 40:
        return "Warm Humid"
    if ppt < 10 and summer > 80:
        return "Hot Dry (Desert)"
    if 10 <= ppt < 20 and 75 <= summer <= 85:
        return "Warm Semi-Arid"
    if ppt < 30 and ppt_winter > ppt_summer and summer > 75:
        return "Mediterranean Mild"
    if 60 <= summer <= 75 and winter < 35:
        return "Mountain Mixed"
    return "Unclassified"

Climate Sparkline (Hover Details Section)

Purpose:
Let users see seasonality at a glance.

Data Source:
	•	PRISM 1991–2020 normals, aggregated to H3 R8.
	•	Columns: temp_month_mean_f_q[1–12], precip_month_in_q[1–12].

Rendering in Hover Details section:
	•	On hover over a hex, load 12 monthly means for both temperature and precipitation.
	•	Display as compact dual mini-sparklines (≈100 px wide, 12 points):
	•	Top line: Temperature (°F), smooth line or bars.
	•	Bottom line: Precipitation (inches), bars or filled area (whatever looks best and is most intuitive).