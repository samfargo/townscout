"""
Merges per-state travel time data and creates nationwide summaries.

Pipeline (anchor-mode only):
1. Load the per-state `t_hex` (long format) parquet files.
2. Concatenate them into a single nationwide file.
3. Generate complete H3 grid covering all states.
4. Build anchor arrays per hex (a{i}_id / a{i}_s) for K best anchors.
5. Merge travel time data onto complete grid (hexes without data get NaN).
6. Save r7 and r8 parquet files for downstream tiling.
"""
import glob
import os
from pathlib import Path
import numpy as np
import h3
import pandas as pd
import polars as pl
import geopandas as gpd
import shapely
from tqdm import tqdm

from vicinity.domains_overlay.climate import classify_climate_expr
from vicinity.domains_overlay.climate.schema import TEMP_SCALE, PPT_MM_SCALE, PPT_IN_SCALE
from vicinity.domains_overlay.validation import check_parquet_files

from config import (
    STATES,
    H3_RES_LOW,
    H3_RES_HIGH,
    STATE_BOUNDING_BOXES,
    STATE_SLUG_TO_CODE,
    STATE_FIPS,
)
from geometry_utils import clean_geoms

BOUNDARIES_DIR = Path("data/boundaries")
COUNTY_BOUNDARY_SHP = BOUNDARIES_DIR / "tl_2024_us_county.shp"
STATE_CODE_TO_FIPS = {abbr.upper(): fips for fips, abbr in STATE_FIPS.items()}
_COUNTY_BOUNDARIES = None
_STATE_GEOM_CACHE = {}
_BOUNDARY_WARNING_EMITTED = False
_H3_GEO_TO_CELLS = getattr(h3, "geo_to_cells", None)
_H3_POLYFILL_GEOJSON = getattr(h3, "polyfill_geojson", None)
_H3_POLYFILL = getattr(h3, "polyfill", None)


def _h3_str_to_int(cell) -> int:
    """Robustly convert an H3 address (string or int) to its uint64 integer form."""
    if isinstance(cell, (int, np.integer)):
        return int(cell)
    converters = [
        getattr(h3, "string_to_h3", None),
        getattr(h3, "str_to_int", None),
        getattr(h3, "string_to_int", None),
    ]
    for fn in converters:
        if callable(fn):
            return int(fn(cell))
    return int(cell, 16)


def _bbox_to_polygon(bbox: dict) -> dict:
    """Return a GeoJSON polygon for the provided lon/lat bounding box."""
    ring = [
        [bbox["west"], bbox["south"]],
        [bbox["east"], bbox["south"]],
        [bbox["east"], bbox["north"]],
        [bbox["west"], bbox["north"]],
        [bbox["west"], bbox["south"]],
    ]
    return {"type": "Polygon", "coordinates": [ring]}


def _get_state_fips(state_slug: str):
    """Map a Geofabrik-style state slug to its two-digit FIPS code."""
    code = STATE_SLUG_TO_CODE.get(state_slug)
    if not code:
        return None
    return STATE_CODE_TO_FIPS.get(code.upper())


def _load_county_boundaries():
    """
    Load nationwide county geometries once and cache them.
    """
    global _COUNTY_BOUNDARIES, _BOUNDARY_WARNING_EMITTED
    if _COUNTY_BOUNDARIES is not None:
        return _COUNTY_BOUNDARIES
    if not COUNTY_BOUNDARY_SHP.exists():
        if not _BOUNDARY_WARNING_EMITTED:
            print(
                f"[warn] County shapefile {COUNTY_BOUNDARY_SHP} missing; "
                "falling back to bounding boxes."
            )
            _BOUNDARY_WARNING_EMITTED = True
        return None
    gdf = gpd.read_file(COUNTY_BOUNDARY_SHP)
    if gdf.empty:
        if not _BOUNDARY_WARNING_EMITTED:
            print(
                f"[warn] County shapefile {COUNTY_BOUNDARY_SHP} contained no geometries; "
                "falling back to bounding boxes."
            )
            _BOUNDARY_WARNING_EMITTED = True
        return None
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    elif gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs("EPSG:4326")
    _COUNTY_BOUNDARIES = gdf[["STATEFP", "geometry"]].copy()
    return _COUNTY_BOUNDARIES


def _union_polygons(geoms):
    """Iteratively union polygon geometries to avoid GEOS collection issues."""
    result = None
    for geom in geoms:
        if geom is None or geom.is_empty:
            continue
        current = shapely.make_valid(geom)
        if current.is_empty:
            continue
        if result is None:
            result = current
        else:
            result = shapely.make_valid(result.union(current))
    return result


def _load_state_geometry(state_slug: str):
    """Dissolve county polygons for the requested state into a single boundary."""
    if state_slug in _STATE_GEOM_CACHE:
        return _STATE_GEOM_CACHE[state_slug]
    fips = _get_state_fips(state_slug)
    if not fips:
        print(f"[warn] No USPS/FIPS mapping for '{state_slug}'; using bounding box coverage.")
        return None
    counties = _load_county_boundaries()
    if counties is None:
        return None
    subset = counties[counties["STATEFP"] == fips]
    if subset.empty:
        print(f"[warn] County geometries missing for '{state_slug}' (FIPS {fips}); using bounding box.")
        return None
    polys = clean_geoms(subset, ["Polygon", "MultiPolygon"])
    if polys.empty:
        print(f"[warn] No polygonal county geometries for '{state_slug}'; using bounding box.")
        return None
    geom = _union_polygons(polys.to_list())
    if geom is None or geom.is_empty:
        print(f"[warn] Unable to dissolve counties for '{state_slug}'; using bounding box.")
        return None
    geom = shapely.make_valid(geom)
    _STATE_GEOM_CACHE[state_slug] = geom
    return geom


def _geometry_to_polygons(geom) -> list[dict]:
    """Split a Shapely geometry into GeoJSON polygon parts."""
    mapping = geom.__geo_interface__
    if mapping["type"] == "Polygon":
        return [mapping]
    if mapping["type"] == "MultiPolygon":
        return [{"type": "Polygon", "coordinates": coords} for coords in mapping["coordinates"]]
    return []


def _polyfill_geojson(polygon: dict, resolution: int):
    """Polyfill a GeoJSON polygon via whichever H3 API is available."""
    if callable(_H3_GEO_TO_CELLS):
        return _H3_GEO_TO_CELLS(polygon, resolution)
    if callable(_H3_POLYFILL_GEOJSON):
        return _H3_POLYFILL_GEOJSON(polygon, resolution)
    if callable(_H3_POLYFILL):
        coords_latlon = [
            [(lat, lon) for lon, lat in ring]
            for ring in polygon["coordinates"]
        ]
        return _H3_POLYFILL(coords_latlon, resolution, geo_json_conformant=True)
    raise RuntimeError("No suitable H3 polyfill function available.")


def _polyfill_geometry(geom, resolution: int):
    """Polyfill a Shapely geometry by iterating through its polygon parts."""
    cells = set()
    for polygon in _geometry_to_polygons(geom):
        result = _polyfill_geojson(polygon, resolution)
        cells.update(_h3_str_to_int(cell) for cell in result)
    return cells


def build_complete_hex_grid(states, resolutions):
    """
    Build an H3 grid that covers the requested states by polyfilling their
    dissolved county boundaries when available, falling back to bounding boxes
    otherwise. This guarantees we have features for every hex even if we never
    computed travel times there, while avoiding spillover beyond true state limits.
    """
    records = []
    for state in states:
        geom = _load_state_geometry(state)
        if geom is not None:
            for res in resolutions:
                cells = _polyfill_geometry(geom, res)
                if not cells:
                    print(f"[warn] polyfill produced 0 cells for {state} at res {res}")
                    continue
                records.extend((cell, res) for cell in cells)
            continue

        bbox = STATE_BOUNDING_BOXES.get(state)
        if not bbox:
            print(f"[warn] Missing bounding box for '{state}'; falling back to observed coverage.")
            continue
        polygon = _bbox_to_polygon(bbox)
        for res in resolutions:
            cells = _polyfill_geojson(polygon, res)
            if not cells:
                print(f"[warn] polyfill produced 0 cells for {state} at res {res}")
                continue
            records.extend((_h3_str_to_int(cell), res) for cell in cells)

    if not records:
        return pd.DataFrame(
            {
                "h3_id": pd.Series(dtype="uint64"),
                "res": pd.Series(dtype="int32"),
            }
        )

    grid = pd.DataFrame(records, columns=["h3_id", "res"])
    grid["h3_id"] = grid["h3_id"].astype("uint64", copy=False)
    grid["res"] = grid["res"].astype("int32", copy=False)
    return grid.drop_duplicates(ignore_index=True)

def main():
    """Main function to merge state data and create summaries."""
    print("--- Merging per-state data and creating summaries ---")
    
    # Use glob to find all per-state outputs from the previous step
    # This makes it easy to add more states by just updating the STATES list.
    drive_time_files = glob.glob("data/minutes/*_drive_t_hex.parquet")
    # Prefer anchors in data/anchors if present; fallback to minutes sites
    anchors_candidates = glob.glob("data/anchors/*_drive_sites.parquet")
    sites_files = anchors_candidates if anchors_candidates else glob.glob("data/minutes/*_drive_sites.parquet")

    if not drive_time_files or not sites_files:
        raise FileNotFoundError("No input files found from step 03. Run 'make minutes' first.")

    print(f"Found {len(drive_time_files)} travel time files and {len(sites_files)} sites files.")

    # 1. Load and concatenate all state data
    all_times = pd.concat([pd.read_parquet(f) for f in drive_time_files], ignore_index=True)
    all_sites = pd.concat([pd.read_parquet(f) for f in sites_files], ignore_index=True)

    # Build a complete H3 grid so the frontend can shade every hex, even if we
    # never computed anchor travel times there (e.g., large parks or rural areas).
    print("[info] Building complete hex coverage from state boundaries (bbox fallback enabled)...")
    grid_hexes = build_complete_hex_grid(STATES, [H3_RES_LOW, H3_RES_HIGH])
    observed_hexes = all_times[["h3_id", "res"]].drop_duplicates()
    observed_hexes["h3_id"] = observed_hexes["h3_id"].astype("uint64", copy=False)
    observed_hexes["res"] = observed_hexes["res"].astype("int32", copy=False)
    initial_observed = len(observed_hexes)
    observed_hexes = observed_hexes.merge(
        grid_hexes[["h3_id", "res"]],
        on=["h3_id", "res"],
        how="inner"
    )
    dropped = initial_observed - len(observed_hexes)
    if dropped > 0:
        print(f"[info] Clipped {dropped} observed hexes outside configured state boundaries.")
    base_hexes = pd.concat([grid_hexes, observed_hexes], ignore_index=True)
    base_hexes = base_hexes.drop_duplicates(ignore_index=True)
    print(f"[info] Base coverage: {len(base_hexes)} hexes across all resolutions")

    # 2. Anchor arrays for frontend (a{i}_id / a{i}_s) — top-K already enforced upstream
    # Sort times per hex and assign rank 0..K-1, then pivot into columns
    K_ANCHORS = 20
    times_sorted = all_times.sort_values(["h3_id", "res", "time_s", "anchor_int_id"]).copy()
    times_sorted["rank"] = times_sorted.groupby(["h3_id", "res"]).cumcount()
    times_topk = times_sorted[times_sorted["rank"] < K_ANCHORS]

    # Pivot IDs
    pivot_ids = times_topk.pivot_table(
        index=["h3_id", "res"],
        columns="rank",
        values="anchor_int_id",
        aggfunc="first"
    )
    if isinstance(pivot_ids.columns, pd.RangeIndex):
        pivot_ids.columns = [f"a{int(c)}_id" for c in pivot_ids.columns]
    else:
        pivot_ids.columns = [f"a{int(c)}_id" for c in pivot_ids.columns.tolist()]

    # Pivot seconds
    pivot_secs = times_topk.pivot_table(
        index=["h3_id", "res"],
        columns="rank",
        values="time_s",
        aggfunc="first"
    )
    if isinstance(pivot_secs.columns, pd.RangeIndex):
        pivot_secs.columns = [f"a{int(c)}_s" for c in pivot_secs.columns]
    else:
        pivot_secs.columns = [f"a{int(c)}_s" for c in pivot_secs.columns.tolist()]

    anchor_cols = pd.concat([pivot_ids, pivot_secs], axis=1).reset_index()
    # Merge anchor arrays onto base hex universe
    base_hexes = pd.merge(base_hexes, anchor_cols, on=["h3_id", "res"], how="left")

    # Anchor-mode only: final_wide is the anchor arrays joined to base hexes
    final_wide = base_hexes

    climate_path = "out/climate/hex_climate.parquet"
    if os.path.exists(climate_path):
        print("[info] Attaching climate data...")
        climate = pd.read_parquet(climate_path, dtype_backend="pyarrow")
        cast_map = {}
        for col in climate.columns:
            if col.endswith("_f_q"):
                cast_map[col] = "int16"
            elif col.endswith("_mm_q") or col.endswith("_in_q"):
                cast_map[col] = "uint16"
        if cast_map:
            climate = climate.astype(cast_map, copy=False)
        if "h3_id" in climate.columns:
            try:
                climate["h3_id"] = climate["h3_id"].astype("uint64", copy=False)
            except TypeError:
                climate["h3_id"] = climate["h3_id"].astype("uint64[pyarrow]", copy=False)
        if "res" in climate.columns:
            try:
                climate["res"] = climate["res"].astype("int32", copy=False)
            except TypeError:
                climate["res"] = climate["res"].astype("int32[pyarrow]", copy=False)
        
        # Convert quantized values back to floats for climate label generation
        climate_pl = pl.from_pandas(climate)
        temp_cols = [c for c in climate_pl.columns if c.endswith("_f_q")]
        ppt_mm_cols = [c for c in climate_pl.columns if c.endswith("_mm_q")]
        ppt_in_cols = [c for c in climate_pl.columns if c.endswith("_in_q")]

        for col in temp_cols:
            new_col = col[:-2]  # strip _q suffix
            climate_pl = climate_pl.with_columns((pl.col(col).cast(pl.Float64) * TEMP_SCALE).alias(new_col))
        for col in ppt_mm_cols:
            new_col = col[:-2]
            climate_pl = climate_pl.with_columns((pl.col(col).cast(pl.Float64) * PPT_MM_SCALE).alias(new_col))
        for col in ppt_in_cols:
            new_col = col[:-2]
            climate_pl = climate_pl.with_columns((pl.col(col).cast(pl.Float64) * PPT_IN_SCALE).alias(new_col))

        # Generate climate label
        climate_pl = climate_pl.with_columns(classify_climate_expr().alias("climate_label"))
        climate = climate_pl.to_pandas()
        
        final_wide = final_wide.merge(climate, on=["h3_id", "res"], how="left")
    else:
        print("[warn] climate parquet missing; skipping weather merge")

    power_corridor_paths = glob.glob("data/power_corridors/*_near_power_corridor.parquet")
    if power_corridor_paths:
        print(f"[info] Attaching power corridor flags ({len(power_corridor_paths)} files)")
        corridor_frames = check_parquet_files(
            power_corridor_paths,
            required_columns={"h3_id", "res", "near_power_corridor"},
            warn_only=True
        )
        corridor_frames = [df[["h3_id", "res", "near_power_corridor"]] for df in corridor_frames]

        if corridor_frames:
            corridor = pd.concat(corridor_frames, ignore_index=True)
            corridor["h3_id"] = corridor["h3_id"].astype("uint64", copy=False)
            corridor["res"] = corridor["res"].astype("int32", copy=False)
            corridor["near_power_corridor"] = corridor["near_power_corridor"].astype(bool, copy=False)
            corridor = corridor.drop_duplicates(subset=["h3_id", "res"], keep="last")

            final_wide = final_wide.merge(corridor, on=["h3_id", "res"], how="left")
            final_wide["near_power_corridor"] = final_wide["near_power_corridor"].fillna(False).astype(bool, copy=False)
        else:
            print("[warn] No valid power corridor parquet found; defaulting to False.")
            final_wide["near_power_corridor"] = False
    else:
        print("[warn] Power corridor parquet missing; defaulting to False.")
        final_wide["near_power_corridor"] = False

    # Politics overlay: political lean based on 2024 presidential election
    politics_paths = glob.glob("data/politics/*_political_lean.parquet")
    if politics_paths:
        print(f"[info] Attaching political lean data ({len(politics_paths)} files)")
        politics_frames = check_parquet_files(
            politics_paths,
            required_columns={"h3_id", "res", "political_lean", "rep_vote_share"},
            warn_only=True
        )
        politics_frames = [df[["h3_id", "res", "political_lean", "rep_vote_share"]] for df in politics_frames]

        if politics_frames:
            politics = pd.concat(politics_frames, ignore_index=True)
            politics["h3_id"] = politics["h3_id"].astype("uint64", copy=False)
            politics["res"] = politics["res"].astype("int32", copy=False)
            politics["political_lean"] = politics["political_lean"].astype("uint8", copy=False)
            politics["rep_vote_share"] = politics["rep_vote_share"].astype("float32", copy=False)
            politics = politics.drop_duplicates(subset=["h3_id", "res"], keep="last")

            final_wide = final_wide.merge(politics, on=["h3_id", "res"], how="left")
            # Keep NaN for hexes without political data (water, unpopulated areas, etc.)
        else:
            print("[warn] No valid politics parquet found; skipping political lean.")
    else:
        print("[info] Politics parquet missing; skipping political lean (optional overlay).")

    # 3. Split by resolution and save
    os.makedirs("state_tiles", exist_ok=True)
    
    for res in [H3_RES_LOW, H3_RES_HIGH]:
        res_df = final_wide[final_wide['res'] == res].copy()
        
        # Drop the 'res' column as it's encoded in the filename
        res_df = res_df.drop(columns=['res'])
        
        output_path = f"state_tiles/us_r{res}.parquet"
        res_df.to_parquet(output_path, index=False)
        print(f"[ok] Saved {len(res_df)} rows to {output_path}")

    print("--- Pipeline step 04 finished ---")
    stamp_path = Path("state_tiles/.merge.stamp")
    stamp_path.parent.mkdir(parents=True, exist_ok=True)
    stamp_path.touch()


if __name__ == "__main__":
    main()
