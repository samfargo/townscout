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
import numpy as np
import h3
import pandas as pd
import polars as pl
from tqdm import tqdm

from climate.prism_to_hex import classify_climate_expr, TEMP_SCALE, PPT_MM_SCALE, PPT_IN_SCALE

from config import STATES, H3_RES_LOW, H3_RES_HIGH, STATE_BOUNDING_BOXES


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


def build_complete_hex_grid(states, resolutions):
    """
    Build an H3 grid that covers the requested states by polyfilling their
    bounding boxes. This guarantees we have features for every hex even if
    we never computed travel times there.
    """
    records = []
    for state in states:
        bbox = STATE_BOUNDING_BOXES.get(state)
        if not bbox:
            print(f"[warn] Missing bounding box for '{state}'; falling back to observed coverage.")
            continue
        polygon = _bbox_to_polygon(bbox)
        for res in resolutions:
            cells = h3.geo_to_cells(polygon, res)
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
    print("[info] Building complete hex coverage from state bounding boxes...")
    grid_hexes = build_complete_hex_grid(STATES, [H3_RES_LOW, H3_RES_HIGH])
    observed_hexes = all_times[["h3_id", "res"]].drop_duplicates()
    observed_hexes["h3_id"] = observed_hexes["h3_id"].astype("uint64", copy=False)
    observed_hexes["res"] = observed_hexes["res"].astype("int32", copy=False)
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
        corridor_frames = []
        for path in power_corridor_paths:
            try:
                df = pd.read_parquet(path)
            except Exception as exc:
                print(f"[warn] Failed to read {path}: {exc}")
                continue
            missing = {"h3_id", "res", "near_power_corridor"} - set(df.columns)
            if missing:
                print(f"[warn] Skipping {path}; missing columns: {missing}")
                continue
            corridor_frames.append(df[["h3_id", "res", "near_power_corridor"]])

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


if __name__ == "__main__":
    main()
