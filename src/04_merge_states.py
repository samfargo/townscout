"""
Merges per-state travel time data and creates nationwide summaries.

Pipeline:
1. Load the per-state `t_hex` (long format) parquet files.
2. Concatenate them into a single nationwide file.
3. Load the canonical anchor sites data.
4. Join `t_hex` with anchor sites to get brand/category info.
5. Compute `min_cat` and `min_brand` summaries.
6. Save all outputs.
"""
import glob
import os
import pandas as pd
import geopandas as gpd
from tqdm import tqdm
import numpy as np

from config import STATES, H3_RES_LOW, H3_RES_HIGH
import glob
import os
import pandas as pd
import numpy as np

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

    # IMPORTANT: Preserve the full set of hexes observed in travel-time compute
    # Some hexes may have no matching brand rows after joins/pivots; we still
    # want them present in the final tiles so the frontend can render them with
    # NaNs for missing brands. Use this as the base universe of (h3_id, res).
    base_hexes = all_times[["h3_id", "res"]].drop_duplicates()

    # 2. Join travel times with site info to get brand/category data
    # We only need a few columns from the sites table for this step.
    # Ensure anchor_int_id is present; if missing, derive from deterministic site_id order
    if 'anchor_int_id' not in all_sites.columns:
        all_sites = all_sites.sort_values('site_id').reset_index(drop=True)
        all_sites['anchor_int_id'] = all_sites.index.astype('int32')
    sites_info = all_sites[['anchor_int_id', 'brands', 'categories']].copy()
    
    # Explode the 'brands' list so each brand has its own row. This makes joining easier.
    sites_info = sites_info.explode('brands')
    sites_info = sites_info.rename(columns={'brands': 'brand_id'})
    sites_info = sites_info.dropna(subset=['brand_id'])

    # Join on anchor_int_id emitted by T_hex
    merged_data = pd.merge(all_times, sites_info, left_on='anchor_int_id', right_on='anchor_int_id', how='inner')

    # 3. For each hex, calculate the minimum travel time to each brand observed
    # Keep only rows where we have a resolved brand_id
    merged_data = merged_data.dropna(subset=['brand_id']).copy()
    if merged_data.empty:
        raise SystemExit("[error] No brand_id values found in merged data. Ensure BRAND_REGISTRY / normalization emits brand_ids.")

    # Group by hex, resolution, and brand, then find the minimum time.
    min_times = merged_data.groupby(['h3_id', 'res', 'brand_id'], as_index=False)['time_s'].min()

    # 4. Pivot the table to create the wide format for the frontend
    # Rows: h3_id, res. Columns: <brand>_drive_min for every observed brand
    pivot = min_times.pivot_table(
        index=['h3_id', 'res'],
        columns='brand_id',
        values='time_s'
    ).reset_index()

    # Merge the pivot back onto the full base set of hexes to ensure coverage
    final_wide = pd.merge(base_hexes, pivot, on=["h3_id", "res"], how="left")

    # Convert seconds to integer minutes (rounding up), rename columns
    # Note: Some brands may be entirely absent at a given resolution; handled implicitly.
    brand_cols = [c for c in final_wide.columns if c not in ('h3_id', 'res')]
    for brand in brand_cols:
        minutes_col = f"{brand}_drive_min"
        final_wide[minutes_col] = (final_wide[brand] / 60).apply(np.ceil).astype('Int16')
        final_wide = final_wide.drop(columns=[brand])

    # 5. Optionally merge brand overlays (nearest-time guarantees)
    #    Layout: data/overlays/mode={0|2}/brand_id=<id>/part-*.parquet with seconds_u16
    overlays_base = os.path.join("data", "overlays", "mode=0")
    if os.path.isdir(overlays_base):
        # Gather available overlay brand ids
        overlay_dirs = [d for d in glob.glob(os.path.join(overlays_base, "brand_id=*")) if os.path.isdir(d)]
        for d in overlay_dirs:
            bid = os.path.basename(d).split("=", 1)[1]
            parts = glob.glob(os.path.join(d, "*.parquet"))
            if not parts:
                continue
            try:
                odf = pd.concat([pd.read_parquet(p, columns=["h3_id", "res", "seconds_u16"]) for p in parts], ignore_index=True)
            except Exception:
                continue
            odf = odf.rename(columns={"seconds_u16": f"{bid}_overlay_s"})
            # Left join onto wide frame on (h3_id,res)
            before_cols = set(final_wide.columns)
            final_wide = pd.merge(final_wide, odf, on=["h3_id", "res"], how="left")
            # Convert overlay seconds to minutes (ceil)
            overlay_min_col = f"{bid}_overlay_min"
            if f"{bid}_overlay_s" in final_wide.columns:
                final_wide[overlay_min_col] = (final_wide[f"{bid}_overlay_s"] / 60.0).apply(np.ceil).astype("Int16")
                final_wide = final_wide.drop(columns=[f"{bid}_overlay_s"])
            # If brand minutes already exist, take min(existing, overlay); else set to overlay
            brand_min_col = f"{bid}_drive_min"
            if overlay_min_col in final_wide.columns:
                if brand_min_col in final_wide.columns:
                    final_wide[brand_min_col] = final_wide[[brand_min_col, overlay_min_col]].min(axis=1, skipna=True)
                else:
                    final_wide[brand_min_col] = final_wide[overlay_min_col]
                final_wide = final_wide.drop(columns=[overlay_min_col])

    # 6. Split by resolution and save
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
