"""
Merges per-state travel time data and creates nationwide summaries.

Pipeline (anchor-mode only):
1. Load the per-state `t_hex` (long format) parquet files.
2. Concatenate them into a single nationwide file.
3. Build anchor arrays per hex (a{i}_id / a{i}_s) for K best anchors.
4. Save r7 and r8 parquet files for downstream tiling.
"""
import glob
import os
import pandas as pd
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
