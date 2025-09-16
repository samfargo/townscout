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

from src.config import STATES, H3_RES_LOW, H3_RES_HIGH

def main():
    """Main function to merge state data and create summaries."""
    print("--- Merging per-state data and creating summaries ---")
    
    # Use glob to find all per-state outputs from the previous step
    # This makes it easy to add more states by just updating the STATES list.
    drive_time_files = glob.glob("data/minutes/*_drive_t_hex.parquet")
    sites_files = glob.glob("data/minutes/*_drive_sites.parquet")

    if not drive_time_files or not sites_files:
        raise FileNotFoundError("No input files found from step 03. Run 'make minutes' first.")

    print(f"Found {len(drive_time_files)} travel time files and {len(sites_files)} sites files.")

    # 1. Load and concatenate all state data
    all_times = pd.concat([pd.read_parquet(f) for f in drive_time_files], ignore_index=True)
    all_sites = pd.concat([pd.read_parquet(f) for f in sites_files], ignore_index=True)

    # 2. Join travel times with site info to get brand/category data
    # We only need a few columns from the sites table for this step.
    sites_info = all_sites[['site_id', 'brands', 'categories']].copy()
    
    # Explode the 'brands' list so each brand has its own row. This makes joining easier.
    sites_info = sites_info.explode('brands')
    sites_info = sites_info.rename(columns={'brands': 'brand_id'})
    sites_info = sites_info.dropna(subset=['brand_id'])

    merged_data = pd.merge(all_times, sites_info, on='site_id', how='inner')

    # 3. For each hex, calculate the minimum travel time to each brand of interest
    # For the MVP, we only care about chipotle and costco.
    mvp_brands = ['chipotle', 'costco']
    merged_data = merged_data[merged_data['brand_id'].isin(mvp_brands)]

    # Group by hex, resolution, and brand, then find the minimum time.
    min_times = merged_data.groupby(['h3_id', 'res', 'brand_id'])['time_s'].min().reset_index()

    # 4. Pivot the table to create the wide format for the frontend
    # Rows: h3_id, res. Columns: chipotle_drive_min, costco_drive_min
    final_wide = min_times.pivot_table(
        index=['h3_id', 'res'],
        columns='brand_id',
        values='time_s'
    ).reset_index()
    
    # Convert seconds to integer minutes (rounding up)
    for brand in mvp_brands:
        col_name = f"{brand}_drive_min"
        # Check if column exists, as some states might not have all brands
        if brand in final_wide.columns:
            final_wide[col_name] = (final_wide[brand] / 60).apply(np.ceil).astype('Int16')
            final_wide = final_wide.drop(columns=[brand])
        else:
            # If a brand is missing entirely, add a null column for schema consistency
            final_wide[col_name] = pd.NA

    # 5. Split by resolution and save
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
