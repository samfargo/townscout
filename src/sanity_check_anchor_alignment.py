#!/usr/bin/env python3
"""
Sanity check to verify that T_hex and D_anchor matrices use consistent anchor_id mappings.
Run this after both matrices are built to catch any alignment issues early.
"""
import pandas as pd
import sys
import os

def check_alignment(mode="drive"):
    """Check alignment for a specific mode (drive or walk)"""
    print(f"\n=== Checking {mode} mode alignment ===")
    
    # File paths
    index_file = f"out/anchors/anchor_index_{mode}.parquet"
    t_hex_file = f"data/minutes/massachusetts_hex_to_anchor_{mode}.parquet"
    d_anchor_file = f"data/minutes/massachusetts_anchor_to_category_{mode}.parquet"
    
    # Check if files exist
    missing_files = []
    for file_path, name in [(index_file, "anchor index"), 
                           (t_hex_file, "T_hex"), 
                           (d_anchor_file, "D_anchor")]:
        if not os.path.exists(file_path):
            missing_files.append(f"{name}: {file_path}")
    
    if missing_files:
        print(f"❌ Missing files for {mode} mode:")
        for missing in missing_files:
            print(f"   - {missing}")
        return False
    
    # Load files
    try:
        index_df = pd.read_parquet(index_file)
        t_hex_df = pd.read_parquet(t_hex_file)
        d_anchor_df = pd.read_parquet(d_anchor_file)
    except Exception as e:
        print(f"❌ Error loading files for {mode} mode: {e}")
        return False
    
    # Check index structure
    expected_index_cols = {"anchor_id", "mode", "node_id"}
    if set(index_df.columns) != expected_index_cols:
        print(f"❌ Index file has wrong columns. Expected {expected_index_cols}, got {set(index_df.columns)}")
        return False
    
    # Check that anchor_id exists in both matrices
    if "anchor_id" not in t_hex_df.columns:
        print(f"❌ T_hex missing anchor_id column. Columns: {list(t_hex_df.columns)}")
        return False
    
    if "anchor_id" not in d_anchor_df.columns:
        print(f"❌ D_anchor missing anchor_id column. Columns: {list(d_anchor_df.columns)}")
        return False
    
    # Check alignment
    valid_anchor_ids = set(index_df["anchor_id"])
    t_hex_anchor_ids = set(t_hex_df["anchor_id"])
    d_anchor_anchor_ids = set(d_anchor_df["anchor_id"])
    
    # T_hex alignment
    invalid_t_hex = t_hex_anchor_ids - valid_anchor_ids
    if invalid_t_hex:
        print(f"❌ T_hex has invalid anchor_ids: {invalid_t_hex}")
        return False
    
    # D_anchor alignment  
    invalid_d_anchor = d_anchor_anchor_ids - valid_anchor_ids
    if invalid_d_anchor:
        print(f"❌ D_anchor has invalid anchor_ids: {invalid_d_anchor}")
        return False
    
    # Success statistics
    print(f"✅ {mode} mode alignment is consistent:")
    print(f"   - Index has {len(index_df):,} anchors")
    print(f"   - T_hex uses {len(t_hex_anchor_ids):,} unique anchor_ids")
    print(f"   - D_anchor uses {len(d_anchor_anchor_ids):,} unique anchor_ids")
    print(f"   - All anchor_ids are valid")
    
    return True

def main():
    print("🔍 Checking anchor_id alignment across T_hex and D_anchor matrices...")
    
    success = True
    for mode in ["drive", "walk"]:
        if not check_alignment(mode):
            success = False
    
    if success:
        print("\n🎉 All anchor_id alignments are consistent!")
        print("✅ Your T_hex and D_anchor matrices will join correctly.")
    else:
        print("\n💥 Anchor_id alignment issues detected!")
        print("❌ Fix these before proceeding - your hex→category joins will be wrong.")
        sys.exit(1)

if __name__ == "__main__":
    main() 