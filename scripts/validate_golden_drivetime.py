#!/usr/bin/env python3
"""
Validate Golden Drive-Time Dataset

Compares hand-verified drive times against computed T_hex + D_anchor values.
Fails if any computed value deviates from expected by more than tolerance.

This script should be run after every `make minutes` or `make d_anchor*` run.
"""
import sys
import csv
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import pandas as pd
import numpy as np


GOLDEN_DATASET_PATH = Path("data/golden_drivetime.csv")
UNREACHABLE_SENTINEL = 65535


def load_golden_dataset() -> List[Dict[str, str]]:
    """Load golden dataset entries."""
    if not GOLDEN_DATASET_PATH.exists():
        print(f"WARNING: Golden dataset not found at {GOLDEN_DATASET_PATH}")
        return []
    
    entries = []
    with open(GOLDEN_DATASET_PATH, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Skip comment lines
            if row.get('h3_id', '').startswith('#'):
                continue
            # Skip empty rows
            if not row.get('h3_id') or not row.get('category_id'):
                continue
            entries.append(row)
    
    return entries


def load_t_hex_data() -> Dict[str, pd.DataFrame]:
    """Load T_hex data (hex -> anchor travel times)."""
    t_hex_data = {}
    
    # Try state_tiles first (wide format)
    state_tiles = Path("state_tiles")
    if state_tiles.exists():
        for parquet_file in state_tiles.glob("*.parquet"):
            df = pd.read_parquet(parquet_file)
            # Convert h3_id to string for matching
            if 'h3_id' in df.columns:
                df['h3_id'] = df['h3_id'].astype(str)
                mode = "drive" if "r8" in parquet_file.name else "drive"  # Could be inferred better
                t_hex_data[mode] = df
    
    # Fallback to minutes directory (long format)
    if not t_hex_data:
        minutes_dir = Path("data/minutes")
        if minutes_dir.exists():
            for parquet_file in minutes_dir.glob("*_t_hex.parquet"):
                df = pd.read_parquet(parquet_file)
                if 'h3_id' in df.columns:
                    df['h3_id'] = df['h3_id'].astype(str)
                    mode = "drive" if "drive" in parquet_file.name else "walk"
                    if mode not in t_hex_data:
                        t_hex_data[mode] = []
                    t_hex_data[mode].append(df)
        
        # Concatenate long format data
        for mode in list(t_hex_data.keys()):
            if isinstance(t_hex_data[mode], list):
                t_hex_data[mode] = pd.concat(t_hex_data[mode], ignore_index=True)
    
    return t_hex_data


def load_d_anchor_data() -> Dict[Tuple[str, str], pd.DataFrame]:
    """
    Load D_anchor data (anchor -> POI category travel times).
    
    Returns dict keyed by (mode, category_id).
    """
    d_anchor_data = {}
    
    # Category D_anchor
    category_dir = Path("data/d_anchor_category")
    if category_dir.exists():
        for mode_dir in category_dir.glob("mode=*"):
            mode_code = mode_dir.name.split("=")[1]
            mode = "drive" if mode_code == "0" else "walk"
            
            for cat_dir in mode_dir.glob("category_id=*"):
                category_id = cat_dir.name.split("=")[1]
                
                # Load all parquet files in this partition
                dfs = []
                for parquet_file in cat_dir.glob("*.parquet"):
                    df = pd.read_parquet(parquet_file)
                    dfs.append(df)
                
                if dfs:
                    combined = pd.concat(dfs, ignore_index=True)
                    d_anchor_data[(mode, category_id)] = combined
    
    return d_anchor_data


def compute_travel_time_wide_format(
    h3_id: str,
    category_id: str,
    t_hex_df: pd.DataFrame,
    d_anchor_data: Dict[Tuple[str, str], pd.DataFrame],
    mode: str = "drive"
) -> Optional[int]:
    """
    Compute travel time for wide-format T_hex data (a{i}_id, a{i}_s columns).
    
    Returns total seconds or None if unreachable.
    """
    # Get hex row
    hex_row = t_hex_df[t_hex_df['h3_id'] == h3_id]
    
    if hex_row.empty:
        return None
    
    hex_row = hex_row.iloc[0]
    
    # Get D_anchor for this category
    d_anchor_key = (mode, category_id)
    if d_anchor_key not in d_anchor_data:
        return None
    
    d_anchor_df = d_anchor_data[d_anchor_key]
    
    # Build anchor_id -> time mapping
    anchor_col = "anchor_int_id" if "anchor_int_id" in d_anchor_df.columns else "anchor_id"
    time_col = "time_s" if "time_s" in d_anchor_df.columns else "seconds"
    
    anchor_map = dict(zip(d_anchor_df[anchor_col], d_anchor_df[time_col]))
    
    # Compute min travel time across K anchors
    min_time = UNREACHABLE_SENTINEL
    
    for i in range(20):  # K=20 anchors
        anchor_id_col = f"a{i}_id"
        anchor_time_col = f"a{i}_s"
        
        if anchor_id_col not in hex_row.index or anchor_time_col not in hex_row.index:
            continue
        
        anchor_id = hex_row[anchor_id_col]
        hex_to_anchor_sec = hex_row[anchor_time_col]
        
        if pd.isna(anchor_id) or pd.isna(hex_to_anchor_sec):
            continue
        
        anchor_to_dest_sec = anchor_map.get(int(anchor_id), UNREACHABLE_SENTINEL)
        total_sec = int(hex_to_anchor_sec) + int(anchor_to_dest_sec)
        
        if total_sec < min_time:
            min_time = total_sec
    
    return min_time if min_time < UNREACHABLE_SENTINEL else None


def compute_travel_time_long_format(
    h3_id: str,
    category_id: str,
    t_hex_df: pd.DataFrame,
    d_anchor_data: Dict[Tuple[str, str], pd.DataFrame],
    mode: str = "drive"
) -> Optional[int]:
    """
    Compute travel time for long-format T_hex data.
    
    Returns total seconds or None if unreachable.
    """
    # Get hex -> anchor times
    hex_anchors = t_hex_df[t_hex_df['h3_id'] == h3_id]
    
    if hex_anchors.empty:
        return None
    
    # Get D_anchor for this category
    d_anchor_key = (mode, category_id)
    if d_anchor_key not in d_anchor_data:
        return None
    
    d_anchor_df = d_anchor_data[d_anchor_key]
    
    # Build anchor_id -> time mapping
    anchor_col = "anchor_int_id" if "anchor_int_id" in d_anchor_df.columns else "anchor_id"
    time_col = "time_s" if "time_s" in d_anchor_df.columns else "seconds"
    
    anchor_map = dict(zip(d_anchor_df[anchor_col], d_anchor_df[time_col]))
    
    # Compute min travel time
    min_time = UNREACHABLE_SENTINEL
    
    for _, row in hex_anchors.iterrows():
        anchor_id = row['anchor_int_id']
        hex_to_anchor_sec = row['time_s']
        
        anchor_to_dest_sec = anchor_map.get(int(anchor_id), UNREACHABLE_SENTINEL)
        total_sec = int(hex_to_anchor_sec) + int(anchor_to_dest_sec)
        
        if total_sec < min_time:
            min_time = total_sec
    
    return min_time if min_time < UNREACHABLE_SENTINEL else None


def main():
    """Main function to validate golden drive times."""
    print("=" * 80)
    print("GOLDEN DRIVE-TIME VALIDATION")
    print("=" * 80)
    print()
    
    # Load golden dataset
    print("[1/4] Loading golden dataset...")
    golden_entries = load_golden_dataset()
    
    if not golden_entries:
        print("  ⚠ WARNING: No golden dataset entries found")
        print("  Skipping validation (not a failure)")
        return 0
    
    print(f"  → Found {len(golden_entries)} golden entries")
    print()
    
    # Load T_hex data
    print("[2/4] Loading T_hex data...")
    t_hex_data = load_t_hex_data()
    
    if not t_hex_data:
        print("  ERROR: No T_hex data found")
        return 1
    
    print(f"  → Loaded T_hex data for modes: {list(t_hex_data.keys())}")
    print()
    
    # Load D_anchor data
    print("[3/4] Loading D_anchor data...")
    d_anchor_data = load_d_anchor_data()
    
    if not d_anchor_data:
        print("  ERROR: No D_anchor data found")
        return 1
    
    print(f"  → Loaded D_anchor data for {len(d_anchor_data)} (mode, category) pairs")
    print()
    
    # Validate each golden entry
    print("[4/4] Validating golden entries...")
    print()
    
    all_passed = True
    failures = []
    
    for entry in golden_entries:
        h3_id = entry['h3_id']
        category_id = entry['category_id']
        expected_sec = int(entry['expected_seconds'])
        tolerance_sec = int(entry['tolerance_seconds'])
        notes = entry.get('notes', '')
        
        # Assume drive mode for now (could be extended)
        mode = "drive"
        
        # Try to compute travel time
        computed_sec = None
        
        if mode in t_hex_data:
            t_hex_df = t_hex_data[mode]
            
            # Check if wide or long format
            if 'a0_id' in t_hex_df.columns:
                # Wide format
                computed_sec = compute_travel_time_wide_format(
                    h3_id, category_id, t_hex_df, d_anchor_data, mode
                )
            elif 'anchor_int_id' in t_hex_df.columns:
                # Long format
                computed_sec = compute_travel_time_long_format(
                    h3_id, category_id, t_hex_df, d_anchor_data, mode
                )
        
        # Compare to expected
        if computed_sec is None:
            status = "✗ FAIL"
            error = "Could not compute travel time (hex or category not found)"
            all_passed = False
            failures.append((h3_id, category_id, notes, error))
        else:
            delta = abs(computed_sec - expected_sec)
            
            if delta <= tolerance_sec:
                status = "✓ PASS"
                error = None
            else:
                status = "✗ FAIL"
                error = f"Delta {delta}s exceeds tolerance {tolerance_sec}s"
                all_passed = False
                failures.append((h3_id, category_id, notes, error))
        
        print(f"{status} {h3_id[:12]}... → {category_id}")
        print(f"     Expected: {expected_sec}s  |  Computed: {computed_sec}s")
        
        if notes:
            print(f"     Notes: {notes}")
        
        if error:
            print(f"     ERROR: {error}")
        
        print()
    
    # Summary
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    
    if all_passed:
        print("✓ ALL GOLDEN ENTRIES VALIDATED")
        print(f"  • {len(golden_entries)} entries passed")
        print(f"  • Max delta within tolerance")
        return 0
    else:
        print("✗ VALIDATION FAILED")
        print(f"  • {len(failures)} / {len(golden_entries)} entries failed")
        print()
        print("Failures:")
        for h3_id, category_id, notes, error in failures:
            print(f"  • {h3_id[:12]}... → {category_id}")
            if notes:
                print(f"    {notes}")
            print(f"    {error}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

