#!/usr/bin/env python3
"""
Check D_anchor Statistics

Validates D_anchor shard files by:
1. Verifying all anchor IDs exist in anchor site files
2. Computing P50/P95 travel time statistics
3. Enforcing P95 <= 7200s (2 hours) threshold
4. Reporting coverage and quality metrics
"""
import sys
import os
from pathlib import Path
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple


UNREACHABLE_SENTINEL = 65535
MAX_P95_SECONDS = 7200  # 2 hours


def find_d_anchor_shards() -> Dict[str, List[Path]]:
    """Find all D_anchor parquet shards organized by type (category/brand)."""
    shards = {
        "category": [],
        "brand": []
    }
    
    # Category shards
    category_dir = Path("data/d_anchor_category")
    if category_dir.exists():
        for mode_dir in category_dir.glob("mode=*"):
            for category_dir in mode_dir.glob("category_id=*"):
                shards["category"].extend(category_dir.glob("*.parquet"))
    
    # Brand shards
    brand_dir = Path("data/d_anchor_brand")
    if brand_dir.exists():
        for mode_dir in brand_dir.glob("mode=*"):
            for brand_dir_path in mode_dir.glob("brand_id=*"):
                shards["brand"].extend(brand_dir_path.glob("*.parquet"))
    
    return shards


def load_valid_anchor_ids() -> set:
    """Load all valid anchor_int_id values from anchor site files."""
    candidates = []
    
    anchors_dir = Path("data/anchors")
    if anchors_dir.exists():
        candidates.extend(anchors_dir.glob("*_sites.parquet"))
        candidates.extend(anchors_dir.glob("*_drive_sites.parquet"))
        candidates.extend(anchors_dir.glob("*_walk_sites.parquet"))
    
    minutes_dir = Path("data/minutes")
    if minutes_dir.exists():
        candidates.extend(minutes_dir.glob("*_sites.parquet"))
        candidates.extend(minutes_dir.glob("*_drive_sites.parquet"))
        candidates.extend(minutes_dir.glob("*_walk_sites.parquet"))
    
    all_anchor_ids = set()
    for file_path in candidates:
        df = pd.read_parquet(file_path)
        
        if "anchor_int_id" in df.columns:
            anchor_ids = df["anchor_int_id"].dropna().astype(np.int32)
            all_anchor_ids.update(anchor_ids.tolist())
        else:
            # Generate from index if not present
            all_anchor_ids.update(range(len(df)))
    
    return all_anchor_ids


def compute_shard_statistics(shard_path: Path, valid_anchor_ids: set) -> Tuple[bool, Dict]:
    """
    Compute statistics for a single D_anchor shard.
    
    Returns:
        (passed, stats_dict)
    """
    df = pd.read_parquet(shard_path)
    
    # Expected columns: anchor_int_id, time_s (or seconds)
    time_col = None
    if "time_s" in df.columns:
        time_col = "time_s"
    elif "seconds" in df.columns:
        time_col = "seconds"
    else:
        return False, {"error": "Missing time column (time_s or seconds)"}
    
    anchor_col = None
    if "anchor_int_id" in df.columns:
        anchor_col = "anchor_int_id"
    elif "anchor_id" in df.columns:
        anchor_col = "anchor_id"
    else:
        return False, {"error": "Missing anchor column (anchor_int_id or anchor_id)"}
    
    # Check for orphan anchors
    referenced_anchors = set(df[anchor_col].astype(np.int32).unique())
    orphans = referenced_anchors - valid_anchor_ids
    
    # Filter out sentinel values for statistics
    real_times = df[df[time_col] < UNREACHABLE_SENTINEL][time_col]
    
    stats = {
        "total_records": len(df),
        "unique_anchors": len(referenced_anchors),
        "orphan_anchors": len(orphans),
        "sentinel_count": (df[time_col] == UNREACHABLE_SENTINEL).sum(),
        "sentinel_ratio": (df[time_col] == UNREACHABLE_SENTINEL).sum() / len(df) if len(df) > 0 else 0,
    }
    
    if len(real_times) > 0:
        stats["min_seconds"] = int(real_times.min())
        stats["p50_seconds"] = int(np.percentile(real_times, 50))
        stats["p95_seconds"] = int(np.percentile(real_times, 95))
        stats["max_seconds"] = int(real_times.max())
    else:
        stats["min_seconds"] = None
        stats["p50_seconds"] = None
        stats["p95_seconds"] = None
        stats["max_seconds"] = None
    
    # Determine if shard passes validation
    passed = True
    
    if len(orphans) > 0:
        passed = False
        stats["error"] = f"Found {len(orphans)} orphan anchor IDs"
    
    if stats["p95_seconds"] is not None and stats["p95_seconds"] > MAX_P95_SECONDS:
        passed = False
        stats["error"] = stats.get("error", "") + f" P95 ({stats['p95_seconds']}s) exceeds threshold ({MAX_P95_SECONDS}s)"
    
    return passed, stats


def main():
    """Main function to check D_anchor statistics."""
    print("=" * 80)
    print("D_ANCHOR STATISTICS CHECK")
    print("=" * 80)
    print()
    
    # Load valid anchor IDs
    print("[1/3] Loading valid anchor IDs...")
    valid_anchor_ids = load_valid_anchor_ids()
    
    if not valid_anchor_ids:
        print("ERROR: No anchor site files found. Cannot validate D_anchor references.")
        return 1
    
    print(f"  → Found {len(valid_anchor_ids):,} valid anchor IDs")
    print()
    
    # Find D_anchor shards
    print("[2/3] Finding D_anchor shards...")
    shards = find_d_anchor_shards()
    
    total_shards = len(shards["category"]) + len(shards["brand"])
    if total_shards == 0:
        print("ERROR: No D_anchor shards found in data/d_anchor_category or data/d_anchor_brand")
        return 1
    
    print(f"  → Found {len(shards['category'])} category shards")
    print(f"  → Found {len(shards['brand'])} brand shards")
    print()
    
    # Validate each shard
    print("[3/3] Validating shards...")
    print()
    
    all_passed = True
    failures = []
    
    for shard_type, shard_list in shards.items():
        if not shard_list:
            continue
        
        print(f"--- {shard_type.upper()} SHARDS ---")
        
        for shard_path in sorted(shard_list):
            shard_name = f"{shard_path.parent.parent.name}/{shard_path.parent.name}/{shard_path.name}"
            
            passed, stats = compute_shard_statistics(shard_path, valid_anchor_ids)
            
            status = "✓ PASS" if passed else "✗ FAIL"
            print(f"{status} {shard_name}")
            
            if stats.get("p50_seconds") is not None and stats.get("p95_seconds") is not None:
                print(f"     P50: {stats['p50_seconds']:>6}s  |  P95: {stats['p95_seconds']:>6}s")
            
            if stats.get("sentinel_ratio", 0) > 0:
                print(f"     Sentinel: {stats['sentinel_ratio']:.1%} ({stats['sentinel_count']:,} / {stats['total_records']:,})")
            
            if not passed:
                all_passed = False
                failures.append((shard_name, stats.get("error", "Unknown error")))
                print(f"     ERROR: {stats.get('error')}")
            
            print()
    
    # Summary
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    
    if all_passed:
        print("✓ ALL CHECKS PASSED")
        print(f"  • {total_shards} shards validated")
        print(f"  • All P95 times <= {MAX_P95_SECONDS}s")
        print(f"  • No orphan anchor references")
        return 0
    else:
        print("✗ VALIDATION FAILED")
        print(f"  • {len(failures)} / {total_shards} shards failed")
        print()
        print("Failures:")
        for shard_name, error in failures:
            print(f"  • {shard_name}")
            print(f"    {error}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

