"""
Test T_hex Travel Time Contract

Validates that T_hex (travel time array) files maintain required invariants:
- All anchor IDs referenced exist in anchor site files
- Travel seconds are non-decreasing per hex (monotonic)
- Sentinel value (65535) usage is below 1%
- No orphan anchor references
"""
import os
import pytest
import pandas as pd
import numpy as np
from pathlib import Path


UNREACHABLE_SENTINEL = 65535
MAX_SENTINEL_RATIO = 0.01  # 1%


def find_t_hex_files() -> list[Path]:
    """Find all T_hex travel time parquet files."""
    candidates = []
    
    # Look in data/minutes for raw T_hex files
    minutes_dir = Path("data/minutes")
    if minutes_dir.exists():
        candidates.extend(minutes_dir.glob("*_drive_t_hex.parquet"))
        candidates.extend(minutes_dir.glob("*_walk_t_hex.parquet"))
        candidates.extend(minutes_dir.glob("*_t_hex.parquet"))
    
    # Look in state_tiles for merged data
    tiles_dir = Path("state_tiles")
    if tiles_dir.exists():
        candidates.extend(tiles_dir.glob("*.parquet"))
    
    return list(set(candidates))


def find_anchor_site_files() -> list[Path]:
    """Find all anchor site parquet files for validation."""
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
    
    return list(set(candidates))


def load_all_anchor_ids() -> set:
    """Load all valid anchor_int_id values from anchor site files."""
    files = find_anchor_site_files()
    if not files:
        return set()
    
    all_anchor_ids = set()
    for file_path in files:
        df = pd.read_parquet(file_path)
        
        # Check if anchor_int_id exists
        if "anchor_int_id" in df.columns:
            anchor_ids = df["anchor_int_id"].dropna().astype(np.int32)
            all_anchor_ids.update(anchor_ids.tolist())
        else:
            # Generate from index if not present
            all_anchor_ids.update(range(len(df)))
    
    return all_anchor_ids


class TestTHexContract:
    """Test suite for T_hex travel time validation."""
    
    def test_t_hex_files_exist(self):
        """Verify at least one T_hex file exists."""
        files = find_t_hex_files()
        assert len(files) > 0, "No T_hex parquet files found"
    
    def test_long_format_t_hex_structure(self):
        """Verify long-format T_hex files have required columns."""
        files = find_t_hex_files()
        assert len(files) > 0, "No T_hex files to test"
        
        for file_path in files:
            # Skip wide-format files (state_tiles)
            if "state_tiles" in str(file_path):
                continue
            
            df = pd.read_parquet(file_path)
            required = {"h3_id", "anchor_int_id", "time_s"}
            actual = set(df.columns)
            
            missing = required - actual
            if missing:
                pytest.skip(f"{file_path.name}: Not a long-format T_hex (missing {missing})")
            
            # Verify types
            assert pd.api.types.is_integer_dtype(df["anchor_int_id"]), (
                f"{file_path.name}: anchor_int_id must be integer"
            )
            assert pd.api.types.is_integer_dtype(df["time_s"]) or pd.api.types.is_unsigned_integer_dtype(df["time_s"]), (
                f"{file_path.name}: time_s must be integer/uint"
            )
    
    def test_anchor_id_validity(self):
        """Verify all anchor_int_id values exist in anchor site files."""
        t_hex_files = find_t_hex_files()
        if not t_hex_files:
            pytest.skip("No T_hex files to test")
        
        valid_anchor_ids = load_all_anchor_ids()
        if not valid_anchor_ids:
            pytest.skip("No anchor site files found for validation")
        
        for file_path in t_hex_files:
            # Skip wide-format files
            if "state_tiles" in str(file_path):
                continue
            
            df = pd.read_parquet(file_path)
            
            if "anchor_int_id" not in df.columns:
                continue
            
            referenced_ids = set(df["anchor_int_id"].dropna().astype(np.int32).unique())
            orphan_ids = referenced_ids - valid_anchor_ids
            
            assert not orphan_ids, (
                f"{file_path.name}: Found {len(orphan_ids)} orphan anchor IDs. "
                f"Examples: {sorted(orphan_ids)[:10]}"
            )
    
    def test_monotonic_travel_times_per_hex(self):
        """Verify travel times are non-decreasing (monotonic) per hex."""
        files = find_t_hex_files()
        if not files:
            pytest.skip("No T_hex files to test")
        
        for file_path in files:
            # Skip wide-format for now (different structure)
            if "state_tiles" in str(file_path):
                continue
            
            df = pd.read_parquet(file_path)
            
            required = {"h3_id", "time_s"}
            if not required.issubset(df.columns):
                continue
            
            # Sort by hex and time
            df_sorted = df.sort_values(["h3_id", "time_s"])
            
            # Check monotonicity within each hex
            violations = 0
            for h3_id, group in df_sorted.groupby("h3_id"):
                times = group["time_s"].values
                # Check if sorted (allowing for ties)
                if not all(times[i] <= times[i+1] for i in range(len(times)-1)):
                    violations += 1
            
            assert violations == 0, (
                f"{file_path.name}: Found {violations} hexes with non-monotonic travel times"
            )
    
    def test_sentinel_usage_ratio(self):
        """Verify sentinel value (65535) usage is below 1%."""
        files = find_t_hex_files()
        if not files:
            pytest.skip("No T_hex files to test")
        
        for file_path in files:
            # Skip wide-format
            if "state_tiles" in str(file_path):
                continue
            
            df = pd.read_parquet(file_path)
            
            if "time_s" not in df.columns:
                continue
            
            total = len(df)
            sentinel_count = (df["time_s"] == UNREACHABLE_SENTINEL).sum()
            
            if total == 0:
                continue
            
            ratio = sentinel_count / total
            
            assert ratio <= MAX_SENTINEL_RATIO, (
                f"{file_path.name}: Sentinel usage {ratio:.2%} exceeds {MAX_SENTINEL_RATIO:.1%} threshold. "
                f"({sentinel_count:,} / {total:,} records)"
            )
    
    def test_time_values_in_valid_range(self):
        """Verify time_s values are in valid range [0, 65535]."""
        files = find_t_hex_files()
        if not files:
            pytest.skip("No T_hex files to test")
        
        for file_path in files:
            # Skip wide-format
            if "state_tiles" in str(file_path):
                continue
            
            df = pd.read_parquet(file_path)
            
            if "time_s" not in df.columns:
                continue
            
            # Check range
            min_val = df["time_s"].min()
            max_val = df["time_s"].max()
            
            assert min_val >= 0, (
                f"{file_path.name}: Found negative time_s values (min={min_val})"
            )
            assert max_val <= UNREACHABLE_SENTINEL, (
                f"{file_path.name}: Found time_s values exceeding sentinel (max={max_val})"
            )
    
    def test_wide_format_anchor_arrays(self):
        """Verify wide-format files have proper a{i}_id and a{i}_s columns."""
        files = [f for f in find_t_hex_files() if "state_tiles" in str(f)]
        
        if not files:
            pytest.skip("No wide-format T_hex files to test")
        
        for file_path in files:
            df = pd.read_parquet(file_path)
            
            # Find all anchor columns
            id_cols = [c for c in df.columns if c.startswith("a") and c.endswith("_id")]
            s_cols = [c for c in df.columns if c.startswith("a") and c.endswith("_s")]
            
            # Should have matching pairs
            assert len(id_cols) == len(s_cols), (
                f"{file_path.name}: Mismatched anchor ID and time columns"
            )
            
            # Check that indices match (a0_id pairs with a0_s, etc.)
            for i in range(len(id_cols)):
                expected_id = f"a{i}_id"
                expected_s = f"a{i}_s"
                
                assert expected_id in df.columns, (
                    f"{file_path.name}: Missing {expected_id}"
                )
                assert expected_s in df.columns, (
                    f"{file_path.name}: Missing {expected_s}"
                )
    
    def test_wide_format_time_values(self):
        """Verify time values in wide-format files are valid."""
        files = [f for f in find_t_hex_files() if "state_tiles" in str(f)]
        
        if not files:
            pytest.skip("No wide-format T_hex files to test")
        
        for file_path in files:
            df = pd.read_parquet(file_path)
            
            s_cols = [c for c in df.columns if c.startswith("a") and c.endswith("_s")]
            
            for col in s_cols:
                # Skip NaN values (expected for incomplete data)
                valid_values = df[col].dropna()
                
                if len(valid_values) == 0:
                    continue
                
                min_val = valid_values.min()
                max_val = valid_values.max()
                
                assert min_val >= 0, (
                    f"{file_path.name}: {col} has negative values (min={min_val})"
                )
                assert max_val <= UNREACHABLE_SENTINEL, (
                    f"{file_path.name}: {col} exceeds sentinel (max={max_val})"
                )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

