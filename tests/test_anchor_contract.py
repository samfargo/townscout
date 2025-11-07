"""
Test Anchor Site Contract

Validates that anchor site parquet files maintain required invariants:
- site_id uniqueness
- allowed transport modes
- every anchor has at least one POI
- node_id references are valid
"""
import os
import pytest
import pandas as pd
from pathlib import Path


ALLOWED_MODES = {"drive", "walk"}
# Note: mode is often encoded in filename (_drive_sites.parquet) rather than as a column
REQUIRED_COLUMNS = {"site_id", "node_id", "lon", "lat", "poi_ids", "categories"}


def find_anchor_site_files() -> list[Path]:
    """Find all anchor site parquet files."""
    # Check both locations
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
    
    return list(set(candidates))  # Remove duplicates


class TestAnchorContract:
    """Test suite for anchor site validation."""
    
    def test_anchor_files_exist(self):
        """Verify at least one anchor site file exists."""
        files = find_anchor_site_files()
        assert len(files) > 0, "No anchor site parquet files found"
    
    def test_anchor_required_columns(self):
        """Verify all anchor files have required columns."""
        files = find_anchor_site_files()
        assert len(files) > 0, "No anchor files to test"
        
        for file_path in files:
            df = pd.read_parquet(file_path)
            actual_cols = set(df.columns)
            missing = REQUIRED_COLUMNS - actual_cols
            
            assert not missing, (
                f"{file_path.name}: Missing required columns: {missing}"
            )
    
    def test_site_id_uniqueness(self):
        """Verify site_id is unique within each file."""
        files = find_anchor_site_files()
        assert len(files) > 0, "No anchor files to test"
        
        for file_path in files:
            df = pd.read_parquet(file_path)
            
            if "site_id" not in df.columns:
                pytest.fail(f"{file_path.name}: Missing 'site_id' column")
            
            duplicates = df["site_id"].duplicated().sum()
            assert duplicates == 0, (
                f"{file_path.name}: Found {duplicates} duplicate site_id values"
            )
    
    def test_allowed_modes(self):
        """Verify mode column contains only allowed values, or mode is in filename."""
        files = find_anchor_site_files()
        assert len(files) > 0, "No anchor files to test"
        
        for file_path in files:
            df = pd.read_parquet(file_path)
            
            # Mode can be in column or encoded in filename
            if "mode" in df.columns:
                modes = set(df["mode"].unique())
                invalid = modes - ALLOWED_MODES
                
                assert not invalid, (
                    f"{file_path.name}: Invalid mode values: {invalid}. "
                    f"Allowed: {ALLOWED_MODES}"
                )
            else:
                # Check if mode is in filename
                filename = file_path.name.lower()
                has_mode_in_name = any(mode in filename for mode in ALLOWED_MODES)
                
                assert has_mode_in_name, (
                    f"{file_path.name}: Mode not found in column or filename. "
                    f"Expected one of {ALLOWED_MODES} in filename."
                )
    
    def test_anchors_have_pois(self):
        """Verify every anchor has at least one POI."""
        files = find_anchor_site_files()
        assert len(files) > 0, "No anchor files to test"
        
        for file_path in files:
            df = pd.read_parquet(file_path)
            
            if "poi_ids" not in df.columns:
                pytest.skip(f"{file_path.name}: No 'poi_ids' column")
            
            # Check that poi_ids is not empty
            empty_count = 0
            for idx, poi_ids in enumerate(df["poi_ids"]):
                if poi_ids is None or (isinstance(poi_ids, (list, tuple)) and len(poi_ids) == 0):
                    empty_count += 1
            
            assert empty_count == 0, (
                f"{file_path.name}: Found {empty_count} anchors with no POIs"
            )
    
    def test_valid_coordinates(self):
        """Verify anchor coordinates are in valid range."""
        files = find_anchor_site_files()
        assert len(files) > 0, "No anchor files to test"
        
        for file_path in files:
            df = pd.read_parquet(file_path)
            
            if "lon" in df.columns:
                assert df["lon"].between(-180, 180).all(), (
                    f"{file_path.name}: longitude values out of range"
                )
            
            if "lat" in df.columns:
                assert df["lat"].between(-90, 90).all(), (
                    f"{file_path.name}: latitude values out of range"
                )
    
    def test_node_id_validity(self):
        """Verify node_id is non-negative integer."""
        files = find_anchor_site_files()
        assert len(files) > 0, "No anchor files to test"
        
        for file_path in files:
            df = pd.read_parquet(file_path)
            
            if "node_id" not in df.columns:
                continue
            
            # Check that node_id is numeric
            assert pd.api.types.is_integer_dtype(df["node_id"]), (
                f"{file_path.name}: node_id must be integer type"
            )
            
            # Check non-negative
            negative_count = (df["node_id"] < 0).sum()
            assert negative_count == 0, (
                f"{file_path.name}: Found {negative_count} negative node_id values"
            )
    
    def test_categories_non_empty(self):
        """Verify categories list is non-empty for each anchor."""
        files = find_anchor_site_files()
        assert len(files) > 0, "No anchor files to test"
        
        for file_path in files:
            df = pd.read_parquet(file_path)
            
            if "categories" not in df.columns:
                continue
            
            empty_count = 0
            for categories in df["categories"]:
                if categories is None or (isinstance(categories, (list, tuple)) and len(categories) == 0):
                    empty_count += 1
            
            assert empty_count == 0, (
                f"{file_path.name}: Found {empty_count} anchors with no categories"
            )
    
    def test_anchor_int_id_if_present(self):
        """If anchor_int_id exists, verify it's unique and non-negative."""
        files = find_anchor_site_files()
        assert len(files) > 0, "No anchor files to test"
        
        for file_path in files:
            df = pd.read_parquet(file_path)
            
            if "anchor_int_id" not in df.columns:
                continue
            
            # Check uniqueness
            duplicates = df["anchor_int_id"].duplicated().sum()
            assert duplicates == 0, (
                f"{file_path.name}: Found {duplicates} duplicate anchor_int_id values"
            )
            
            # Check non-negative
            negative_count = (df["anchor_int_id"] < 0).sum()
            assert negative_count == 0, (
                f"{file_path.name}: Found {negative_count} negative anchor_int_id values"
            )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

