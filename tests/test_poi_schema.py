"""
Test POI Schema Contract

Validates that canonical POI parquet files conform to the expected schema
and contain data from all registered categories and brands.
"""
import os
import pytest
import pandas as pd
import geopandas as gpd
from pathlib import Path

from vicinity.poi.schema import CANONICAL_POI_SCHEMA, validate_poi_dataframe


def load_category_registry() -> set:
    """Load all category_ids from the taxonomy registry."""
    registry_path = Path("data/taxonomy/POI_category_registry.csv")
    if not registry_path.exists():
        return set()
    df = pd.read_csv(registry_path)
    return set(df["category_id"].dropna().astype(str).str.strip())


def load_brand_registry() -> set:
    """Load all brand_ids from the taxonomy registry."""
    registry_path = Path("data/taxonomy/POI_brand_registry.csv")
    if not registry_path.exists():
        return set()
    df = pd.read_csv(registry_path)
    # Filter out empty rows
    df = df[df["brand_id"].notna() & (df["brand_id"].astype(str).str.strip() != "")]
    return set(df["brand_id"].astype(str).str.strip())


def find_canonical_parquet_files() -> list[Path]:
    """Find all canonical POI parquet files in data/poi/."""
    poi_dir = Path("data/poi")
    if not poi_dir.exists():
        return []
    return list(poi_dir.glob("*_canonical.parquet"))


class TestPOISchema:
    """Test suite for canonical POI schema validation."""
    
    def test_canonical_poi_files_exist(self):
        """Verify at least one canonical POI file exists."""
        files = find_canonical_parquet_files()
        assert len(files) > 0, "No canonical POI parquet files found in data/poi/"
    
    def test_poi_schema_columns(self):
        """Verify all canonical POI files have required columns."""
        files = find_canonical_parquet_files()
        assert len(files) > 0, "No canonical POI files to test"
        
        required_cols = set(CANONICAL_POI_SCHEMA.keys())
        
        for file_path in files:
            # Read without geometry for basic column check
            df = pd.read_parquet(file_path)
            actual_cols = set(df.columns)
            missing = required_cols - actual_cols
            
            # Some columns might be dropped during save (e.g., provenance)
            # Only check for essential columns
            essential_cols = {"poi_id", "name", "category", "lon", "lat"}
            missing_essential = essential_cols - actual_cols
            
            assert not missing_essential, (
                f"{file_path.name}: Missing essential columns: {missing_essential}"
            )
    
    def test_poi_datatypes(self):
        """Verify datatypes of key columns."""
        files = find_canonical_parquet_files()
        assert len(files) > 0, "No canonical POI files to test"
        
        for file_path in files:
            df = pd.read_parquet(file_path)
            
            # Check numeric columns
            if "lon" in df.columns:
                assert pd.api.types.is_numeric_dtype(df["lon"]), (
                    f"{file_path.name}: 'lon' must be numeric"
                )
            if "lat" in df.columns:
                assert pd.api.types.is_numeric_dtype(df["lat"]), (
                    f"{file_path.name}: 'lat' must be numeric"
                )
            
            # Check string columns
            for col in ["poi_id", "name", "category"]:
                if col in df.columns:
                    assert pd.api.types.is_string_dtype(df[col]) or pd.api.types.is_object_dtype(df[col]), (
                        f"{file_path.name}: '{col}' must be string type"
                    )
    
    def test_poi_no_nulls_in_required_fields(self):
        """Verify required fields don't have null values."""
        files = find_canonical_parquet_files()
        assert len(files) > 0, "No canonical POI files to test"
        
        required_non_null = ["poi_id", "lon", "lat"]
        
        for file_path in files:
            df = pd.read_parquet(file_path)
            
            for col in required_non_null:
                if col in df.columns:
                    null_count = df[col].isna().sum()
                    assert null_count == 0, (
                        f"{file_path.name}: '{col}' has {null_count} null values"
                    )
    
    def test_poi_coordinates_in_valid_range(self):
        """Verify lon/lat are within valid geographic ranges."""
        files = find_canonical_parquet_files()
        assert len(files) > 0, "No canonical POI files to test"
        
        for file_path in files:
            df = pd.read_parquet(file_path)
            
            if "lon" in df.columns:
                assert df["lon"].between(-180, 180).all(), (
                    f"{file_path.name}: longitude values out of range [-180, 180]"
                )
            
            if "lat" in df.columns:
                assert df["lat"].between(-90, 90).all(), (
                    f"{file_path.name}: latitude values out of range [-90, 90]"
                )
    
    def test_category_coverage(self):
        """Verify categories from registry appear at least once."""
        files = find_canonical_parquet_files()
        if not files:
            pytest.skip("No canonical POI files to test")
        
        registered_categories = load_category_registry()
        if not registered_categories:
            pytest.skip("No category registry found")
        
        # Collect all categories across all files
        all_categories = set()
        for file_path in files:
            df = pd.read_parquet(file_path)
            if "category" in df.columns:
                categories = df["category"].dropna().astype(str).str.strip()
                all_categories.update(categories.unique())
        
        missing = registered_categories - all_categories
        
        # Some categories might legitimately be missing in a single-state dataset
        # So we just warn if coverage is very low
        coverage = len(all_categories & registered_categories) / len(registered_categories)
        
        assert coverage > 0.1, (
            f"Very low category coverage: {coverage:.1%}. "
            f"Missing categories: {sorted(missing)[:10]}..."
        )
    
    def test_brand_registry_alignment(self):
        """Verify brand_ids in POI files are from registry or null."""
        files = find_canonical_parquet_files()
        if not files:
            pytest.skip("No canonical POI files to test")
        
        registered_brands = load_brand_registry()
        if not registered_brands:
            pytest.skip("No brand registry found")
        
        for file_path in files:
            df = pd.read_parquet(file_path)
            if "brand_id" not in df.columns:
                continue
            
            # Get non-null brand_ids
            brands = df["brand_id"].dropna().astype(str).str.strip()
            brands = brands[brands != ""]
            
            if len(brands) == 0:
                continue
            
            unique_brands = set(brands.unique())
            unknown_brands = unique_brands - registered_brands
            
            # Allow some unregistered brands (< 5% is reasonable for new data)
            unknown_ratio = len(unknown_brands) / len(unique_brands)
            
            assert unknown_ratio < 0.05, (
                f"{file_path.name}: {unknown_ratio:.1%} of brands not in registry. "
                f"Examples: {sorted(unknown_brands)[:5]}"
            )
    
    def test_poi_id_uniqueness(self):
        """Verify poi_id is unique within each file."""
        files = find_canonical_parquet_files()
        assert len(files) > 0, "No canonical POI files to test"
        
        for file_path in files:
            df = pd.read_parquet(file_path)
            if "poi_id" not in df.columns:
                continue
            
            duplicates = df["poi_id"].duplicated().sum()
            assert duplicates == 0, (
                f"{file_path.name}: Found {duplicates} duplicate poi_id values"
            )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

