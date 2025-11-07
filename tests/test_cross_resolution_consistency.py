"""
Test Cross-Resolution Consistency

Validates that r7 and r8 tiles have consistent data - if a parent r7 hex
has certain properties, its r8 children should have compatible properties.

This catches issues where zoom-in/zoom-out shows different shading.
"""
import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, Set
import h3
import sys

SRC_DIR = Path("src")
if SRC_DIR.exists():
    src_path = str(SRC_DIR.resolve())
    if src_path not in sys.path:
        sys.path.append(src_path)

from config import H3_RES_LOW, H3_RES_HIGH  # type: ignore


def find_state_tile_files() -> Dict[int, Path]:
    """Find r7 and r8 state tile parquet files."""
    tiles = {}
    
    state_tiles_dir = Path("state_tiles")
    if state_tiles_dir.exists():
        for res in [7, 8]:
            for parquet_file in state_tiles_dir.glob(f"*_r{res}.parquet"):
                tiles[res] = parquet_file
                break
    
    return tiles


class TestCrossResolutionConsistency:
    """Test suite for cross-resolution data consistency."""
    
    def test_both_resolutions_exist(self):
        """Verify both r7 and r8 tile files exist."""
        tiles = find_state_tile_files()
        
        assert 7 in tiles, "Missing r7 tile file in state_tiles/"
        assert 8 in tiles, "Missing r8 tile file in state_tiles/"
    
    def test_h3_parent_child_relationship(self):
        """Verify r8 hexes are valid children of r7 hexes."""
        tiles = find_state_tile_files()
        
        if 7 not in tiles or 8 not in tiles:
            pytest.skip("Both r7 and r8 tiles needed for this test")
        
        r7_df = pd.read_parquet(tiles[7])
        r8_df = pd.read_parquet(tiles[8])
        
        # Convert h3_id uint64 to hex strings for h3 operations
        r7_hexes = set(h3.int_to_str(int(h)) for h in r7_df['h3_id'])
        r8_hexes = set(h3.int_to_str(int(h)) for h in r8_df['h3_id'])
        
        # Sample some r8 hexes and verify their parents exist in r7
        sample_size = min(1000, len(r8_hexes))
        sample_r8 = np.random.choice(list(r8_hexes), sample_size, replace=False)
        
        orphans = []
        for r8_hex in sample_r8:
            try:
                parent = h3.cell_to_parent(r8_hex, 7)
                if parent not in r7_hexes:
                    orphans.append((r8_hex, parent))
            except Exception as e:
                pytest.fail(f"Invalid H3 ID: {r8_hex} - {e}")
        
        # Allow a small number of orphans at tile boundaries
        max_orphan_ratio = 0.01  # 1%
        orphan_ratio = len(orphans) / sample_size
        
        assert orphan_ratio <= max_orphan_ratio, (
            f"Found {len(orphans)} / {sample_size} r8 hexes whose r7 parents don't exist "
            f"({orphan_ratio:.1%} > {max_orphan_ratio:.1%} threshold). "
            f"Examples: {orphans[:5]}"
        )
    
    def test_anchor_data_consistency(self):
        """
        Verify that if an r7 parent has anchor data, at least some of its
        r8 children also have anchor data.
        """
        tiles = find_state_tile_files()
        
        if 7 not in tiles or 8 not in tiles:
            pytest.skip("Both r7 and r8 tiles needed for this test")
        
        r7_df = pd.read_parquet(tiles[7])
        r8_df = pd.read_parquet(tiles[8])
        
        # Find r7 hexes with anchor data (have at least a0_id)
        if 'a0_id' not in r7_df.columns:
            pytest.skip("No anchor data (a0_id) in r7 tiles")
        
        r7_with_anchors = r7_df[r7_df['a0_id'].notna()].copy()
        
        if len(r7_with_anchors) == 0:
            pytest.skip("No r7 hexes with anchor data")
        
        # Convert h3_id uint64 to hex strings for h3 operations
        r7_with_anchors['h3_id_str'] = r7_with_anchors['h3_id'].apply(lambda x: h3.int_to_str(int(x)))
        r8_df['h3_id_str'] = r8_df['h3_id'].apply(lambda x: h3.int_to_str(int(x)))
        
        # Sample some r7 hexes with anchors
        sample_size = min(100, len(r7_with_anchors))
        sample_r7 = r7_with_anchors.sample(sample_size)
        
        inconsistencies = []
        
        for _, r7_row in sample_r7.iterrows():
            r7_hex = r7_row['h3_id_str']
            
            # Get all r8 children
            try:
                r8_children = list(h3.cell_to_children(r7_hex, 8))
            except Exception:
                continue
            
            # Check if children exist in r8 data
            children_df = r8_df[r8_df['h3_id_str'].isin(r8_children)]
            
            if len(children_df) == 0:
                # No children in r8 data at all - this is suspicious
                inconsistencies.append({
                    'r7_hex': r7_hex,
                    'issue': 'No r8 children found in tiles',
                    'expected_children': len(r8_children)
                })
                continue
            
            # Check if ANY children have anchor data
            children_with_anchors = children_df[children_df['a0_id'].notna()]
            
            if len(children_with_anchors) == 0:
                # Parent has anchors but NO children do - this is the bug!
                inconsistencies.append({
                    'r7_hex': r7_hex,
                    'issue': 'Parent has anchor data but no children do',
                    'children_count': len(children_df),
                    'parent_a0_id': r7_row['a0_id']
                })
        
        # Allow some inconsistency at borders, but not too much
        max_allowed_ratio = 0.10  # Allow 10% inconsistency
        inconsistency_ratio = len(inconsistencies) / len(sample_r7)
        
        assert inconsistency_ratio <= max_allowed_ratio, (
            f"Found {len(inconsistencies)} / {len(sample_r7)} r7 hexes with inconsistent r8 children "
            f"({inconsistency_ratio:.1%} > {max_allowed_ratio:.1%} threshold). "
            f"Examples: {inconsistencies[:3]}"
        )
    
    def test_no_anchor_data_mismatch_example(self):
        """
        Specific test for user-reported issue:
        Hex 872a32688ffffff should have consistent shading at both resolutions.
        """
        tiles = find_state_tile_files()
        
        if 7 not in tiles or 8 not in tiles:
            pytest.skip("Both r7 and r8 tiles needed for this test")
        
        # The reported problematic hex
        problematic_r7_hex = "872a32688ffffff"
        
        r7_df = pd.read_parquet(tiles[7])
        r8_df = pd.read_parquet(tiles[8])
        
        # Check if this specific hex exists in r7
        r7_df['h3_id_str'] = r7_df['h3_id'].apply(lambda x: h3.int_to_str(int(x)))
        r7_row = r7_df[r7_df['h3_id_str'] == problematic_r7_hex]
        
        if len(r7_row) == 0:
            pytest.skip(f"Test hex {problematic_r7_hex} not in r7 tiles")
        
        r7_row = r7_row.iloc[0]
        
        # Check if it has anchor data
        has_r7_anchor = 'a0_id' in r7_row and pd.notna(r7_row['a0_id'])
        
        # Get its r8 children
        try:
            r8_children = list(h3.cell_to_children(problematic_r7_hex, 8))
        except Exception as e:
            pytest.fail(f"Failed to get r8 children for {problematic_r7_hex}: {e}")
        
        # Check children in r8 data
        r8_df['h3_id_str'] = r8_df['h3_id'].apply(lambda x: h3.int_to_str(int(x)))
        children_df = r8_df[r8_df['h3_id_str'].isin(r8_children)]
        
        # Count how many children have anchor data
        children_with_anchors = children_df['a0_id'].notna().sum() if 'a0_id' in children_df.columns else 0
        
        # If parent is shaded (has anchors), at least SOME children should be shaded
        if has_r7_anchor:
            assert children_with_anchors > 0, (
                f"Hex {problematic_r7_hex} has anchor data at r7 but NONE of its "
                f"{len(children_df)} r8 children have anchor data. This causes zoom "
                f"inconsistency - shaded when zoomed out, not shaded when zoomed in."
            )
        
        # Report the findings for debugging
        print(f"\n[DEBUG] Hex {problematic_r7_hex}:")
        print(f"  R7 has anchor data: {has_r7_anchor}")
        print(f"  R8 children count: {len(children_df)}")
        print(f"  R8 children with anchors: {children_with_anchors}")

    def test_parent_travel_times_never_better_than_children(self):
        """
        Vectorized regression: parent hex travel times should never be
        strictly better than the minimum of their children for the same anchor.
        """
        minutes_dir = Path("data/minutes")
        parquet_files = sorted(minutes_dir.glob("*_drive_t_hex.parquet"))
        if not parquet_files:
            pytest.skip("No drive minutes parquet files found")

        frames = []
        for file in parquet_files:
            frames.append(pd.read_parquet(file))
        minutes_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

        r7_df = minutes_df[minutes_df["res"] == H3_RES_LOW].copy()
        r8_df = minutes_df[minutes_df["res"] == H3_RES_HIGH].copy()

        if r7_df.empty or r8_df.empty:
            pytest.skip("Missing required resolutions in minutes parquet")

        def _parent_int(h_int: int) -> int:
            try:
                parent_hex = h3.cell_to_parent(h3.int_to_str(int(h_int)), H3_RES_LOW)
                return int(h3.str_to_int(parent_hex))
            except Exception:
                return 0

        r8_df["parent_h3"] = r8_df["h3_id"].apply(_parent_int).astype("uint64")
        valid_children = r8_df[r8_df["parent_h3"] != 0]

        child_min = (
            valid_children.groupby(["parent_h3", "anchor_int_id"], as_index=False)["time_s"]
            .min()
            .rename(columns={"time_s": "child_min_time"})
        )

        merged = r7_df.merge(
            child_min,
            how="left",
            left_on=["h3_id", "anchor_int_id"],
            right_on=["parent_h3", "anchor_int_id"],
        )

        violations = merged[
            merged["child_min_time"].notna() & (merged["time_s"] < merged["child_min_time"])
        ]

        assert violations.empty, (
            f"{len(violations)} parent/anchor rows have faster times than "
            f"their children. Examples: "
            f"{violations[['h3_id', 'anchor_int_id', 'time_s', 'child_min_time']].head().to_dict('records')}"
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
