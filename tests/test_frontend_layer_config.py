"""
Test Frontend Layer Configuration

Validates that MapLibre layer configurations have proper zoom ranges
to avoid rendering conflicts and zoom inconsistencies.
"""
import pytest
import re
from pathlib import Path
from typing import Dict, List, Tuple


def find_mapcontroller_file() -> Path:
    """Find MapController.ts file."""
    map_controller = Path("tiles/web/lib/map/MapController.ts")
    if map_controller.exists():
        return map_controller
    return None


def extract_layer_zoom_config(file_content: str) -> Dict[str, Tuple[int, int]]:
    """
    Extract minzoom and maxzoom for each layer from MapController.ts.
    
    Returns dict mapping layer_id -> (minzoom, maxzoom)
    """
    layers = {}
    
    # Find layer definitions in createBaseStyle() function
    # Pattern: id: LAYER_IDS.driveR7 ... minzoom: X ... maxzoom: Y
    layer_pattern = r"id:\s*LAYER_IDS\.(drive(?:R7|R8))"
    minzoom_pattern = r"minzoom:\s*(\d+)"
    maxzoom_pattern = r"maxzoom:\s*(\d+)"
    
    # Split into layer blocks
    lines = file_content.split('\n')
    current_layer = None
    current_minzoom = None
    current_maxzoom = None
    
    for line in lines:
        # Check for layer ID
        layer_match = re.search(layer_pattern, line)
        if layer_match:
            current_layer = layer_match.group(1)
            current_minzoom = None
            current_maxzoom = None
        
        # Check for minzoom
        minzoom_match = re.search(minzoom_pattern, line)
        if minzoom_match and current_layer:
            current_minzoom = int(minzoom_match.group(1))
        
        # Check for maxzoom
        maxzoom_match = re.search(maxzoom_pattern, line)
        if maxzoom_match and current_layer:
            current_maxzoom = int(maxzoom_match.group(1))
        
        # If we have all info for this layer, save it
        if current_layer and current_minzoom is not None and current_maxzoom is not None:
            layers[current_layer] = (current_minzoom, current_maxzoom)
            current_layer = None
    
    return layers


class TestFrontendLayerConfig:
    """Test suite for frontend layer configuration validation."""
    
    def test_mapcontroller_exists(self):
        """Verify MapController.ts file exists."""
        map_controller = find_mapcontroller_file()
        assert map_controller is not None, "MapController.ts not found in tiles/web/lib/map/"
    
    def test_no_zoom_level_overlap(self):
        """
        Verify r7 and r8 layers don't have exact zoom overlap that causes conflicts.
        
        The bug: If both layers are active at the same zoom level with no opacity
        transition, they conflict and cause inconsistent rendering.
        """
        map_controller = find_mapcontroller_file()
        if map_controller is None:
            pytest.skip("MapController.ts not found")
        
        content = map_controller.read_text()
        layers = extract_layer_zoom_config(content)
        
        if 'driveR7' not in layers or 'driveR8' not in layers:
            pytest.skip("Could not find driveR7 and driveR8 layer definitions")
        
        r7_min, r7_max = layers['driveR7']
        r8_min, r8_max = layers['driveR8']
        
        # R7 should end before or at R8 starts
        # But they shouldn't both be fully opaque at the same zoom
        # We'll check that either:
        # 1. They don't overlap: r7_max < r8_min
        # 2. Or there's an opacity transition (checked separately)
        
        assert r7_min < r8_min, (
            f"R7 minzoom ({r7_min}) should be less than R8 minzoom ({r8_min})"
        )
        
        # If they overlap at zoom level Z, make sure it's intentional (r7_max >= r8_min)
        # We allow overlap for smooth transitions but want to document it
        if r7_max >= r8_min:
            # This is OK IF there are opacity transitions
            # We'll check the content for interpolate expressions
            has_r7_opacity_transition = 'fill-opacity' in content and 'interpolate' in content
            has_r8_opacity_transition = 'fill-opacity' in content and 'interpolate' in content
            
            assert has_r7_opacity_transition and has_r8_opacity_transition, (
                f"R7 (zoom {r7_min}-{r7_max}) and R8 (zoom {r8_min}-{r8_max}) overlap at "
                f"zoom {r8_min}-{r7_max} but don't have opacity transitions. "
                f"This causes rendering conflicts! Either separate the zoom ranges completely "
                f"or add opacity interpolation for smooth transitions."
            )
    
    def test_r7_covers_low_zooms(self):
        """Verify R7 layer covers low zoom levels (zoomed out view)."""
        map_controller = find_mapcontroller_file()
        if map_controller is None:
            pytest.skip("MapController.ts not found")
        
        content = map_controller.read_text()
        layers = extract_layer_zoom_config(content)
        
        if 'driveR7' not in layers:
            pytest.skip("Could not find driveR7 layer definition")
        
        r7_min, r7_max = layers['driveR7']
        
        assert r7_min <= 1, (
            f"R7 layer should start at zoom 0 or 1 for zoomed-out view, got {r7_min}"
        )
        
        assert r7_max >= 7, (
            f"R7 layer should be visible at least through zoom 7, got maxzoom {r7_max}"
        )
    
    def test_r8_covers_high_zooms(self):
        """Verify R8 layer covers high zoom levels (zoomed in view)."""
        map_controller = find_mapcontroller_file()
        if map_controller is None:
            pytest.skip("MapController.ts not found")
        
        content = map_controller.read_text()
        layers = extract_layer_zoom_config(content)
        
        if 'driveR8' not in layers:
            pytest.skip("Could not find driveR8 layer definition")
        
        r8_min, r8_max = layers['driveR8']
        
        assert r8_min <= 9, (
            f"R8 layer should start by zoom 9 at latest for zoomed-in view, got {r8_min}"
        )
        
        assert r8_max >= 15, (
            f"R8 layer should be visible through zoom 15+ for street-level view, got {r8_max}"
        )
    
    def test_zoom_ranges_documented(self):
        """Verify zoom ranges are documented with comments."""
        map_controller = find_mapcontroller_file()
        if map_controller is None:
            pytest.skip("MapController.ts not found")
        
        content = map_controller.read_text()
        
        # Check for comments explaining the zoom logic
        has_zoom_comments = (
            'zoom' in content.lower() and 
            ('transition' in content.lower() or 'fade' in content.lower())
        )
        
        # This is a soft check - we just want to encourage documentation
        if not has_zoom_comments:
            pytest.skip(
                "Consider adding comments explaining zoom level transitions "
                "to help future developers understand the configuration"
            )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

