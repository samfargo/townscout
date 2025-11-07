#!/usr/bin/env python3
"""
Check Tile Schema Contract

Validates PMTiles against the minimal contract defined in docs/tile_contract.json:
1. Correct layer names
2. Appropriate zoom levels
3. Required properties present
4. Anchor array structure (a{i}_id, a{i}_s pairs)
"""
import sys
import json
import subprocess
from pathlib import Path
from typing import Dict, Any, List


def load_tile_contract() -> Dict[str, Any]:
    """Load the tile contract specification."""
    contract_path = Path("docs/tile_contract.json")
    if not contract_path.exists():
        print(f"ERROR: Tile contract not found at {contract_path}")
        sys.exit(1)
    
    with open(contract_path) as f:
        return json.load(f)


def find_pmtiles() -> List[Path]:
    """Find all PMTiles files in the tiles directory."""
    tiles_dir = Path("tiles")
    if not tiles_dir.exists():
        return []
    
    return list(tiles_dir.glob("*.pmtiles"))


def get_pmtiles_metadata(pmtiles_path: Path) -> Dict[str, Any]:
    """
    Extract metadata from a PMTiles file using the pmtiles CLI tool.
    
    Returns a dict with metadata or raises an error if pmtiles tool is not available.
    """
    try:
        result = subprocess.run(
            ["pmtiles", "show", str(pmtiles_path), "--json"],
            capture_output=True,
            text=True,
            check=True
        )
        return json.loads(result.stdout)
    except FileNotFoundError:
        print("ERROR: 'pmtiles' CLI tool not found. Please install it to validate tiles.")
        print("  Install: npm install -g pmtiles")
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Failed to read {pmtiles_path.name}: {e}")
        return {}
    except json.JSONDecodeError as e:
        print(f"ERROR: Failed to parse metadata from {pmtiles_path.name}: {e}")
        return {}


def extract_layer_properties(metadata: Dict[str, Any]) -> Dict[str, List[str]]:
    """
    Extract property names from PMTiles metadata.
    
    Returns a dict mapping layer names to lists of property names.
    """
    layers = {}
    
    # PMTiles metadata typically has a 'vector_layers' key
    vector_layers = metadata.get("vector_layers", [])
    
    for layer in vector_layers:
        layer_name = layer.get("id", "")
        fields = layer.get("fields", {})
        layers[layer_name] = list(fields.keys())
    
    return layers


def validate_tile(tile_path: Path, contract_spec: Dict[str, Any], tile_contract: Dict[str, Any]) -> tuple[bool, List[str]]:
    """
    Validate a single PMTiles file against its contract specification.
    
    Returns:
        (passed, list of error messages)
    """
    errors = []
    
    # Get metadata
    metadata = get_pmtiles_metadata(tile_path)
    if not metadata:
        return False, [f"Failed to read metadata from {tile_path.name}"]
    
    # Check layer name
    expected_layer = contract_spec["layer_name"]
    layers = extract_layer_properties(metadata)
    
    if expected_layer not in layers:
        errors.append(f"Missing expected layer '{expected_layer}'. Found: {list(layers.keys())}")
    
    # Check zoom levels (if available in metadata)
    min_zoom = metadata.get("minzoom")
    max_zoom = metadata.get("maxzoom")
    
    if min_zoom is not None and min_zoom != contract_spec["min_zoom"]:
        errors.append(f"min_zoom mismatch: expected {contract_spec['min_zoom']}, got {min_zoom}")
    
    if max_zoom is not None and max_zoom != contract_spec["max_zoom"]:
        errors.append(f"max_zoom mismatch: expected {contract_spec['max_zoom']}, got {max_zoom}")
    
    # Check required properties
    if expected_layer in layers:
        layer_props = set(layers[expected_layer])
        required_props = set(contract_spec["required_properties"])
        missing_props = required_props - layer_props
        
        if missing_props:
            errors.append(f"Missing required properties: {sorted(missing_props)}")
        
        # Check for anchor array structure
        anchor_spec = tile_contract["anchor_array_spec"]
        max_k = anchor_spec["max_k"]
        
        # We should have at least a0_id and a0_s
        has_a0 = f"a0{anchor_spec['id_suffix']}" in layer_props and f"a0{anchor_spec['time_suffix']}" in layer_props
        
        if not has_a0:
            errors.append(f"Missing anchor array base (a0_id, a0_s)")
    
    return len(errors) == 0, errors


def main():
    """Main function to validate tile schema."""
    print("=" * 80)
    print("TILE SCHEMA VALIDATION")
    print("=" * 80)
    print()
    
    # Load contract
    print("[1/3] Loading tile contract...")
    contract = load_tile_contract()
    print(f"  → Loaded contract v{contract.get('version', 'unknown')}")
    print()
    
    # Find PMTiles
    print("[2/3] Finding PMTiles...")
    pmtiles_files = find_pmtiles()
    
    if not pmtiles_files:
        print("ERROR: No PMTiles found in tiles/ directory")
        return 1
    
    print(f"  → Found {len(pmtiles_files)} PMTiles files")
    print()
    
    # Validate each tile
    print("[3/3] Validating tiles...")
    print()
    
    all_passed = True
    results = []
    
    for tile_path in sorted(pmtiles_files):
        # Find matching contract spec
        matching_spec = None
        for spec in contract["tiles"]:
            # Simple pattern matching (exact filename or glob-like)
            pattern = spec["filename_pattern"]
            if pattern == tile_path.name or tile_path.name.startswith(pattern.replace(".pmtiles", "")):
                matching_spec = spec
                break
        
        if not matching_spec:
            print(f"⚠ SKIP {tile_path.name} (no contract specification)")
            print()
            continue
        
        passed, errors = validate_tile(tile_path, matching_spec, contract)
        
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{status} {tile_path.name}")
        
        if not passed:
            all_passed = False
            for error in errors:
                print(f"     ERROR: {error}")
        
        results.append((tile_path.name, passed, errors))
        print()
    
    # Summary
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    
    if all_passed:
        print("✓ ALL TILES PASSED")
        print(f"  • {len([r for r in results if r[1]])} tiles validated successfully")
        return 0
    else:
        failed_tiles = [r for r in results if not r[1]]
        print("✗ VALIDATION FAILED")
        print(f"  • {len(failed_tiles)} / {len(results)} tiles failed")
        print()
        print("Failed tiles:")
        for name, _, errors in failed_tiles:
            print(f"  • {name}")
            for error in errors:
                print(f"    - {error}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

