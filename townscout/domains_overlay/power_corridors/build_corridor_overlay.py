"""
Compute per-hex power corridor proximity flags.

This module extracts high-voltage power lines from OSM, buffers them,
and flags H3 hexagons within the buffer zone.
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from typing import Iterable, Optional, Sequence, Set

import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon
from shapely.ops import unary_union

try:
    from shapely import union_all  # type: ignore
except ImportError:  # Shapely < 2.0
    union_all = None  # type: ignore

# Add src to path to import config
src_path = Path(__file__).parent.parent.parent.parent / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from config import H3_RES_LOW, H3_RES_HIGH, STATE_BOUNDING_BOXES
from geometry_utils import clean_geoms
from .schema import BUFFER_METERS_DEFAULT, MIN_VOLTAGE_KV_DEFAULT

try:
    import h3  # type: ignore
    H3_GEO_TO_CELLS = getattr(h3, "geo_to_cells", None)
    H3_POLYFILL_GEOJSON = getattr(h3, "polyfill_geojson", None)
    H3_POLYFILL = getattr(h3, "polyfill", None)
    H3_STRING_TO_INT = next(
        (getattr(h3, attr, None) for attr in ("string_to_h3", "str_to_int", "string_to_int")),
        None,
    )
except ImportError:
    try:
        from h3.api.basic_int import h3 as h3  # type: ignore
        H3_GEO_TO_CELLS = getattr(h3, "geo_to_cells", None)
        H3_POLYFILL_GEOJSON = getattr(h3, "polyfill_geojson", None)
        H3_POLYFILL = getattr(h3, "polyfill", None)
        H3_STRING_TO_INT = getattr(h3, "string_to_h3", None)
    except Exception as exc:  # pragma: no cover - import guard
        raise RuntimeError("h3 library is required") from exc


def _cell_to_int(cell) -> int:
    """Robustly convert an H3 address (string or int) to its uint64 integer form."""
    if isinstance(cell, (int, np.integer)):
        return int(cell)
    if isinstance(cell, str):
        if callable(H3_STRING_TO_INT):
            return int(H3_STRING_TO_INT(cell))
        # Fall back to base-16 parsing for legacy APIs
        return int(cell, 16)
    # Allow numpy scalars (float64) emitted by some h3 builds
    try:
        return int(cell)
    except Exception as exc:  # pragma: no cover - defensive
        raise TypeError(f"Unsupported H3 cell type: {type(cell)!r}") from exc


def _bbox_polygon(bbox: dict) -> dict:
    return {
        "type": "Polygon",
        "coordinates": [[
            [bbox["west"], bbox["south"]],
            [bbox["east"], bbox["south"]],
            [bbox["east"], bbox["north"]],
            [bbox["west"], bbox["north"]],
            [bbox["west"], bbox["south"]],
        ]]
    }


def _extract_voltage_values(raw) -> Sequence[float]:
    """Parse voltage tag values into numeric volts."""
    if raw is None:
        return []
    if isinstance(raw, (int, float, np.integer, np.floating)):
        return [float(raw)]
    if isinstance(raw, (list, tuple, set)):
        values: list[float] = []
        for item in raw:
            values.extend(_extract_voltage_values(item))
        return values
    if isinstance(raw, str):
        tokens = re.split(r"[;,]", raw)
        values: list[float] = []
        for token in tokens:
            token = token.strip()
            if not token:
                continue
            match = re.search(r"(\d+(?:\.\d+)?)", token)
            if not match:
                continue
            magnitude = float(match.group(1))
            token_lower = token.lower()
            if "kv" in token_lower:
                values.append(magnitude * 1000.0)
            elif magnitude >= 1000.0:
                # Bare volt value like "230000"
                values.append(magnitude)
            else:
                # Assume kilovolt shorthand such as "230" without unit
                values.append(magnitude * 1000.0)
        return values
    return []


def _is_high_voltage(raw, threshold_volts: float) -> bool:
    for value in _extract_voltage_values(raw):
        if value >= threshold_volts:
            return True
    return False


def _parse_osm_tag(tags_str: str, key: str) -> Optional[str]:
    """Parse a tag value from OSM other_tags column format."""
    if not tags_str or pd.isna(tags_str):
        return None
    # other_tags format: "key1"=>"value1","key2"=>"value2"
    pattern = rf'"{key}"=>"([^"]*)"'
    match = re.search(pattern, tags_str)
    return match.group(1) if match else None


def _load_power_lines(pbf_path: str) -> gpd.GeoDataFrame:
    """
    Load OSM power lines (ways) with voltage tag using GeoPandas.
    
    This uses GeoPandas/Fiona's OSM driver instead of pyrosm to avoid
    Shapely 2.x compatibility issues. Power infrastructure tags are in
    the 'other_tags' column and need to be parsed.
    """
    try:
        # Read the lines layer from OSM PBF
        lines_gdf = gpd.read_file(pbf_path, layer='lines')
        
        # Filter for lines with power tag in other_tags
        power_mask = lines_gdf['other_tags'].fillna('').str.contains('"power"=>')
        power_lines = lines_gdf[power_mask].copy()
        
        if power_lines.empty:
            return gpd.GeoDataFrame(columns=['geometry', 'power', 'voltage', 'name'], 
                                  geometry='geometry', crs='EPSG:4326')
        
        # Parse power, voltage, and name from other_tags
        power_lines['power'] = power_lines['other_tags'].apply(lambda x: _parse_osm_tag(x, 'power'))
        power_lines['voltage'] = power_lines['other_tags'].apply(lambda x: _parse_osm_tag(x, 'voltage'))
        # name might be in the main columns or other_tags
        if 'name' not in power_lines.columns or power_lines['name'].isna().all():
            power_lines['name'] = power_lines['other_tags'].apply(lambda x: _parse_osm_tag(x, 'name'))
        
        # Keep only relevant columns
        cols = ['geometry', 'power', 'voltage', 'name']
        power_lines = power_lines[[c for c in cols if c in power_lines.columns]]
        
        # Ensure all expected columns exist
        for col in ['power', 'voltage', 'name']:
            if col not in power_lines.columns:
                power_lines[col] = None
        
        return power_lines[['geometry', 'power', 'voltage', 'name']]
        
    except Exception as exc:
        print(f"[error] Failed to load power lines from {pbf_path}: {exc}")
        return gpd.GeoDataFrame(columns=['geometry', 'power', 'voltage', 'name'], 
                              geometry='geometry', crs='EPSG:4326')


def _dissolve_and_buffer(lines: gpd.GeoDataFrame, buffer_meters: float) -> Optional[Polygon | MultiPolygon]:
    if lines.empty:
        return None

    # Project to UTM for accurate distance calculations
    projected = lines.to_crs(lines.estimate_utm_crs() or 3857)
    
    # Use clean_geoms to avoid Shapely 2.x 'create_collection' errors
    geom_col = clean_geoms(projected, ["LineString", "MultiLineString"])
    if geom_col.empty:
        return None

    try:
        # Buffer each line individually first
        buffered_list = []
        for geom in geom_col:
            try:
                buffered = geom.buffer(buffer_meters)
                if not buffered.is_empty:
                    buffered_list.append(buffered)
            except Exception:
                continue
        
        if not buffered_list:
            return None
        
        # Work around Shapely 2.x 'create_collection' errors by using iterative union
        # This is slower but avoids the ufunc compatibility issue
        print(f"[info] Dissolving {len(buffered_list)} buffered power corridors...")
        dissolved = buffered_list[0]
        for i, geom in enumerate(buffered_list[1:], 1):
            try:
                dissolved = dissolved.union(geom)
                if i % 500 == 0:
                    print(f"[info] Dissolved {i}/{len(buffered_list)-1} geometries...")
            except Exception as e:
                print(f"[warn] Failed to union geometry {i}: {e}")
                continue
        
    except Exception as exc:
        print(f"[warn] Failed to dissolve power lines: {exc}")
        import traceback
        traceback.print_exc()
        return None

    if dissolved is None or dissolved.is_empty:
        return None

    # Convert back to WGS84
    try:
        geodetic = gpd.GeoSeries([dissolved], crs=projected.crs).to_crs(4326)
        geom = geodetic.iloc[0]
        if geom.is_empty:
            return None
        return geom
    except Exception as exc:
        print(f"[warn] Failed to convert buffered geometry to WGS84: {exc}")
        return None


def _polygon_to_cells(geom: Polygon | MultiPolygon, resolution: int) -> Set[int]:
    if geom is None or geom.is_empty:
        return set()
    mapping = geom.__geo_interface__
    polygons: Iterable[dict]
    if mapping["type"] == "Polygon":
        polygons = [mapping]
    elif mapping["type"] == "MultiPolygon":
        polygons = (
            {"type": "Polygon", "coordinates": coords}
            for coords in mapping["coordinates"]
        )
    else:
        return set()

    result: Set[int] = set()
    for poly in polygons:
        cells: Iterable = []
        if callable(H3_GEO_TO_CELLS):
            cells = H3_GEO_TO_CELLS(poly, resolution)
        elif callable(H3_POLYFILL_GEOJSON):
            cells = H3_POLYFILL_GEOJSON(poly, resolution)
        elif callable(H3_POLYFILL):
            # Legacy API expects lat/long tuples
            coords_latlon = [
                [(lat, lon) for lon, lat in ring]
                for ring in poly["coordinates"]
            ]
            cells = H3_POLYFILL(coords_latlon, resolution, geo_json_conformant=True)
        else:  # pragma: no cover - defensive
            raise RuntimeError("No suitable H3 polyfill function available.")
        result.update(_cell_to_int(cell) for cell in cells)
    return result


def _state_hex_universe(state: str, resolutions: Sequence[int]) -> pd.DataFrame:
    bbox = STATE_BOUNDING_BOXES.get(state)
    if not bbox:
        raise ValueError(f"No bounding box configured for state '{state}'")
    polygon = _bbox_polygon(bbox)
    records: list[tuple[int, int]] = []
    for res in resolutions:
        if callable(H3_GEO_TO_CELLS):
            cells = H3_GEO_TO_CELLS(polygon, res)
        elif callable(H3_POLYFILL_GEOJSON):
            cells = H3_POLYFILL_GEOJSON(polygon, res)
        elif callable(H3_POLYFILL):
            coords_latlon = [
                [(lat, lon) for lon, lat in ring]
                for ring in polygon["coordinates"]
            ]
            cells = H3_POLYFILL(coords_latlon, res, geo_json_conformant=True)
        else:  # pragma: no cover - defensive
            raise RuntimeError("No suitable H3 polyfill function available.")
        for cell in cells:
            records.append((_cell_to_int(cell), res))

    if not records:
        return pd.DataFrame(columns=["h3_id", "res"])
    df = pd.DataFrame(records, columns=["h3_id", "res"])
    df["h3_id"] = df["h3_id"].astype("uint64", copy=False)
    df["res"] = df["res"].astype("int32", copy=False)
    return df.drop_duplicates(ignore_index=True)


def compute_power_corridor_flags(
    state: str,
    pbf_path: str,
    output_path: str,
    buffer_meters: float = BUFFER_METERS_DEFAULT,
    min_voltage_kv: float = MIN_VOLTAGE_KV_DEFAULT,
    resolutions: Optional[Sequence[int]] = None,
) -> None:
    """
    Compute power corridor proximity flags for H3 hexes.
    
    Args:
        state: State slug (e.g., 'massachusetts')
        pbf_path: Path to the state's OSM PBF extract
        output_path: Output parquet path
        buffer_meters: Buffer distance around power lines (default: 200m)
        min_voltage_kv: Minimum voltage in kV to consider high-voltage (default: 100kV)
        resolutions: H3 resolutions to compute (default: [H3_RES_LOW, H3_RES_HIGH])
    """
    if not os.path.exists(pbf_path):
        raise FileNotFoundError(f"Missing OSM PBF at {pbf_path}")
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    target_res = list(resolutions or [H3_RES_LOW, H3_RES_HIGH])
    print(f"[info] Loading power lines from {pbf_path}")
    power_lines = _load_power_lines(pbf_path)

    threshold_volts = min_voltage_kv * 1000.0
    if not power_lines.empty and "voltage" in power_lines.columns:
        mask = power_lines["voltage"].apply(_is_high_voltage, args=(threshold_volts,))
        power_lines = power_lines[mask]
        print(f"[info] Retained {len(power_lines)} high-voltage ways")
    else:
        print("[warn] No voltage column in power lines; resulting dataset may be empty.")
        power_lines = power_lines.iloc[0:0]

    buffered_geom = _dissolve_and_buffer(power_lines, buffer_meters)
    if buffered_geom is None:
        print("[warn] No buffered corridor geometry produced; writing all False flags.")

    print("[info] Building state hex universe")
    base = _state_hex_universe(state, target_res)

    if buffered_geom is None:
        base["near_power_corridor"] = False
    else:
        all_hits: dict[int, Set[int]] = {}
        for res in target_res:
            hits = _polygon_to_cells(buffered_geom, res)
            all_hits[res] = hits
            print(f"[info] res={res}: {len(hits)} hexes flagged")

        # Use vectorized operations for better performance and reliability
        base["near_power_corridor"] = False
        for res, hit_set in all_hits.items():
            if hit_set:
                mask = (base['res'] == res) & (base['h3_id'].isin(hit_set))
                base.loc[mask, "near_power_corridor"] = True

    base["near_power_corridor"] = base["near_power_corridor"].astype(bool, copy=False)
    base.to_parquet(output_path, index=False)
    print(f"[ok] Wrote {len(base)} rows to {output_path}")
