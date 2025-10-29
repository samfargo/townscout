"""Utilities for robust OSM data access via pyrosm across modules."""
from __future__ import annotations

from typing import Optional, Sequence, Tuple

import pandas as pd
import geopandas as gpd
from pyrosm import OSM  # type: ignore


def _to_wgs84(df: Optional[gpd.GeoDataFrame]) -> gpd.GeoDataFrame:
    if df is None or len(df) == 0:
        return gpd.GeoDataFrame(columns=["geometry"], geometry="geometry", crs="EPSG:4326")
    if df.crs is None:
        return df.set_crs("EPSG:4326")
    return df.to_crs("EPSG:4326")


def _combine(dfs: list[gpd.GeoDataFrame]) -> gpd.GeoDataFrame:
    if not dfs:
        return _to_wgs84(None)
    # Already normalized to WGS84 prior to appending
    combined = pd.concat(dfs, ignore_index=True)
    return gpd.GeoDataFrame(combined, geometry="geometry", crs="EPSG:4326")


def get_osm_data(
    pbf_path: str,
    custom_filter: dict,
    *,
    tags_as_columns: Sequence[str] | None = None,
    keep_nodes: bool = False,
    keep_ways: bool = True,
    keep_relations: bool = False,
    osm: OSM | None = None,
) -> gpd.GeoDataFrame:
    """Fetch OSM data robustly across pyrosm versions and normalize CRS."""

    osm_obj = osm if osm is not None else OSM(pbf_path)
    tag_cols = list(tags_as_columns or ())

    # 1) Preferred modern API (returns combined nodes/ways/relations when requested)
    try:
        df = osm_obj.get_data_by_custom_criteria(
            custom_filter=custom_filter,
            tags_as_columns=tag_cols,
            keep_nodes=keep_nodes,
            keep_ways=keep_ways,
            keep_relations=keep_relations,
        )
        df = _to_wgs84(df)
        # Ensure all requested tag columns exist
        if not df.empty and tag_cols:
            for col in tag_cols:
                if col not in df.columns:
                    df[col] = None
        return df
    except Exception:
        pass

    # Build filter list in priority order for fallbacks
    filter_types: list[Tuple[str, str]] = []
    if keep_ways:
        filter_types.append(("ways", "way"))
    if keep_nodes:
        filter_types.append(("nodes", "node"))
    if keep_relations:
        filter_types.append(("relations", "relation"))
    if not filter_types:
        filter_types = [("ways", "way")]

    # 2) get_data with explicit filter_type, concatenating requested layers
    collected: list[gpd.GeoDataFrame] = []
    for ft_new, _ in filter_types:
        try:
            df = osm_obj.get_data(
                custom_filter=custom_filter,
                filter_type=ft_new,
                tags_as_columns=tag_cols,
            )
            df = _to_wgs84(df)
            if not df.empty:
                # Ensure all requested tag columns exist
                if tag_cols:
                    for col in tag_cols:
                        if col not in df.columns:
                            df[col] = None
                collected.append(df)
        except Exception:
            continue
    if collected:
        result = _combine(collected)
        # Ensure all requested tag columns exist in the final result
        if not result.empty and tag_cols:
            for col in tag_cols:
                if col not in result.columns:
                    result[col] = None
        return result

    # 3) Very old API name, try each filter type individually
    collected = []
    for _, ft_old in filter_types:
        try:
            df = osm_obj.get_data_by_custom_filter(
                custom_filter,
                filter_type=ft_old,
                keep_nodes=keep_nodes or ft_old == "node",
                keep_relations=keep_relations or ft_old == "relation",
            )
            df = _to_wgs84(df)
            if not df.empty:
                if tag_cols:
                    missing = [col for col in tag_cols if col not in df.columns]
                    for col in missing:
                        df[col] = None
                collected.append(df)
        except Exception:
            continue

    result = _combine(collected)
    # Ensure all requested tag columns exist in the final result
    if not result.empty and tag_cols:
        for col in tag_cols:
            if col not in result.columns:
                result[col] = None
    return result
