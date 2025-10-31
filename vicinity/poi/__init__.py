"""
vicinity.poi - Shared POI ingestion, normalization, and conflation logic.

This module provides the core functionality for handling POIs from multiple sources
(Overture, OSM, CSV) and normalizing them into a canonical schema.
"""

from .schema import CANONICAL_POI_SCHEMA, validate_poi_dataframe
from .ingest_osm import load_osm_pois
from .ingest_overture import load_overture_pois
from .normalize import normalize_overture_pois, normalize_osm_pois
from .conflate import conflate_pois

__all__ = [
    "CANONICAL_POI_SCHEMA",
    "validate_poi_dataframe",
    "load_osm_pois",
    "load_overture_pois",
    "normalize_overture_pois",
    "normalize_osm_pois",
    "conflate_pois",
]

