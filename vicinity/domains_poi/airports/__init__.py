"""
Airport POI handling.

Airports use a curated CSV source and have special snapping requirements
(arterial roads within 5km).
"""
from .curate_airports import load_airports_csv

__all__ = ["load_airports_csv"]

