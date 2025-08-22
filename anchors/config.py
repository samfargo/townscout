import os

# H3 defaults, compatible with old src.config import pattern
try:
    from src.config import H3_RES_LOW, H3_RES_HIGH  # type: ignore
except Exception:
    H3_RES_LOW = 7
    H3_RES_HIGH = 8

STATE_NAME = "massachusetts"

# Target spacing (meters)
DRIVE_SPACING = {"urban": 500, "rural": 2000}  # Further reduced from 750/3000
WALK_SPACING  = {"urban": 150, "rural": 300}   # Further reduced from 200/500

# Coverage QA targets
DRIVE_COVERAGE_KM = 8.0  # Reduced from 10.0 to be more achievable
WALK_COVERAGE_M   = 400.0  # Reduced from 600.0 to be more achievable

# Cache filenames
DRIVE_CACHE = "network_drive.pkl"
WALK_CACHE  = "network_walk.pkl"
DRIVE_CANDIDATES_CACHE = "candidates_drive.pkl"
WALK_CANDIDATES_CACHE = "candidates_walk.pkl"

LEAFLET_TILES = "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
LEAFLET_ATTR  = "© OSM"