"""
Beach-specific schema and constants.
"""

# Beach classification categories
BEACH_CLASS = "natural"
BEACH_TYPES = {
    "ocean": "beach_ocean",
    "lake": "beach_lake",
    "river": "beach_river",
    "other": "beach_other",
}

# Classification distance thresholds (in meters, EPSG:3857)
DISTANCE_OCEAN_M = 500.0
DISTANCE_LAKE_M = 300.0
DISTANCE_RIVER_M = 200.0

# Water subtypes for classification
OCEAN_SUBTYPES = ['ocean', 'sea']
LAKE_SUBTYPES = ['lake', 'reservoir', 'pond', 'lagoon']
RIVER_SUBTYPES = ['river', 'canal']  # excluding 'stream' to reduce false positives

