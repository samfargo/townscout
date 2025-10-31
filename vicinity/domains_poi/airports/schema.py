"""
Airport-specific schema and constants.
"""

# Airport categories
AIRPORT_CLASS = "transport"
AIRPORT_CATEGORY = "airport"
AIRPORT_SUBCAT = "airport"

# Snapping configuration
AIRPORT_SNAP_MAX_DISTANCE_M = 5000.0  # 5km max distance to arterial roads
AIRPORT_SNAP_ROAD_TYPES = ["motorway", "trunk", "primary", "secondary"]  # Arterial roads only

