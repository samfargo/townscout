from src.config import STATES, GEOFABRIK_BASE
from src.util_osm import download_geofabrik

for s in STATES:
    path = download_geofabrik(s, GEOFABRIK_BASE)
    print(f"[ok] {path}") 