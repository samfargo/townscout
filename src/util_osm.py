import os
import urllib.request


def download_geofabrik(state: str, base: str, out_dir: str = "data/osm"):
    """Download a Geofabrik PBF for the given state to data/osm/<state>.osm.pbf."""
    url = f"{base}/{state}-latest.osm.pbf"
    os.makedirs(out_dir, exist_ok=True)
    out = os.path.join(out_dir, f"{state}.osm.pbf")
    if os.path.exists(out) and os.path.getsize(out) > 0:
        return out
    urllib.request.urlretrieve(url, out)
    return out

