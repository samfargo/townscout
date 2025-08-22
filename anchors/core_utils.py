# Combined core utilities module
# core_utils.py
import json
import os
import pickle
import h3
import numpy as np
import pandas as pd
import networkx as nx
from typing import Dict, List, Optional, Tuple, Iterable
from .config import LEAFLET_TILES, LEAFLET_ATTR

# =====================
# Cache utilities
# =====================

def load_graph(cache_path: str):
    if os.path.exists(cache_path):
        print(f"[anchors] Loading cached network from {cache_path}")
        with open(cache_path, "rb") as f:
            return pickle.load(f)
    return None

def save_graph(G, cache_path: str):
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    print(f"[anchors] Caching network to {cache_path}")
    with open(cache_path, "wb") as f:
        pickle.dump(G, f)

def load_candidates(cache_path: str) -> Optional[List[Dict]]:
    if os.path.exists(cache_path):
        print(f"[anchors] Loading cached candidates from {cache_path}")
        with open(cache_path, "rb") as f:
            return pickle.load(f)
    return None

def save_candidates(candidates: List[Dict], cache_path: str):
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    print(f"[anchors] Caching candidates to {cache_path}")
    with open(cache_path, "wb") as f:
        pickle.dump(candidates, f)

# =====================
# Utility functions
# =====================

def normalize_hw(val):
    if isinstance(val, (list, tuple, set)):
        return ",".join(sorted(map(str, val)))
    return str(val)

def hw_in(hw_val, classes) -> bool:
    if isinstance(hw_val, (list, tuple, set)):
        return any(str(h) in classes for h in hw_val)
    return str(hw_val) in classes

def h3_edge_len_m(res: int) -> float:
    try:
        return h3.average_hexagon_edge_length(res, 'm')     # h3>=4
    except AttributeError:
        try:
            return h3.get_hexagon_edge_length_avg(res, 'm') # older h3
        except AttributeError:
            return {7: 1221.6, 8: 465.6, 9: 177.6}.get(res, 500.0)

def h3_disk(cell: str, k: int):
    try:
        return h3.grid_disk(cell, k)                    # h3>=4
    except AttributeError:
        return set(h3.k_ring(cell, k))                  # older h3

def res_for_spacing(m: float) -> int:
    return 7 if m >= 1200 else 8 if m >= 450 else 9

def haversine_m(lat1, lon1, lat2, lon2) -> float:
    from math import radians, cos, sin, asin, sqrt
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1; dlon = lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1)*cos(lat2)*sin(dlon/2)**2
    return 2 * asin(sqrt(a)) * 6371000.0

def haversine_km(lat1, lon1, lat2, lon2) -> float:
    return haversine_m(lat1, lon1, lat2, lon2) / 1000.0

def classify_region(lat: float, lon: float) -> str:
    # Simple heuristic for MA; replace with census UA when available
    boston = haversine_km(lat, lon, 42.3601, -71.0589)
    worc   = haversine_km(lat, lon, 42.2626, -71.8023)
    spring = haversine_km(lat, lon, 42.1015, -72.5898)
    return "urban" if (boston < 60 or worc < 25 or spring < 25) else "rural"

def ma_bbox() -> Tuple[float, float, float, float]:
    # (min_lat, min_lon, max_lat, max_lon)
    return (41.2, -73.5, 42.9, -69.9)

def fill_h3_in_bbox(bbox, res: int, step: float) -> List[str]:
    min_lat, min_lon, max_lat, max_lon = bbox
    cells = set()
    lat = min_lat
    while lat <= max_lat:
        lon = min_lon
        while lon <= max_lon:
            cells.add(h3.latlng_to_cell(lat, lon, res))
            lon += step
        lat += step
    return list(cells)

def edge_len_m(d) -> float:
    if "length" in d and d["length"] is not None:
        return float(d["length"])
    if "travel_time" in d and "speed_kph" in d and d["travel_time"] is not None and d["speed_kph"] is not None:
        return float(d["travel_time"]) / 3600.0 * float(d["speed_kph"]) * 1000.0
    return 50.0  # small fallback

# =====================
# I/O utilities
# =====================

def save_anchors(anchors: List[Dict], mode: str, out_dir: str):
    if not anchors:
        print(f"[warn] no {mode} anchors to save")
        return

    df = pd.DataFrame(anchors)
    df["mode"] = mode
    df["meta_json"] = df.apply(lambda r: json.dumps({
        "source": r.get("source"),
        "score":  r.get("score"),
        "kind":   r.get("kind")
    }), axis=1)

    # --- NEW: stable per-mode int id (aid) ---
    # sort deterministically so aid is reproducible
    df = df.sort_values(["mandatory", "score", "node_id"], ascending=[False, False, True]).reset_index(drop=True)
    df["aid"] = df.index.astype("int32")

    cols = [
        "aid",                 # NEW (int32 dense)
        "id",                  # keep your human-readable id
        "node_id","lon","lat",
        "road_class","kind","region","mandatory",
        "mode","meta_json"
    ]
    out = os.path.join(out_dir, f"anchors_{mode}.parquet")
    df[cols].to_parquet(out, index=False)
    print(f"[ok] {out} ({len(df)} rows)")

def write_qa_map(drive_anchors: List[Dict], walk_anchors: List[Dict], out_dir: str):
    path = os.path.join(out_dir, "anchors_map.html")
    html = f"""<!doctype html><html><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<style>#map{{height:100vh}}.legend{{background:#fff;padding:10px;border-radius:6px;box-shadow:0 0 10px rgba(0,0,0,.2)}} </style>
</head><body><div id="map"></div>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
var map=L.map('map').setView([42.36,-71.06],8);
L.tileLayer('{LEAFLET_TILES}',{{attribution:'{LEAFLET_ATTR}'}}).addTo(map);
var d={json.dumps([[a["lat"],a["lon"],a.get("kind","?")] for a in drive_anchors])};
d.forEach(function(p){{L.circleMarker([p[0],p[1]],{{color:'blue',radius:3,fillOpacity:.7}}).bindPopup('Drive: '+p[2]).addTo(map);}});
var w={json.dumps([[a["lat"],a["lon"],a.get("kind","?")] for a in walk_anchors])};
w.forEach(function(p){{L.circleMarker([p[0],p[1]],{{color:'green',radius:2,fillOpacity:.7}}).bindPopup('Walk: '+p[2]).addTo(map);}});
var leg=L.control({{position:'topright'}});leg.onAdd=function(m){{var d=document.createElement('div');d.className='legend';
d.innerHTML='<b>Anchors</b><br><span style="color:blue">●</span> Drive ({len(drive_anchors)})<br><span style="color:green">●</span> Walk ({len(walk_anchors)})';return d;}};leg.addTo(map);
</script></body></html>"""
    with open(path, "w") as f:
        f.write(html)
    print(f"[ok] {path}") 