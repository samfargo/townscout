# mandatories.py
from typing import Dict, List
import networkx as nx
from pyrosm import OSM

from .candidates import mk_cand

def bridgeheads(G: nx.MultiDiGraph, mode: str, region_fn) -> List[Dict]:
    heads = set()
    for u, v, k, d in G.edges(keys=True, data=True):
        # Handle OSM bridge tags properly - can be "yes", True, or other string values
        bridge = d.get("bridge")
        tunnel = d.get("tunnel") 
        layer = d.get("layer", 0)
        
        # Bridge/tunnel detection - handle various OSM tag formats
        is_bridge = False
        is_tunnel = False
        
        if bridge:
            if isinstance(bridge, bool):
                is_bridge = bridge
            elif isinstance(bridge, str):
                is_bridge = bridge.lower() in ("yes", "true", "viaduct", "boardwalk", "cantilever", "covered", "low_water_crossing", "movable", "trestle", "suspension", "swing", "pontoon")
            elif isinstance(bridge, list):
                is_bridge = any(str(b).lower() in ("yes", "true", "viaduct", "boardwalk") for b in bridge)
        
        if tunnel:
            if isinstance(tunnel, bool):
                is_tunnel = tunnel
            elif isinstance(tunnel, str):
                is_tunnel = tunnel.lower() in ("yes", "true", "building_passage", "culvert", "avalanche_protector")
            elif isinstance(tunnel, list):
                is_tunnel = any(str(t).lower() in ("yes", "true", "building_passage") for t in tunnel)
        
        # Layer-based detection for overpasses/underpasses
        has_layer = False
        try:
            layer_val = int(layer) if layer else 0
            has_layer = layer_val != 0
        except (ValueError, TypeError):
            # Handle string layer values
            if isinstance(layer, str) and layer.strip():
                try:
                    has_layer = int(layer.strip()) != 0
                except ValueError:
                    pass
        
        if is_bridge or is_tunnel or has_layer:
            heads.add(u)
            heads.add(v)
    
    out = []
    for nid in heads:
        if nid in G.nodes:
            data = G.nodes[nid]
            out.append(mk_cand(nid, data, mode, "bridge", 15, "bridge_access", region_fn, True))
    return out

def ferry_terminals(G: nx.MultiDiGraph, mode: str, pbf_path: str, snap_drive_public, snap_to_node, region_fn) -> List[Dict]:
    out = []
    osm = OSM(pbf_path)
    try:
        ferries = osm.get_pois({"amenity": ["ferry_terminal"]})
        if ferries is not None and not ferries.empty:
            for _, row in ferries.iterrows():
                # Handle both point and polygon geometries
                geom = row.geometry
                if hasattr(geom, 'x') and hasattr(geom, 'y'):
                    # Point geometry
                    lon, lat = geom.x, geom.y
                else:
                    # Polygon or other geometry - use centroid
                    centroid = geom.centroid
                    lon, lat = centroid.x, centroid.y
                
                nid = snap_drive_public(G, lat, lon, r=120) if mode == "drive" else \
                      snap_to_node(G, lat, lon, max_m=120)
                if nid:
                    data = G.nodes[nid]
                    out.append(mk_cand(nid, data, mode, "ferry", 12, "ferry_access", region_fn, True))
    except Exception as e:
        print(f"[{mode}] ferry warn: {e}")
    return out

def airport_terminals(G: nx.MultiDiGraph, pbf_path: str, snap_drive_public, region_fn) -> List[Dict]:
    out = []
    osm = OSM(pbf_path)
    try:
        terms = osm.get_pois({"aeroway": ["terminal", "aerodrome"]})
        if terms is not None and not terms.empty:
            for _, row in terms.iterrows():
                # Handle both point and polygon geometries
                geom = row.geometry
                if hasattr(geom, 'x') and hasattr(geom, 'y'):
                    # Point geometry
                    lon, lat = geom.x, geom.y
                else:
                    # Polygon or other geometry - use centroid
                    centroid = geom.centroid
                    lon, lat = centroid.x, centroid.y
                
                nid = snap_drive_public(G, lat, lon, r=80)
                if nid:
                    data = G.nodes[nid]
                    out.append(mk_cand(nid, data, "drive", "airport", 12, "airport_access", region_fn, True))
    except Exception as e:
        print(f"[drive] airport warn: {e}")
    return out