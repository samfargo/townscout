from typing import Optional, Tuple
import re
import pandas as pd
import networkx as nx
import osmnx as ox
from pyrosm import OSM

ox.settings.log_console = False


def _first(x):
    """Return a representative scalar from possibly-list/set values."""
    if isinstance(x, list) and x:
        return x[0]
    if isinstance(x, tuple) and x:
        return x[0]
    if isinstance(x, set) and x:
        return next(iter(x))
    return x


def _parse_maxspeed_kph(v) -> Optional[float]:
    """
    Convert OSM maxspeed variants to kph.
    Examples: '50', '50 mph', '35 mph', '80 km/h', 50, [50, 'signals'], None
    """
    v = _first(v)
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)

    s = str(v).strip().lower()
    # Common tokens like 'signals', 'national', 'walk', etc. -> ignore
    if not s or s in {"none", "signals", "variable", "walk"}:
        return None

    # Extract number
    m = re.search(r"(\d+(\.\d+)?)", s)
    if not m:
        return None
    num = float(m.group(1))

    # Units
    if "mph" in s:
        return num * 1.60934
    # default km/h if unit missing or km/h present
    return num


def build_network(pbf_path: str, mode: str) -> nx.MultiDiGraph:
    """
    Build a routable MultiDiGraph from a PBF with consistent edge 'length' (m),
    'speed_kph', and 'travel_time' (s). All metrics are recomputed AFTER
    graph simplification to avoid list-valued attributes that break Dijkstra.
    """
    osm = OSM(pbf_path)
    net = "driving" if mode in ("driving", "drive") else "walking"

    # Ask Pyrosm for useful attrs. 'maxspeed' helpful for driving.
    extra = ["highway", "access", "length"]
    if net == "driving":
        extra.append("maxspeed")

    nodes, edges = osm.get_network(network_type=net, nodes=True, extra_attributes=extra)

    # Keep only columns we actually use (some may be missing depending on region)
    keep_cols = ["u", "v", "key", "highway", "access", "length", "geometry", "maxspeed"]
    edges = edges[[c for c in keep_cols if c in edges.columns]]

    # Ensure node x/y exist
    if "x" not in nodes or "y" not in nodes:
        nodes = nodes.copy()
        nodes["x"] = nodes.geometry.x
        nodes["y"] = nodes.geometry.y

    # Stable int64 node index
    if nodes.index.name != "id":
        if "id" in nodes.columns:
            nodes = nodes.set_index("id", drop=True)
        else:
            nodes.index = nodes.index.rename("id")
    nodes = nodes[~nodes.index.duplicated(keep="first")].copy()
    try:
        nodes.index = nodes.index.astype("int64")
    except Exception:
        pass

    # Stable edge multi-index (u,v,key) with numeric u/v
    if "key" not in edges:
        edges = edges.copy()
        edges["key"] = edges.groupby(["u", "v"]).cumcount().astype("int64")
    else:
        # Coerce to int where possible; fill NaNs; then fix collisions anyway
        edges["key"] = pd.to_numeric(edges["key"], errors="coerce").fillna(0).astype("int64")
        # Detect collisions and rebuild keys per (u,v) when needed
        dup_mask = edges.duplicated(subset=["u", "v", "key"], keep=False)
        if dup_mask.any():
            # rebuild a fresh unique key that includes cumcount
            edges["key"] = edges.groupby(["u", "v"]).cumcount().astype("int64")
    
    edges = edges.reset_index(drop=True)
    edges = edges[edges["u"].notna() & edges["v"].notna()].copy()
    edges["u"] = pd.to_numeric(edges["u"], errors="coerce")
    edges["v"] = pd.to_numeric(edges["v"], errors="coerce")
    edges = edges.dropna(subset=["u", "v"])
    edges["u"] = edges["u"].astype("int64")
    edges["v"] = edges["v"].astype("int64")
    # Keep only edges that reference existing nodes
    edges = edges[edges["u"].isin(nodes.index) & edges["v"].isin(nodes.index)]
    edges = edges.set_index(["u", "v", "key"], drop=True)
    edges = edges[~edges.index.duplicated(keep="first")].copy()

    # Build graph
    G = ox.graph_from_gdfs(nodes, edges)

    # ---- Pre-simplify: remove fully private edges (helps simplify produce cleaner components)
    rm = []
    for u, v, k, d in G.edges(keys=True, data=True):
        acc = _first(d.get("access"))
        if str(acc) in ("private", "no"):
            rm.append((u, v, k))
    if rm:
        G.remove_edges_from(rm)

    # ---- Simplify graph (merge straight segments into single edges)
    print(f"[{mode}] Simplifying graph for faster routing...")
    G = ox.simplify_graph(G)

    # ---- Recompute accurate lengths on simplified edges (meters)
    edge_gdf = ox.graph_to_gdfs(G, nodes=False)
    utm_crs = edge_gdf.estimate_utm_crs()
    edge_gdf_utm = edge_gdf.to_crs(utm_crs)
    edge_lengths = edge_gdf_utm.geometry.length

    for (u, v, k), length in zip(edge_gdf.index, edge_lengths):
        G[u][v][k]["length"] = float(length)

    # ---- Set speeds and travel times on simplified edges
    if mode in ("walk", "walking"):
        # Walking: fixed 4.8 kph
        for u, v, k, d in G.edges(keys=True, data=True):
            d["speed_kph"] = 4.8
            d["travel_time"] = (d["length"] / 1000.0) / d["speed_kph"] * 3600.0
    else:
        # Driving: use highway-based defaults, then override with parsed maxspeed if present
        HWY_SPEEDS = {
            "motorway": 110, "motorway_link": 70, "trunk": 100, "trunk_link": 60,
            "primary": 80, "primary_link": 60, "secondary": 65, "secondary_link": 50,
            "tertiary": 55, "tertiary_link": 45, "residential": 40, "living_street": 10,
            "service": 20, "unclassified": 45, "road": 45, "track": 30,
        }

        for u, v, k, d in G.edges(keys=True, data=True):
            # Highway tag may be list after simplification → normalize
            hwy = _first(d.get("highway"))
            base = HWY_SPEEDS.get(str(hwy), 40.0)

            # Prefer explicit maxspeed if available
            ms = _parse_maxspeed_kph(d.get("maxspeed"))
            spd = float(ms if (ms and ms > 0) else base)

            # Cap absurd values
            if spd < 5:
                spd = 5.0
            if spd > 130:
                spd = 130.0

            d["speed_kph"] = spd
            d["travel_time"] = (d["length"] / 1000.0) / spd * 3600.0

        # Drop any remaining fully private edges (access can again be list after simplify)
        rm = []
        for u, v, k, d in G.edges(keys=True, data=True):
            acc = _first(d.get("access"))
            if str(acc) in ("private", "no"):
                rm.append((u, v, k))
        if rm:
            G.remove_edges_from(rm)

    return G