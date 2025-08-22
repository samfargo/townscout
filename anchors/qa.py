# qa.py
from typing import Dict, List, Tuple
import math
import numpy as np
import networkx as nx
from scipy.spatial import cKDTree
import h3
import osmnx as ox

from .config import (
    H3_RES_LOW,
    H3_RES_HIGH,
    DRIVE_COVERAGE_KM,
    WALK_COVERAGE_M,
)
from .core_utils import classify_region

# -----------------------
# H3 compatibility shim (v3 & v4)
# -----------------------
_HAS_POLYFILL = hasattr(h3, "polyfill")               # v3
_HAS_LATLNG_TO_CELL = hasattr(h3, "latlng_to_cell")   # v4
_HAS_CELL_TO_LATLNG = hasattr(h3, "cell_to_latlng")   # v4

def polyfill_cells(geojson, res: int):
    """
    Return list of H3 cells covering a GeoJSON polygon/multipolygon.
    Supports:
      - v3: h3.polyfill(geojson, res, geo_json_conformant=True|False, keep_class_three=...)
      - v4: h3.polygon_to_cells(LatLngPoly(...), res)
    """
    # v3 first (has polyfill)
    if _HAS_POLYFILL:
        try:
            return list(h3.polyfill(geojson, res, keep_class_three=False, geo_json_conformant=True))
        except TypeError:
            # older v3 without kwargs
            return list(h3.polyfill(geojson, res))

    # v4: need to convert GeoJSON to LatLngPoly
    try:
        # Convert GeoJSON coordinates to LatLngPoly format
        # GeoJSON has [lon, lat] format, LatLngPoly expects (lat, lon)
        if geojson["type"] == "Polygon":
            outer_ring = geojson["coordinates"][0]  # First ring is outer
            outer_latlng = [(lat, lon) for lon, lat in outer_ring]
            
            # Handle holes if present
            holes = []
            if len(geojson["coordinates"]) > 1:
                for hole_ring in geojson["coordinates"][1:]:
                    hole_latlng = [(lat, lon) for lon, lat in hole_ring]
                    holes.append(hole_latlng)
            
            if holes:
                poly = h3.LatLngPoly(outer_latlng, *holes)
            else:
                poly = h3.LatLngPoly(outer_latlng)
            
            return list(h3.polygon_to_cells(poly, res))
        
        elif geojson["type"] == "MultiPolygon":
            # Handle MultiPolygon by processing each polygon separately
            all_cells = set()
            for polygon_coords in geojson["coordinates"]:
                outer_ring = polygon_coords[0]
                outer_latlng = [(lat, lon) for lon, lat in outer_ring]
                
                holes = []
                if len(polygon_coords) > 1:
                    for hole_ring in polygon_coords[1:]:
                        hole_latlng = [(lat, lon) for lon, lat in hole_ring]
                        holes.append(hole_latlng)
                
                if holes:
                    poly = h3.LatLngPoly(outer_latlng, *holes)
                else:
                    poly = h3.LatLngPoly(outer_latlng)
                
                cells = h3.polygon_to_cells(poly, res)
                all_cells.update(cells)
            
            return list(all_cells)
        
        else:
            raise ValueError(f"Unsupported geometry type: {geojson['type']}")
            
    except Exception as e:
        raise ValueError(f"Failed to convert GeoJSON to H3 cells: {e}")

def to_cell(lat: float, lon: float, res: int) -> str:
    if _HAS_LATLNG_TO_CELL:
        return h3.latlng_to_cell(lat, lon, res)  # v4
    return h3.geo_to_h3(lat, lon, res)          # v3

def from_cell(cell: str):
    if _HAS_CELL_TO_LATLNG:
        return tuple(h3.cell_to_latlng(cell))    # v4 -> (lat, lon)
    lat, lon = h3.h3_to_geo(cell)                # v3
    return (lat, lon)

# -- Cache the MA polygon once (avoids repeated geocodes) --
_MA_GDF = ox.geocode_to_gdf("Massachusetts, USA")
_MA_GEOJSON = _MA_GDF.__geo_interface__["features"][0]["geometry"]


def acceptance(drive_anchors: List[Dict], walk_anchors: List[Dict], G_drive, G_walk) -> None:
    """Runs all QA checks and prints human-friendly diagnostics."""
    qa_coverage(drive_anchors, walk_anchors)
    qa_mandatory(drive_anchors, walk_anchors)
    qa_density(drive_anchors, walk_anchors)
    qa_connectivity(drive_anchors, walk_anchors, G_drive, G_walk)


# -----------------------
# Coverage (polygon-based)
# -----------------------
def qa_coverage(drive_anchors: List[Dict], walk_anchors: List[Dict]) -> None:
    """Percent of H3 cells within a threshold of at least one anchor (drive: r7; walk: urban r8)."""
    r7 = polyfill_cells(_MA_GEOJSON, H3_RES_LOW)
    r8 = polyfill_cells(_MA_GEOJSON, H3_RES_HIGH)

    def pct_within(points_ll: List[Tuple[float, float]], cells: List[str], thresh_m: float) -> float:
        if not points_ll or not cells:
            return 0.0
        lat0 = float(np.mean([p[0] for p in points_ll]))
        cos0 = math.cos(math.radians(lat0))
        P = np.c_[
            [p[1] * cos0 * 111000.0 for p in points_ll],  # x = lon*cosφ*111km
            [p[0] * 111000.0 for p in points_ll],          # y = lat*111km
        ]
        tree = cKDTree(P)
        miss = 0
        for c in cells:
            lat, lon = from_cell(c)
            qx, qy = lon * cos0 * 111000.0, lat * 111000.0
            d, _ = tree.query((qx, qy), k=1)
            if float(d) > float(thresh_m):
                miss += 1
        return 100.0 * (len(cells) - miss) / max(1, len(cells))

    drive_pts = [(a["lat"], a["lon"]) for a in drive_anchors]
    walk_pts  = [(a["lat"], a["lon"]) for a in walk_anchors]

    drive_pct = pct_within(drive_pts, r7, DRIVE_COVERAGE_KM * 1000.0)
    print(f"[QA] Drive coverage: {drive_pct:.1f}% of r7 cells within {DRIVE_COVERAGE_KM:.0f} km (target ≥95%)")
    if drive_pct < 95.0:
        print("  ⚠️  Below coverage target")

    urban_r8 = [c for c in r8 if classify_region(*from_cell(c)) == "urban"]
    walk_pct = pct_within(walk_pts, urban_r8, float(WALK_COVERAGE_M))
    print(f"[QA] Walk coverage: {walk_pct:.1f}% of urban r8 cells within {int(WALK_COVERAGE_M)} m (target ≥95%)")
    if walk_pct < 95.0:
        print("  ⚠️  Below coverage target")


# -----------------------
# Mandatory counts
# -----------------------
def qa_mandatory(drive_anchors: List[Dict], walk_anchors: List[Dict]) -> None:
    def count(anchors: List[Dict], kind: str) -> int:
        return sum(1 for a in anchors if a.get("mandatory") and a.get("kind") == kind)

    print(
        f"[QA] Mandatories: drive bridge={count(drive_anchors,'bridge')} "
        f"airport={count(drive_anchors,'airport')} ferry={count(drive_anchors,'ferry')}"
    )
    print(
        f"[QA] Mandatories: walk  bridge={count(walk_anchors,'bridge')} "
        f"airport={count(walk_anchors,'airport')} ferry={count(walk_anchors,'ferry')}"
    )


# -----------------------
# Anchor density (diagnostic)
# -----------------------
def qa_density(drive_anchors: List[Dict], walk_anchors: List[Dict]) -> None:
    """Median nearest-neighbor distance among anchors (meters), by mode."""
    for name, anchors in (("Drive", drive_anchors), ("Walk", walk_anchors)):
        if len(anchors) < 2:
            continue
        pts_ll = np.array([(a["lat"], a["lon"]) for a in anchors], dtype=float)
        lat0 = float(np.nanmean(pts_ll[:, 0])) if len(pts_ll) else 42.0
        cos0 = math.cos(math.radians(lat0))
        P = np.c_[
            pts_ll[:, 1] * cos0 * 111000.0,
            pts_ll[:, 0] * 111000.0,
        ]
        tree = cKDTree(P)
        dists = []
        for p in P:
            ds, _ = tree.query(p, k=2)
            if len(ds) > 1:
                dists.append(float(ds[1]))
        if dists:
            print(f"[QA] {name} median NN distance: {np.median(dists):.0f} m")


# -----------------------
# Connectivity (near-network sampling)
# -----------------------
def qa_connectivity(
    drive_anchors: List[Dict],
    walk_anchors: List[Dict],
    G_drive: nx.MultiDiGraph,
    G_walk: nx.MultiDiGraph,
) -> None:
    """
    Sample only cells near the road network, then test reachability
    to nearest anchor under a reasonable cutoff (time if present, else length).
    """
    def has_numeric_tt(G: nx.MultiDiGraph) -> bool:
        for _, _, d in G.edges(data=True):
            v = d.get("travel_time", None)
            if isinstance(v, (int, float)) and v > 0:
                return True
        return False

    def node_tree(G: nx.MultiDiGraph):
        ids = list(G.nodes)
        if not ids:
            return [], 42.0, 1.0, None
        xs = np.array([G.nodes[n]["x"] for n in ids], dtype=float)
        ys = np.array([G.nodes[n]["y"] for n in ids], dtype=float)
        lat0 = float(np.mean(ys))
        cos0 = math.cos(math.radians(lat0))
        P = np.c_[xs * cos0 * 111000.0, ys * 111000.0]
        return ids, lat0, cos0, cKDTree(P)

    # Land-only cells at low res
    r7_cells = polyfill_cells(_MA_GEOJSON, H3_RES_LOW)

    for name, anchors, G in (("Drive", drive_anchors, G_drive), ("Walk", walk_anchors, G_walk)):
        if not anchors or G.number_of_nodes() == 0:
            print(f"[QA] {name} connectivity success: 0% (no anchors or empty graph)")
            print("  ⚠️  Connectivity below target")
            continue

        # Graph node KD-tree
        ids, lat0, cos0, ntree = node_tree(G)
        if ntree is None:
            print(f"[QA] {name} connectivity success: 0% (no graph nodes)")
            print("  ⚠️  Connectivity below target")
            continue

        # Candidate cells near the network
        max_m = 5000 if name == "Drive" else 2000
        cand: List[Tuple[float, float, int]] = []
        for c in r7_cells:
            lat, lon = from_cell(c)
            qx, qy = lon * cos0 * 111000.0, lat * 111000.0
            d, idx = ntree.query((qx, qy), k=1)
            if float(d) <= float(max_m):
                cand.append((lat, lon, ids[int(idx)]))
        if not cand:
            print(f"[QA] {name} connectivity success: 0% (no near-network cells)")
            print("  ⚠️  Connectivity below target")
            continue

        # Anchor KD-tree (planar)
        A = np.c_[
            [a["lon"] * cos0 * 111000.0 for a in anchors],
            [a["lat"] * 111000.0 for a in anchors],
        ]
        atree = cKDTree(A)

        # Undirected to avoid one-way artifacts
        Gx = G.to_undirected(reciprocal=False)
        weight = "travel_time" if has_numeric_tt(G) else "length"
        cutoff = (60 * 40) if weight == "travel_time" else 60000  # 40 min or 60 km

        # Sample up to 60
        rng = np.random.default_rng(42)
        sample_idx = rng.choice(len(cand), size=min(60, len(cand)), replace=False)
        fails = 0
        for i in sample_idx:
            lat, lon, src_node = cand[i]
            # nearest anchor by planar distance
            qx, qy = lon * cos0 * 111000.0, lat * 111000.0
            _, aidx = atree.query((qx, qy), k=1)
            a_node = anchors[int(aidx)].get("node_id")

            ok = False
            if src_node in Gx and a_node in Gx:
                try:
                    dist = nx.single_source_dijkstra_path_length(
                        Gx, src_node, cutoff=cutoff, weight=weight
                    ).get(a_node)
                    ok = dist is not None
                except Exception:
                    ok = False
            fails += 0 if ok else 1

        success = 100.0 * (len(sample_idx) - fails) / max(1, len(sample_idx))
        print(f"[QA] {name} connectivity success: {success:.0f}% (target ≥60%)")
        if success < 60.0:
            print("  ⚠️  Connectivity below target")