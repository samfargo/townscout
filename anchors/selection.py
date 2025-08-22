# selection.py
import h3
import numpy as np
import pandas as pd
import networkx as nx
from typing import Dict, List, Set, Optional, Tuple
from tqdm import tqdm
from scipy.spatial import cKDTree
import osmnx as ox

from .core_utils import res_for_spacing, h3_edge_len_m, h3_disk, haversine_m

# =====================
# Snapping utilities
# =====================

def build_node_kdtree(G: nx.MultiDiGraph):
    ids = np.fromiter(G.nodes, dtype=np.int64)
    xs = np.array([G.nodes[n]["x"] for n in ids])  # lon
    ys = np.array([G.nodes[n]["y"] for n in ids])  # lat
    lat0 = np.deg2rad(np.mean(ys))
    X = np.c_[ (xs * np.cos(lat0)) * 111000.0, ys * 111000.0 ]
    tree = cKDTree(X)
    return ids, X, tree

def _scale_query(tree, lon: float, lat: float):
    lat0_rad = np.deg2rad(tree.data[:, 1].mean() / 111000.0)
    return np.array([(lon * np.cos(lat0_rad)) * 111000.0, lat * 111000.0])

def nearest_node_kdtree(ids, tree, lon: float, lat: float, max_m: float):
    Xq = _scale_query(tree, lon, lat)
    d, i = tree.query(Xq, k=1)
    return int(ids[i]) if float(d) <= max_m else None

def snap_to_node(G: nx.MultiDiGraph, lat: float, lon: float, max_m: float = 1000) -> Optional[int]:
    try:
        nid = ox.nearest_nodes(G, lon, lat)
        n = G.nodes[nid]
        if haversine_m(lat, lon, n["y"], n["x"]) <= max_m:
            return int(nid)
    except Exception:
        return None
    return None

def snap_drive_public_kdtree(G, lat, lon, ids, tree, r=80) -> Optional[int]:
    nid = nearest_node_kdtree(ids, tree, lon, lat, max_m=r)
    if not nid:
        return None
    for _, _, ed in G.edges(nid, data=True):
        if str(ed.get("access")) not in ("private", "no"):
            return int(nid)
    return int(nid)

# =====================
# Thinning utilities
# =====================

def select_anchors(cands: List[Dict], mode: str, spacing_cfg: Dict[str, float]) -> List[Dict]:
    if not cands:
        return []
    try:
        cands = pd.DataFrame(cands).drop_duplicates("node_id").to_dict("records")
    except Exception:
        pass

    cands.sort(key=lambda r: (not r.get("mandatory", False), -float(r.get("score", 0))))
    occupied: Set[str] = set()
    kept: List[Dict] = []

    for c in tqdm(cands, desc=f"[thin] {mode}", unit="anchor"):
        if c.get("mandatory", False):
            kept.append(c)
            r_m = spacing_cfg["urban"] * 0.3  # Reduced from 0.5
            res = res_for_spacing(r_m)
            cell = h3.latlng_to_cell(c["lat"], c["lon"], res)
            edge_m = h3_edge_len_m(res)
            k = max(1, int(round(r_m / (edge_m * 1.5))))
            occupied.update(h3_disk(cell, k))
            continue

        region = c.get("region", "rural")
        kind   = c.get("kind", "")
        road   = str(c.get("road_class", ""))

        # Base radius by region - reduced for better coverage
        r_m = spacing_cfg[region if region in spacing_cfg else "rural"]
        
        # For coverage candidates, use much smaller radius to maximize anchors
        if kind == "coverage":
            r_m *= 0.4  # Further reduced from 0.6 for maximum coverage
        
        # For intersections and crossings, also use smaller radius 
        if kind in ("intersection", "crossing"):
            r_m *= 0.5  # Allow denser intersection anchors

        if mode == "walk" and region == "urban" and kind == "coverage":
            r_m *= 0.75   # small extra squeeze for dense urban cover

        # Backbone gets denser spacing
        is_backbone = kind in ("mw_chain", "mw_junction", "ramp", "major_end")
        is_arterial = road in ("motorway","trunk","primary","secondary","tertiary","arterial")
        if mode == "drive":
            if is_backbone: r_m *= 0.6  # Further reduced from 0.75
            if is_arterial and float(c.get("score", 0)) >= 8.0: r_m *= 0.7  # Reduced from 0.85

        res = res_for_spacing(r_m)
        cell = h3.latlng_to_cell(c["lat"], c["lon"], res)
        edge_m = h3_edge_len_m(res)
        k = max(1, int(round(r_m / (edge_m * 1.8))))  # Increased divisor from 1.5 to 1.8
        ring = h3_disk(cell, k)

        if occupied.isdisjoint(ring):
            kept.append(c)
            occupied.update(ring)
    return kept

def guarantee_coverage(
    anchors: List[Dict],
    candidates: List[Dict],
    mode: str,
    target_km: float,
    region_filter: Optional[str] = None
) -> List[Dict]:
    """
    Ensure coverage targets are met by adding anchors to fill gaps.
    Uses expanding radius search to find the best candidates for each gap.
    """
    from .qa import _MA_GEOJSON, polyfill_cells, from_cell
    from .config import H3_RES_LOW, H3_RES_HIGH
    from .core_utils import classify_region, haversine_m
    from scipy.spatial import cKDTree
    import math
    import numpy as np

    if not anchors:
        print(f"[guarantee] No initial {mode} anchors, cannot guarantee coverage")
        return anchors

    # Get target cells to evaluate
    res = H3_RES_LOW if mode == "drive" else H3_RES_HIGH
    cells = polyfill_cells(_MA_GEOJSON, res)
    if region_filter:
        cells = [c for c in cells if classify_region(*from_cell(c)) == region_filter]
    
    if not cells:
        print(f"[guarantee] No {mode} cells to check coverage for")
        return anchors

    print(f"[guarantee] {mode}: Checking coverage for {len(cells)} cells")
    
    target_m = float(target_km) * 1000.0
    
    # Build anchor KD-tree for coverage checking
    def build_anchor_tree(anchor_list):
        if not anchor_list:
            return None, None, None
        anchor_pts = [(a["lat"], a["lon"]) for a in anchor_list]
        lat0 = float(np.mean([p[0] for p in anchor_pts]))
        cos0 = math.cos(math.radians(lat0))
        A = np.c_[
            [p[1] * cos0 * 111000.0 for p in anchor_pts],
            [p[0] * 111000.0 for p in anchor_pts],
        ]
        return cKDTree(A), lat0, cos0

    atree, lat0, cos0 = build_anchor_tree(anchors)
    
    # Find cells lacking coverage
    gaps = []
    for cell in cells:
        lat, lon = from_cell(cell)
        if atree is None:
            gaps.append((cell, lat, lon))
            continue
            
        qx, qy = lon * cos0 * 111000.0, lat * 111000.0
        d, _ = atree.query((qx, qy), k=1)
        if float(d) > target_m:
            gaps.append((cell, lat, lon))
    
    if not gaps:
        print(f"[guarantee] {mode}: Already meeting coverage target")
        return anchors
    
    print(f"[guarantee] {mode}: Found {len(gaps)} cells lacking coverage")
    
    # Build candidate KD-tree for efficient search
    cand_list = [c for c in candidates if c.get("lat") is not None and c.get("lon") is not None]
    if not cand_list:
        print(f"[guarantee] {mode}: No candidates available for gap fill")
        return anchors

    cand_lat0 = float(np.mean([c["lat"] for c in cand_list]))
    cand_cos0 = math.cos(math.radians(cand_lat0))
    C = np.c_[
        [c["lon"] * cand_cos0 * 111000.0 for c in cand_list],
        [c["lat"] * 111000.0 for c in cand_list],
    ]
    ctree = cKDTree(C)


    # Track used candidates
    used_nodes = {int(a.get("node_id")) for a in anchors if a.get("node_id") is not None}
    added = []
    
    # Limits to prevent runaway
    MAX_ADD = 12000 if mode == "walk" else 2000
    
    for gap_idx, (cell, glat, glon) in enumerate(gaps):
        if len(added) >= MAX_ADD:
            print(f"[guarantee] {mode}: Hit limit of {MAX_ADD} gap-fill anchors")
            break
            
        # Expanding radius search for candidates
        # Start with reasonable radius, expand if needed
        search_radii = [target_m * 0.5, target_m * 0.8, target_m * 1.0, target_m * 1.5, target_m * 2.0]
        
        best_candidate = None
        best_score = -1.0
        
        for search_radius in search_radii:
            # Find candidates within search radius
            qx, qy = glon * cand_cos0 * 111000.0, glat * 111000.0
            candidate_indices = ctree.query_ball_point((qx, qy), r=search_radius)
            
            if not candidate_indices:
                print(f"[guarantee] {mode}: 0 candidates within {int(search_radius)}m for gap {gap_idx}")
                continue  # Try larger radius
                
            # Evaluate each candidate within this radius
            for cidx in candidate_indices:
                c = cand_list[cidx]
                nid = c.get("node_id")
                
                # Skip if already used
                if nid is None or int(nid) in used_nodes:
                    continue
                
                # Check if this candidate can actually cover the gap
                coverage_dist = haversine_m(glat, glon, c["lat"], c["lon"])
                if coverage_dist > target_m:
                    continue  # Can't cover this gap
                
                # This candidate can cover the gap - is it better than current best?
                score = float(c.get("score", 0.0))
                
                # Prefer higher score, break ties with closer distance
                if score > best_score or (score == best_score and best_candidate is None):
                    best_candidate = c
                    best_score = score
            
            # If we found a good candidate, don't search larger radii
            if best_candidate is not None:
                break
        
        # Add the best candidate we found
        if best_candidate is not None:
            new_anchor = dict(best_candidate)
            new_anchor["source"] = "guarantee"
            new_anchor["kind"] = f"gap_fill_{mode}"
            added.append(new_anchor)
            used_nodes.add(int(best_candidate["node_id"]))
            
            # Periodically update anchor tree to account for new coverage
            if len(added) % 500 == 0:
                atree, lat0, cos0 = build_anchor_tree(anchors + added)
                print(f"[guarantee] {mode}: Progress {len(added)}/{len(gaps)} gaps filled")
    
    if added:
        print(f"[guarantee] {mode}: Added {len(added)} gap-fill anchors")
        
        # Final coverage check to report improvement
        final_atree, final_lat0, final_cos0 = build_anchor_tree(anchors + added)
        if final_atree is not None:
            remaining_gaps = 0
            for cell in cells:
                lat, lon = from_cell(cell)
                qx, qy = lon * final_cos0 * 111000.0, lat * 111000.0
                d, _ = final_atree.query((qx, qy), k=1)
                if float(d) > target_m:
                    remaining_gaps += 1
            
            coverage_pct = 100.0 * (len(cells) - remaining_gaps) / len(cells)
            print(f"[guarantee] {mode}: Coverage improved to {coverage_pct:.1f}%")
    else:
        print(f"[guarantee] {mode}: No suitable gap-fill candidates found")
    
    return anchors + added