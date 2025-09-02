#!/usr/bin/env python3
# scripts/precompute_d_anchor.py
"""
Precompute Anchor → Category travel times (D_anchor).

What this emits:
  Parquet with columns:
    anchor_int_id (int32)   # matches precompute_t_hex anchor index
    category_id   (int32)
    mode          (string)  # "drive" | "walk"
    seconds_u16   (uint16)  # travel time to nearest POI in category (capped)
    snapshot_ts   (string, YYYY-MM-DD)

Design choices:
- Driving uses the REVERSED directed graph and runs multi-source Dijkstra
  from POIs. That equals anchor→POI on the original graph and respects one-ways.
- Walking uses an undirected graph (directionless).
- Values are in SECONDS (uint16) with UNREACH_U16 sentinel=65535, consistent with T_hex.
- Optionally consumes an anchor index parquet (anchor_int_id ↔ anchor_stable_id)
  to guarantee IDs match T_hex tiles.

Inputs expected:
- --pbf:          PBF extract for the state/region
- --anchors:      anchors parquet with at least columns: id (stable), node_id (int64), [mode]
- --anchor-index: parquet with columns: anchor_int_id(int32), anchor_stable_id(string) [optional but recommended]
- POIs:           GeoParquet per category at data/poi/{state}_{category}.parquet (geometry in WGS84)

"""

import argparse
import os
import time
from typing import Dict, List, Tuple, Set

import numpy as np
import pandas as pd
import geopandas as gpd
import networkx as nx
import osmnx as ox
import pyarrow as pa
import pyarrow.parquet as pq
from pyrosm import OSM
from scipy.spatial import cKDTree
from tqdm import tqdm

from src import categories, util_osm, config

SNAPSHOT_TS = time.strftime("%Y-%m-%d")

# Sentinels (keep consistent with T_hex)
UNREACH_U16 = config.UNREACH_U16
U16_MAX_FLOOR = 65534


def assert_no_nulls(df, cols):
    bad = [c for c in cols if df[c].isna().any()]
    if bad:
        raise SystemExit(f"Nulls in required columns: {bad}")


# -----------------------------
# KD-tree snapping (lon/lat → nearest node within meters)
# -----------------------------
def build_node_kdtree(G: nx.MultiDiGraph) -> Tuple[np.ndarray, cKDTree, float, float]:
    ids = np.fromiter(G.nodes, dtype=np.int64)
    xs = np.array([G.nodes[n]["x"] for n in ids], dtype="float64")  # lon
    ys = np.array([G.nodes[n]["y"] for n in ids], dtype="float64")  # lat
    lat0 = float(np.deg2rad(np.mean(ys)))
    m_per_deg = 111000.0
    X = np.c_[(xs * np.cos(lat0)) * m_per_deg, ys * m_per_deg]
    tree = cKDTree(X)
    return ids, tree, lat0, m_per_deg

def snap_points_to_nodes(G: nx.MultiDiGraph, pts: gpd.GeoDataFrame, max_m: float, mode: str = "drive") -> List[int]:
    if pts is None or pts.empty:
        return []
    ids, tree, lat0, m_per_deg = build_node_kdtree(G)
    out = []
    for geom in pts.geometry:
        if geom is None:
            continue
        # Handle both Point and Polygon geometries
        if hasattr(geom, 'x'):  # Point geometry
            qx, qy = float(geom.x), float(geom.y)
        else:  # Polygon or other geometry - use centroid
            centroid = geom.centroid
            qx, qy = float(centroid.x), float(centroid.y)
        
        # Find candidate nodes within radius
        Xq = np.array([(qx * np.cos(lat0)) * m_per_deg, qy * m_per_deg])
        
        # For driving mode, try to find publicly accessible nodes
        if mode == "drive":
            # Search for multiple candidates within radius and pick the best accessible one
            distances, indices = tree.query(Xq, k=min(10, len(ids)), distance_upper_bound=max_m)
            
            best_node = None
            for dist, idx in zip(distances, indices):
                if not np.isfinite(dist) or dist > max_m:
                    continue
                    
                candidate_node = int(ids[idx])
                
                # Check if this node has any publicly accessible edges
                has_public_access = False
                for _, _, d in G.edges(candidate_node, data=True):
                    if str(d.get("access")) not in ("private", "no"):
                        has_public_access = True
                        break
                
                # If no outgoing edges checked, check incoming edges
                if not has_public_access:
                    for _, _, d in G.in_edges(candidate_node, data=True):
                        if str(d.get("access")) not in ("private", "no"):
                            has_public_access = True
                            break
                
                # Use the first publicly accessible node we find
                if has_public_access:
                    best_node = candidate_node
                    break
            
            # If no publicly accessible node found, fall back to closest node
            if best_node is None:
                d, idx = tree.query(Xq, k=1)
                if float(d) <= float(max_m):
                    best_node = int(ids[int(idx)])
            
            if best_node is not None:
                out.append(best_node)
        else:
            # For walking mode, use simple nearest node (access restrictions less relevant)
            d, idx = tree.query(Xq, k=1)
            if float(d) <= float(max_m):
                out.append(int(ids[int(idx)]))
    
    # unique & stable
    return list(pd.Index(out).unique().astype("int64"))


# -----------------------------
# Airport-specific snapping: project to nearest PUBLIC arterial node
# -----------------------------
def snap_points_to_public_arterials(G: nx.MultiDiGraph, pts: gpd.GeoDataFrame, max_m: float = 5000.0) -> List[int]:
    """
    Snap each point to the nearest node that is connected to at least one
    PUBLIC, non-service carriageway (arterial/residential/unclassified, incl. _link).

    Rationale: Airports often sit on service/private fabric. Snapping to public
    arterials guarantees connectivity to the main road network and avoids
    stranding the source on internal loops/parking aisles.
    """
    if pts is None or pts.empty:
        return []

    allowed_highways: Set[str] = {
        "motorway", "motorway_link",
        "trunk", "trunk_link",
        "primary", "primary_link",
        "secondary", "secondary_link",
        "tertiary", "tertiary_link",
        "residential", "unclassified", "living_street",
    }

    # Collect candidate nodes that touch an allowed public edge
    candidate_nodes: Set[int] = set()
    for u, v, k, d in G.edges(keys=True, data=True):
        hw = str(d.get("highway", "")).lower()
        if hw not in allowed_highways:
            continue
        access = str(d.get("access", "")).lower()
        if access in ("private", "no"):
            # Keep private edges for routing elsewhere, but do not use them as snap targets
            continue
        if u in G and v in G:
            candidate_nodes.add(int(u))
            candidate_nodes.add(int(v))

    if not candidate_nodes:
        # Fallback: no filtering, use general snapping
        return snap_points_to_nodes(G, pts, max_m=max_m, mode="drive")

    # Build KD-tree on candidate nodes only (in meters)
    ids = np.fromiter(candidate_nodes, dtype=np.int64)
    xs = np.array([G.nodes[n]["x"] for n in ids], dtype="float64")
    ys = np.array([G.nodes[n]["y"] for n in ids], dtype="float64")
    lat0 = float(np.deg2rad(np.mean(ys)))
    m_per_deg = 111000.0
    X = np.c_[(xs * np.cos(lat0)) * m_per_deg, ys * m_per_deg]
    tree = cKDTree(X)

    chosen: List[int] = []
    for geom in pts.geometry:
        if geom is None:
            continue
        if hasattr(geom, 'x'):
            qx, qy = float(geom.x), float(geom.y)
        else:
            c = geom.centroid
            qx, qy = float(c.x), float(c.y)

        Xq = np.array([(qx * np.cos(lat0)) * m_per_deg, qy * m_per_deg])
        d, idx = tree.query(Xq, k=1)
        if np.isfinite(d) and float(d) <= float(max_m):
            chosen.append(int(ids[int(idx)]))

    if not chosen:
        # Last resort
        return snap_points_to_nodes(G, pts, max_m=max_m, mode="drive")

    return list(pd.Index(chosen).unique().astype("int64"))


# -----------------------------
# POI loading (one category file)
# -----------------------------
def load_pois_for_category(state_slug: str, cat_slug: str) -> gpd.GeoDataFrame:
    path = f"data/poi/{state_slug}_{cat_slug}.parquet"
    if not os.path.exists(path):
        print(f"[warn] POI parquet missing: {path}")
        return gpd.GeoDataFrame({"geometry": []}, geometry="geometry", crs="EPSG:4326")
    gdf = gpd.read_parquet(path)
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    return gdf[["geometry"]]


# -----------------------------
# Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser(description="Precompute Anchor→Category seconds (D_anchor)")
    ap.add_argument("--pbf", required=True)
    ap.add_argument("--anchors", required=True, help="anchors parquet (id, node_id, [mode])")
    ap.add_argument("--anchor-index", required=True, help="anchor index parquet (anchor_int_id, anchor_stable_id)")
    ap.add_argument("--mode", required=True, choices=["drive", "walk"])
    ap.add_argument("--state", required=True, help="state slug, e.g., 'massachusetts'")
    ap.add_argument("--categories", nargs="+", required=True, help="category slugs")
    ap.add_argument("--out", required=True)
    ap.add_argument("--drive-cutoff-min", type=int, default=240, help="max minutes for drive leg (cap to fit uint16 seconds)")
    ap.add_argument("--walk-cutoff-min", type=int, default=60, help="max minutes for walk leg")
    ap.add_argument("--snap-max-m", type=int, default=1200, help="drive snap radius (m); walking will override smaller")
    args = ap.parse_args()

    # Load graph using the centralized utility function
    G = util_osm.load_graph(args.pbf, args.mode)

    # Prepare metadata to embed in output
    metadata = {
        "source_pbf": os.path.basename(args.pbf),
        "mode": args.mode,
        "state": args.state,
        "categories": ",".join(args.categories),
        "snap_max_m": str(args.snap_max_m),
        "drive_cutoff_min": str(args.drive_cutoff_min),
        "walk_cutoff_min": str(args.walk_cutoff_min),
        "graph_config": str(config.GRAPH_CONFIG.get(args.mode, {})),
        "creation_date": SNAPSHOT_TS,
        "dataset_version": config.DATASET_VERSION,
        "id_space": "anchor_int_id",
    }

    # Load anchor data
    print("[info] Loading anchor data...")
    anc = pd.read_parquet(args.anchors)
    if "mode" in anc.columns:
        anc = anc[anc["mode"] == args.mode]
    if not {"id", "node_id"}.issubset(anc.columns):
        raise SystemExit("anchors parquet must include columns: id, node_id")
    anc = anc[["id", "node_id"]].copy()
    anc["node_id"] = pd.to_numeric(anc["node_id"], errors="coerce").astype("Int64")
    anc = anc.dropna(subset=["node_id"])
    anc["node_id"] = anc["node_id"].astype("int64")
    anc["id"] = anc["id"].astype("string")

    # Anchor ID mapping (stable -> int32) to match T_hex
    if not (args.anchor_index and os.path.exists(args.anchor_index)):
        raise SystemExit(f"--anchor-index file not found: {args.anchor_index}")

    idx = pd.read_parquet(args.anchor_index)
    if not {"anchor_int_id", "anchor_stable_id"}.issubset(idx.columns):
        raise SystemExit("--anchor-index parquet must have columns: anchor_int_id, anchor_stable_id")
    idx["anchor_stable_id"] = idx["anchor_stable_id"].astype("string")
    # Join to get anchor_int_id
    anc = anc.merge(idx, left_on="id", right_on="anchor_stable_id", how="left")
    if anc["anchor_int_id"].isna().any():
        missing = anc[anc["anchor_int_id"].isna()]["id"].unique().tolist()
        raise SystemExit(f"Some anchors missing in anchor-index: {missing[:10]} …")
    anc["anchor_int_id"] = anc["anchor_int_id"].astype("int32")
    print(f"[info] Anchors mapped via index: {len(anc)} rows, "
          f"{anc['anchor_int_id'].nunique()} unique anchor_int_id")

    anchor_nodes = anc[["anchor_int_id", "node_id"]].copy()
    # Sanity: one row per anchor_int_id in anchor_nodes
    anchor_nodes = anchor_nodes.drop_duplicates(subset=["anchor_int_id"])

    # Build graph & pick query graph per mode
    if args.mode == "drive":
        # Direction-correct: POIs → anchors on REVERSED graph equals anchor→POI on forward
        Gq = G.reverse(copy=True)
        snap_max_m = int(args.snap_max_m)
        cutoff_sec = int(min(args.drive_cutoff_min, 1092) * 60)  # cap to ~18.2hr to fit uint16
    else:
        Gq = G.to_undirected(reciprocal=False)
        snap_max_m = 400  # tighter for walks
        cutoff_sec = int(min(args.walk_cutoff_min, 1092) * 60)  # cap to ~18.2hr to fit uint16

    out_rows = []

    for slug in args.categories:
        cat = categories.get_category(slug)  # expect .id and optional default_cutoff_min
        cat_cutoff_min = getattr(cat, "default_cutoff", None)
        if cat_cutoff_min is not None:
            cutoff_sec_eff = int(min(cat_cutoff_min * 60, cutoff_sec))
        else:
            cutoff_sec_eff = cutoff_sec

        print(f"[cat] {slug} (category_id={int(cat.id)}) cutoff={cutoff_sec_eff//60} min")
        pois = load_pois_for_category(args.state, slug)
        if slug == "airports" and args.mode == "drive":
            # Use arterial snapping to avoid getting stranded on service/private fabric
            src_nodes = snap_points_to_public_arterials(G, pois, max_m=max(5000, snap_max_m))
        else:
            src_nodes = snap_points_to_nodes(Gq, pois, max_m=snap_max_m, mode=args.mode)
        if not src_nodes:
            print(f"[cat] {slug}: 0 POIs snapped; skipping.")
            continue

        # Multi-source Dijkstra from POIs on query graph
        node_sec: Dict[int, float] = nx.multi_source_dijkstra_path_length(
            Gq, src_nodes, weight="travel_time", cutoff=cutoff_sec_eff
        )

        # Map anchor nodes to seconds
        tmp = pd.DataFrame({"node_id": list(node_sec.keys()),
                            "seconds": list(node_sec.values())})
        merged = anchor_nodes.merge(tmp, on="node_id", how="left")

        # seconds → uint16 with UNREACH sentinel
        def to_u16(s) -> np.uint16:
            if pd.isna(s) or not np.isfinite(s):
                return UNREACH_U16
            v = int(round(float(s)))
            if v < 0:
                v = 0
            if v > U16_MAX_FLOOR:
                # Cap at 65534; 65535 reserved as UNREACH
                v = U16_MAX_FLOOR
            return np.uint16(v)

        merged["seconds_u16"] = merged["seconds"].apply(to_u16).astype("uint16")
        merged["category_id"] = int(cat.id)
        merged["mode"] = args.mode
        merged["snapshot_ts"] = SNAPSHOT_TS

        out_rows.append(merged[["anchor_int_id", "category_id", "mode", "seconds_u16", "snapshot_ts"]])

        # QA print
        secs = merged.loc[merged["seconds_u16"] < UNREACH_U16, "seconds_u16"].astype("int")
        if len(secs):
            med = int(np.median(secs))
            p95 = int(np.percentile(secs, 95))
            cover = 100.0 * len(secs) / max(1, len(merged))
            print(f"[QA] {slug}: median={med}s p95={p95}s coverage={cover:.1f}%")

    if not out_rows:
        raise SystemExit("No categories produced output.")

    out_df = pd.concat(out_rows, ignore_index=True)

    # ---- enforce schema & partitioning ----
    MODE_MAP = {"drive": 0, "walk": 2}  # keep reserved ids (bike=1, transit=3)
    out_df["category_id"] = out_df["category_id"].astype("uint16")
    out_df["mode_u8"]      = out_df["mode"].map(MODE_MAP).astype("uint8")

    # epoch ms int64
    snapshot_ms = int(pd.Timestamp(SNAPSHOT_TS).tz_localize("UTC").timestamp() * 1000)
    out_df["snapshot_ts_ms"] = np.int64(snapshot_ms)

    # final column order
    out_df = out_df[["anchor_int_id", "category_id", "mode_u8", "seconds_u16", "snapshot_ts_ms"]]
    out_df = out_df.rename(columns={
        "mode_u8": "mode",
        "snapshot_ts_ms": "snapshot_ts",
        "seconds_u16": "seconds"
    })

    assert_no_nulls(out_df, ["anchor_int_id", "category_id", "mode", "seconds", "snapshot_ts"])
    assert out_df.dtypes["seconds"] == "uint16"

    # Write Parquet
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)

    # Add metadata
    metadata_bytes = {k: v.encode('utf-8') for k, v in metadata.items()}

    # write partitioned
    pq.write_to_dataset(
        pa.Table.from_pandas(out_df, preserve_index=False).replace_schema_metadata(metadata_bytes),
        root_path=os.path.dirname(args.out) or ".",
        partition_cols=["mode", "category_id"],
        basename_template="part-{i}.parquet"
    )
    print(f"[ok] wrote partitioned dataset under {os.path.dirname(args.out) or '.'}")


if __name__ == "__main__":
    main()