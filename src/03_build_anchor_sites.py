"""
Build Anchor Sites per state and persist mapping to stable integer IDs.

Inputs:
- Canonical POIs: data/poi/<state>_canonical.parquet
- OSM graph (CSR via cache): data/osm/<state>.osm.pbf

Outputs:
- data/anchors/<state>_<mode>_sites.parquet  (with columns: site_id, node_id, lon, lat, poi_ids, brands, categories, anchor_int_id)
- data/anchors/<state>_<mode>_site_id_map.parquet (anchor_int_id:int32, site_id:str)

Notes:
- Anchorable POIs are those with category or brand_id.
- Snap radius comes from config.
"""
import os
import argparse
import pandas as pd
import geopandas as gpd
import numpy as np
from scipy.spatial import cKDTree

from graph.pyrosm_csr import load_or_build_csr
import config

_TRAUMA_CATEGORY_ALIASES = {
    "trauma_level_1_adult": "trauma_level_1_adult",
    "trauma_level_1_pediatric": "trauma_level_1_pediatric",
    "adult": "trauma_level_1_adult",
    "pediatric": "trauma_level_1_pediatric",
    "peds": "trauma_level_1_pediatric",
}


def _expand_categories(row: pd.Series) -> list[str]:
    """
    Expand a POI's categories to include specialty buckets.
    Ensures Level 1 trauma centers stay accessible under both the general
    'hospital' label and their trauma-specific filters.
    """
    categories: set[str] = set()
    cat = row.get("category")
    if isinstance(cat, str) and cat.strip():
        categories.add(cat.strip())

    # Subcategory string may already include "trauma_level_1_*"
    subcat = row.get("subcat")
    if isinstance(subcat, str) and subcat.strip():
        alias = _TRAUMA_CATEGORY_ALIASES.get(subcat.strip().lower())
        if alias:
            categories.add("hospital")
            categories.add(alias)

    # Trauma level field can be "adult"/"pediatric"
    trauma_level = row.get("trauma_level")
    if isinstance(trauma_level, str) and trauma_level.strip():
        alias = _TRAUMA_CATEGORY_ALIASES.get(trauma_level.strip().lower())
        if alias:
            categories.add("hospital")
            categories.add(alias)

    return sorted(categories)


def assign_anchor_int_ids(sites_df: pd.DataFrame) -> pd.DataFrame:
    # Deterministic stable ordering by site_id (uuid string)
    out = sites_df.sort_values("site_id").reset_index(drop=True).copy()
    out["anchor_int_id"] = out.index.astype(np.int32)
    return out


def build_anchor_sites_from_nodes(
    canonical_pois: gpd.GeoDataFrame,
    node_ids: np.ndarray,
    node_lats: np.ndarray,
    node_lons: np.ndarray,
    mode: str,
    indptr: np.ndarray = None,
) -> pd.DataFrame:
    print(f"--- Building anchor sites for {mode} mode ---")
    if canonical_pois.empty or node_ids.size == 0:
        print("[warn] Canonical POIs or graph is empty. No anchor sites will be built.")
        return pd.DataFrame()

    before = len(canonical_pois)
    # Tighten to overhaul scope: allowlisted categories OR allowlisted brands (A-list)
    allowlist_path = os.path.join("data", "taxonomy", "category_allowlist.txt")
    allowed: set[str] = set()
    if os.path.isfile(allowlist_path):
        try:
            with open(allowlist_path, "r") as f:
                allowed = {ln.strip() for ln in f if ln.strip() and not ln.strip().startswith("#")}
        except Exception:
            allowed = set()
    # Optional brand allowlist (canonical brand_id per line)
    brand_allow_path = os.path.join("data", "brands", "allowlist.txt")
    brand_allowed: set[str] = set()
    if os.path.isfile(brand_allow_path):
        try:
            with open(brand_allow_path, "r") as f:
                brand_allowed = {ln.strip() for ln in f if ln.strip() and not ln.strip().startswith("#")}
        except Exception:
            brand_allowed = set()

    if allowed or brand_allowed:
        cond_cat = canonical_pois["category"].isin(sorted(allowed)) if allowed else False
        cond_brand = canonical_pois["brand_id"].isin(sorted(brand_allowed)) if brand_allowed else False
        mask = cond_cat | cond_brand
    else:
        # Fallback: previous broader behavior
        mask = canonical_pois["category"].notna() | canonical_pois["brand_id"].notna()
    canonical_pois = canonical_pois.loc[mask].copy()
    after = len(canonical_pois)
    print(f"[info] Anchorable POIs (overhaul scope): {after} / {before}")

    print(f"[info] Building KD-tree from {len(node_ids)} graph nodes...")
    lat0 = float(np.deg2rad(float(np.mean(node_lats))))
    m_per_deg = 111000.0
    X = np.c_[ (node_lons.astype(np.float64) * np.cos(lat0)) * m_per_deg, node_lats.astype(np.float64) * m_per_deg ]
    tree = cKDTree(X)

    print(f"[info] Snapping {len(canonical_pois)} POIs to nearest graph nodes (connectivity-aware)...")
    poi_coords = np.c_[
        (canonical_pois.geometry.x.to_numpy() * np.cos(lat0)) * m_per_deg,
        canonical_pois.geometry.y.to_numpy() * m_per_deg,
    ]
    
    # Compute node connectivity (number of outgoing edges per node)
    # indptr is loaded via load_or_build_csr, but we need it here
    # For now, we'll use a heuristic: query k nearest neighbors and pick the best-connected one
    # that's within a reasonable distance factor (e.g., 2x the nearest)
    K_CANDIDATES = 10  # Consider up to 10 nearest nodes
    MAX_DISTANCE_FACTOR = 2.0  # Accept nodes up to 2x the nearest distance
    MIN_ACCEPTABLE_EDGES = 2  # Prefer nodes with at least 2 edges
    
    # Query k nearest neighbors for each POI
    dists_k, indices_k = tree.query(poi_coords, k=K_CANDIDATES)
    
    # For each POI, select the best node among candidates
    selected_indices = np.zeros(len(canonical_pois), dtype=np.int64)
    selected_dists = np.zeros(len(canonical_pois), dtype=np.float64)
    
    # Compute node connectivity if indptr is available
    if indptr is not None:
        node_edge_counts = np.diff(indptr).astype(np.int32)
    else:
        node_edge_counts = None
    
    improved_snaps = 0  # Track how many POIs got better-connected nodes
    
    for i in range(len(canonical_pois)):
        candidates_dists = dists_k[i] if dists_k.ndim > 1 else np.array([dists_k[i]])
        candidates_indices = indices_k[i] if indices_k.ndim > 1 else np.array([indices_k[i]])
        
        # Filter candidates within acceptable distance
        nearest_dist = candidates_dists[0]
        max_acceptable_dist = nearest_dist * MAX_DISTANCE_FACTOR
        valid_mask = candidates_dists <= max_acceptable_dist
        
        valid_dists = candidates_dists[valid_mask]
        valid_indices = candidates_indices[valid_mask]
        
        if node_edge_counts is not None and len(valid_indices) > 1:
            # Connectivity-aware selection: prefer nodes with more edges
            # Get edge counts for all valid candidates
            valid_edge_counts = node_edge_counts[valid_indices]
            nearest_edge_count = valid_edge_counts[0]
            
            # If the nearest node has only 1 edge, try to find a better-connected alternative
            if nearest_edge_count == 1 and np.max(valid_edge_counts) >= MIN_ACCEPTABLE_EDGES:
                # Find the best-connected node among candidates with at least MIN_ACCEPTABLE_EDGES
                better_mask = valid_edge_counts >= MIN_ACCEPTABLE_EDGES
                if np.any(better_mask):
                    better_indices = np.where(better_mask)[0]
                    # Among better-connected nodes, pick the one with most edges (breaking ties by distance)
                    best_idx = better_indices[np.argmax(valid_edge_counts[better_mask])]
                    selected_indices[i] = valid_indices[best_idx]
                    selected_dists[i] = valid_dists[best_idx]
                    improved_snaps += 1
                else:
                    # No better options, use nearest
                    selected_indices[i] = valid_indices[0]
                    selected_dists[i] = valid_dists[0]
            else:
                # Nearest is acceptable, use it
                selected_indices[i] = valid_indices[0]
                selected_dists[i] = valid_dists[0]
        else:
            # No connectivity info or only one candidate, use nearest
            selected_indices[i] = valid_indices[0]
            selected_dists[i] = valid_dists[0]
    
    if improved_snaps > 0:
        print(f"[info] Improved connectivity for {improved_snaps} POIs by selecting better-connected nodes")
    
    pois_with_nodes = canonical_pois.copy()
    pois_with_nodes['node_id'] = node_ids[selected_indices]
    pois_with_nodes['snap_dist_m'] = selected_dists

    MAX_SNAP_DISTANCE_M = config.SNAP_RADIUS_M_DRIVE if mode == 'drive' else config.SNAP_RADIUS_M_WALK
    pois_with_nodes = pois_with_nodes[pois_with_nodes['snap_dist_m'] <= MAX_SNAP_DISTANCE_M]
    print(f"[info] {len(pois_with_nodes)} POIs snapped within {MAX_SNAP_DISTANCE_M}m of the graph.")

    # Expand per-POI categories to include trauma specialties alongside hospitals.
    pois_with_nodes["anchor_categories"] = pois_with_nodes.apply(_expand_categories, axis=1)

    print("[info] Grouping POIs into anchor sites...")
    aggs = {
        'poi_id': lambda x: list(dict.fromkeys([val for val in x.tolist() if pd.notna(val)])),
        'brand_id': lambda x: list(x.dropna().unique()),
        'anchor_categories': lambda series: sorted({
            cat
            for cats in series
            if isinstance(cats, (list, tuple, set))
            for cat in cats
            if isinstance(cat, str) and cat
        }),
    }
    sites = pois_with_nodes.groupby('node_id').agg(aggs).reset_index()

    node_coords = pd.DataFrame({
        'node_id': node_ids,
        'lon': node_lons.astype(np.float64),
        'lat': node_lats.astype(np.float64),
    }).set_index('node_id')
    sites = sites.join(node_coords, on='node_id', how='left')

    import uuid
    sites['site_id'] = sites.apply(
        lambda row: str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{mode}|{row['node_id']}")),
        axis=1
    )

    sites = sites.rename(columns={
        'poi_id': 'poi_ids',
        'brand_id': 'brands',
        'anchor_categories': 'categories',
    })

    final_cols = ['site_id', 'node_id', 'lon', 'lat', 'poi_ids', 'brands', 'categories']
    sites = sites[final_cols]
    
    # Track Costco count in anchor sites
    costco_sites = sum(1 for brands in sites['brands'] if any('costco' in str(b).lower() for b in brands))
    print(f"[COSTCO] Anchor sites: {costco_sites} sites with Costco")
    
    print(f"[ok] Built {len(sites)} anchor sites from {len(pois_with_nodes)} POIs.")
    return sites


def main():
    ap = argparse.ArgumentParser(description="Build Anchor Sites per state")
    ap.add_argument("--state", required=True)
    ap.add_argument("--mode", required=True, choices=["drive", "walk"])
    ap.add_argument("--pois", required=True)
    ap.add_argument("--pbf", required=True)
    ap.add_argument("--out-sites", required=True)
    ap.add_argument("--out-map", required=True)
    args = ap.parse_args()

    # Load canonical POIs
    df = pd.read_parquet(args.pois)
    gdf = gpd.GeoDataFrame(df.drop(columns=["geometry"]), geometry=gpd.GeoSeries.from_wkb(df["geometry"]))
    
    # Track Costco count in canonical POIs
    costco_canonical = len(gdf[gdf['brand_id'] == 'costco']) if 'brand_id' in gdf.columns else 0
    print(f"[COSTCO] Canonical POIs: {costco_canonical} POIs")

    # Load CSR (and H3 cache if needed)
    node_ids, indptr, indices, w_sec, node_lats, node_lons, node_h3_by_res, res_used = load_or_build_csr(
        args.pbf, args.mode, [7, 8], False
    )

    # Build sites (with connectivity-aware snapping)
    sites = build_anchor_sites_from_nodes(gdf, node_ids, node_lats, node_lons, args.mode, indptr)
    if sites.empty:
        raise SystemExit("No anchor sites built. Check inputs.")

    # Assign stable int IDs and persist
    sites_with_ids = assign_anchor_int_ids(sites)
    os.makedirs(os.path.dirname(args.out_sites) or ".", exist_ok=True)
    sites_with_ids.to_parquet(args.out_sites, index=False)

    # Sidecar id map
    id_map = sites_with_ids[["anchor_int_id", "site_id"]].copy()
    os.makedirs(os.path.dirname(args.out_map) or ".", exist_ok=True)
    id_map.to_parquet(args.out_map, index=False)

    print(f"[ok] Wrote {len(sites_with_ids)} sites to {args.out_sites}")
    print(f"[ok] Wrote id map to {args.out_map}")


if __name__ == "__main__":
    main()
