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

    print(f"[info] Snapping {len(canonical_pois)} POIs to nearest graph nodes...")
    poi_coords = np.c_[
        (canonical_pois.geometry.x.to_numpy() * np.cos(lat0)) * m_per_deg,
        canonical_pois.geometry.y.to_numpy() * m_per_deg,
    ]
    dists, indices = tree.query(poi_coords, k=1)

    pois_with_nodes = canonical_pois.copy()
    pois_with_nodes['node_id'] = node_ids[indices]
    pois_with_nodes['snap_dist_m'] = dists

    MAX_SNAP_DISTANCE_M = config.SNAP_RADIUS_M_DRIVE if mode == 'drive' else config.SNAP_RADIUS_M_WALK
    pois_with_nodes = pois_with_nodes[pois_with_nodes['snap_dist_m'] <= MAX_SNAP_DISTANCE_M]
    print(f"[info] {len(pois_with_nodes)} POIs snapped within {MAX_SNAP_DISTANCE_M}m of the graph.")

    print("[info] Grouping POIs into anchor sites...")
    aggs = {
        'poi_id': lambda x: list(x),
        'brand_id': lambda x: list(x.dropna().unique()),
        'category': lambda x: list(x.dropna().unique()),
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
        'category': 'categories',
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

    # Build sites
    sites = build_anchor_sites_from_nodes(gdf, node_ids, node_lats, node_lons, args.mode)
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
