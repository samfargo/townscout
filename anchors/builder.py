import os
from typing import List, Dict, Optional, Tuple

from . import config
from .networks import build_network
from .core_utils import load_graph, save_graph, load_candidates, save_candidates, save_anchors, write_qa_map, classify_region, haversine_m
from .candidates import (
    drive_candidates,
    walk_candidates,
    add_rural_coverage_fast,
    add_ped_hubs_fast,
    add_motorway_chain,
)
from .mandatories import bridgeheads, ferry_terminals, airport_terminals
from .selection import (
    build_node_kdtree,
    snap_drive_public_kdtree,
    snap_to_node,
    nearest_node_kdtree,
    select_anchors,
    guarantee_coverage,
)
from .qa import acceptance


class AnchorBuilder:
    def __init__(self, pbf_path: str, output_dir: str):
        self.pbf_path = pbf_path
        self.output_dir = output_dir
        self.state = config.STATE_NAME

        # Target spacing (meters)
        self.drive_spacing = config.DRIVE_SPACING
        self.walk_spacing = config.WALK_SPACING

        # Coverage QA targets
        self.drive_coverage_km = config.DRIVE_COVERAGE_KM
        self.walk_coverage_m = config.WALK_COVERAGE_M

        os.makedirs(output_dir, exist_ok=True)

    # -----------------------------
    # Internal helpers
    # -----------------------------
    def _ensure_snapped(
        self,
        G,
        cands: List[Dict],
        mode: str,
        kdtree_data: Optional[Tuple] = None,
    ) -> List[Dict]:
        """
        Ensure every candidate is snapped to a real graph node exactly once.
        Also sets c['snap_dist_m'] for QA (no more NaNs).
        """
        out: List[Dict] = []
        if kdtree_data is None:
            ids, X, tree = build_node_kdtree(G)
        else:
            # tolerate either (ids, tree) or (ids, X, tree)
            if len(kdtree_data) == 2:
                ids, tree = kdtree_data
            else:
                ids, _, tree = kdtree_data

        for c in cands:
            # preserve original for distance calc
            orig_lat = float(c.get("lat", float("nan")))
            orig_lon = float(c.get("lon", float("nan")))
            nid = c.get("node_id")

            # snap if missing/invalid
            if nid is None or nid not in G:
                if mode == "drive":
                    nid = snap_drive_public_kdtree(G, orig_lat, orig_lon, ids, tree, r=200)
                else:
                    # (lon, lat) order for KD
                    nid = nearest_node_kdtree(ids, tree, orig_lon, orig_lat, max_m=200)

                if nid is None or int(nid) not in G:
                    # can't snap confidently, drop it
                    continue

                n = G.nodes[int(nid)]
                c["node_id"] = int(nid)
                c["lon"] = float(n["x"])
                c["lat"] = float(n["y"])
                c["snap_dist_m"] = float(haversine_m(orig_lat, orig_lon, n["y"], n["x"]))
            else:
                # already a valid graph node; set dist=0 and normalize coords off graph
                n = G.nodes[int(nid)]
                c["lon"] = float(n.get("x"))
                c["lat"] = float(n.get("y"))
                c["snap_dist_m"] = 0.0

            out.append(c)

        return out

    # -----------------------------
    # Public API
    # -----------------------------
    def run(self):
        print(f"[anchors] Building for {self.state} from {self.pbf_path}")

        drive_cache = os.path.join(self.output_dir, config.DRIVE_CACHE)
        walk_cache = os.path.join(self.output_dir, config.WALK_CACHE)
        drive_candidates_cache = os.path.join(self.output_dir, config.DRIVE_CANDIDATES_CACHE)
        walk_candidates_cache = os.path.join(self.output_dir, config.WALK_CANDIDATES_CACHE)

        # Load or build graphs (and cache)
        G_drive = load_graph(drive_cache)
        if G_drive is None:
            print("[anchors] Building drive network…")
            G_drive = build_network(self.pbf_path, "driving")
            save_graph(G_drive, drive_cache)
        else:
            print(f"[anchors] Loading cached drive network from {drive_cache}")

        G_walk = load_graph(walk_cache)
        if G_walk is None:
            print("[anchors] Building walk network…")
            G_walk = build_network(self.pbf_path, "walk")
            save_graph(G_walk, walk_cache)
        else:
            print(f"[anchors] Loading cached walk network from {walk_cache}")

        # Load or generate candidates (and cache)
        drive_cands = load_candidates(drive_candidates_cache)
        if drive_cands is None:
            print("[anchors] Generating drive candidates…")
            drive_cands = drive_candidates(
                G_drive,
                self.pbf_path,
                add_rural_cov=lambda G, c, mode="drive": add_rural_coverage_fast(
                    G, c, mode, classify_region
                ),
                add_motorway_chain=lambda G, c, spacing_m=2500: add_motorway_chain(
                    G, c, spacing_m, classify_region
                ),
                region_fn=classify_region,
            )
            # Debug output after generation
            from collections import Counter
            print(f"[debug] drive candidate count: {len(drive_cands)}")
            print(f"[debug] drive kinds: {Counter([c['kind'] for c in drive_cands]).most_common()[:10]}")
            save_candidates(drive_cands, drive_candidates_cache)
        else:
            print(f"[anchors] Loading cached drive candidates from {drive_candidates_cache}")
            # Debug output for cached too
            from collections import Counter
            print(f"[debug] cached drive candidate count: {len(drive_cands)}")
            print(f"[debug] cached drive kinds: {Counter([c['kind'] for c in drive_cands]).most_common()[:10]}")

        walk_cands = load_candidates(walk_candidates_cache)
        if walk_cands is None:
            print("[anchors] Generating walk candidates…")
            walk_cands = walk_candidates(
                G_walk,
                self.pbf_path,
                add_ped_hubs_fast=lambda G, c, kd, pbf_path, region_fn: add_ped_hubs_fast(
                    G, c, kd, pbf_path, region_fn
                ),
                region_fn=classify_region,
            )
            # Debug output after generation
            from collections import Counter
            print(f"[debug] walk candidate count: {len(walk_cands)}")
            print(f"[debug] walk kinds: {Counter([c['kind'] for c in walk_cands]).most_common()[:10]}")
            save_candidates(walk_cands, walk_candidates_cache)
        else:
            print(f"[anchors] Loading cached walk candidates from {walk_candidates_cache}")
            # Debug output for cached too
            from collections import Counter
            print(f"[debug] cached walk candidate count: {len(walk_cands)}")
            print(f"[debug] cached walk kinds: {Counter([c['kind'] for c in walk_cands]).most_common()[:10]}")

        # Mandatories
        print("[anchors] Adding mandatory anchors…")
        m_drive: List[Dict] = []
        m_drive += bridgeheads(G_drive, "drive", classify_region)

        # Pre-build KD trees to reuse during mandatory snapping
        ids_d, X_d, tree_d = build_node_kdtree(G_drive)
        ids_w, X_w, tree_w = build_node_kdtree(G_walk)

        m_drive += ferry_terminals(
            G_drive,
            "drive",
            self.pbf_path,
            snap_drive_public=lambda G, lat, lon, r=80: snap_drive_public_kdtree(
                G, lat, lon, ids_d, tree_d, r=r
            ),
            snap_to_node=snap_to_node,
            region_fn=classify_region,
        )
        m_drive += airport_terminals(
            G_drive,
            self.pbf_path,
            snap_drive_public=lambda G, lat, lon, r=80: snap_drive_public_kdtree(
                G, lat, lon, ids_d, tree_d, r=r
            ),
            region_fn=classify_region,
        )

        m_walk: List[Dict] = []
        m_walk += bridgeheads(G_walk, "walk", classify_region)
        m_walk += ferry_terminals(
            G_walk,
            "walk",
            self.pbf_path,
            # For walking, just snap to nearest node with a slightly larger radius
            snap_drive_public=lambda G, lat, lon, r=120: nearest_node_kdtree(
                ids_w, tree_w, lon, lat, max_m=r
            ),
            snap_to_node=lambda G, lat, lon, max_m=120: nearest_node_kdtree(
                ids_w, tree_w, lon, lat, max_m=max_m
            ),
            region_fn=classify_region,
        )

        # Snap to nodes
        print("[anchors] Building KD-trees for efficient snapping…")
        ids_d, X_d, tree_d = build_node_kdtree(G_drive)
        ids_w, X_w, tree_w = build_node_kdtree(G_walk)

        # === CRS/coordinate sanity check (add this) ===
        def _rng(a):
            import numpy as np
            return float(np.nanmin(a)), float(np.nanmax(a))
        import numpy as np
        drive_lats = np.array([G_drive.nodes[n]["y"] for n in G_drive.nodes()])
        drive_lons = np.array([G_drive.nodes[n]["x"] for n in G_drive.nodes()])
        print(f"[diag] drive graph lat range: {_rng(drive_lats)}, lon range: {_rng(drive_lons)}")
        walk_lats = np.array([G_walk.nodes[n]["y"] for n in G_walk.nodes()])
        walk_lons = np.array([G_walk.nodes[n]["x"] for n in G_walk.nodes()])
        print(f"[diag] walk  graph lat range: {_rng(walk_lats)}, lon range: {_rng(walk_lons)}")
        # === end sanity check ===
        
        print("[anchors] Snapping candidates to graph nodes…")
        drive_cands = self._ensure_snapped(G_drive, drive_cands, "drive", (ids_d, tree_d))
        walk_cands = self._ensure_snapped(G_walk, walk_cands, "walk", (ids_w, tree_w))
        m_drive = self._ensure_snapped(G_drive, m_drive, "drive", (ids_d, tree_d))
        m_walk = self._ensure_snapped(G_walk, m_walk, "walk", (ids_w, tree_w))
        
        # Debug after snapping
        import numpy as np
        drive_snap_dists = np.array([c.get("snap_dist_m", np.nan) for c in drive_cands])
        walk_snap_dists = np.array([c.get("snap_dist_m", np.nan) for c in walk_cands])
        print(f"[debug] drive after snap: {len(drive_cands)}, median snap dist: {np.nanmedian(drive_snap_dists):.1f}m, p95: {np.nanpercentile(drive_snap_dists, 95):.1f}m")
        print(f"[debug] walk after snap: {len(walk_cands)}, median snap dist: {np.nanmedian(walk_snap_dists):.1f}m, p95: {np.nanpercentile(walk_snap_dists, 95):.1f}m")

        # Select anchors (thinning)
        print("[anchors] Selecting/thinning drive anchors…")
        selected_drive = select_anchors(drive_cands + m_drive, "drive", self.drive_spacing)
        print("[anchors] Selecting/thinning walk anchors…")
        selected_walk = select_anchors(walk_cands + m_walk, "walk", self.walk_spacing)
        
        # Debug after thinning
        print(f"[debug] selected drive: {len(selected_drive)} (from {len(drive_cands + m_drive)})")
        print(f"[debug] selected walk: {len(selected_walk)} (from {len(walk_cands + m_walk)})")

        # Guarantee coverage targets are met
        print("[anchors] Ensuring drive coverage target…")
        selected_drive = guarantee_coverage(
            selected_drive, 
            drive_cands + m_drive, 
            "drive", 
            self.drive_coverage_km
        )
        print("[anchors] Ensuring walk coverage target…")
        selected_walk = guarantee_coverage(
            selected_walk, 
            walk_cands + m_walk, 
            "walk", 
            self.walk_coverage_m / 1000.0,  # Convert meters to km
            region_filter="urban"
        )
        
        # Debug after guarantee
        print(f"[debug] final drive: {len(selected_drive)}")
        print(f"[debug] final walk: {len(selected_walk)}")

        # === Sanity + quick connectivity probe (add this) ===
        drv_ids = [a["node_id"] for a in selected_drive]
        wlk_ids = [a["node_id"] for a in selected_walk]
        missing_drv = sum(1 for n in drv_ids if n not in G_drive)
        missing_wlk = sum(1 for n in wlk_ids if n not in G_walk)
        print(f"[sanity] missing drive nodes in graph: {missing_drv}/{len(drv_ids)}")
        print(f"[sanity] missing walk nodes in graph: {missing_wlk}/{len(wlk_ids)}")

        import random
        import networkx as nx
        def probe(G, ids, k=10):
            ids = [n for n in ids if n in G]
            ok = 0
            for _ in range(min(k, len(ids)//2)):
                u, v = random.sample(ids, 2)
                try:
                    nx.shortest_path_length(G, u, v, weight="length")
                    ok += 1
                except Exception:
                    pass
            return ok, min(k, len(ids)//2)

        d_ok, d_tot = probe(G_drive, drv_ids, k=20)
        w_ok, w_tot = probe(G_walk,  wlk_ids, k=20)
        print(f"[probe] drive connectivity: {d_ok}/{d_tot}")
        print(f"[probe] walk  connectivity: {w_ok}/{w_tot}")
        # === end sanity + probe ===

        # Persist + QA
        print("[anchors] Saving parquet outputs…")
        save_anchors(selected_drive, "drive", self.output_dir)
        save_anchors(selected_walk, "walk", self.output_dir)

        print("[anchors] Generating QA map…")
        write_qa_map(selected_drive, selected_walk, self.output_dir)

        print("[anchors] Running acceptance tests…")
        acceptance(selected_drive, selected_walk, G_drive, G_walk)

        print(
            f"✅ Done. Drive={len(selected_drive)} Walk={len(selected_walk)} → {self.output_dir}"
        )
        print(
            f"💾 Networks cached for next run. Delete {drive_cache} or {walk_cache} to rebuild."
        )

    def clear_cache(self) -> None:
        """Clear cached networks and candidates to force rebuild."""
        drive_cache = os.path.join(self.output_dir, config.DRIVE_CACHE)
        walk_cache = os.path.join(self.output_dir, config.WALK_CACHE)
        drive_candidates_cache = os.path.join(self.output_dir, config.DRIVE_CANDIDATES_CACHE)
        walk_candidates_cache = os.path.join(self.output_dir, config.WALK_CANDIDATES_CACHE)

        for cache_file in (drive_cache, walk_cache, drive_candidates_cache, walk_candidates_cache):
            if os.path.exists(cache_file):
                os.remove(cache_file)
                print(f"[cache] Removed {cache_file}")
        print("[cache] Network and candidate cache cleared")