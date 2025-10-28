#!/usr/bin/env python3
"""
Validate triangle inequality approximation errors in the anchor-based routing system.

This script measures the error distribution between:
- Anchor-based approximation: min(hex→anchor_i + anchor_i→POI) 
- Ground truth: direct hex→POI shortest path

Usage:
    python scripts/validate_triangle_approximation.py --state massachusetts --sample-size 1000
"""

import argparse
import numpy as np
import pandas as pd
from tqdm import tqdm
from typing import List, Tuple, Dict
import random
import sys
import os

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from graph.pyrosm_csr import load_or_build_csr
from t_hex import kbest_multisource_bucket_csr
import config


def sample_hex_poi_pairs(
    hex_ids: np.ndarray,
    anchor_sites: pd.DataFrame,
    canonical_pois: pd.DataFrame,
    sample_size: int
) -> List[Tuple[str, str, float, float, float, float]]:
    """Sample random (hex, POI) pairs for validation."""
    
    # Convert H3 IDs to lat/lon for routing
    import h3
    
    pairs = []
    pois_sample = canonical_pois.sample(min(len(canonical_pois), sample_size * 2))
    
    for _, poi in tqdm(pois_sample.iterrows(), desc="Sampling pairs", total=len(pois_sample)):
        if len(pairs) >= sample_size:
            break
            
        # Sample a random hex
        hex_id = random.choice(hex_ids)
        hex_lat, hex_lon = h3.h3_to_geo(hex_id)
        
        pairs.append((
            hex_id, poi['poi_id'], 
            hex_lat, hex_lon,
            poi.geometry.y, poi.geometry.x
        ))
    
    return pairs


def compute_anchor_approximation(
    hex_lat: float, hex_lon: float,
    poi_lat: float, poi_lon: float,
    anchors_df: pd.DataFrame,
    anchor_to_poi_times: Dict[int, float],
    node_ids: np.ndarray,
    node_lats: np.ndarray,
    node_lons: np.ndarray,
    indptr: np.ndarray,
    indices: np.ndarray,
    weights: np.ndarray
) -> float:
    """Compute travel time using current anchor-based approximation."""
    
    # Find nearest node to hex
    from scipy.spatial import cKDTree
    
    lat0 = np.deg2rad(np.mean(node_lats))
    m_per_deg = 111000.0
    X = np.c_[(node_lons * np.cos(lat0)) * m_per_deg, node_lats * m_per_deg]
    tree = cKDTree(X)
    
    hex_coords = np.array([(hex_lon * np.cos(lat0)) * m_per_deg, hex_lat * m_per_deg])
    _, hex_node_idx = tree.query(hex_coords)
    
    # Get K=20 best anchors for this hex (simulating T_hex)
    # This would normally come from precomputed T_hex, but we compute on-demand for validation
    anchor_node_indices = anchors_df['node_id'].values
    K = 20
    cutoff_s = 30 * 60  # 30 minutes
    
    # Single-source Dijkstra from hex node to all anchors
    from t_hex import kbest_multisource_bucket_csr
    # Note: This is a simplified version - full implementation would use the actual K-best algorithm
    
    # For now, approximate with distance to nearest anchors
    anchor_coords = np.c_[
        (anchors_df['lon'].values * np.cos(lat0)) * m_per_deg,
        anchors_df['lat'].values * m_per_deg
    ]
    anchor_tree = cKDTree(anchor_coords)
    
    hex_to_anchor_dists, nearest_anchor_indices = anchor_tree.query(hex_coords, k=min(K, len(anchors_df)))
    
    # Convert distances to approximate travel times (rough heuristic)
    # Real implementation would use actual graph distances
    avg_speed_m_per_s = 15.0  # ~35 mph average
    hex_to_anchor_times = hex_to_anchor_dists / avg_speed_m_per_s
    
    # Find minimum total time across anchors
    min_time = float('inf')
    for i, anchor_idx in enumerate(nearest_anchor_indices):
        anchor_id = anchors_df.iloc[anchor_idx]['anchor_int_id']
        if anchor_id in anchor_to_poi_times:
            total_time = hex_to_anchor_times[i] + anchor_to_poi_times[anchor_id]
            min_time = min(min_time, total_time)
    
    return min_time if min_time != float('inf') else None


def compute_ground_truth(
    hex_lat: float, hex_lon: float,
    poi_lat: float, poi_lon: float,
    node_ids: np.ndarray,
    node_lats: np.ndarray, 
    node_lons: np.ndarray,
    indptr: np.ndarray,
    indices: np.ndarray,
    weights: np.ndarray
) -> float:
    """Compute ground truth direct routing time from hex to POI."""
    
    # Find nearest nodes to hex and POI
    from scipy.spatial import cKDTree
    
    lat0 = np.deg2rad(np.mean(node_lats))
    m_per_deg = 111000.0
    X = np.c_[(node_lons * np.cos(lat0)) * m_per_deg, node_lats * m_per_deg]
    tree = cKDTree(X)
    
    hex_coords = np.array([(hex_lon * np.cos(lat0)) * m_per_deg, hex_lat * m_per_deg])
    poi_coords = np.array([(poi_lon * np.cos(lat0)) * m_per_deg, poi_lat * m_per_deg])
    
    _, hex_node_idx = tree.query(hex_coords)
    _, poi_node_idx = tree.query(poi_coords)
    
    # Single-source Dijkstra from hex node to POI node
    # This is a simplified implementation - would need proper SSSP
    
    # For validation purposes, use Euclidean distance as approximation
    # Real implementation would use actual shortest path algorithm
    distance = np.sqrt((hex_lon - poi_lon)**2 + (hex_lat - poi_lat)**2) * m_per_deg
    avg_speed_m_per_s = 15.0  # ~35 mph average
    
    return distance / avg_speed_m_per_s


def analyze_errors(approximations: List[float], ground_truths: List[float]) -> Dict:
    """Analyze error distribution between approximations and ground truth."""
    
    approx_arr = np.array(approximations)
    truth_arr = np.array(ground_truths)
    
    # Remove invalid entries
    valid_mask = (~np.isnan(approx_arr)) & (~np.isnan(truth_arr)) & (truth_arr > 0)
    approx_arr = approx_arr[valid_mask]
    truth_arr = truth_arr[valid_mask]
    
    if len(approx_arr) == 0:
        return {"error": "No valid samples"}
    
    # Calculate errors
    absolute_errors = np.abs(approx_arr - truth_arr)
    relative_errors = absolute_errors / truth_arr
    
    # Error statistics
    return {
        "sample_size": len(approx_arr),
        "mean_absolute_error_minutes": np.mean(absolute_errors) / 60,
        "median_absolute_error_minutes": np.median(absolute_errors) / 60,
        "p95_absolute_error_minutes": np.percentile(absolute_errors, 95) / 60,
        "max_absolute_error_minutes": np.max(absolute_errors) / 60,
        
        "mean_relative_error_pct": np.mean(relative_errors) * 100,
        "median_relative_error_pct": np.median(relative_errors) * 100,
        "p95_relative_error_pct": np.percentile(relative_errors, 95) * 100,
        "max_relative_error_pct": np.max(relative_errors) * 100,
        
        # Cases where approximation is significantly wrong
        "large_errors_5min_pct": np.mean(absolute_errors > 300) * 100,  # >5 minutes
        "large_errors_10min_pct": np.mean(absolute_errors > 600) * 100,  # >10 minutes
        "large_errors_50pct_pct": np.mean(relative_errors > 0.5) * 100,  # >50% error
    }


def main():
    parser = argparse.ArgumentParser(description="Validate triangle inequality approximation errors")
    parser.add_argument("--state", required=True, help="State to analyze (e.g., massachusetts)")
    parser.add_argument("--sample-size", type=int, default=1000, help="Number of (hex, POI) pairs to sample")
    parser.add_argument("--mode", default="drive", choices=["drive", "walk"])
    parser.add_argument("--output", help="Output CSV path for detailed results")
    
    args = parser.parse_args()
    
    print(f"🔍 Validating triangle inequality approximation for {args.state}")
    print(f"   Sample size: {args.sample_size}")
    print(f"   Mode: {args.mode}")
    
    # Load data
    print("\n📊 Loading data...")
    
    # Load POIs
    poi_path = f"data/poi/{args.state}_canonical.parquet"
    if not os.path.exists(poi_path):
        raise FileNotFoundError(f"POI file not found: {poi_path}")
    
    canonical_pois = pd.read_parquet(poi_path)
    print(f"   POIs: {len(canonical_pois):,}")
    
    # Load anchors
    anchor_path = f"data/anchors/{args.state}_{args.mode}_sites.parquet"
    if not os.path.exists(anchor_path):
        raise FileNotFoundError(f"Anchor file not found: {anchor_path}")
    
    anchors_df = pd.read_parquet(anchor_path)
    print(f"   Anchors: {len(anchors_df):,}")
    
    # Load T_hex data to get hex coverage
    thex_path = f"data/minutes/{args.state}_{args.mode}_t_hex.parquet"
    if not os.path.exists(thex_path):
        raise FileNotFoundError(f"T_hex file not found: {thex_path}")
    
    t_hex_df = pd.read_parquet(thex_path)
    hex_ids = t_hex_df['h3_id'].unique()
    print(f"   Hex cells: {len(hex_ids):,}")
    
    # Sample validation pairs
    print(f"\n🎯 Sampling {args.sample_size} (hex, POI) pairs...")
    sample_pairs = sample_hex_poi_pairs(hex_ids, anchors_df, canonical_pois, args.sample_size)
    print(f"   Generated {len(sample_pairs)} pairs")
    
    # For now, create a simplified analysis
    print(f"\n⚠️  NOTE: This is a framework for validation.")
    print(f"   Full implementation requires:")
    print(f"   1. Actual graph shortest path computation")
    print(f"   2. Integration with existing T_hex/D_anchor data")
    print(f"   3. Proper anchor-to-POI distance computation")
    
    print(f"\n✅ Validation framework created at: {__file__}")
    print(f"   Run with actual routing implementation to get error statistics")


if __name__ == "__main__":
    main()
