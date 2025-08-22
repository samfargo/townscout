#!/usr/bin/env python3
import argparse, pandas as pd

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--anchors", required=True)        # e.g. out/anchors/anchors_drive.parquet
    ap.add_argument("--mode", required=True, choices=["drive","walk"])
    ap.add_argument("--out", required=True)            # e.g. out/anchors/anchor_index_drive.parquet
    args = ap.parse_args()

    # Load anchors and create a deterministic ordering:
    # sort by id (the stable string ID), then assign 0..N-1 as anchor_int_id
    df = pd.read_parquet(args.anchors)
    # Expect df to have at least: id (string stable ID) and node_id (int)
    if "id" not in df.columns:
        raise SystemExit("anchors parquet must contain 'id'")

    idx = (df
           .loc[:, ["id"]]
           .drop_duplicates()
           .sort_values(["id"])  # Sort by stable ID for deterministic ordering
           .reset_index(drop=True)
           .reset_index(names="anchor_int_id"))  # 0..N-1

    # Rename to match precompute_d_anchor.py expectations
    idx = idx.rename(columns={"id": "anchor_stable_id"})
    
    # Final columns: anchor_int_id (int), anchor_stable_id (str)
    idx.to_parquet(args.out, index=False)
    print(f"[ok] wrote {args.out} rows={len(idx)}")

if __name__ == "__main__":
    main() 