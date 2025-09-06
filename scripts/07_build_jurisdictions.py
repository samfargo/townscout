#!/usr/bin/env python3
import argparse, json, os, sys
from typing import List

import geopandas as gpd

# Allow importing util_boundaries from src/
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.util_boundaries import build_jurisdiction_layer  # noqa: E402


def gdf_to_ndjson(gdf: gpd.GeoDataFrame, out_path: str, keep_props: List[str]) -> None:
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w") as f:
        for _, row in gdf.iterrows():
            props = {k: row.get(k) for k in keep_props if k in gdf.columns}
            geom = row.geometry.__geo_interface__
            feat = {"type": "Feature", "geometry": geom, "properties": props}
            f.write(json.dumps(feat, separators=(",", ":")) + "\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--state-fips", default="25", help="Two-digit FIPS (default: 25 for MA)")
    ap.add_argument("--ndjson", default="tiles/ma_jurisdictions.geojson.nd", help="Output NDJSON path")
    ap.add_argument("--keep-cols", nargs="*", default=["juris_name", "state_abbr", "juris_geoid"], help="Columns to keep as properties")
    ap.add_argument("--boundaries-dir", default="data/boundaries", help="Directory with TIGER/Line extracts")
    args = ap.parse_args()

    gdf = build_jurisdiction_layer(args.state_fips, data_dir=args.boundaries_dir)
    if gdf is None or gdf.empty:
        sys.exit("[error] No jurisdictions available. Run src/08_download_boundaries.py first.")

    # Ensure WGS84
    if gdf.crs and gdf.crs.to_string() != "EPSG:4326":
        gdf = gdf.to_crs("EPSG:4326")

    # Keep only needed columns
    keep = [c for c in args.keep_cols if c in gdf.columns]
    cols = keep + ["geometry"]
    gdf = gdf[cols]

    gdf_to_ndjson(gdf, args.ndjson, keep)
    print(f"[ok] Wrote jurisdictions NDJSON -> {args.ndjson}")


if __name__ == "__main__":
    main()


