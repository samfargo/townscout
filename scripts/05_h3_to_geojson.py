#!/usr/bin/env python3
import argparse, json, os, sys
import numpy as np
import pandas as pd

# Handle h3 v3 and v4
try:
    import h3
    int_to_str = getattr(h3, "int_to_string", None) or getattr(h3, "h3_to_string", None)
    to_boundary = h3.h3_to_geo_boundary
except Exception:
    from h3.api.basic_int import h3 as h3v4
    h3 = h3v4
    int_to_str = h3v4.h3_to_string
    to_boundary = h3v4.h3_to_geo_boundary

def hex_polygon_lonlat(h3_addr: str):
    # returns a closed lon/lat ring for GeoJSON
    boundary_latlon = to_boundary(h3_addr, geo_json=True)  # [(lat, lon), ...]
    ring = [[lon, lat] for (lat, lon) in boundary_latlon]
    if ring[0] != ring[-1]:
        ring.append(ring[0])
    return ring

def coerce_jsonable(props: dict):
    out = {}
    for k, v in props.items():
        if isinstance(v, (np.integer,)) or str(v).isdigit():
            out[k] = int(v)
        elif isinstance(v, (np.floating,)):
            out[k] = float(v)
        elif pd.isna(v):
            # drop nulls to keep props small
            continue
        else:
            out[k] = v
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Input T_hex parquet file")
    ap.add_argument("--output", required=True, help="Output NDJSON (one feature per line)")
    ap.add_argument("--h3-col", default="h3_id")
    ap.add_argument("--keep-cols", nargs="*", default=[
        "k","a0_id","a0_s","a0_flags","a1_id","a1_s","a1_flags",
        "a2_id","a2_s","a2_flags","a3_id","a3_s","a3_flags","walkscore"
    ])
    args = ap.parse_args()

    if not os.path.exists(args.input):
        sys.exit(f"[error] Input not found: {args.input}")

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)

    # Read in one shot for now; for US scale switch to pyarrow.dataset and row-group streaming.
    df = pd.read_parquet(args.input)

    if args.h3_col not in df.columns:
        sys.exit("[error] Missing h3_id column")

    # Keep only required columns to shrink JSON
    keep = [c for c in args.keep_cols if c in df.columns]
    cols = [args.h3_col] + keep
    df = df[cols]

    with open(args.output, "w") as out:
        for _, row in df.iterrows():
            # h3 id as address string (safe for JS) — never emit 64-bit ints
            h3_addr = int_to_str(int(row[args.h3_col])) if isinstance(row[args.h3_col], (int, np.integer)) else str(row[args.h3_col])
            ring = hex_polygon_lonlat(h3_addr)

            props = row.drop(labels=[args.h3_col]).to_dict()
            props = coerce_jsonable(props)
            # keep h3 id as string property if you want debugging; drop if not needed
            props["h3_id"] = h3_addr

            feat = {
                "type": "Feature",
                "geometry": {"type": "Polygon", "coordinates": [ring]},
                "properties": props
            }
            out.write(json.dumps(feat, separators=(",", ":")) + "\n")

    print("[ok] Wrote NDJSON features.")
if __name__ == "__main__":
    main()
