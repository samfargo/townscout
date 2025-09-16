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
    try:
        from h3.api.basic_int import h3 as h3v4
        h3 = h3v4
        int_to_str = h3v4.h3_to_string
        to_boundary = h3v4.h3_to_geo_boundary
    except Exception:
        # Direct H3 v4 API
        import h3
        int_to_str = h3.int_to_str
        to_boundary = h3.cell_to_boundary

def hex_polygon_lonlat(h3_addr: str):
    # returns a closed lon/lat ring for GeoJSON
    try:
        # H3 v3 API
        boundary_latlon = to_boundary(h3_addr, geo_json=True)  # [(lat, lon), ...]
    except TypeError:
        # H3 v4 API - no geo_json parameter
        boundary_latlon = to_boundary(h3_addr)  # [(lat, lon), ...]
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
    ap.add_argument("--input", required=True, help="Input parquet file with hex data")
    ap.add_argument("--output", required=True, help="Output GeoJSON features (NDJSON)")
    ap.add_argument("--h3-col", default="h3_id")
    ap.add_argument("--keep-cols", nargs="*", default=[
        "chipotle_drive_min", "costco_drive_min"
    ], help="Columns to keep as properties in the GeoJSON features.")
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
        # Use .itertuples() instead of .iterrows() to preserve dtypes
        for i, row in enumerate(df.itertuples(index=False)):
            # Get h3_id directly from the named tuple to preserve uint64
            h3_col_idx = df.columns.get_loc(args.h3_col)
            h3_val = row[h3_col_idx]
            
            # Convert to H3 string address
            if isinstance(h3_val, (int, np.integer)):
                python_int = int(h3_val)
                h3_addr = int_to_str(python_int)
            else:
                # Fallback for unexpected types
                try:
                    python_int = int(float(str(h3_val)))
                    h3_addr = int_to_str(python_int)
                except (ValueError, TypeError):
                    h3_addr = str(h3_val)
            
            # Debug: print first few conversions
            if i < 3:
                print(f"Debug: h3_val={h3_val} ({type(h3_val)}) -> h3_addr={h3_addr}")
            
            ring = hex_polygon_lonlat(h3_addr)

            # Build properties from the row, excluding h3_col
            props = {}
            for col_idx, col_name in enumerate(df.columns):
                if col_name != args.h3_col:
                    props[col_name] = row[col_idx]
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
