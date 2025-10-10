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

TEMP_SCALE = 0.1
PPT_MM_SCALE = 0.1
PPT_IN_SCALE = 0.1

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
            # Convert nulls to high value for unreachable areas
            if k.endswith('_drive_min') or k.endswith('_walk_min'):
                out[k] = 9999  # Unreachable but visible
            else:
                continue  # Drop other nulls to keep props small
        else:
            out[k] = v
    return out

def decode_climate_columns(df: pd.DataFrame) -> pd.DataFrame:
    decoded = df.copy()
    temp_cols = [c for c in decoded.columns if c.endswith("_f_q")]
    ppt_mm_cols = [c for c in decoded.columns if c.endswith("_mm_q")]
    ppt_in_cols = [c for c in decoded.columns if c.endswith("_in_q")]

    for col in temp_cols:
        new_col = col[:-2]  # strip _q suffix
        decoded[new_col] = decoded[col].astype("float64", copy=False) * TEMP_SCALE
    for col in ppt_mm_cols:
        new_col = col[:-2]
        decoded[new_col] = decoded[col].astype("float64", copy=False) * PPT_MM_SCALE
    for col in ppt_in_cols:
        new_col = col[:-2]
        decoded[new_col] = decoded[col].astype("float64", copy=False) * PPT_IN_SCALE

    drop_cols = temp_cols + ppt_mm_cols + ppt_in_cols
    if drop_cols:
        decoded = decoded.drop(columns=drop_cols)
    return decoded

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Input parquet file with hex data")
    ap.add_argument("--merge", nargs="*", default=[], help="Optional additional parquet files to left-join on h3/res")
    ap.add_argument("--output", required=True, help="Output GeoJSON features (NDJSON)")
    ap.add_argument("--h3-col", default="h3_id")
    ap.add_argument(
        "--keep-cols",
        nargs="*",
        default=None,
        help="Columns to keep as properties in the GeoJSON features. If omitted, keeps all columns except h3_col."
    )
    args = ap.parse_args()

    if not os.path.exists(args.input):
        sys.exit(f"[error] Input not found: {args.input}")

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)

    # Read in one shot for now; for US scale switch to pyarrow.dataset and row-group streaming.
    df = pd.read_parquet(args.input)

    # Optionally merge in additional properties from other parquet files
    for mpath in (args.merge or []):
        if not mpath:
            continue
        if not os.path.exists(mpath):
            print(f"[warn] Merge file not found: {mpath}; skipping")
            continue
        mdf = pd.read_parquet(mpath)
        # Determine join keys: prefer both h3_id and res if available
        join_keys = [k for k in [args.h3_col, 'res'] if k in df.columns and k in mdf.columns]
        if not join_keys:
            # Fall back to h3 only
            if args.h3_col in mdf.columns:
                join_keys = [args.h3_col]
            else:
                print(f"[warn] Merge file {mpath} lacks join key; skipping")
                continue
        # Drop duplicate columns except join keys
        dup_cols = [c for c in mdf.columns if c in df.columns and c not in join_keys]
        mdf = mdf.drop(columns=dup_cols)
        before_cols = set(df.columns)
        df = pd.merge(df, mdf, on=join_keys, how='left')
        added = [c for c in df.columns if c not in before_cols]
        print(f"[merge] {mpath}: +{len(added)} cols")

    if args.h3_col not in df.columns:
        sys.exit("[error] Missing h3_id column")

    # Keep only required columns to shrink JSON (or keep all if not specified)
    if args.keep_cols:
        keep = [c for c in args.keep_cols if c in df.columns]
        cols = [args.h3_col] + keep
        df = df[cols]
    else:
        # Keep everything except the h3 column itself
        df = df[[c for c in df.columns if True]]

    if os.environ.get("CLIMATE_DECODE_AT_EXPORT", "").lower() in {"1", "true", "yes"}:
        df = decode_climate_columns(df)

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
