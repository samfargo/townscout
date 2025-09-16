#!/usr/bin/env python3
import argparse, os, subprocess, sys, tempfile

def run(cmd):
    try:
        subprocess.run(cmd, check=True)
    except FileNotFoundError:
        sys.exit(f"[error] Not found: {cmd[0]}")
    except subprocess.CalledProcessError as e:
        sys.exit(f"[error] {' '.join(cmd)} -> {e.returncode}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Input NDJSON (one feature per line)")
    ap.add_argument("--output", required=True, help="Output PMTiles path")
    ap.add_argument("--layer", required=True)
    ap.add_argument("--minzoom", type=int, default=5)
    ap.add_argument("--maxzoom", type=int, default=12)  # crisp edges at local zooms
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    with tempfile.TemporaryDirectory() as td:
        mb = os.path.join(td, "out.mbtiles")
        tip = [
            "tippecanoe",
            "-o", mb,
            "-l", args.layer,
            "-Z", str(args.minzoom),
            "-z", str(args.maxzoom),
            "--force",
            "--read-parallel",
            "--drop-densest-as-needed",
            "--detect-shared-borders",
            "--no-tiny-polygon-reduction",
            "--simplification=2",
            args.input
        ]
        run(tip)
        run(["pmtiles", "convert", mb, args.output])
    print(f"[ok] {args.output}")
if __name__ == "__main__":
    main()
