#!/usr/bin/env python3
"""
Compute per-hex political lean flags based on 2024 US Presidential election results.

Usage:

    PYTHONPATH=src python src/politics/politics_to_hex.py \
        --csv townscout/domains_overlay/politics/countypres_2000-2024.csv \
        --out data/politics/political_lean.parquet

    # For state-specific processing:
    PYTHONPATH=src python src/politics/politics_to_hex.py \
        --csv townscout/domains_overlay/politics/countypres_2000-2024.csv \
        --out data/politics/massachusetts_political_lean.parquet \
        --state Massachusetts

The output parquet contains columns:
    - h3_id (uint64)
    - res (int32)
    - political_lean (uint8): 0=Strong Dem, 1=Lean Dem, 2=Moderate, 3=Lean Rep, 4=Strong Rep
    - rep_vote_share (float32): Republican vote share 0.0-1.0
    - county_fips (str): County FIPS code
    - county_name (str): County name

This is a thin wrapper around the townscout.domains_overlay.politics module.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional, Sequence

# Add project root to path to import townscout and config
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Add src to path to import config
src_path = Path(__file__).parent.parent
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from config import H3_RES_LOW, H3_RES_HIGH

# Import from the townscout.domains_overlay.politics module
from townscout.domains_overlay.politics import compute_political_lean_flags


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute political lean flags per H3 hex from 2024 presidential election results."
    )
    parser.add_argument(
        "--csv",
        required=True,
        help="Path to countypres_2000-2024.csv (MIT Election Lab dataset).",
    )
    parser.add_argument("--out", required=True, help="Output parquet path.")
    parser.add_argument(
        "--state",
        help="Optional state name filter (e.g., 'Massachusetts'). If omitted, processes all states.",
    )
    parser.add_argument(
        "--resolutions",
        type=int,
        nargs="*",
        help="Optional list of H3 resolutions to compute (defaults to config constants).",
    )
    parser.add_argument(
        "--data-dir",
        default="data",
        help="Base data directory for county boundaries (default: 'data').",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    try:
        compute_political_lean_flags(
            csv_path=args.csv,
            output_path=args.out,
            state_filter=args.state,
            resolutions=args.resolutions,
            data_dir=args.data_dir,
        )
    except Exception as exc:
        import traceback
        print(f"[error] {exc}")
        traceback.print_exc()
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

