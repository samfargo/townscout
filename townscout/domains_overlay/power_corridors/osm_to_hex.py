#!/usr/bin/env python3
"""
Compute a per-hex flag indicating proximity (<= 200 m) to high-voltage
transmission corridors derived from OSM power infrastructure data.

Usage (per state):

    PYTHONPATH=. python townscout/domains_overlay/power_corridors/osm_to_hex.py \
        --state massachusetts \
        --pbf data/osm/massachusetts.osm.pbf \
        --out data/power_corridors/massachusetts_near_power_corridor.parquet

The output parquet contains columns:
    - h3_id (uint64)
    - res (int32)
    - near_power_corridor (bool)

This is now a thin wrapper around the townscout.domains_overlay.power_corridors module.
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

# Import from the new townscout.domains_overlay.power_corridors module
from townscout.domains_overlay.power_corridors import compute_power_corridor_flags
from townscout.domains_overlay.power_corridors.schema import (
    BUFFER_METERS_DEFAULT,
    MIN_VOLTAGE_KV_DEFAULT
)

# All core logic is now in townscout.domains_overlay.power_corridors.build_corridor_overlay


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute power-corridor proximity flags per H3 hex.")
    parser.add_argument("--state", required=True, help="State slug (e.g. 'massachusetts').")
    parser.add_argument("--pbf", required=True, help="Path to the state's OSM .pbf extract.")
    parser.add_argument("--out", required=True, help="Output parquet path.")
    parser.add_argument("--buffer-meters", type=float, default=BUFFER_METERS_DEFAULT, help="Buffer distance around power lines.")
    parser.add_argument("--min-voltage-kv", type=float, default=MIN_VOLTAGE_KV_DEFAULT, help="Minimum voltage (in kV) to consider a line high-voltage.")
    parser.add_argument("--resolutions", type=int, nargs="*", help="Optional list of H3 resolutions to compute (defaults to config constants).")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    try:
        compute_power_corridor_flags(
            state=args.state,
            pbf_path=args.pbf,
            output_path=args.out,
            buffer_meters=args.buffer_meters,
            min_voltage_kv=args.min_voltage_kv,
            resolutions=args.resolutions,
        )
    except Exception as exc:
        print(f"[error] {exc}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

