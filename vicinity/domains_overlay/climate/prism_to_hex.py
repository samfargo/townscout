"""
Convert PRISM climate normals to per-hex parquet with quantized values.

This is now a thin wrapper around the vicinity.domains_overlay.climate module.
"""
from __future__ import annotations

import argparse
import pathlib
import sys
from pathlib import Path
from typing import Sequence

# Add project root to path to import vicinity
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Import from the new vicinity.domains_overlay.climate module
from vicinity.domains_overlay.climate import process_climate_data
from vicinity.domains_overlay.climate.schema import (
    PRISM_DIR_DEFAULT,
    MINUTES_GLOB_DEFAULT,
    OUT_PARQUET_DEFAULT,
)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert PRISM normals to per-hex parquet.")
    parser.add_argument(
        "--prism-dir",
        default=str(PRISM_DIR_DEFAULT),
        help="Directory containing PRISM normals organised by variable/month.",
    )
    parser.add_argument(
        "--minutes-glob",
        default=MINUTES_GLOB_DEFAULT,
        help="Glob pointing at the per-state minutes parquet files.",
    )
    parser.add_argument(
        "--output",
        default=str(OUT_PARQUET_DEFAULT),
        help="Destination parquet path.",
    )
    parser.add_argument(
        "--resolutions",
        nargs="+",
        type=int,
        default=[7, 8],
        help="H3 resolutions to include.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    process_climate_data(
        prism_dir=pathlib.Path(args.prism_dir),
        minutes_glob=args.minutes_glob,
        output=pathlib.Path(args.output),
        resolutions=tuple(args.resolutions),
    )


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    main()
