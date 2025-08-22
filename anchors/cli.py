import argparse
import os
import sys

from .builder import AnchorBuilder
from . import config


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description=f"Build TownScout drive/walk anchors for {config.STATE_NAME}"
    )
    ap.add_argument(
        "--pbf",
        required=True,
        help=f"Path to {config.STATE_NAME} OSM PBF (e.g., {config.EXAMPLE_PBF if hasattr(config, 'EXAMPLE_PBF') else 'massachusetts.osm.pbf'})",
    )
    ap.add_argument(
        "--out",
        required=True,
        help="Output directory for parquet + QA HTML",
    )
    ap.add_argument(
        "--clear-cache",
        action="store_true",
        help="Clear cached networks and rebuild from scratch",
    )
    return ap


def run_cli(pbf_path: str, output_dir: str, clear_cache: bool = False) -> int:
    if not os.path.exists(pbf_path):
        print(f"ERROR: PBF not found: {pbf_path}")
        return 1

    os.makedirs(output_dir, exist_ok=True)

    builder = AnchorBuilder(pbf_path, output_dir)

    if clear_cache:
        builder.clear_cache()

    try:
        builder.run()
    except KeyboardInterrupt:
        print("\n[cli] Interrupted.")
        return 130  # 128 + SIGINT
    except Exception as e:
        # Keep it simple but useful; detailed logging belongs inside modules
        print(f"[cli] Fatal error: {e}")
        return 2

    return 0


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return run_cli(args.pbf, args.out, args.clear_cache)


if __name__ == "__main__":
    sys.exit(main())