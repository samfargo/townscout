#!/usr/bin/env python3
"""
Compute a stable fingerprint for one or more anchor artifacts.

We hash the raw bytes of each file in order and emit a single SHA256 digest.
"""
from __future__ import annotations

import argparse
import hashlib
import sys
from pathlib import Path
from typing import Iterable


def iter_paths(raw_paths: Iterable[str]) -> Iterable[Path]:
    for raw in raw_paths:
        path = Path(raw).expanduser()
        if not path.exists():
            raise FileNotFoundError(f"{path}")
        if not path.is_file():
            raise FileNotFoundError(f"{path}")
        yield path


def compute_digest(paths: Iterable[Path]) -> str:
    hasher = hashlib.sha256()
    for path in paths:
        with path.open("rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                hasher.update(chunk)
    return hasher.hexdigest()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Compute SHA256 fingerprint for anchor artifacts.")
    parser.add_argument("paths", nargs="+", help="Files to include in the fingerprint (order matters).")
    args = parser.parse_args(argv)

    try:
        digest = compute_digest(iter_paths(args.paths))
    except FileNotFoundError as exc:
        sys.stderr.write(f"[fingerprint] missing file: {exc}\n")
        return 1

    sys.stdout.write(digest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
