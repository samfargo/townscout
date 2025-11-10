#!/usr/bin/env python3
from __future__ import annotations

"""
Update Source Acquisition Ledger

Maintains a CSV ledger of source files with:
- File hashes (SHA256)
- Download timestamps
- File sizes
- Optional notes

Validates:
- Files haven't been unchanged for >7 days (staleness)
- File size changes are <25% (anomaly detection)
"""
import sys
import hashlib
import csv
import math
import numbers
import struct
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Iterable, Sequence

try:
    import duckdb  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    duckdb = None

try:
    import pandas as pd  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    pd = None


LEDGER_PATH = Path("data/source_ledger.csv")
STALENESS_DAYS = 7
MAX_SIZE_DELTA = 0.25  # 25%


def compute_file_hash(file_path: Path) -> str:
    """Compute SHA256 hash of a file."""
    sha256 = hashlib.sha256()
    
    with open(file_path, 'rb') as f:
        while True:
            chunk = f.read(8192)
            if not chunk:
                break
            sha256.update(chunk)
    
    return sha256.hexdigest()


def load_ledger() -> List[Dict[str, str]]:
    """Load existing ledger entries."""
    if not LEDGER_PATH.exists():
        return []
    
    entries = []
    with open(LEDGER_PATH, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Skip comment lines
            if row.get('file_path', '').startswith('#'):
                continue
            entries.append(row)
    
    return entries


def save_ledger(entries: List[Dict[str, str]]):
    """Save ledger entries to CSV."""
    LEDGER_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    with open(LEDGER_PATH, 'w', newline='') as f:
        fieldnames = ['file_path', 'download_timestamp', 'file_hash', 'file_size_bytes', 'notes']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        
        writer.writeheader()
        f.write("# Source acquisition ledger\n")
        f.write("# Track file hashes and timestamps to detect staleness and corruption\n")
        f.write("# Format: file_path,download_timestamp (ISO8601),file_hash (SHA256),file_size_bytes,notes\n")
        
        for entry in entries:
            writer.writerow(entry)


def update_file_entry(file_path: Path, notes: str = "") -> Dict[str, str]:
    """Create or update a ledger entry for a file."""
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    file_hash = compute_file_hash(file_path)
    file_size = file_path.stat().st_size
    timestamp = datetime.now().isoformat()
    
    return {
        'file_path': str(file_path),
        'download_timestamp': timestamp,
        'file_hash': file_hash,
        'file_size_bytes': str(file_size),
        'notes': notes
    }


def check_staleness(entries: List[Dict[str, str]]) -> List[Tuple[str, int]]:
    """Check for files that haven't been updated in >7 days."""
    stale_files = []
    now = datetime.now()
    
    for entry in entries:
        try:
            timestamp = datetime.fromisoformat(entry['download_timestamp'])
            age_days = (now - timestamp).days
            
            if age_days > STALENESS_DAYS:
                stale_files.append((entry['file_path'], age_days))
        except (ValueError, KeyError):
            continue
    
    return stale_files


def check_size_anomalies(entries: List[Dict[str, str]], new_entries: List[Dict[str, str]]) -> List[Tuple[str, float]]:
    """Check for files with >25% size changes."""
    anomalies = []
    
    # Build index of old entries
    old_sizes = {e['file_path']: int(e['file_size_bytes']) for e in entries if e.get('file_size_bytes')}
    
    for new_entry in new_entries:
        file_path = new_entry['file_path']
        new_size = int(new_entry['file_size_bytes'])
        
        if file_path in old_sizes:
            old_size = old_sizes[file_path]
            
            if old_size > 0:
                delta = abs(new_size - old_size) / old_size
                
                if delta > MAX_SIZE_DELTA:
                    anomalies.append((file_path, delta))
    
    return anomalies


def main():
    """Main function to update source ledger."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Update source acquisition ledger")
    parser.add_argument("files", nargs="*", help="Files to add/update in ledger")
    parser.add_argument("--check-only", action="store_true", help="Only check for issues, don't update")
    parser.add_argument("--auto-scan", action="store_true", help="Automatically scan common source directories")
    parser.add_argument("--notes", default="", help="Optional notes for this update")
    parser.add_argument(
        "--anchor-fingerprint",
        nargs=2,
        metavar=("ANCHOR_SITES", "ANCHOR_ID_MAP"),
        help="Compute anchor fingerprint for incremental D_anchor builds and exit",
    )
    args = parser.parse_args()

    if args.anchor_fingerprint:
        anchor_sites, anchor_map = (Path(p) for p in args.anchor_fingerprint)
        try:
            fingerprint = compute_anchor_fingerprint(anchor_sites, anchor_map)
        except Exception as exc:
            print(f"[ledger] Failed to compute anchor fingerprint: {exc}", file=sys.stderr)
            return 1
        print(fingerprint)
        return 0
    
    print("=" * 80)
    print("SOURCE ACQUISITION LEDGER")
    print("=" * 80)
    print()
    
    # Load existing ledger
    print("[1/4] Loading existing ledger...")
    existing_entries = load_ledger()
    print(f"  → Found {len(existing_entries)} existing entries")
    print()
    
    # Determine files to process
    files_to_process = []
    
    if args.auto_scan:
        print("[2/4] Auto-scanning source directories...")
        # Common source file patterns
        scan_patterns = [
            "data/osm/*.pbf",
            "data/overture/*.parquet",
            "data/boundaries/*.zip",
            "data/taxonomy/*.csv",
            "Future/*.csv"
        ]
        
        for pattern in scan_patterns:
            files_to_process.extend(Path(".").glob(pattern))
        
        print(f"  → Found {len(files_to_process)} files to track")
    elif args.files:
        files_to_process = [Path(f) for f in args.files]
        print(f"[2/4] Processing {len(files_to_process)} specified files...")
    else:
        print("[2/4] No files specified")
        files_to_process = []
    
    print()
    
    # Update entries for each file
    print("[3/4] Computing hashes and metadata...")
    new_entries = []
    
    for file_path in files_to_process:
        if not file_path.exists():
            print(f"  ⚠ SKIP {file_path} (not found)")
            continue
        
        try:
            entry = update_file_entry(file_path, notes=args.notes)
            new_entries.append(entry)
            print(f"  ✓ {file_path}")
        except Exception as e:
            print(f"  ✗ ERROR {file_path}: {e}")
    
    print()
    
    # Check for issues
    print("[4/4] Checking for issues...")
    
    stale_files = check_staleness(existing_entries)
    if stale_files:
        print(f"  ⚠ WARNING: {len(stale_files)} files haven't been updated in >{STALENESS_DAYS} days:")
        for file_path, age_days in sorted(stale_files, key=lambda x: x[1], reverse=True)[:5]:
            print(f"    - {file_path} ({age_days} days old)")
    
    size_anomalies = check_size_anomalies(existing_entries, new_entries)
    if size_anomalies:
        print(f"  ⚠ WARNING: {len(size_anomalies)} files have >25% size changes:")
        for file_path, delta in size_anomalies:
            print(f"    - {file_path} ({delta:.1%} change)")
    
    if not stale_files and not size_anomalies:
        print("  ✓ No issues detected")
    
    print()
    
    # Save updated ledger
    if not args.check_only and new_entries:
        # Merge new entries with existing (replace duplicates)
        entry_map = {e['file_path']: e for e in existing_entries}
        for entry in new_entries:
            entry_map[entry['file_path']] = entry
        
        all_entries = sorted(entry_map.values(), key=lambda x: x['file_path'])
        save_ledger(all_entries)
        
        print(f"✓ Ledger updated with {len(new_entries)} new/updated entries")
        print(f"  Total entries: {len(all_entries)}")
    elif args.check_only:
        print("✓ Check-only mode: ledger not modified")
    else:
        print("✓ No updates needed")
    
    # Exit with failure if serious issues found
    if size_anomalies:
        print()
        print("⚠ Exiting with error due to size anomalies")
        return 1
    
    return 0


SITE_COLUMNS: Sequence[str] = (
    "site_id",
    "anchor_int_id",
    "node_id",
    "lon",
    "lat",
    "poi_ids",
    "brands",
    "categories",
)

MAP_COLUMNS: Sequence[str] = ("anchor_int_id", "site_id")


def _require_duckdb_dependencies():
    if duckdb is None or pd is None:
        raise RuntimeError(
            "duckdb and pandas are required for anchor fingerprinting. "
            "Please install them in your virtual environment."
        )


def _canonicalize_sequence(values: Iterable[str | None] | float | None) -> tuple[str, ...]:
    if values is None:
        return ()
    if isinstance(values, float) and math.isnan(values):
        return ()
    canonical: list[str] = []
    for value in values:  # type: ignore[arg-type]
        if value is None:
            continue
        if isinstance(value, float) and math.isnan(value):
            continue
        if pd is not None and pd.isna(value):
            continue
        canonical.append(str(value))
    canonical.sort()
    return tuple(canonical)


def _load_sites_frame(path: Path):
    _require_duckdb_dependencies()
    relation = duckdb.read_parquet(str(path))
    missing = [col for col in SITE_COLUMNS if col not in relation.columns]
    if missing:
        raise ValueError(f"{path} is missing expected site columns: {missing}")
    df = relation.project(", ".join(SITE_COLUMNS)).order("site_id").df()
    for col in ("poi_ids", "brands", "categories"):
        df[col] = df[col].apply(_canonicalize_sequence)
    df["node_id"] = df["node_id"].fillna(-1).astype("int64")
    df["anchor_int_id"] = df["anchor_int_id"].fillna(-1).astype("int64")
    df["site_id"] = df["site_id"].astype(str)
    return df.reset_index(drop=True)


def _load_map_frame(path: Path):
    _require_duckdb_dependencies()
    relation = duckdb.read_parquet(str(path))
    missing = [col for col in MAP_COLUMNS if col not in relation.columns]
    if missing:
        raise ValueError(f"{path} is missing expected map columns: {missing}")
    df = relation.project(", ".join(MAP_COLUMNS)).order("anchor_int_id, site_id").df()
    df["anchor_int_id"] = df["anchor_int_id"].fillna(-1).astype("int64")
    df["site_id"] = df["site_id"].astype(str)
    return df.reset_index(drop=True)


def _update_hash_for_value(hasher: hashlib._Hash, value) -> None:
    if value is None:
        hasher.update(b"\x00")
    elif isinstance(value, tuple):
        hasher.update(b"[")
        for item in value:
            _update_hash_for_value(hasher, item)
            hasher.update(b",")
        hasher.update(b"]")
    elif isinstance(value, numbers.Integral):
        hasher.update(struct.pack(">q", int(value)))
    elif isinstance(value, float):
        if math.isnan(value):
            hasher.update(b"NaN")
        else:
            hasher.update(struct.pack(">d", float(value)))
    else:
        hasher.update(str(value).encode("utf-8"))


def _hash_frame(df, columns: Sequence[str]) -> str:
    hasher = hashlib.sha256()
    subset = df.loc[:, columns]
    for row in subset.itertuples(index=False, name=None):
        for value in row:
            hasher.update(b"\x1f")
            _update_hash_for_value(hasher, value)
        hasher.update(b"\x1e")
    return hasher.hexdigest()


def compute_anchor_fingerprint(site_path: Path, map_path: Path) -> str:
    sites_df = _load_sites_frame(site_path)
    map_df = _load_map_frame(map_path)
    site_hash = _hash_frame(sites_df, SITE_COLUMNS)
    map_hash = _hash_frame(map_df, MAP_COLUMNS)
    final_hash = hashlib.sha256()
    final_hash.update(site_hash.encode("utf-8"))
    final_hash.update(map_hash.encode("utf-8"))
    return final_hash.hexdigest()


if __name__ == "__main__":
    sys.exit(main())
