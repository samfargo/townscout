# Produces list of L1 Trauma Centers from ACS and writes to parquet
# If refreshing list:
# URL: https://www.facs.org/find-a-hospital/?nearMe=off&orderBy=a-z
# Network tab -> XHR/Fetch -> Payload

#!/usr/bin/env python3
import os, sys, json, uuid, math, time, glob
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

# Optional streaming for giant local JSONs
try:
    import ijson  # type: ignore
except Exception:
    ijson = None

import requests
import pyarrow as pa
import pyarrow.parquet as pq

# ---------------- Constants / schema ----------------
LEVEL_MAP = {
    "Level I Trauma Center": ("trauma_level_1_adult", "adult"),
    "Level I Pediatric Trauma Center": ("trauma_level_1_pediatric", "pediatric"),
}

SCHEMA = pa.schema([
    ("poi_id", pa.string()),
    ("name", pa.string()),
    ("brand_id", pa.null()),
    ("brand_name", pa.null()),
    ("class", pa.string()),
    ("category", pa.string()),
    ("subcat", pa.string()),
    ("trauma_level", pa.string()),
    ("lon", pa.float32()),
    ("lat", pa.float32()),
    ("geom_type", pa.uint8()),
    ("area_m2", pa.float32()),
    ("source", pa.string()),
    ("ext_id", pa.string()),
    ("h3_r9", pa.null()),
    ("node_drive_id", pa.int64()),
    ("node_walk_id", pa.int64()),
    ("dist_drive_m", pa.float32()),
    ("dist_walk_m", pa.float32()),
    ("anchorable", pa.bool_()),
    ("exportable", pa.bool_()),
    ("license", pa.string()),
    ("source_updated_at", pa.string()),
    ("ingested_at", pa.string()),
    ("provenance", pa.list_(pa.string())),
])

BATCH_ROWS = 5000
DEFAULT_API = "https://www.facs.org/umbraco/surface/institutionsearchsurface/search"

# ---------------- Helpers ----------------
def is_url(s: str) -> bool:
    return s.startswith("http://") or s.startswith("https://")

def coalesce(*vals):
    for v in vals:
        if v is not None and str(v).strip():
            return v
    return None

def round5(x: float) -> float:
    return round(float(x), 5)

def make_poi_id(ext_id: str, lon: float, lat: float, trauma_level: str) -> str:
    ns = uuid.uuid5(uuid.NAMESPACE_URL, "townscout:acs")
    seed = f"acs|{ext_id}|{round5(lon)}|{round5(lat)}|{trauma_level}"
    return str(uuid.uuid5(ns, seed))

def loc_point(d: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    lp = d.get("locationPoint") or {}
    return lp.get("longitude"), lp.get("latitude")  # ACS returns {latitude, longitude}

def levels_in_entity(programs: Any) -> List[str]:
    out: List[str] = []
    if isinstance(programs, list):
        for p in programs:
            lv = p.get("levels") if isinstance(p, dict) else None
            if isinstance(lv, list):
                for s in lv:
                    if isinstance(s, str) and s.strip() in LEVEL_MAP:
                        out.append(s.strip())
    return sorted(set(out))

def row_for_level(ent: Dict[str, Any], parent: Optional[Dict[str, Any]], level_str: str) -> Optional[Dict[str, Any]]:
    subcat, trauma_level = LEVEL_MAP[level_str]
    lon, lat = loc_point(ent)
    if lon is None or lat is None:
        if parent:
            lon, lat = loc_point(parent)
    if lon is None or lat is None:
        return None
    name = str(coalesce(ent.get("name"), parent.get("name") if parent else None) or "").strip()
    ext_id = str(coalesce(ent.get("guid"), ent.get("id"), name))
    return {
        "poi_id": make_poi_id(ext_id, float(lon), float(lat), trauma_level),
        "name": name,
        "brand_id": None,
        "brand_name": None,
        "class": "health",
        "category": "hospital",
        "subcat": subcat,
        "trauma_level": trauma_level,
        "lon": float(lon),
        "lat": float(lat),
        "geom_type": 0,
        "area_m2": 0.0,
        "source": "acs",
        "ext_id": ext_id,
        "h3_r9": None,
        "node_drive_id": None,
        "node_walk_id": None,
        "dist_drive_m": None,
        "dist_walk_m": None,
        "anchorable": True,
        "exportable": True,
        "license": "unknown",
        "source_updated_at": str(ent.get("lastUpdated") or ""),
        "ingested_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "provenance": ["acs_trauma_level1"],
    }

def iter_entities_with_parent(top: Dict[str, Any]):
    yield (top, None)
    for c in top.get("childInstitutions") or []:
        if isinstance(c, dict):
            yield (c, top)

# ---------------- Local file readers ----------------
def iter_results_from_file(path: str) -> Iterable[Dict[str, Any]]:
    # Try streaming "results.item"
    if ijson is not None:
        try:
            with open(path, "rb") as f:
                for item in ijson.items(f, "results.item"):
                    if isinstance(item, dict):
                        yield item
                return
        except Exception:
            pass
    # Full load
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        results = data.get("results")
        if isinstance(results, list):
            for item in results:
                if isinstance(item, dict):
                    yield item
            return
        if isinstance(data, dict) and data:
            yield data
            return
    except Exception:
        pass
    # NDJSON
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    if isinstance(obj.get("results"), list):
                        for it in obj["results"]:
                            if isinstance(it, dict):
                                yield it
                    else:
                        yield obj
            except Exception:
                continue

def iter_results_from_dir(path: str) -> Iterable[Dict[str, Any]]:
    files = [p for p in glob.glob(os.path.join(path, "*.json")) if os.path.isfile(p)]
    files.sort(key=os.path.getmtime)
    for fp in files:
        yield from iter_results_from_file(fp)

# ---------------- ACS API pagination (POST) ----------------
def iter_results_from_acs_api(
    url: str = DEFAULT_API,
    page_size: int = 1000,
    order_by: str = "a-z",
    delay_s: float = 0.2,
) -> Iterable[Dict[str, Any]]:
    """
    Paginates ACS POST endpoint:
      body = {"SearchTerm":"","CompanyType":null,"StateCityZip":"","Distance":"",
              "Page":<n>,"PageSize":<page_size>,"OrderBy":order_by}
    Stops when we've fetched totalResults or a page returns < page_size.
    """
    sess = requests.Session()
    total = None
    page = 1
    fetched = 0

    while True:
        payload = {
            "SearchTerm": "",
            "CompanyType": None,
            "StateCityZip": "",
            "Distance": "",
            "Page": page,
            "PageSize": page_size,
            "OrderBy": order_by,
        }
        resp = sess.post(
            url,
            json=payload,
            headers={
                "Accept": "application/json, */*",
                "Content-Type": "application/json",
                "Origin": "https://www.facs.org",
                "Referer": "https://www.facs.org/find-a-hospital/?nearMe=off&orderBy=a-z",
                "User-Agent": "TownScout/acs-ingest (requests)",
            },
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()

        if total is None:
            total = data.get("totalResults") if isinstance(data, dict) else None

        results = data.get("results") if isinstance(data, dict) else None
        if not isinstance(results, list) or not results:
            break

        for item in results:
            if isinstance(item, dict):
                yield item
                fetched += 1

        # Stop conditions: fewer than page_size, or we've hit total
        if len(results) < page_size:
            break
        if isinstance(total, int) and fetched >= total:
            break

        page += 1
        if delay_s:
            time.sleep(delay_s)

# ---------------- Unified source iterator ----------------
def iter_all_results(source: Optional[str]) -> Iterable[Dict[str, Any]]:
    """
    If source is None or equals DEFAULT_API -> pull from ACS API (POST pagination).
    If source is a URL (custom) -> also ACS POST, same body.
    If source is a dir/file -> read local JSON/NDJSON.
    """
    if source is None:
        yield from iter_results_from_acs_api(DEFAULT_API)
    elif is_url(source):
        yield from iter_results_from_acs_api(source)
    elif os.path.isdir(source):
        yield from iter_results_from_dir(source)
    else:
        yield from iter_results_from_file(source)

# ---------------- Main ----------------
def main():
    """
    Usage:
      # Pull directly from ACS API (recommended)
      python3 src/ingest/acs_trauma_to_parquet.py

      # Or specify a local dir/file or a custom URL:
      python3 src/ingest/acs_trauma_to_parquet.py /Users/sam/Downloads/ACS_download
      python3 src/ingest/acs_trauma_to_parquet.py /path/to/page_1.json
      python3 src/ingest/acs_trauma_to_parquet.py "https://www.facs.org/umbraco/surface/institutionsearchsurface/search"
    """
    source = sys.argv[1] if len(sys.argv) > 1 else None
    out_dir = "/Users/sam/townscout/out/level1_trauma"
    os.makedirs(out_dir, exist_ok=True)
    out_parquet = os.path.join(out_dir, "acs_trauma.parquet")

    writer: Optional[pq.ParquetWriter] = None
    batch: List[Dict[str, Any]] = []
    seen = set()  # dedupe by (name, lon5, lat5, trauma_level)

    scanned = 0
    for top in iter_all_results(source):
        scanned += 1
        for ent, parent in iter_entities_with_parent(top):
            if (ent.get("country") or "").strip() != "United States":
                continue
            entity_levels = levels_in_entity(ent.get("programs", []))
            if not entity_levels:
                continue
            for lv in entity_levels:
                row = row_for_level(ent, parent, lv)
                if not row:
                    continue
                key = (row["name"], round5(row["lon"]), round5(row["lat"]), row["trauma_level"])
                if key in seen:
                    continue
                seen.add(key)
                batch.append(row)
                if len(batch) >= BATCH_ROWS:
                    tbl = pa.Table.from_pylist(batch, schema=SCHEMA)
                    if writer is None:
                        writer = pq.ParquetWriter(out_parquet, SCHEMA, compression="zstd")
                    writer.write_table(tbl)
                    batch.clear()

    # Flush
    if batch:
        tbl = pa.Table.from_pylist(batch, schema=SCHEMA)
        if writer is None:
            writer = pq.ParquetWriter(out_parquet, SCHEMA, compression="zstd")
        writer.write_table(tbl)
    if writer:
        writer.close()

    # QA CSV
    qa_csv = out_parquet.replace(".parquet", ".qa.csv")
    if os.path.exists(out_parquet):
        pq.read_table(out_parquet).select(
            ["name", "trauma_level", "lon", "lat", "ext_id", "source_updated_at"]
        ).to_pandas().to_csv(qa_csv, index=False)

    print(f"Scanned {scanned} top-level records/pages")
    print(f"Wrote {len(seen)} POIs -> {out_parquet}")
    print(f"QA -> {qa_csv}")

if __name__ == "__main__":
    main()
