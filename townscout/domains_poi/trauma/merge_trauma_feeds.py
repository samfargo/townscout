"""
Trauma Center Ingestion from ACS

Produces list of Level 1 Trauma Centers from the American College of Surgeons (ACS)
and normalizes them to the canonical POI schema.

Data source: https://www.facs.org/find-a-hospital/
"""
import os
import sys
import json
import uuid
import time
import glob
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
import pandas as pd
import geopandas as gpd
import pyarrow as pa
import pyarrow.parquet as pq

# Add src to path to import config
src_path = Path(__file__).parent.parent.parent.parent / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from config import STATE_BOUNDING_BOXES
from townscout.poi.schema import create_empty_poi_dataframe, CANONICAL_POI_SCHEMA
from .schema import (
    TRAUMA_CLASS, TRAUMA_CATEGORY, LEVEL_MAP,
    ACS_TRAUMA_SCHEMA, ACS_API_URL
)

# Optional streaming for giant local JSONs
try:
    import ijson  # type: ignore
except Exception:
    ijson = None


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
    return lp.get("longitude"), lp.get("latitude")


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
        "class": TRAUMA_CLASS,
        "category": TRAUMA_CATEGORY,
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
    url: str = ACS_API_URL,
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
    If source is None or equals ACS_API_URL -> pull from ACS API (POST pagination).
    If source is a URL (custom) -> also ACS POST, same body.
    If source is a dir/file -> read local JSON/NDJSON.
    """
    if source is None:
        yield from iter_results_from_acs_api(ACS_API_URL)
    elif is_url(source):
        yield from iter_results_from_acs_api(source)
    elif os.path.isdir(source):
        yield from iter_results_from_dir(source)
    else:
        yield from iter_results_from_file(source)


# ---------------- Main ingestion function ----------------
def fetch_acs_trauma_centers(
    source: Optional[str] = None,
    output_path: str = None
) -> str:
    """
    Fetch trauma centers from ACS and write to parquet.
    
    Args:
        source: Optional path to local JSON/dir or custom URL. If None, fetches from ACS API.
        output_path: Path to output parquet file. If None, uses out/level1_trauma/acs_trauma.parquet
        
    Returns:
        Path to output parquet file
    """
    if output_path is None:
        output_path = os.path.join("out", "level1_trauma", "acs_trauma.parquet")
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    writer: Optional[pq.ParquetWriter] = None
    batch: List[Dict[str, Any]] = []
    seen = set()  # dedupe by (name, lon5, lat5, trauma_level)
    batch_rows = 5000

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
                if len(batch) >= batch_rows:
                    tbl = pa.Table.from_pylist(batch, schema=ACS_TRAUMA_SCHEMA)
                    if writer is None:
                        writer = pq.ParquetWriter(output_path, ACS_TRAUMA_SCHEMA, compression="zstd")
                    writer.write_table(tbl)
                    batch.clear()

    # Flush
    if batch:
        tbl = pa.Table.from_pylist(batch, schema=ACS_TRAUMA_SCHEMA)
        if writer is None:
            writer = pq.ParquetWriter(output_path, ACS_TRAUMA_SCHEMA, compression="zstd")
        writer.write_table(tbl)
    if writer:
        writer.close()

    # QA CSV
    qa_csv = output_path.replace(".parquet", ".qa.csv")
    if os.path.exists(output_path):
        pq.read_table(output_path).select(
            ["name", "trauma_level", "lon", "lat", "ext_id", "source_updated_at"]
        ).to_pandas().to_csv(qa_csv, index=False)

    print(f"Scanned {scanned} top-level records/pages")
    print(f"Wrote {len(seen)} POIs -> {output_path}")
    print(f"QA -> {qa_csv}")
    
    return output_path


def load_level1_trauma_pois(state: str, trauma_parquet: str = None) -> gpd.GeoDataFrame:
    """
    Load ACS Level 1 trauma centers and filter them to the requested state.
    
    The ACS export is nationwide; we clip to a coarse bounding box per state.
    
    Args:
        state: State name (e.g., 'massachusetts')
        trauma_parquet: Optional path to trauma parquet. If None, uses out/level1_trauma/acs_trauma.parquet
        
    Returns:
        GeoDataFrame with trauma centers in canonical POI schema
    """
    if trauma_parquet is None:
        trauma_parquet = os.path.join("out", "level1_trauma", "acs_trauma.parquet")
    
    if not os.path.exists(trauma_parquet):
        print(f"[warn] ACS trauma parquet not found at {trauma_parquet}; skipping.")
        return create_empty_poi_dataframe()

    try:
        df = pd.read_parquet(trauma_parquet)
    except Exception as exc:
        print(f"[warn] Failed to read ACS trauma parquet: {exc}")
        return create_empty_poi_dataframe()

    if df.empty:
        print("[info] ACS trauma parquet is empty.")
        return create_empty_poi_dataframe()

    bbox = STATE_BOUNDING_BOXES.get(state, {})
    west = bbox.get("west", -180.0)
    east = bbox.get("east", 180.0)
    south = bbox.get("south", -90.0)
    north = bbox.get("north", 90.0)
    before = len(df)
    df = df[(df["lon"] >= west) & (df["lon"] <= east) & (df["lat"] >= south) & (df["lat"] <= north)].copy()
    after = len(df)
    print(f"[info] ACS trauma centers clipped to {state}: {after} / {before}")

    if df.empty:
        return create_empty_poi_dataframe()

    try:
        geometry = gpd.points_from_xy(df["lon"], df["lat"])
    except Exception as exc:
        print(f"[warn] Failed to create geometry for ACS trauma centers: {exc}")
        return create_empty_poi_dataframe()

    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")

    # Ensure all canonical columns exist
    for col in CANONICAL_POI_SCHEMA.keys():
        if col not in gdf.columns:
            gdf[col] = None

    # Normalize provenance to list-of-str
    if "provenance" in gdf.columns:
        gdf["provenance"] = gdf["provenance"].apply(
            lambda v: list(v) if isinstance(v, (list, tuple)) else ([v] if pd.notna(v) else [])
        )

    gdf = gdf[list(CANONICAL_POI_SCHEMA.keys())]
    trauma_counts = gdf["subcat"].value_counts().to_dict() if "subcat" in gdf.columns else {}
    if trauma_counts:
        summary = ", ".join(f"{k}={v}" for k, v in sorted(trauma_counts.items()))
        print(f"[ok] Loaded {len(gdf)} ACS Level 1 trauma centers for {state}: {summary}")
    else:
        print(f"[ok] Loaded {len(gdf)} ACS Level 1 trauma centers for {state}")
    return gdf

