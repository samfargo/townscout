"""
Trauma center schema and constants.
"""
import pyarrow as pa

# Trauma center classification
TRAUMA_CLASS = "health"
TRAUMA_CATEGORY = "hospital"

# Trauma level mappings (ACS labels -> TownScout schema)
LEVEL_MAP = {
    "Level I Trauma Center": ("trauma_level_1_adult", "adult"),
    "Level I Pediatric Trauma Center": ("trauma_level_1_pediatric", "pediatric"),
}

# Full schema for ACS trauma parquet (compatible with anchor system)
ACS_TRAUMA_SCHEMA = pa.schema([
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

# ACS API endpoint
ACS_API_URL = "https://www.facs.org/umbraco/surface/institutionsearchsurface/search"

