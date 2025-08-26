import json
import pandas as pd
from shapely.geometry import mapping
from .util_h3 import cell_polygon


def parquet_to_geojson(parquet_path: str, out_geojson: str, property_cols: list):
    df = pd.read_parquet(parquet_path)
    features = []
    for _, r in df.iterrows():
        poly = cell_polygon(r["h3"])
        props = {c: (None if pd.isna(r[c]) else int(r[c])) for c in property_cols}
        features.append({
            "type": "Feature",
            "geometry": mapping(poly),
            "properties": {"h3": r["h3"], **props},
        })
    with open(out_geojson, "w") as f:
        json.dump({"type": "FeatureCollection", "features": features}, f)
    return out_geojson 