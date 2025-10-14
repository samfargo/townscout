# src/osm_beaches.py
import os
import uuid
import pandas as pd
import geopandas as gpd
from shapely.ops import unary_union
from shapely.geometry import Point
from pyrosm import OSM

from taxonomy import BRAND_REGISTRY

NATURAL_WATER_TAGS = {
    "natural": ["beach", "water", "coastline"],
    "waterway": ["riverbank"],  # optional
}

def load_osm_beach_layers(state: str) -> dict[str, gpd.GeoDataFrame]:
    pbf_path = f"data/osm/{state}.osm.pbf"
    empty = gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
    if not os.path.exists(pbf_path):
        print(f"[error] OSM PBF not found at {pbf_path}.")
        return {"beach": empty, "coastline": empty, "water": empty, "riverbank": empty}

    osm = OSM(pbf_path)
    gdf = osm.get_data_by_custom_criteria(
        custom_filter=NATURAL_WATER_TAGS,
        tags_as_columns=["name", "natural", "water", "waterway"],
        keep_nodes=True, keep_ways=True, keep_relations=True,  # relations ON
    )
    if gdf is None or gdf.empty:
        return {"beach": empty, "coastline": empty, "water": empty, "riverbank": empty}

    gdf = gdf.to_crs("EPSG:4326")
    return {
        "beach":     gdf[gdf["natural"] == "beach"].copy(),
        "coastline": gdf[gdf["natural"] == "coastline"].copy(),
        "water":     gdf[gdf["natural"] == "water"].copy(),
        "riverbank": gdf[gdf["waterway"] == "riverbank"].copy(),
    }

def classify_beaches_gpd(beach_gdf: gpd.GeoDataFrame,
                         coastline_gdf: gpd.GeoDataFrame,
                         water_gdf: gpd.GeoDataFrame,
                         riverbank_gdf: gpd.GeoDataFrame | None = None) -> gpd.GeoDataFrame:
    if beach_gdf.empty:
        return beach_gdf.assign(beach_type=[])

    # project to metric for distance ops
    b = beach_gdf.to_crs(3857).copy()
    c = coastline_gdf.to_crs(3857).copy() if not coastline_gdf.empty else coastline_gdf
    w = water_gdf.to_crs(3857).copy() if not water_gdf.empty else water_gdf
    r = riverbank_gdf.to_crs(3857).copy() if (riverbank_gdf is not None and not riverbank_gdf.empty) else riverbank_gdf

    D_COAST, D_LAKE, D_RIVER = 150, 100, 80

    coast_buf = unary_union(c.buffer(D_COAST)) if (c is not None and not c.empty) else None

    w_lake = w[w.get("water", "").str.lower().isin(["lake", "reservoir", "lagoon"])].copy() if (w is not None and not w.empty) else w
    lake_buf = unary_union(w_lake.buffer(D_LAKE)) if (w_lake is not None and not w_lake.empty) else None

    river_buf = None
    if r is not None and not r.empty:
        river_buf = unary_union(r.buffer(D_RIVER))
        # also accept natural=water & water=river from water_gdf
        if w is not None and not w.empty:
            w_river = w[w.get("water", "").str.lower().eq("river")]
            if not w_river.empty:
                river_buf = unary_union([river_buf, unary_union(w_river.buffer(D_RIVER))])

    b["pt"] = b.geometry.representative_point()

    types = []
    for geom, pt in zip(b.geometry, b["pt"]):
        is_ocean = bool(coast_buf and (geom.intersects(coast_buf) or pt.within(coast_buf)))
        is_lake  = bool(lake_buf  and (geom.intersects(lake_buf)  or pt.within(lake_buf)))
        is_river = bool(river_buf and (geom.intersects(river_buf) or pt.within(river_buf)))

        if is_ocean:
            types.append("ocean")
        elif is_lake:
            types.append("lake")
        elif is_river:
            types.append("river")
        else:
            types.append("other")

    out = beach_gdf.copy()
    out["beach_type"] = types
    # Use representative point for canonical point geometry
    out["geometry"] = out.geometry.representative_point()
    return out.set_crs("EPSG:4326", allow_override=True)

def build_beach_pois_for_state(state: str) -> gpd.GeoDataFrame:
    layers = load_osm_beach_layers(state)
    beaches = layers["beach"]; coast = layers["coastline"]; water = layers["water"]; riverbank = layers["riverbank"]

    if beaches.empty:
        return gpd.GeoDataFrame(columns=["poi_id","name","brand_id","brand_name","class","category","subcat","lon","lat","geometry","source","ext_id","provenance"],
                                geometry="geometry", crs="EPSG:4326")

    classified = classify_beaches_gpd(beaches, coast, water, riverbank)

    rows = []
    for _, r in classified.iterrows():
        name = r.get("name")
        source_id = r.get("id") if "id" in r else None
        poi_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"osm|beach|{source_id}"))
        pt = r.geometry if isinstance(r.geometry, Point) else r.geometry.representative_point()
        rows.append({
            "poi_id": poi_id,
            "name": name,
            "brand_id": None,
            "brand_name": None,
            "class": "recreation",
            "category": "beach",
            "subcat": r["beach_type"],  # ocean|lake|river|other
            "lon": pt.x, "lat": pt.y,
            "geometry": pt,
            "source": "osm",
            "ext_id": str(source_id) if source_id is not None else None,
            "provenance": ["osm"],
        })
    return gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326")