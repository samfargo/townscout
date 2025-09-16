import h3
from shapely.geometry import Polygon
import geopandas as gpd
import pandas as pd

import util_osm
import config

H3_CRS = "EPSG:4326"

def cell_polygon(cell: str):
    boundary = h3.cell_to_boundary(cell)
    coords = [(lng, lat) for lat, lng in boundary]
    return Polygon(coords) 