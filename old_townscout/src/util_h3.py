import h3
from shapely.geometry import Polygon


def cell_polygon(cell: str):
    boundary = h3.cell_to_boundary(cell)
    coords = [(lng, lat) for lat, lng in boundary]
    return Polygon(coords) 