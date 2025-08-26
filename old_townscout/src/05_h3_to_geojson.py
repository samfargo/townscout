from src.util_tiles import parquet_to_geojson

PROPS = ["chipotle_drive_min", "costco_drive_min", "airports_drive_min", "crime_rate"]

if __name__ == "__main__":
    parquet_to_geojson("state_tiles/us_r7.parquet", "tiles/us_r7.geojson", PROPS)
    parquet_to_geojson("state_tiles/us_r8.parquet", "tiles/us_r8.geojson", PROPS) 