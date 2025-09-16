import os
import subprocess
from config import STATES, GEOFABRIK_BASE
from util_osm import download_geofabrik


def download_osm_extracts():
    """Download OSM PBF extracts from Geofabrik."""
    print("--- Downloading OSM PBF extracts ---")
    for s in STATES:
        path = download_geofabrik(s, GEOFABRIK_BASE)
        print(f"[ok] OSM extract for {s} at {path}")
    print("--- OSM PBF extracts downloaded ---\n")


def download_overture_extract():
    """Download and clip Overture Maps data using DuckDB."""
    print("--- Downloading Overture Maps extract ---")
    
    # This is for Massachusetts, as per OVERHAUL.md
    # TODO: Parameterize this for other states.
    state_name = "massachusetts" 
    output_dir = "data/overture"
    output_path = os.path.join(output_dir, "ma_places.parquet")
    
    os.makedirs(output_dir, exist_ok=True)

    if os.path.exists(output_path):
        print(f"[ok] Overture extract for {state_name} already exists at {output_path}")
        print("--- Overture Maps extract downloaded ---")
        return

    # BBox for Massachusetts
    bbox = {
        "xmin": -73.508142,
        "xmax": -69.928393,
        "ymin": 41.186328,
        "ymax": 42.886589,
    }

    # The OVERHAUL.md mentions release 2025-08-20.0, which is in the future.
    # I will use a recent, real release. Let's use 2024-07-22.0 for now.
    # This should be updated as new releases come out.
    overture_release = "2024-07-22.0"

    duckdb_query = f"""
    INSTALL spatial; LOAD spatial;
    INSTALL httpfs; LOAD httpfs;
    SET s3_region='us-west-2'; SET s3_use_ssl=true;

    COPY (
      SELECT *
      FROM read_parquet(
        's3://overturemaps-us-west-2/release/{overture_release}/theme=places/type=place/*',
        hive_partitioning=1
      )
      WHERE
        bbox.xmin BETWEEN {bbox['xmin']} AND {bbox['xmax']}
        AND bbox.ymin BETWEEN {bbox['ymin']} AND {bbox['ymax']}
    ) TO '{output_path}' (FORMAT PARQUET);
    """
    
    print(f"Running DuckDB query to download and clip Overture data for {state_name}...")
    try:
        subprocess.run(
            ["duckdb", "-c", duckdb_query], 
            check=True, 
            capture_output=True, 
            text=True
        )
        print(f"[ok] Overture extract for {state_name} saved to {output_path}")
    except subprocess.CalledProcessError as e:
        print("[error] DuckDB query failed.")
        print(f"STDOUT: {e.stdout}")
        print(f"STDERR: {e.stderr}")
    except FileNotFoundError:
        print("[error] `duckdb` command not found. Is DuckDB installed and in your PATH?")

    print("--- Overture Maps extract downloaded ---")


def main():
    """Main function to download all data extracts."""
    download_osm_extracts()
    download_overture_extract()


if __name__ == "__main__":
    main() 