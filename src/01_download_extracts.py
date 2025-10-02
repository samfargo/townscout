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

    # Allow override via env var, fallback to latest known release
    overture_release = os.getenv("OVERTURE_RELEASE", "2025-09-24.0")
    use_azure = os.getenv("OVERTURE_USE_AZURE", "0") == "1"

    if use_azure:
        # Azure mirror
        duckdb_query = f"""
        INSTALL spatial; LOAD spatial;
        INSTALL azure; LOAD azure;
        COPY (
          SELECT *
          FROM read_parquet(
            'az://overturemapswestus2.blob.core.windows.net/release/{overture_release}/theme=places/type=place/*',
            hive_partitioning=1
          )
          WHERE
            bbox.xmin BETWEEN {bbox['xmin']} AND {bbox['xmax']}
            AND bbox.ymin BETWEEN {bbox['ymin']} AND {bbox['ymax']}
        ) TO '{output_path}' (FORMAT PARQUET);
        """
    else:
        # AWS S3 public bucket (force anonymous access)
        duckdb_query = f"""
        INSTALL spatial; LOAD spatial;
        INSTALL httpfs; LOAD httpfs;
        SET s3_region='us-west-2';
        SET s3_use_ssl=true;
        SET s3_access_key_id='';
        SET s3_secret_access_key='';
        SET s3_session_token='';
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
            text=True,
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