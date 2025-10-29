from __future__ import annotations

import argparse
import logging
import os
import pathlib
import zipfile
from dataclasses import dataclass
from io import BytesIO
from typing import Iterable, Sequence

import requests
import time

DEFAULT_BASE_URL = "https://data.prism.oregonstate.edu/normals"
DEFAULT_REGION = "us"
DEFAULT_RESOLUTION = "4km"
DEFAULT_PERIOD = "2020_avg_30y"  # PRISM normals (1991-2020)
TIMEOUT_SECONDS = 300

LOGGER = logging.getLogger("townscout.prism_fetch")
MONTH_IDS = ("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12")


@dataclass(frozen=True)
class DownloadSpec:
    variable: str
    zip_url: str
    zip_filename: str
    tif_filename: str
    dest_path: pathlib.Path
    month_id: str


def filename_resolution_segment(resolution: str) -> str:
    """
    Map the resolution directory (e.g. '4km') to the filename segment used by PRISM.
    PRISM packages the 4km normals as '25m' in the file names.
    """
    resolution = resolution.lower()
    if resolution == "4km":
        return "25m"
    return resolution


def build_specs(
    *,
    variables: Sequence[str],
    base_url: str,
    region: str,
    resolution: str,
    period: str,
    output_dir: pathlib.Path,
) -> Iterable[DownloadSpec]:
    filename_res = filename_resolution_segment(resolution)
    for var in variables:
        var = var.lower()
        year, remainder = period.split("_", 1)
        for month_id in MONTH_IDS:
            period_with_month = f"{year}{month_id}_{remainder}"
            zip_name = f"prism_{var}_{region}_{filename_res}_{period_with_month}.zip"
            tif_name = zip_name.replace(".zip", ".tif")
            url = "/".join(
                [
                    base_url.rstrip("/"),
                    region,
                    resolution,
                    var,
                    "monthly",
                    zip_name,
                ]
            )
            dest = output_dir / var / tif_name
            yield DownloadSpec(
                variable=var,
                zip_url=url,
                zip_filename=zip_name,
                tif_filename=tif_name,
                dest_path=dest,
                month_id=month_id,
            )


def download_file(url: str, *, attempts: int = 3, backoff: float = 2.0) -> bytes:
    for idx in range(1, attempts + 1):
        try:
            LOGGER.info("Downloading %s (attempt %s/%s)", url, idx, attempts)
            response = requests.get(url, timeout=TIMEOUT_SECONDS)
            response.raise_for_status()
            return response.content
        except requests.RequestException as exc:
            if idx == attempts:
                raise RuntimeError(f"Failed to download {url}") from exc
            LOGGER.warning("Download failed (%s); retrying in %.1fs", exc, backoff)
            time.sleep(backoff)
    raise RuntimeError(f"Failed to download {url}")  # unreachable


def extract_tif_from_zip(payload: bytes, expected_name: str, destination: pathlib.Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(BytesIO(payload)) as archive:
        members = [name for name in archive.namelist() if name.lower().endswith(".tif")]
        if not members:
            raise RuntimeError(f"No GeoTIFF found inside archive expected to contain {expected_name}")
        if expected_name in members:
            member = expected_name
        else:
            member = members[0]
            LOGGER.warning("Expected %s inside archive, found %s; extracting first match", expected_name, member)
        LOGGER.info("Writing %s", destination)
        with archive.open(member) as src, destination.open("wb") as dst:
            dst.write(src.read())


def download_normals(
    out_dir: str | os.PathLike[str],
    *,
    variables: Sequence[str],
    base_url: str,
    region: str,
    resolution: str,
    period: str,
    force: bool = False,
) -> None:
    output_dir = pathlib.Path(out_dir)
    for spec in build_specs(
        variables=variables,
        base_url=base_url,
        region=region,
        resolution=resolution,
        period=period,
        output_dir=output_dir,
    ):
        if spec.dest_path.exists() and not force:
            LOGGER.info("Skipping %s (already exists)", spec.dest_path)
            continue
        payload = download_file(spec.zip_url)
        extract_tif_from_zip(payload, spec.tif_filename, spec.dest_path)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download PRISM climate normals (multi-band monthly TIFFs).")
    parser.add_argument(
        "--output",
        default="data/climate/prism/normals_1991_2020",
        help="Destination directory root for the downloaded rasters.",
    )
    parser.add_argument(
        "--variables",
        nargs="+",
        default=["tmean", "tmin", "tmax", "ppt"],
        help="Variables to download (default: tmean tmin tmax ppt).",
    )
    parser.add_argument(
        "--base-url",
        default=os.environ.get("PRISM_BASE_URL", DEFAULT_BASE_URL),
        help="PRISM base URL (defaults to official normals endpoint).",
    )
    parser.add_argument(
        "--region",
        default=os.environ.get("PRISM_REGION", DEFAULT_REGION),
        help="PRISM region segment (e.g. 'us', 'ak').",
    )
    parser.add_argument(
        "--resolution",
        default=os.environ.get("PRISM_RESOLUTION", DEFAULT_RESOLUTION),
        help="PRISM resolution directory (e.g. '4km').",
    )
    parser.add_argument(
        "--period",
        default=DEFAULT_PERIOD,
        help="Period suffix embedded in filenames (default: 2020_avg_30y).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download files even if they already exist.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ...).",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, str(args.log_level).upper(), logging.INFO))
    download_normals(
        args.output,
        variables=args.variables,
        base_url=args.base_url,
        region=args.region,
        resolution=args.resolution,
        period=args.period,
        force=args.force,
    )


if __name__ == "__main__":
    main()
