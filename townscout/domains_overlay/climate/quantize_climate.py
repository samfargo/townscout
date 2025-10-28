"""
Climate data quantization and processing.

Converts PRISM climate normals to per-hex parquet with quantized values.
"""
from __future__ import annotations

import datetime as dt
import glob
import json
import pathlib
from dataclasses import dataclass
from inspect import signature
from functools import lru_cache
from typing import Dict, Mapping, Sequence

import geopandas as gpd
import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import rasterio
from rasterstats import zonal_stats
from shapely.geometry import Polygon

from .schema import (
    MONTHS, CLIMATE_VARIABLES,
    PRISM_DIR_DEFAULT, MINUTES_GLOB_DEFAULT, OUT_PARQUET_DEFAULT,
    TEMP_SCALE, PPT_MM_SCALE, PPT_IN_SCALE, MM_PER_INCH
)

try:
    import h3

    if hasattr(h3, "h3_to_geo_boundary"):
        def boundary_for(h: str):
            return h3.h3_to_geo_boundary(h, geo_json=True)

        def to_h3_str(h):
            if isinstance(h, str):
                return h
            if isinstance(h, int):
                return h3.h3_to_string(h)
            return str(h)
    else:
        def boundary_for(h: str):
            return h3.cell_to_boundary(h)

        def to_h3_str(h):
            if isinstance(h, str):
                return h
            if isinstance(h, int):
                return h3.int_to_str(h)
            return str(h)
except ImportError as exc:  # pragma: no cover
    raise RuntimeError("h3 package is required for climate processing") from exc


@dataclass(frozen=True)
class RasterSpec:
    path: pathlib.Path
    band: int | None = None


def classify_climate_expr() -> pl.Expr:
    """Return a Polars expression that maps derived climate metrics to a label."""
    summer = pl.col("temp_mean_summer_f")
    winter = pl.col("temp_mean_winter_f")
    ppt = pl.col("ppt_ann_in")
    ppt_summer = pl.col("ppt_jul_in")
    ppt_winter = pl.col("ppt_dec_in")

    def cond(expression: pl.Expr) -> pl.Expr:
        return expression.fill_null(False)

    return (
        pl.when(cond((summer < 60) & (winter < 30)))
        .then(pl.lit("Arctic Cold"))
        .when(cond((summer >= 65) & (winter < 32) & (ppt > 20)))
        .then(pl.lit("Cold Seasonal"))
        .when(cond((summer >= 70) & (summer <= 80) & (winter >= 32) & (winter <= 45) & (ppt >= 25) & (ppt <= 50)))
        .then(pl.lit("Mild Continental"))
        .when(cond((summer < 70) & (winter > 35) & (ppt > 35)))
        .then(pl.lit("Cool Maritime"))
        .when(cond((summer > 80) & (winter > 45) & (ppt > 40)))
        .then(pl.lit("Warm Humid"))
        .when(cond((ppt < 10) & (summer > 80)))
        .then(pl.lit("Hot Dry (Desert)"))
        .when(cond((ppt >= 10) & (ppt < 20) & (summer >= 75) & (summer <= 85)))
        .then(pl.lit("Warm Semi-Arid"))
        .when(cond((ppt < 30) & (ppt_winter > ppt_summer) & (summer > 75)))
        .then(pl.lit("Mediterranean Mild"))
        .when(cond((summer >= 60) & (summer <= 75) & (winter < 35)))
        .then(pl.lit("Mountain Mixed"))
        .otherwise(pl.lit("Unclassified"))
    )


def prism_filename(var: str, month: str) -> str:
    """Return the expected filename for a PRISM normal."""
    return f"PRISM_{var}_30yr_normal_4kmM2_1991-2020_{month}.tif"


@lru_cache
def load_hex_ids(minutes_glob: str, res: int) -> list[str]:
    """Load the unique H3 ids for a resolution from the minutes parquet collection."""
    scan = _scan_minutes_lazy(minutes_glob, ["h3_id", "res"])
    series = (
        scan.select(["h3_id", "res"])
        .filter(pl.col("res") == res)
        .select("h3_id")
        .unique()
        .collect()
        .to_series()
    )
    ids = series.to_list()
    if not ids:
        raise ValueError(f"No hex ids found for resolution {res} in {minutes_glob}")
    return ids


def _scan_minutes_lazy(minutes_glob: str, columns: Sequence[str]) -> pl.LazyFrame:
    """Return a lazy scan over the minutes files handling Polars API differences."""
    files = sorted(glob.glob(minutes_glob))
    if not files:
        raise FileNotFoundError(f"No minutes parquet files match pattern: {minutes_glob}")

    # Prefer glob scanning when available to avoid materialising path lists.
    scan_source: str | Sequence[str]
    scan_kwargs: dict[str, object] = {}
    try:
        params = signature(pl.scan_parquet).parameters
    except (ValueError, TypeError):
        # Fallback to the simple path list if signature introspection fails.
        params = {}

    if "glob" in params:
        scan_source = minutes_glob
        scan_kwargs["glob"] = True
    else:
        scan_source = files if len(files) > 1 else files[0]

    if "columns" in params:
        scan_kwargs["columns"] = list(columns)
    elif "with_columns" in params:
        scan_kwargs["with_columns"] = list(columns)

    try:
        return pl.scan_parquet(scan_source, **scan_kwargs)
    except TypeError:
        # Retry without optional kwargs for older Polars versions.
        scan = pl.scan_parquet(scan_source)
        return scan.select(columns)


def build_hex_frame(minutes_glob: str, res: int) -> gpd.GeoDataFrame:
    """Construct a GeoDataFrame of H3 polygons for the given resolution."""
    raw_ids = load_hex_ids(minutes_glob, res)

    def polygon_from_h3(h) -> Polygon:
        boundary = boundary_for(to_h3_str(h))
        ring = []
        first = boundary[0]
        if isinstance(first, dict):
            ring = [(pt["lng"], pt["lat"]) for pt in boundary]
        else:
            for lat, lon in boundary:
                ring.append((lon, lat))
        if ring[0] != ring[-1]:
            ring.append(ring[0])
        return Polygon(ring)

    polygons = [polygon_from_h3(h) for h in raw_ids]
    gdf = gpd.GeoDataFrame({"h3_id": raw_ids, "res": res}, geometry=polygons, crs="EPSG:4326")
    return gdf


def discover_rasters(prism_dir: pathlib.Path, variables: Sequence[str]) -> Mapping[str, Mapping[str, RasterSpec]]:
    """Locate monthly rasters (per-month files or multi-band stack) for the variables."""

    def get_band_count(path: pathlib.Path, cache: Dict[pathlib.Path, int]) -> int:
        if path not in cache:
            with rasterio.open(path) as src:
                cache[path] = src.count
        return cache[path]

    result: dict[str, dict[str, RasterSpec]] = {}
    band_cache: Dict[pathlib.Path, int] = {}

    for var in variables:
        var_dir = prism_dir / var
        if not var_dir.exists():
            raise FileNotFoundError(f"Expected directory missing: {var_dir}")

        tif_files = sorted(var_dir.glob("*.tif"))
        if not tif_files:
            raise FileNotFoundError(f"No GeoTIFFs found in {var_dir}")

        monthly_specs: dict[str, RasterSpec] = {}

        # Try to match per-month files via name patterns.
        for idx, month in enumerate(MONTHS):
            patterns = [
                f"*_{month}_*.tif",
                f"*{month}.tif",
                f"*{month}_*.tif",
                f"*{month.upper()}*.tif",
                f"*{month.capitalize()}*.tif",
                f"*_2020{idx+1:02d}_*.tif",  # Match PRISM format: *_202001_*, *_202002_*, etc.
                f"*{idx+1:02d}_*.tif",        # More general: *01_*, *02_*, etc.
                f"*{idx+1:02d}.tif",          # Ends with month number
            ]
            match_path: pathlib.Path | None = None
            for pattern in patterns:
                matches = list(var_dir.glob(pattern))
                if matches:
                    # Sort matches to ensure deterministic selection
                    match_path = sorted(matches)[0]
                    break
            if match_path:
                band_count = get_band_count(match_path, band_cache)
                band = idx + 1 if band_count > 1 else None
                monthly_specs[month] = RasterSpec(match_path, band)

        if len(monthly_specs) < len(MONTHS):
            # Fall back to using a single multi-band raster.
            stack_path = tif_files[0]
            band_count = get_band_count(stack_path, band_cache)
            if band_count < len(MONTHS):
                raise FileNotFoundError(
                    f"Multi-band raster at {stack_path} has {band_count} band(s); expected >= {len(MONTHS)}."
                )
            monthly_specs = {
                month: RasterSpec(stack_path, idx + 1)
                for idx, month in enumerate(MONTHS)
            }

        result[var] = monthly_specs

    return result


def get_geometry_for_crs(
    base_gdf: gpd.GeoDataFrame,
    crs_cache: dict[str | None, gpd.GeoDataFrame],
    target_crs,
) -> gpd.GeoDataFrame:
    """Return the GeoDataFrame projected into the target CRS, caching results."""
    key = None
    if target_crs is not None:
        try:
            key = target_crs.to_string()
        except AttributeError:
            key = str(target_crs)
    if key in (None, "EPSG:4326", "epsg:4326"):
        key = "EPSG:4326"
    if key not in crs_cache:
        if key == "EPSG:4326":
            crs_cache[key] = base_gdf
        else:
            crs_cache[key] = base_gdf.to_crs(target_crs)
    return crs_cache[key]


def zonal_mean(
    base_gdf: gpd.GeoDataFrame,
    spec: RasterSpec,
    *,
    geometry_cache: dict[str | None, gpd.GeoDataFrame],
) -> np.ndarray:
    """Compute the mean raster value for each polygon."""
    with rasterio.open(spec.path) as src:
        target = get_geometry_for_crs(base_gdf, geometry_cache, src.crs)
        if spec.band is not None:
            if spec.band > src.count:
                raise ValueError(f"Requested band {spec.band} exceeds count {src.count} for {spec.path}")
            nodata_val = src.nodatavals[spec.band - 1] if src.nodatavals else None
        else:
            nodata_val = src.nodata
        nodata = nodata_val if nodata_val is not None else -9999.0

    zs_kwargs = {"band": spec.band} if spec.band is not None else {}
    stats = zonal_stats(
        vectors=target.geometry,
        raster=str(spec.path),
        stats=["mean"],
        all_touched=True,
        nodata=nodata,
        **zs_kwargs,
    )
    return np.array(
        [entry["mean"] if entry["mean"] is not None else np.nan for entry in stats],
        dtype="float64",
    )


def c_to_f(value: float | None) -> float | None:
    """Convert Celsius to Fahrenheit while preserving null."""
    if value is None or isinstance(value, float) and np.isnan(value):
        return value
    return value * 9.0 / 5.0 + 32.0


def quantize_column(expr: pl.Expr, scale: float, dtype: pl.datatypes.DataType) -> pl.Expr:
    """Quantize a floating column into an integer representation."""
    return (
        (expr * (1.0 / scale))
        .round(0)
        .cast(dtype, strict=False)
    )


def mm_to_inches(expr: pl.Expr) -> pl.Expr:
    """Convert millimetres to inches."""
    return expr / MM_PER_INCH


def clip_min_expr(expr: pl.Expr, minimum: float) -> pl.Expr:
    """Clip an expression to a lower bound with backwards-compatible Polars APIs."""
    if hasattr(expr, "clip_min"):
        return expr.clip_min(minimum)
    if hasattr(expr, "clip"):
        try:
            return expr.clip(minimum, None)
        except TypeError:
            try:
                return expr.clip(lower_bound=minimum)
            except TypeError:
                pass
    return pl.when(expr < minimum).then(minimum).otherwise(expr)


def process_climate_data(
    *,
    prism_dir: pathlib.Path = PRISM_DIR_DEFAULT,
    minutes_glob: str = MINUTES_GLOB_DEFAULT,
    output: pathlib.Path = OUT_PARQUET_DEFAULT,
    resolutions: Sequence[int] = (7, 8),
) -> pathlib.Path:
    """
    Main climate processing entrypoint.
    
    Reads PRISM normals, computes zonal statistics for H3 hexes,
    derives climate metrics, quantizes values, and writes to parquet.
    
    Args:
        prism_dir: Directory containing PRISM normals organized by variable/month
        minutes_glob: Glob pattern for minutes parquet files (to get hex universe)
        output: Destination parquet path
        resolutions: H3 resolutions to include
        
    Returns:
        Output parquet path
    """
    prism_dir = prism_dir.expanduser()
    output = output.expanduser()
    output.parent.mkdir(parents=True, exist_ok=True)

    rasters = discover_rasters(prism_dir, CLIMATE_VARIABLES)

    frames: list[pl.DataFrame] = []
    for res in resolutions:
        gdf = build_hex_frame(minutes_glob, res)
        geometry_cache: dict[str | None, gpd.GeoDataFrame] = {}
        data: dict[str, np.ndarray | list] = {
            "h3_id": gdf["h3_id"].to_numpy(),
            "res": np.full(len(gdf), res, dtype=np.int32),
        }

        # Temperature variables (Celsius → Fahrenheit later)
        for var in ("tmean", "tmin", "tmax"):
            for month in MONTHS:
                spec = rasters[var][month]
                values = zonal_mean(gdf, spec, geometry_cache=geometry_cache)
                data[f"{var}_{month}_c"] = values

        # Precipitation (already in mm)
        for month in MONTHS:
            spec = rasters["ppt"][month]
            data[f"ppt_{month}_mm"] = zonal_mean(gdf, spec, geometry_cache=geometry_cache)

        df = pl.DataFrame(data)

        # Convert Celsius → Fahrenheit
        for month in MONTHS:
            for prefix in ("tmean", "tmin", "tmax"):
                col = f"{prefix}_{month}_c"
                if col in df.columns:
                    df = df.with_columns(
                        pl.col(col).map_elements(c_to_f, return_dtype=pl.Float64).alias(f"{prefix}_{month}_f")
                    )

        rename_map = {
            f"tmean_{month}_f": f"temp_mean_{month}_f"
            for month in MONTHS
            if f"tmean_{month}_f" in df.columns
        }
        if rename_map:
            df = df.rename(rename_map)

        # Aggregations
        temp_mean_cols = [f"temp_mean_{m}_f" for m in MONTHS if f"temp_mean_{m}_f" in df.columns]
        if temp_mean_cols:
            df = df.with_columns(
                pl.mean_horizontal(temp_mean_cols).alias("temp_mean_ann_f"),
            )

        summer_cols = [f"temp_mean_{m}_f" for m in ("jun", "jul", "aug") if f"temp_mean_{m}_f" in df.columns]
        if len(summer_cols) == 3:
            df = df.with_columns(pl.mean_horizontal(summer_cols).alias("temp_mean_summer_f"))
        winter_cols = [f"temp_mean_{m}_f" for m in ("dec", "jan", "feb") if f"temp_mean_{m}_f" in df.columns]
        if len(winter_cols) == 3:
            df = df.with_columns(pl.mean_horizontal(winter_cols).alias("temp_mean_winter_f"))

        tmax_cols = [f"tmax_{m}_f" for m in MONTHS if f"tmax_{m}_f" in df.columns]
        if tmax_cols:
            df = df.with_columns(pl.max_horizontal(tmax_cols).alias("temp_max_hot_month_f"))

        tmin_cols = [f"tmin_{m}_f" for m in MONTHS if f"tmin_{m}_f" in df.columns]
        if tmin_cols:
            df = df.with_columns(pl.min_horizontal(tmin_cols).alias("temp_min_cold_month_f"))

        ppt_cols = [f"ppt_{m}_mm" for m in MONTHS if f"ppt_{m}_mm" in df.columns]
        if ppt_cols:
            df = df.with_columns(pl.sum_horizontal(ppt_cols).alias("ppt_ann_mm"))
            # Monthly + annual inches
            for col in ppt_cols:
                month = col.split("_")[1]
                inch_col = f"ppt_{month}_in"
                df = df.with_columns(mm_to_inches(pl.col(col)).alias(inch_col))
            df = df.with_columns(mm_to_inches(pl.col("ppt_ann_mm")).alias("ppt_ann_in"))

        required_for_climate = {
            "temp_mean_summer_f",
            "temp_mean_winter_f",
            "ppt_ann_in",
            "ppt_jul_in",
            "ppt_dec_in",
        }
        if required_for_climate.issubset(set(df.columns)):
            df = df.with_columns(classify_climate_expr().alias("climate_label"))

        # Remove monthly min/max source columns before quantization to keep schema compact
        drop_pre_quantize = [c for c in df.columns if c.startswith("tmin_") or c.startswith("tmax_")]
        if drop_pre_quantize:
            df = df.drop(drop_pre_quantize)

        # Quantize temperatures (tenths of °F)
        temp_float_cols = [c for c in df.columns if c.endswith("_f")]
        if temp_float_cols:
            df = df.with_columns(
                [
                    quantize_column(pl.col(col), TEMP_SCALE, pl.Int16).alias(f"{col}_q")
                    for col in temp_float_cols
                ]
            )

        ppt_mm_cols = [c for c in df.columns if c.endswith("_mm")]
        if ppt_mm_cols:
            df = df.with_columns(
                [
                    quantize_column(clip_min_expr(pl.col(col), 0.0), PPT_MM_SCALE, pl.UInt16).alias(f"{col}_q")
                    for col in ppt_mm_cols
                ]
            )

        ppt_in_cols = [c for c in df.columns if c.endswith("_in")]
        if ppt_in_cols:
            df = df.with_columns(
                [
                    quantize_column(clip_min_expr(pl.col(col), 0.0), PPT_IN_SCALE, pl.UInt16).alias(f"{col}_q")
                    for col in ppt_in_cols
                ]
            )

        drop_cols = [
            col
            for col in df.columns
            if col.endswith("_c") or col.endswith("_f") or col.endswith("_mm") or col.endswith("_in")
        ]
        if drop_cols:
            df = df.drop(drop_cols)

        frames.append(df)

    climate_df = pl.concat(frames, how="vertical_relaxed")

    temp_q_cols = [c for c in climate_df.columns if c.endswith("_f_q")]
    if temp_q_cols:
        climate_df = climate_df.with_columns(
            [pl.col(col).cast(pl.Int16, strict=False) for col in temp_q_cols]
        )
    ppt_q_cols = [c for c in climate_df.columns if c.startswith("ppt_") and c.endswith("_mm_q")]
    if ppt_q_cols:
        climate_df = climate_df.with_columns(
            [pl.col(col).cast(pl.UInt16, strict=False) for col in ppt_q_cols]
        )
    ppt_in_q_cols = [c for c in climate_df.columns if c.startswith("ppt_") and c.endswith("_in_q")]
    if ppt_in_q_cols:
        climate_df = climate_df.with_columns(
            [pl.col(col).cast(pl.UInt16, strict=False) for col in ppt_in_q_cols]
        )

    table = climate_df.to_arrow()
    existing_meta = dict(table.schema.metadata or {})
    generated_ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    climate_meta = {
        "townscout_prism": json.dumps(
            {
                "source": "PRISM Normals 1991-2020",
                "temp_scale": TEMP_SCALE,
                "ppt_mm_scale": PPT_MM_SCALE,
                "ppt_in_scale": PPT_IN_SCALE,
                "generated_utc": generated_ts,
            }
        ).encode("utf-8")
    }
    table = table.replace_schema_metadata({**existing_meta, **climate_meta})
    pq.write_table(table, output)
    return output

