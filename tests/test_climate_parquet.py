from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


def test_climate_parquet_quantized_dtypes() -> None:
    parquet_path = Path("out/climate/hex_climate.parquet")
    if not parquet_path.exists():
        pytest.skip("climate parquet not built")

    table = pq.read_table(parquet_path)
    schema = table.schema

    for field in schema:
        name = field.name
        if name.endswith("_f_q"):
            assert field.type == pa.int16(), f"{name} should use int16"
        elif name.endswith("_mm_q") or name.endswith("_in_q"):
            assert field.type == pa.uint16(), f"{name} should use uint16"
