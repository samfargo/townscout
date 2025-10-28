"""
Climate data overlay.

Processes PRISM climate normals into per-hex quantized climate attributes.
"""
from .quantize_climate import process_climate_data, classify_climate_expr
from .climate_validation import validate_climate_ranges

__all__ = ["process_climate_data", "classify_climate_expr", "validate_climate_ranges"]

