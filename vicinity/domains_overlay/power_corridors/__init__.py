"""
Power corridor proximity overlay.

Computes per-hex flags indicating proximity to high-voltage transmission corridors.
"""
from .build_corridor_overlay import compute_power_corridor_flags

__all__ = ["compute_power_corridor_flags"]

