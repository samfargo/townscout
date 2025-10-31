"""
Beach POI handling.

Beaches are classified into ocean/lake/river/other based on proximity to
water features from Overture Maps.
"""
from .classify_beaches import build_beach_pois_for_state, classify_beaches_with_overture

__all__ = ["build_beach_pois_for_state", "classify_beaches_with_overture"]

