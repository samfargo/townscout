#!/usr/bin/env python3
"""
TownScout POI Categories — Maps brand slugs to standardized category objects.

Each category has:
- id: unique integer for database storage
- slug: string identifier matching data/poi/{state}_{slug}.parquet files
- default_mode: "drive" or "walk"
- default_cutoff: minutes (used if not specified in queries)
"""

from dataclasses import dataclass
from typing import Optional, Dict

@dataclass
class Category:
    id: int
    slug: str
    default_mode: str = "drive"
    default_cutoff: Optional[int] = None

# Category registry
CATEGORIES: Dict[str, Category] = {
    "chipotle": Category(
        id=1,
        slug="chipotle",
        default_mode="drive",
        default_cutoff=30
    ),
    "costco": Category(
        id=2,
        slug="costco", 
        default_mode="drive",
        default_cutoff=60
    ),
    "airports": Category(
        id=3,
        slug="airports",
        default_mode="drive",
        default_cutoff=240
    ),
    # Future categories can be added here with sequential IDs
    # "hospitals": Category(id=4, slug="hospitals", default_mode="drive", default_cutoff=45),
    # "major_airport": Category(id=5, slug="major_airport", default_mode="drive", default_cutoff=180),
}

def get_category(slug: str) -> Category:
    """Get category by slug, raise if not found."""
    if slug not in CATEGORIES:
        raise ValueError(f"Unknown category slug: {slug}. Available: {list(CATEGORIES.keys())}")
    return CATEGORIES[slug]

def list_categories() -> Dict[str, Category]:
    """Return all available categories."""
    return CATEGORIES.copy() 