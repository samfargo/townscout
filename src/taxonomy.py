"""
Defines the Townscout canonical taxonomy for POIs and a brand registry.

This module provides the data structures and mappings required to normalize
POIs from various sources (Overture, OSM) into a consistent, canonical schema.

Config-driven: if present, the following files override/extend built-ins:
- data/brands/registry.csv (brand_id,canonical,aliases,wikidata?)
- data/taxonomy/categories.yml (keys: overture_map, osm_map with mapping entries)
"""
from __future__ import annotations
import csv
import os
from typing import Dict, Tuple, List
try:
    import yaml  # optional; for categories.yml
except Exception:
    yaml = None

# --- Townscout POI Taxonomy ---
# A hierarchical classification system: class -> category -> subcat
# This is a starting point and should be expanded based on data analysis.
TAXONOMY = {
    "food_and_drink": {
        "supermarket": ["supermarket", "grocery", "hypermarket"],
        "convenience_store": ["convenience"],
        "restaurant": ["restaurant"],
        "fast_food": ["fast_food"],
        "cafe": ["cafe", "coffee_shop"],
        "bakery": ["bakery"],
        "pizza": ["pizza"],
        "ice_cream": ["ice_cream"],
        "bar": ["bar", "pub", "biergarten"],
    },
    "retail": {
        "department_store": ["department_store"],
        "shopping_mall": ["mall", "shopping_centre", "shopping_center"],
        "hardware_store": ["hardware", "doityourself", "home_improvement"],
        "electronics_store": ["electronics"],
        "furniture_store": ["furniture"],
        "warehouse_club": ["warehouse_club"],
    },
    "health": {
        "hospital": ["hospital"],
        "pharmacy": ["pharmacy"],
        "clinic": ["clinic", "urgent_care", "doctors"],
        "dentist": ["dentist"],
    },
    "education": {
        "school": ["school", "kindergarten"],
        "university": ["university", "college"],
        "preschool": ["preschool"],
    },
    "civic": {
        "library": ["library"],
        "post_office": ["post_office"],
        "town_hall": ["townhall"],
        "courthouse": ["courthouse"],
        "police": ["police"],
        "fire_station": ["fire_station"],
    },
    "recreation": {
        "park": ["park"],
        "playground": ["playground"],
        "sports_centre": ["sports_centre", "sport_centre"],
        "gym": ["gym", "fitness_centre", "fitness_center"],
        "swimming_pool": ["swimming_pool"],
        "golf_course": ["golf_course"],
        "cinema": ["cinema", "theatre", "theater"],
        "community_center": ["community_centre", "community_center"],
    },
    "transport": {
        "railway_station": ["railway_station", "train_station"],
        "bus_station": ["bus_station"],
        "bus_stop": ["bus_stop"],
        "subway_station": ["subway_station", "metro_station"],
        "light_rail_station": ["light_rail_station"],
        "airport": ["airport", "aerodrome"],
        "ferry_terminal": ["ferry_terminal"],
        "park_and_ride": ["park_and_ride"],
        "fuel": ["fuel", "charging_station"],
    },
    "natural": {
        "beach": ["beach"],
        "trailhead": ["trailhead"],
        "nature_reserve": ["nature_reserve"],
    },
}


# --- Brand Registry ---
# Provides a mapping from various name aliases to a canonical brand ID and name.
# This helps in deduplicating brands that appear with slightly different names.
BRAND_REGISTRY = {
    # Canonical Brand ID: (Canonical Name, [Aliases])
    "chipotle": ("Chipotle Mexican Grill", ["chipotle"]),
    "costco": ("Costco", ["costco wholesale"]),
    "starbucks": ("Starbucks", ["starbucks coffee", "starbucks reserve"]),
    "mcdonalds": ("McDonald's", ["mcdonalds", "mcdonald's"]),
    "dunkin": ("Dunkin'", ["dunkin donuts", "dunkin’"]),
    "whole_foods": ("Whole Foods Market", ["whole foods", "wholefoods"]),
    "trader_joes": ("Trader Joe's", ["trader joes", "trader joe’s"]),
    "wegmans": ("Wegmans", []),
    "market_basket": ("Market Basket", ["demoulas market basket", "demoulas"]),
    "stop_and_shop": ("Stop & Shop", ["stop & shop", "stop and shop"]),
    "aldi": ("ALDI", ["aldi"],),
    "walmart": ("Walmart", ["wal-mart"]),
    "target": ("Target", []),
    "home_depot": ("The Home Depot", ["home depot"]),
    "lowes": ("Lowe's", ["lowe's", "lowes home improvement"]),
    "cvs": ("CVS Pharmacy", ["cvs", "cvs/pharmacy", "cvs health"]),
    "walgreens": ("Walgreens", ["walgreen"]),
    "rite_aid": ("Rite Aid", ["riteaid"]),
    "bjs": ("BJ's Wholesale Club", ["bj's", "bjs wholesale"]),
    "sams_club": ("Sam's Club", ["sams club"]),
    "panera": ("Panera Bread", ["panera"]),
    "ikea": ("IKEA", []),
    "best_buy": ("Best Buy", []),
}


def _load_brand_registry_csv(path: str) -> Dict[str, Tuple[str, List[str]]]:
    out: Dict[str, Tuple[str, List[str]]] = {}
    try:
        with open(path, newline="") as f:
            rdr = csv.DictReader(f)
            for row in rdr:
                bid = str(row.get("brand_id",""))
                if not bid:
                    continue
                canonical = str(row.get("canonical",""))
                aliases_raw = row.get("aliases", "") or row.get("alias", "") or ""
                sep = ";" if ";" in aliases_raw else "|" if "|" in aliases_raw else ","
                aliases = [a.strip() for a in aliases_raw.split(sep) if a.strip()]
                out[bid] = (canonical or bid.replace("_"," ").title(), aliases)
    except FileNotFoundError:
        return {}
    except Exception:
        return {}
    return out

# --- Overture Category Mapping ---
# Maps Overture's primary and alternate categories to the Townscout Taxonomy.
# Key: lowercase overture category
# Value: (Townscout class, Townscout category, Townscout subcat)
OVERTURE_CATEGORY_MAP = {
    # Food & drink
    "supermarket": ("food_and_drink", "supermarket", "supermarket"),
    "supermarkets": ("food_and_drink", "supermarket", "supermarket"),
    "grocery_store": ("food_and_drink", "supermarket", "grocery"),
    "convenience_store": ("food_and_drink", "convenience_store", "convenience"),
    "restaurants": ("food_and_drink", "restaurant", "restaurant"),
    "fast_food_restaurant": ("food_and_drink", "fast_food", "fast_food"),
    "cafe": ("food_and_drink", "cafe", "cafe"),
    "bakery": ("food_and_drink", "bakery", "bakery"),
    "pizza_restaurant": ("food_and_drink", "pizza", "pizza"),
    "ice_cream_shop": ("food_and_drink", "ice_cream", "ice_cream"),
    # Retail
    "department_store": ("retail", "department_store", "department_store"),
    "shopping_center": ("retail", "shopping_mall", "shopping_center"),
    "hardware_store": ("retail", "hardware_store", "hardware"),
    "home_improvement_store": ("retail", "hardware_store", "home_improvement"),
    "electronics_store": ("retail", "electronics_store", "electronics"),
    "furniture_store": ("retail", "furniture_store", "furniture"),
    # Health
    "hospital": ("health", "hospital", "hospital"),
    "pharmacy": ("health", "pharmacy", "pharmacy"),
    "clinic": ("health", "clinic", "clinic"),
    "urgent_care": ("health", "clinic", "urgent_care"),
    "dentist": ("health", "dentist", "dentist"),
    # Education
    "school": ("education", "school", "school"),
    "university": ("education", "university", "university"),
    "college": ("education", "university", "college"),
    # Civic
    "library": ("civic", "library", "library"),
    "post_office": ("civic", "post_office", "post_office"),
    "town_hall": ("civic", "town_hall", "town_hall"),
    "courthouse": ("civic", "courthouse", "courthouse"),
    # Recreation
    "park": ("recreation", "park", "park"),
    "playground": ("recreation", "playground", "playground"),
    "fitness_center": ("recreation", "gym", "fitness_center"),
    "gym": ("recreation", "gym", "gym"),
    "swimming_pool": ("recreation", "swimming_pool", "swimming_pool"),
    "golf_course": ("recreation", "golf_course", "golf_course"),
    "cinema": ("recreation", "cinema", "cinema"),
    # Transport
    "airport": ("transport", "airport", "airport"),
    "train_station": ("transport", "railway_station", "train_station"),
    "railway_station": ("transport", "railway_station", "railway_station"),
    "bus_station": ("transport", "bus_station", "bus_station"),
    "bus_stop": ("transport", "bus_stop", "bus_stop"),
    "subway_station": ("transport", "subway_station", "subway_station"),
    "ferry_terminal": ("transport", "ferry_terminal", "ferry_terminal"),
}


# --- OSM Tag Mapping ---
# Maps OSM tags (e.g., from 'amenity', 'shop', 'leisure') to the Townscout Taxonomy.
# Key: (tag_key, tag_value)
# Value: (Townscout class, Townscout category, Townscout subcat)
OSM_TAG_MAP = {
    # Food & drink
    ("shop", "supermarket"): ("food_and_drink", "supermarket", "supermarket"),
    ("shop", "convenience"): ("food_and_drink", "convenience_store", "convenience"),
    ("shop", "bakery"): ("food_and_drink", "bakery", "bakery"),
    ("amenity", "restaurant"): ("food_and_drink", "restaurant", "restaurant"),
    ("amenity", "fast_food"): ("food_and_drink", "fast_food", "fast_food"),
    ("amenity", "cafe"): ("food_and_drink", "cafe", "cafe"),
    ("amenity", "bar"): ("food_and_drink", "bar", "bar"),
    ("amenity", "pub"): ("food_and_drink", "bar", "pub"),
    ("amenity", "biergarten"): ("food_and_drink", "bar", "biergarten"),
    ("cuisine", "pizza"): ("food_and_drink", "pizza", "pizza"),
    ("amenity", "ice_cream"): ("food_and_drink", "ice_cream", "ice_cream"),

    # Retail
    ("shop", "department_store"): ("retail", "department_store", "department_store"),
    ("shop", "mall"): ("retail", "shopping_mall", "mall"),
    ("shop", "hardware"): ("retail", "hardware_store", "hardware"),
    ("shop", "doityourself"): ("retail", "hardware_store", "doityourself"),
    ("shop", "electronics"): ("retail", "electronics_store", "electronics"),
    ("shop", "furniture"): ("retail", "furniture_store", "furniture"),

    # Health
    ("amenity", "hospital"): ("health", "hospital", "hospital"),
    ("amenity", "pharmacy"): ("health", "pharmacy", "pharmacy"),
    ("amenity", "clinic"): ("health", "clinic", "clinic"),
    ("amenity", "doctors"): ("health", "clinic", "doctors"),
    ("amenity", "dentist"): ("health", "dentist", "dentist"),

    # Education
    ("amenity", "school"): ("education", "school", "school"),
    ("amenity", "kindergarten"): ("education", "school", "kindergarten"),

    # Civic
    ("amenity", "library"): ("civic", "library", "library"),
    ("amenity", "post_office"): ("civic", "post_office", "post_office"),
    ("amenity", "townhall"): ("civic", "town_hall", "townhall"),
    ("amenity", "courthouse"): ("civic", "courthouse", "courthouse"),
    ("amenity", "police"): ("civic", "police", "police"),
    ("amenity", "fire_station"): ("civic", "fire_station", "fire_station"),

    # Recreation
    ("leisure", "park"): ("recreation", "park", "park"),
    ("leisure", "playground"): ("recreation", "playground", "playground"),
    ("leisure", "sports_centre"): ("recreation", "sports_centre", "sports_centre"),
    ("leisure", "swimming_pool"): ("recreation", "swimming_pool", "swimming_pool"),
    ("leisure", "golf_course"): ("recreation", "golf_course", "golf_course"),
    ("amenity", "cinema"): ("recreation", "cinema", "cinema"),

    # Transport
    ("aeroway", "aerodrome"): ("transport", "airport", "aerodrome"),
    ("aeroway", "terminal"): ("transport", "airport", "terminal"),
    ("railway", "station"): ("transport", "railway_station", "station"),
    ("public_transport", "station"): ("transport", "railway_station", "station"),
    ("highway", "bus_stop"): ("transport", "bus_stop", "bus_stop"),
    ("amenity", "bus_station"): ("transport", "bus_station", "bus_station"),
    ("amenity", "ferry_terminal"): ("transport", "ferry_terminal", "ferry_terminal"),
    ("amenity", "fuel"): ("transport", "fuel", "fuel"),
    ("amenity", "charging_station"): ("transport", "fuel", "charging_station"),
}

# --- Optional external config overrides ---
_BRANDS_CSV = os.path.join("data", "brands", "registry.csv")
_CATS_YML = os.path.join("data", "taxonomy", "categories.yml")

# Override/extend brand registry if CSV present
_from_csv = _load_brand_registry_csv(_BRANDS_CSV)
if _from_csv:
    BRAND_REGISTRY.update(_from_csv)

# Merge category mappings if YAML present AND explicitly enabled
_USE_CATS_YAML = os.environ.get("TS_TAXONOMY_YAML", "0").strip() in ("1", "true", "yes")
if _USE_CATS_YAML and yaml is not None and os.path.isfile(_CATS_YML):
    try:
        with open(_CATS_YML, "r") as f:
            data = yaml.safe_load(f) or {}
            over = data.get("overture_map") or {}
            osm_map = data.get("osm_map") or {}
            # Expect same structures as dicts above
            if isinstance(over, dict):
                for k, v in over.items():
                    if isinstance(v, (list, tuple)) and len(v) >= 3:
                        OVERTURE_CATEGORY_MAP[str(k).lower()] = (str(v[0]), str(v[1]), str(v[2]))
            if isinstance(osm_map, dict):
                for k, v in osm_map.items():
                    try:
                        tag_key, tag_val = k.split(":", 1)
                    except Exception:
                        continue
                    if isinstance(v, (list, tuple)) and len(v) >= 3:
                        OSM_TAG_MAP[(str(tag_key), str(tag_val))] = (str(v[0]), str(v[1]), str(v[2]))
    except Exception:
        pass
