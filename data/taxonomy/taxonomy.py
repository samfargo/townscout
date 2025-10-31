"""
Defines the vicinity canonical taxonomy for POIs and registries.

This module provides the data structures and mappings required to normalize
POIs from various sources (Overture, OSM) into a consistent, canonical schema.

Single source of truth for allowlists (anti-drift design):
- POI_brand_registry.csv (brand_id,canonical,aliases,wikidata) - all brands in registry are allowlisted
- POI_category_registry.csv (category_id,numeric_id,display_name) - all categories in CSV are allowlisted with explicit IDs

Optional override:
- categories.yml (keys: overture_map, osm_map) - extends category mappings if TS_TAXONOMY_YAML=1
"""
from __future__ import annotations
import csv
import os
from typing import Dict, Tuple, List
try:
    import yaml  # optional; for categories.yml
except Exception:
    yaml = None

# --- vicinity POI Taxonomy ---
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
    "religious": {
        "place_of_worship_church": ["church", "christian"],
        "place_of_worship_synagogue": ["synagogue", "jewish"],
        "place_of_worship_temple": ["temple", "hindu", "buddhist", "jain", "sikh"],
        "place_of_worship_mosque": ["mosque", "muslim"],
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
        "beach_ocean": ["ocean"],
        "beach_lake": ["lake"],
        "beach_river": ["river"],
        "beach_other": ["other"],
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
    "dunkin": ("Dunkin'", ["dunkin donuts", "dunkin'"]),
    "whole_foods": ("Whole Foods Market", ["whole foods", "wholefoods"]),
    "trader_joes": ("Trader Joe's", ["trader joes", "trader joe's"]),
    "wegmans": ("Wegmans", []),
    "walmart": ("Walmart", ["wal-mart"]),
    "target": ("Target", []),
    "home_depot": ("The Home Depot", ["home depot"]),
    "lowes": ("Lowe's", ["lowe's", "lowes home improvement"]),
    "cvs": ("CVS Pharmacy", ["cvs", "cvs/pharmacy", "cvs health"]),
    "walgreens": ("Walgreens", ["walgreen"]),
    "rite_aid": ("Rite Aid", ["riteaid"]),
    "sams_club": ("Sam's Club", ["sams club"]),
    "panera": ("Panera Bread", ["panera"]),
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


def get_allowlisted_brands(path: str = None) -> set[str]:
    """
    Load all brand_ids from the brand registry CSV.
    All brands in the registry are considered allowlisted.
    
    Args:
        path: Path to registry CSV file. If None, uses default POI_brand_registry.csv in same directory.
        
    Returns:
        Set of all brand_ids in the registry.
    """
    if path is None:
        # Relative to this file's directory
        path = os.path.join(os.path.dirname(__file__), "POI_brand_registry.csv")
    
    brand_ids: set[str] = set()
    try:
        with open(path, newline="") as f:
            rdr = csv.DictReader(f)
            for row in rdr:
                bid = str(row.get("brand_id", "")).strip()
                if bid:
                    brand_ids.add(bid)
    except FileNotFoundError:
        return set()
    except Exception:
        return set()
    return brand_ids


def get_categories(path: str = None) -> Dict[str, Tuple[int, str]]:
    """
    Load categories from CSV with explicit numeric IDs (anti-drift design).
    All categories in the CSV are considered allowlisted.
    
    Args:
        path: Path to categories CSV file. If None, uses default POI_category_registry.csv in same directory.
        
    Returns:
        Dict mapping category_id -> (numeric_id, display_name)
        
    Raises:
        ValueError: If duplicate numeric_ids are found (prevents ID drift).
    """
    if path is None:
        # Relative to this file's directory
        path = os.path.join(os.path.dirname(__file__), "POI_category_registry.csv")
    
    categories: Dict[str, Tuple[int, str]] = {}
    numeric_ids_seen: Dict[int, str] = {}
    
    try:
        with open(path, newline="") as f:
            rdr = csv.DictReader(f)
            for row_num, row in enumerate(rdr, start=2):  # start=2 accounts for header
                cat_id = str(row.get("category_id", "")).strip()
                if not cat_id:
                    continue
                    
                try:
                    numeric_id = int(row.get("numeric_id", "0"))
                except (ValueError, TypeError):
                    raise ValueError(f"Invalid numeric_id in {path} row {row_num}: {row.get('numeric_id')}")
                
                # Check for duplicate numeric IDs (critical for preventing drift)
                if numeric_id in numeric_ids_seen:
                    raise ValueError(
                        f"Duplicate numeric_id={numeric_id} in {path}: "
                        f"'{numeric_ids_seen[numeric_id]}' and '{cat_id}'"
                    )
                numeric_ids_seen[numeric_id] = cat_id
                
                display_name = str(row.get("display_name", "")).strip() or cat_id.replace("_", " ").title()
                categories[cat_id] = (numeric_id, display_name)
                
    except FileNotFoundError:
        return {}
    except ValueError:
        raise  # Re-raise validation errors
    except Exception as e:
        print(f"[warn] Failed to load categories from {path}: {e}")
        return {}
    
    return categories


def get_allowlisted_categories(path: str = None) -> set[str]:
    """
    Load all category_ids from the categories CSV.
    All categories in the CSV are considered allowlisted.
    
    Args:
        path: Path to categories CSV file. If None, uses default location.
        
    Returns:
        Set of all category_ids in the CSV.
    """
    categories = get_categories(path)
    return set(categories.keys())

# --- Overture Category Mapping ---
# Maps Overture's primary and alternate categories to the vicinity Taxonomy.
# Key: lowercase overture category
# Value: (vicinity class, vicinity category, vicinity subcat)
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
    # Religious / Places of Worship
    "place_of_worship": ("religious", "place_of_worship_church", "church"),  # fallback if no religion specified
    "church": ("religious", "place_of_worship_church", "church"),
    "synagogue": ("religious", "place_of_worship_synagogue", "synagogue"),
    "temple": ("religious", "place_of_worship_temple", "temple"),
    "mosque": ("religious", "place_of_worship_mosque", "mosque"),
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
# Maps OSM tags (e.g., from 'amenity', 'shop', 'leisure') to the vicinity Taxonomy.
# Key: (tag_key, tag_value)
# Value: (vicinity class, vicinity category, vicinity subcat)
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

    # Religious / Places of Worship
    # Note: The classification by religion type is handled in the normalization script
    # based on the "religion" tag in OSM
    ("amenity", "place_of_worship"): ("religious", "place_of_worship_church", "church"),  # fallback

    # Natural features
    # Note: Beaches are handled specially via osm_beaches.py build_beach_pois_for_state()
    # which classifies them into beach_ocean, beach_lake, beach_river, beach_other
    # based on proximity to coastlines and water bodies. The mappings below are
    # fallbacks if the classification system is bypassed.
    ("natural", "beach"): ("natural", "beach_other", "other"),

    # Natural / Recreation (overture) - beaches from Overture (if any)
    ("amenity", "beach"): ("natural", "beach_other", "other"),
    ("amenity", "beach_access"): ("natural", "beach_other", "other"),
    }

# --- Optional external config overrides ---
# Paths relative to this file's directory
_BRANDS_CSV = os.path.join(os.path.dirname(__file__), "POI_brand_registry.csv")
_CATS_YML = os.path.join(os.path.dirname(__file__), "categories.yml")

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

