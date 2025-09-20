"""
Defines the Townscout canonical taxonomy for POIs and a brand registry.

This module provides the data structures and mappings required to normalize
POIs from various sources (Overture, OSM) into a consistent, canonical schema.
"""

# --- Townscout POI Taxonomy ---
# A hierarchical classification system: class -> category -> subcat
# This is a starting point and should be expanded based on data analysis.
TAXONOMY = {
    "food_and_drink": {
        "supermarket": ["supermarket", "grocery", "hypermarket"],
        "restaurant": ["restaurant", "fast_food", "cafe"],
        "bar": ["bar", "pub"],
    },
    "retail": {
        "clothing_store": ["clothing", "fashion"],
        "department_store": ["department_store"],
        "hardware_store": ["hardware"],
    },
    "health": {
        "hospital": ["hospital"],
        "pharmacy": ["pharmacy"],
        "clinic": ["clinic", "doctors"],
    },
    # ... more classes and categories to be added
}


# --- Brand Registry ---
# Provides a mapping from various name aliases to a canonical brand ID and name.
# This helps in deduplicating brands that appear with slightly different names.
BRAND_REGISTRY = {
    # Canonical Brand ID: (Canonical Name, [Aliases])
    "chipotle": ("Chipotle Mexican Grill", ["chipotle"]),
    "costco": ("Costco", ["costco wholesale"]),
    "starbucks": ("Starbucks", ["starbucks coffee", "starbucks reserve", "starbucks mashpee commons", "starbucks wallingford ct"]),
    "mcdonalds": ("McDonald's", ["mcdonalds", "mcdonald's"]),
    # ... more brands to be added
}

# --- Overture Category Mapping ---
# Maps Overture's primary and alternate categories to the Townscout Taxonomy.
# Key: lowercase overture category
# Value: (Townscout class, Townscout category, Townscout subcat)
OVERTURE_CATEGORY_MAP = {
    "supermarket": ("food_and_drink", "supermarket", "supermarket"),
    "supermarkets": ("food_and_drink", "supermarket", "supermarket"),
    "grocery_store": ("food_and_drink", "supermarket", "grocery"),
    "restaurants": ("food_and_drink", "restaurant", "restaurant"),
    "fast_food_restaurant": ("food_and_drink", "restaurant", "fast_food"),
    "cafe": ("food_and_drink", "restaurant", "cafe"),
    # ... more mappings to be added
}


# --- OSM Tag Mapping ---
# Maps OSM tags (e.g., from 'amenity', 'shop', 'leisure') to the Townscout Taxonomy.
# Key: (tag_key, tag_value)
# Value: (Townscout class, Townscout category, Townscout subcat)
OSM_TAG_MAP = {
    ("shop", "supermarket"): ("food_and_drink", "supermarket", "supermarket"),
    ("shop", "convenience"): ("food_and_drink", "supermarket", "grocery"),
    ("amenity", "restaurant"): ("food_and_drink", "restaurant", "restaurant"),
    ("amenity", "fast_food"): ("food_and_drink", "restaurant", "fast_food"),
    ("amenity", "cafe"): ("food_and_drink", "restaurant", "cafe"),
    ("amenity", "bar"): ("food_and_drink", "bar", "bar"),
    # ... more mappings to be added
}
