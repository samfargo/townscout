# Regions to compute first
STATES = [
    "massachusetts"
]
GEOFABRIK_BASE = "https://download.geofabrik.de/north-america/us"

# H3 resolutions
H3_RES_LOW = 7   # overview
H3_RES_HIGH = 8  # detail

# Crime rate enrichment configuration
CRIME_RATE_SOURCE = "ma_crime_rates.csv"  # Path to crime rate data

# TIGER/Line 2024 URLs for boundaries
TIGER_BASE_URL = "https://www2.census.gov/geo/tiger/TIGER2024"
TIGER_PLACES_URL = f"{TIGER_BASE_URL}/PLACE"  # tl_2024_SS_place.zip
TIGER_COUSUB_URL = f"{TIGER_BASE_URL}/COUSUB"  # tl_2024_SS_cousub.zip

# States where MCDs (County Subdivisions) function as municipalities
# From 2024 TIGER technical documentation
MCD_STATES = {
    "09": "CT",  # Connecticut
    "23": "ME",  # Maine  
    "25": "MA",  # Massachusetts
    "26": "MI",  # Michigan
    "27": "MN",  # Minnesota
    "33": "NH",  # New Hampshire
    "34": "NJ",  # New Jersey
    "36": "NY",  # New York
    "42": "PA",  # Pennsylvania
    "44": "RI",  # Rhode Island
    "50": "VT",  # Vermont
    "55": "WI",  # Wisconsin
}

# All US state FIPS codes for complete national coverage
STATE_FIPS = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA", "08": "CO",
    "09": "CT", "10": "DE", "11": "DC", "12": "FL", "13": "GA", "15": "HI",
    "16": "ID", "17": "IL", "18": "IN", "19": "IA", "20": "KS", "21": "KY",
    "22": "LA", "23": "ME", "24": "MD", "25": "MA", "26": "MI", "27": "MN",
    "28": "MS", "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
    "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND", "39": "OH",
    "40": "OK", "41": "OR", "42": "PA", "44": "RI", "45": "SC", "46": "SD",
    "47": "TN", "48": "TX", "49": "UT", "50": "VT", "51": "VA", "53": "WA",
    "54": "WV", "55": "WI", "56": "WY"
}

# Brands/POI categories for MVP
POI_BRANDS = {
    "chipotle": {
        "tags": {
            "amenity": ["fast_food", "restaurant", "cafe"], 
            "name": ["Chipotle", "Chipotle Mexican Grill"], 
            "brand": ["Chipotle", "Chipotle Mexican Grill"],
            "operator": ["Chipotle", "Chipotle Mexican Grill"]
        }
    },
    "costco": {
        "tags": {
            "shop": ["supermarket", "wholesale", "department_store"], 
            "name": ["Costco", "Costco Wholesale"], 
            "brand": ["Costco", "Costco Wholesale"],
            "operator": ["Costco", "Costco Wholesale"]
        }
    },
    "airports": {
        "tags": {
            "aeroway": ["aerodrome"],
            "name": ["International"],
        }
    },
    # Template for adding new POI brands:
    # "brand_name": {
    #     "tags": {
    #         "amenity": ["category1", "category2"],  # or omit if not applicable
    #         "shop": ["category1", "category2"],     # or omit if not applicable  
    #         "name": ["Brand Name", "Brand Name Inc"],
    #         "brand": ["Brand Name", "Brand Name Inc"],
    #         "operator": ["Brand Name", "Brand Name Inc"]
    #     }
    # },
}

# Definition examples for future categories
MAJOR_AIRPORT_MIN_PAX = 500_000  # illustrative

# Mapping from Geofabrik state slug to USPS state code (minimal set for current STATES/Makefile)
STATE_SLUG_TO_CODE = {
    "massachusetts": "MA",
    "new-hampshire": "NH",
    "rhode-island": "RI",
    "connecticut": "CT",
    "maine": "ME",
} 