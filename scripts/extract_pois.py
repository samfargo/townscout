import os
from typing import Dict
import geopandas as gpd
from src.config import STATES, POI_BRANDS, STATE_SLUG_TO_CODE
from src.util_osm import pois_from_pbf, ogr_find_brand_features, find_major_airports, airports_from_csv
from src.poi_ski import fetch_and_build_ski_areas_for_state
from src.poi_transit import fetch_and_build_public_transit_for_state

# Expected minimum counts per brand for validation (adjust based on geography)
EXPECTED_MIN_COUNTS = {
	"costco": 8,     # MA has ~10 warehouses
	"chipotle": 20,  # MA has ~56 locations
	"airports": 1,
	"ski-areas": 1,
	"public-transit": 10,
	# Default for unknown brands: 3 (can be overridden here)
}

os.makedirs("data/poi", exist_ok=True)
os.makedirs("data/poi/cache", exist_ok=True)


ONLY_CATEGORY = os.environ.get("TS_ONLY_CATEGORY")


def extract_and_validate_pois(state: str, brand: str, cfg: dict) -> gpd.GeoDataFrame:
	"""
	Multi-stage POI extraction with automatic fallbacks and validation.
	Returns comprehensive POI coverage for the given brand.
	"""
	pbf = f"data/osm/{state}.osm.pbf"
	
	if brand == "airports":
		# Use manual spreadsheet instead of OSM extraction
		state_code = STATE_SLUG_TO_CODE.get(state)
		if not state_code:
			print(f"[airports] Unknown state slug '{state}', cannot map to USPS code")
			return gpd.GeoDataFrame(columns=["geometry"], geometry="geometry")
		csv_path = "airports_coordinates.csv"
		print(f"[airports] loading from CSV for state {state_code}")
		gdf = airports_from_csv(csv_path, state_code)
		print(f"[airports] found {len(gdf)} entries from CSV")
		return gdf
	
	# Special handler for ski-areas (fetch via Overpass + dedupe)
	if brand == "ski-areas":
		return fetch_and_build_ski_areas_for_state(state)

	# Special handler for public-transit (fetch via Overpass template)
	if brand == "public-transit":
		return fetch_and_build_public_transit_for_state(state)

	# Stage 1: Try Pyrosm with exact shop/amenity filters
	print(f"[stage1] {brand}: Pyrosm with shop/amenity filters")
	gdf = pois_from_pbf(
		pbf,
		amenity=cfg["tags"].get("amenity"),
		shop=cfg["tags"].get("shop"),
	)
	
	# Apply name/brand/operator matching
	target_names = set()
	for field in ["name", "brand", "operator"]:
		if field in cfg["tags"]:
			target_names.update([n.lower() for n in cfg["tags"][field]])
	
	def matches_brand(row) -> bool:
		match_cols = [c for c in ["name", "brand", "operator"] if c in row.index]
		for col in match_cols:
			val = row.get(col)
			if isinstance(val, str):
				v = val.lower()
				if v in target_names:
					return True
				for t in target_names:
					if t in v or v in t:
						return True
		return False
	
	if not gdf.empty and target_names:
		gdf = gdf[gdf.apply(matches_brand, axis=1)]
	
	stage1_count = len(gdf)
	print(f"[stage1] {brand}: {stage1_count} locations from Pyrosm")
	
	# Stage 2: If count is suspiciously low, try OGR brand search
	expected_min = EXPECTED_MIN_COUNTS.get(brand, 3)  # Default minimum
	if stage1_count < expected_min:
		print(f"[stage2] {brand}: Count {stage1_count} < expected {expected_min}, trying OGR fallback")
		
		# Collect all brand keywords including common variants
		keywords = list({
			*cfg["tags"].get("name", []),
			*cfg["tags"].get("brand", []), 
			*cfg["tags"].get("operator", []),
		})
		
		# Add common variants for specific brands
		if brand == "costco":
			keywords.extend(["Costco Gas", "Costco Gasoline", "Costco Store", "Costco #"])
		elif brand == "chipotle":
			keywords.extend(["Chipotle #", "Chipotle Restaurant"])
		
		ogr_gdf = ogr_find_brand_features(pbf, keywords)
		
		if not ogr_gdf.empty:
			# Filter out irrelevant features (gas stations for retail brands, etc.)
			if brand in ["costco"]:  # Retail stores, exclude fuel
				cols = ogr_gdf.columns
				if "name" in cols:
					ogr_gdf = ogr_gdf[~ogr_gdf["name"].astype(str).str.contains("(?i)gas|fuel", regex=True, na=False)]
				if "shop" in cols:
					ogr_gdf = ogr_gdf[ogr_gdf["shop"].fillna("").str.lower() != "fuel"]
				if "amenity" in cols:
					ogr_gdf = ogr_gdf[ogr_gdf["amenity"].fillna("").str.lower() != "fuel"]
			
			if len(ogr_gdf) > stage1_count:
				gdf = ogr_gdf
				print(f"[stage2] {brand}: OGR found {len(gdf)} locations (improved from {stage1_count})")
			else:
				print(f"[stage2] {brand}: OGR found {len(ogr_gdf)} locations (no improvement)")
	
	# Stage 3: Final validation and reporting
	final_count = len(gdf)
	if final_count < expected_min:
		print(f"[WARNING] {brand}: Final count {final_count} still below expected {expected_min}")
		print(f"          Consider checking OSM data completeness for {state}")
	else:
		print(f"[validated] {brand}: {final_count} locations meets expectations")
	
	return gdf


# Ski-areas Overpass/dedupe logic moved to src.poi_ski

def _should_process(brand: str) -> bool:
	if not ONLY_CATEGORY:
		return True
	return brand == ONLY_CATEGORY

# Main extraction loop
for state in STATES:
	for brand, cfg in POI_BRANDS.items():
		if not _should_process(brand):
			continue
		out = f"data/poi/{state}_{brand}.parquet"
		if os.path.exists(out):
			print(f"[skip] {out}")
			continue
		print(f"\n=== Extracting {brand} for {state} ===")
		gdf = extract_and_validate_pois(state, brand, cfg)
		if gdf.crs is None:
			gdf.set_crs("EPSG:4326", inplace=True)
		gdf = gdf[["geometry"]]
		gdf.to_parquet(out)
		print(f"[ok] {out} ({len(gdf)} locations)")
		if len(gdf) > 0:
			print(f"     Sample coordinates:")
			for i, row in gdf.head(3).iterrows():
				lat, lon = row.geometry.y, row.geometry.x
				print(f"     - {lat:.6f}, {lon:.6f}")
		print()

	# Ski-areas (first-class, even if not in POI_BRANDS)
	if _should_process("ski-areas"):
		out = f"data/poi/{state}_ski-areas.parquet"
		if os.path.exists(out):
			print(f"[skip] {out}")
		else:
			print(f"\n=== Extracting ski-areas for {state} ===")
			gdf = extract_and_validate_pois(state, "ski-areas", cfg={"tags": {}})
			if gdf.crs is None:
				gdf.set_crs("EPSG:4326", inplace=True)
			gdf = gdf[["geometry"]]
			gdf.to_parquet(out)
			print(f"[ok] {out} ({len(gdf)} locations)")
			if len(gdf) > 0:
				print(f"     Sample coordinates:")
				for i, row in gdf.head(3).iterrows():
					lat, lon = row.geometry.y, row.geometry.x
					print(f"     - {lat:.6f}, {lon:.6f}")
			print()