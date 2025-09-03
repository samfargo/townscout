import os
import re
import json
from typing import Dict, Any, List, Optional

import geopandas as gpd
from shapely.geometry import Point
import requests

from src.config import STATE_SLUG_TO_CODE

OVERPASS_URLS = [
	"https://overpass-api.de/api/interpreter",
	"https://overpass.kumi.systems/api/interpreter",
]

def _load_overpass_template() -> str:
	query_path = "queries/MA_public_transit.overpass"
	if not os.path.exists(query_path):
		raise SystemExit(f"Missing Overpass query template at {query_path}")
	with open(query_path, "r", encoding="utf-8") as f:
		return f.read()

def _render_overpass_query_for_state(template: str, state_code: str) -> str:
	# Replace the ISO3166-2 code occurrence US-XX with the target state's code
	return re.sub(r'US-[A-Z]{2}', f'US-{state_code}', template)

def _http_post_with_backoff(url: str, data: Dict[str, Any], max_attempts: int = 6) -> Optional[requests.Response]:
	wait = 1.5
	for attempt in range(1, max_attempts + 1):
		try:
			resp = requests.post(url, data=data, timeout=180)
			if resp.status_code == 200:
				return resp
			if resp.status_code in (429, 504, 502, 503):
				print(f"[overpass] {url} returned {resp.status_code}; retrying in {wait:.1f}s (attempt {attempt}/{max_attempts})")
				import time as _t
				_t.sleep(wait)
				wait = min(wait * 1.8, 20.0)
				continue
			print(f"[overpass] {url} error {resp.status_code}: {resp.text[:120]}")
		except requests.RequestException as e:
			print(f"[overpass] request error: {e}; retrying in {wait:.1f}s (attempt {attempt}/{max_attempts})")
			import time as _t
			_t.sleep(wait)
			wait = min(wait * 1.8, 20.0)
	return None

def _fetch_overpass_json(query: str) -> Dict[str, Any]:
	for url in OVERPASS_URLS:
		resp = _http_post_with_backoff(url, data={"data": query})
		if resp is not None:
			return resp.json()
	raise SystemExit("Overpass request failed at all endpoints")

def _elements_to_gdf(elements: List[Dict[str, Any]]) -> gpd.GeoDataFrame:
	rows = []
	for el in elements:
		lat = el.get("lat")
		lon = el.get("lon")
		if lat is None or lon is None:
			center = el.get("center") or {}
			lat = center.get("lat")
			lon = center.get("lon")
		if lat is None or lon is None:
			continue
		rows.append({"geometry": Point(float(lon), float(lat))})
	if not rows:
		return gpd.GeoDataFrame(columns=["geometry"], geometry="geometry", crs="EPSG:4326")
	gdf = gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326")
	return gdf[["geometry"]]

def fetch_and_build_public_transit_for_state(state_slug: str) -> gpd.GeoDataFrame:
	state_code = STATE_SLUG_TO_CODE.get(state_slug, "MA")
	os.makedirs("data/poi", exist_ok=True)
	os.makedirs("data/poi/cache", exist_ok=True)
	cache_path = f"data/poi/cache/{state_slug}_public-transit_raw.json"

	# Try cache first
	if os.path.exists(cache_path):
		try:
			with open(cache_path, "r", encoding="utf-8") as f:
				data = json.load(f)
			elements = data.get("elements", [])
			gdf = _elements_to_gdf(elements)
			if not gdf.empty:
				return gdf
		except Exception as e:
			print(f"[public-transit] Failed to read cache {cache_path}: {e}; re-fetching")

	# Fetch via Overpass
	template = _load_overpass_template()
	query = _render_overpass_query_for_state(template, state_code)
	print(f"[overpass] fetching public-transit for {state_slug} ({state_code})")
	data = _fetch_overpass_json(query)
	with open(cache_path, "w", encoding="utf-8") as f:
		json.dump(data, f)
	return _elements_to_gdf(data.get("elements", []))


