import os
import re
import json
import time
from typing import Dict, Any, List, Tuple, Optional

import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from scipy.spatial import cKDTree
import requests

from src.config import STATE_SLUG_TO_CODE


OVERPASS_URLS = [
	"https://overpass-api.de/api/interpreter",
	"https://overpass.kumi.systems/api/interpreter",
]


def _load_overpass_template() -> str:
	query_path = "queries/ski_areas.overpass"
	if not os.path.exists(query_path):
		raise SystemExit(f"Missing Overpass query template at {query_path}")
	with open(query_path, "r", encoding="utf-8") as f:
		return f.read()


def _render_overpass_query_for_state(template: str, state_code: str) -> str:
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
				time.sleep(wait)
				wait = min(wait * 1.8, 20.0)
				continue
			print(f"[overpass] {url} error {resp.status_code}: {resp.text[:120]}")
		except requests.RequestException as e:
			print(f"[overpass] request error: {e}; retrying in {wait:.1f}s (attempt {attempt}/{max_attempts})")
			time.sleep(wait)
			wait = min(wait * 1.8, 20.0)
	return None


def _fetch_overpass_json(query: str) -> Dict[str, Any]:
	for url in OVERPASS_URLS:
		resp = _http_post_with_backoff(url, data={"data": query})
		if resp is not None:
			return resp.json()
	raise SystemExit("Overpass request failed at all endpoints")


def _prioritize_row(row: pd.Series) -> int:
	aw = str(row.get("aerialway", "")).lower()
	station = str(row.get("station", "")).lower()
	leisure = str(row.get("leisure", "")).lower()
	sport = str(row.get("sport", "")).lower()
	landuse = str(row.get("landuse", "")).lower()
	if aw == "station" and station in ("valley", "base"):
		return 1
	if leisure == "sports_centre" or leisure == "sports centre":
		if ("ski" in sport) or ("snow" in sport):
			return 2
	if landuse == "winter_sports":
		return 3
	return 4


def _kdtree_from_latlon(lat: np.ndarray, lon: np.ndarray) -> Tuple[cKDTree, float, float]:
	lat0 = float(np.deg2rad(np.mean(lat))) if len(lat) else 0.0
	m_per_deg = 111000.0
	X = np.c_[(lon * np.cos(lat0)) * m_per_deg, lat * m_per_deg]
	return cKDTree(X), lat0, m_per_deg


def _cluster_points(gdf: gpd.GeoDataFrame, max_m: float = 1000.0) -> np.ndarray:
	pts = gdf.geometry.apply(lambda g: (g.x, g.y)).to_list()
	if not pts:
		return np.array([], dtype=int)
	lon = np.array([p[0] for p in pts], dtype=float)
	lat = np.array([p[1] for p in pts], dtype=float)
	tree, lat0, m_per_deg = _kdtree_from_latlon(lat, lon)
	labels = -np.ones(len(pts), dtype=int)
	cur = 0
	for i in range(len(pts)):
		if labels[i] != -1:
			continue
		labels[i] = cur
		Xq = np.array([(lon[i] * np.cos(lat0)) * m_per_deg, lat[i] * m_per_deg])
		idxs = tree.query_ball_point(Xq, r=max_m)
		for j in idxs:
			labels[j] = cur
		cur += 1
	return labels


def _dedupe_ski_areas(gdf_raw: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
	if gdf_raw.empty:
		return gdf_raw
	gdf = gdf_raw.copy()
	gdf = gdf[~gdf.geometry.is_empty & gdf.geometry.notna()]
	gdf = gdf.to_crs("EPSG:4326")
	for col in ["aerialway", "station", "leisure", "sport", "landuse", "name"]:
		if col not in gdf.columns:
			gdf[col] = None
	labels = _cluster_points(gdf, max_m=1000.0)
	if len(labels) == 0:
		return gdf
	gdf["cluster_id"] = labels
	gdf["prio"] = gdf.apply(_prioritize_row, axis=1)
	def _pick_group(df: pd.DataFrame) -> pd.Series:
		df = df.copy()
		df["_namelen"] = df["name"].fillna("").astype(str).str.len()
		best = df.sort_values(["prio", "_namelen"], ascending=[True, False], kind="mergesort").iloc[0]
		return best
	chosen = gdf.groupby("cluster_id", as_index=False).apply(_pick_group, include_groups=False)
	chosen = gpd.GeoDataFrame(chosen, geometry="geometry", crs="EPSG:4326")
	return chosen[["geometry"]]


def _elements_to_gdf(elements: List[Dict[str, Any]]) -> gpd.GeoDataFrame:
	rows = []
	for el in elements:
		tags = el.get("tags", {}) or {}
		if "piste:type" in tags:
			continue
		aerialway = str(tags.get("aerialway", "")).lower()
		station = str(tags.get("station", "")).lower()
		leisure = str(tags.get("leisure", "")).lower()
		sport = str(tags.get("sport", "")).lower()
		landuse = str(tags.get("landuse", "")).lower()
		is_candidate = False
		if aerialway == "station" and station in ("valley", "base"):
			is_candidate = True
		elif leisure in ("sports_centre", "sports centre") and ("ski" in sport or "snow" in sport):
			is_candidate = True
		elif landuse == "winter_sports":
			is_candidate = True
		if not is_candidate:
			continue
		lat = el.get("lat")
		lon = el.get("lon")
		if lat is None or lon is None:
			center = el.get("center") or {}
			lat = center.get("lat")
			lon = center.get("lon")
		if lat is None or lon is None:
			continue
		rows.append({
			"aerialway": aerialway,
			"station": station,
			"leisure": leisure,
			"sport": sport,
			"landuse": landuse,
			"name": tags.get("name"),
			"geometry": Point(float(lon), float(lat)),
		})
	if not rows:
		return gpd.GeoDataFrame(columns=["geometry"], geometry="geometry", crs="EPSG:4326")
	gdf = gpd.GeoDataFrame(pd.DataFrame(rows), geometry="geometry", crs="EPSG:4326")
	return gdf


def fetch_and_build_ski_areas_for_state(state_slug: str) -> gpd.GeoDataFrame:
	state_code = STATE_SLUG_TO_CODE.get(state_slug, "MA")
	os.makedirs("data/poi", exist_ok=True)
	os.makedirs("data/poi/cache", exist_ok=True)
	cache_path = f"data/poi/cache/{state_slug}_ski-areas_raw.json"
	if os.path.exists(cache_path):
		try:
			with open(cache_path, "r", encoding="utf-8") as f:
				data = json.load(f)
			elements = data.get("elements", [])
			raw_gdf = _elements_to_gdf(elements)
		except Exception as e:
			print(f"[ski-areas] Failed to read cache {cache_path}: {e}; re-fetching")
			raw_gdf = gpd.GeoDataFrame(columns=["geometry"], geometry="geometry", crs="EPSG:4326")
	else:
		raw_gdf = gpd.GeoDataFrame(columns=["geometry"], geometry="geometry", crs="EPSG:4326")

	if raw_gdf.empty:
		template = _load_overpass_template()
		query = _render_overpass_query_for_state(template, state_code)
		print(f"[overpass] fetching ski-areas for {state_slug} ({state_code})")
		data = _fetch_overpass_json(query)
		with open(cache_path, "w", encoding="utf-8") as f:
			json.dump(data, f)
		elements = data.get("elements", [])
		raw_gdf = _elements_to_gdf(elements)

	if raw_gdf.crs is None:
		raw_gdf = raw_gdf.set_crs("EPSG:4326")
	deduped = _dedupe_ski_areas(raw_gdf)
	return deduped


