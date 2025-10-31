// API services for fetching data from the vicinity backend

// ==================== BASE API UTILITIES ====================

export function resolveApiUrl(path: string): string {
  // In production/deployment, use relative paths
  // In development, could use env var for API base URL
  const base = process.env.NEXT_PUBLIC_API_URL || '';
  return `${base}${path}`;
}

export async function fetchApi<T>(path: string, options?: RequestInit): Promise<T> {
  const url = resolveApiUrl(path);
  const response = await fetch(url, options);
  
  if (!response.ok) {
    throw new Error(`API request failed: ${response.status} ${response.statusText}`);
  }
  
  return response.json() as Promise<T>;
}

// ==================== API PROXY UTILITIES ====================

import type { FeatureCollection, Point } from "geojson";
import { NextResponse } from "next/server";

export interface ApiProxyOptions<T> {
  /** The API endpoint path (e.g., '/api/d_anchor') */
  endpoint: string;
  /** Fallback value to return when the upstream API fails */
  fallback: T;
  /** Optional transform function to normalize the response */
  transform?: (payload: any) => T;
  /** Log label for error messages */
  logLabel: string;
}

export async function safeReadBody(response: Response): Promise<string> {
  try {
    return await response.text();
  } catch {
    return "<no-body>";
  }
}

export async function createApiProxy<T>(
  request: Request,
  options: ApiProxyOptions<T>
): Promise<NextResponse<T>> {
  const requestUrl = new URL(request.url);
  const upstreamUrl = resolveApiUrl(`${options.endpoint}${requestUrl.search}`);

  try {
    const upstream = new URL(upstreamUrl);
    if (upstream.origin === requestUrl.origin && upstream.pathname === requestUrl.pathname) {
      throw new Error(`Upstream ${options.endpoint} URL resolves to this Next.js route; aborting to avoid loop.`);
    }

    const response = await fetch(upstreamUrl, {
      headers: {
        Accept: "application/json"
      },
      cache: "no-store"
    });

    if (!response.ok) {
      const body = await safeReadBody(response);
      throw new Error(`Upstream ${options.endpoint} request failed: ${response.status} ${response.statusText}: ${body}`);
    }

    const rawPayload = await response.json();
    const finalPayload = options.transform 
      ? options.transform(rawPayload)
      : (rawPayload ?? options.fallback);

    return NextResponse.json(finalPayload, {
      headers: {
        "cache-control": "no-store"
      }
    });
  } catch (error) {
    console.error(`[${options.logLabel}] Falling back to empty payload`, error);
    return NextResponse.json(options.fallback, {
      headers: {
        "cache-control": "no-store"
      }
    });
  }
}

// ==================== CATALOG SERVICE ====================

export interface Brand {
  id: string;
  name?: string;
  label: string;
  group?: string;
}

export interface Category {
  id: string | number;
  label: string;
  group?: string;
}

export interface Catalog {
  brands: Brand[];
  categories: Category[];
  loaded: boolean;
  catToBrands: Record<string, string[]>;
}

export interface CategoryGroup {
  id: string;
  label: string;
  brandIds: string[];
}

export async function fetchCatalog(): Promise<Catalog> {
  const payload = await fetchApi<any>('/api/catalog');

  const categories: Category[] = (payload.categories ?? []).map((cat: any) => {
    const id = cat?.id ?? cat?.category_id ?? cat?.key ?? cat?.value ?? null;
    return {
      id: id != null ? String(id) : '',
      label: cat?.label ?? cat?.name ?? (id != null ? `Category ${id}` : 'Category'),
      group: cat?.group
    };
  }).filter((cat: Category) => Boolean(cat.id));

  const brands: Brand[] = (payload.brands ?? []).map((brand: any) => {
    const id = brand?.id ?? brand?.brand_id ?? brand?.value ?? null;
    return {
      id: id != null ? String(id) : '',
      label: brand?.label ?? brand?.name ?? (id != null ? String(id) : 'Brand'),
      name: brand?.name,
      group: brand?.group
    };
  }).filter((brand: Brand) => Boolean(brand.id));

  const rawMapping = payload.catToBrands ?? payload.cat_to_brands ?? {};
  const catToBrands: Record<string, string[]> = {};
  for (const [key, value] of Object.entries(rawMapping)) {
    const normalizedKey = String(key);
    const normalizedValues = Array.isArray(value)
      ? value.map((item) => String(item))
      : [];
    catToBrands[normalizedKey] = normalizedValues;
  }

  return {
    categories,
    brands,
    catToBrands,
    loaded: true
  };
}

export function buildCategoryGroups(input: {
  categories: Category[];
  brands: Brand[];
  catToBrands: Record<string, string[]>;
}): CategoryGroup[] {
  return input.categories.map((cat) => ({
    id: String(cat.id),
    label: cat.label,
    brandIds: input.catToBrands[String(cat.id)] || []
  }));
}

// ==================== D_ANCHOR SERVICE ====================

export type DAnchorMap = Record<string, number>;

export async function fetchDAnchor(
  poiId: string,
  mode: 'drive' | 'walk',
  catalog: Catalog
): Promise<DAnchorMap> {
  try {
    const normalizedId = String(poiId);
    const brands = catalog?.brands ?? [];
    const isBrand = brands.some((brand) => String(brand.id) === normalizedId);

    const params = new URLSearchParams({ mode });
    let path: string;

    if (isBrand) {
      params.set('brand', normalizedId);
      path = `/api/d_anchor_brand?${params.toString()}`;
    } else {
      params.set('category', normalizedId);
      path = `/api/d_anchor?${params.toString()}`;
    }

    // Fetch the distance-to-anchor map for the given POI and mode
    return await fetchApi<DAnchorMap>(path);
  } catch (error) {
    console.error(`❌ [fetchDAnchor] Failed to fetch dAnchor for ${poiId} (${mode}):`, error);
    return {};
  }
}

export async function fetchCustomDAnchor(
  lon: number,
  lat: number,
  mode: 'drive' | 'walk',
  cutoff: number = 30,
  overflowCutoff: number = 90
): Promise<DAnchorMap> {
  try {
    const params = new URLSearchParams({
      lon: String(lon),
      lat: String(lat),
      mode,
      cutoff: String(cutoff),
      overflow_cutoff: String(overflowCutoff)
    });
    
    return await fetchApi<DAnchorMap>(`/api/d_anchor_custom?${params.toString()}`);
  } catch (error) {
    console.error(`❌ [fetchCustomDAnchor] Failed to fetch custom dAnchor for (${lon}, ${lat}) (${mode}):`, error);
    return {};
  }
}

// ==================== POI PINS SERVICE ====================

export interface PoiPinProperties {
  brand_id?: string;
  name?: string;
  address?: string;
  approx_address?: string;
  category?: string;
}

export type PoiPointsResponse = FeatureCollection<Point, PoiPinProperties>;

export interface Bounds {
  west: number;
  south: number;
  east: number;
  north: number;
}

export interface FetchPoiPointsOptions {
  brands?: string[];
  categoryId?: string | number;
  bounds?: Bounds;
}

export async function fetchPoiPoints({
  brands,
  categoryId,
  bounds
}: FetchPoiPointsOptions): Promise<PoiPointsResponse> {
  const params = new URLSearchParams();
  const normalizedBrands = (brands ?? [])
    .map((value) => String(value).trim())
    .filter((value) => value.length > 0);
  if (normalizedBrands.length) {
    params.set('brands', normalizedBrands.join(','));
  }
  if (categoryId != null) {
    const categoryValue = String(categoryId).trim();
    if (categoryValue.length) {
      params.set('category', categoryValue);
    }
  }
  if (!params.has('brands') && !params.has('category')) {
    return {
      type: 'FeatureCollection',
      features: []
    };
  }
  if (bounds) {
    params.set('bbox', `${bounds.west},${bounds.south},${bounds.east},${bounds.north}`);
  }

  try {
    return await fetchApi<PoiPointsResponse>(`/api/poi_points?${params.toString()}`);
  } catch (error) {
    console.error('Failed to fetch POI pins:', error);
    return {
      type: 'FeatureCollection',
      features: []
    };
  }
}

// ==================== PLACES SERVICE ====================

export interface PlaceSuggestion {
  id: string;
  label: string;
  sublabel?: string;
  description?: string;
  lat?: number;
  lon?: number;
  structured_formatting?: {
    main_text: string;
    secondary_text: string;
  };
}

export interface PlaceDetails {
  id: string;
  label: string;
  lat: number;
  lon: number;
  formatted_address?: string;
}

export async function fetchPlaceSuggestions(options: {
  query: string;
  session?: string;
  limit?: number;
}): Promise<{ suggestions: PlaceSuggestion[] }> {
  if (!options.query || options.query.length < 2) {
    return { suggestions: [] };
  }
  
  try {
    const params = new URLSearchParams({ input: options.query });
    if (options.session) {
      params.append('session', options.session);
    }
    if (options.limit) {
      params.append('limit', String(options.limit));
    }
    
    return await fetchApi<{ suggestions: PlaceSuggestion[] }>(
      `/api/places/autocomplete?${params}`
    );
  } catch (error) {
    console.error('Failed to fetch place suggestions:', error);
    return { suggestions: [] };
  }
}

export async function fetchPlaceDetails(
  placeId: string,
  sessionToken?: string
): Promise<PlaceDetails | null> {
  try {
    const params = new URLSearchParams({ place_id: placeId });
    if (sessionToken) {
      params.append('session', sessionToken);
    }
    
    const response = await fetchApi<{ result: PlaceDetails }>(`/api/places/details?${params}`);
    return response.result;
  } catch (error) {
    console.error('Failed to fetch place details:', error);
    return null;
  }
}
