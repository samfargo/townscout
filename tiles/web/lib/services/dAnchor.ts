// dAnchor service for distance-to-anchor calculations
import { fetchApi } from './api';
import type { Catalog } from './catalog';

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
    console.error(`Failed to fetch dAnchor for ${poiId} (${mode}):`, error);
    return {};
  }
}
