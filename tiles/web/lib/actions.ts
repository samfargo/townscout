// Client-side actions for managing POIs, filters, and map interactions
'use client';
import { useStore, type Mode } from './state/store';
import type { Catalog } from './services/catalog';
import { fetchCatalog } from './services/catalog';
import { fetchDAnchor } from './services/dAnchor';
import { getMapWorker } from './map/workerRegistry';

// Set to true for verbose debugging
const DEBUG = false;

export const MIN_MINUTES = 5;
export const MAX_MINUTES = 60;
export const MINUTE_STEP = 5;

// Catalog management
export async function ensureCatalogLoaded(): Promise<Catalog> {
  const store = useStore.getState();
  
  if (store.catalog) {
    return store.catalog;
  }
  
  const catalog = await fetchCatalog();
  store.setCatalog(catalog);
  return catalog;
}

// POI management
export async function addBrand(brandId: string, label: string): Promise<void> {
  if (DEBUG) console.log('🎯 [addBrand] Starting - brandId:', brandId, 'label:', label);
  
  // Ensure catalog is ready so we can correctly fetch dAnchor data
  await ensureCatalogLoaded();
  const store = useStore.getState();
  
  // Check if already added
  if (store.pois.some((p) => p.id === brandId)) {
    if (DEBUG) console.log('⚠️ [addBrand] Brand already added, skipping');
    return;
  }
  
  store.addPoi({ id: brandId, label, type: 'brand' });
  store.setSlider(brandId, 30); // Default 30 minutes
  
  // Fetch dAnchor data
  await loadDAnchor(brandId, store.mode);
  await applyCurrentFilter();
}

export async function addCategory(categoryId: string, label: string, ids: string[]): Promise<void> {
  // Ensure catalog is ready so we can correctly fetch dAnchor data
  await ensureCatalogLoaded();
  const store = useStore.getState();
  
  if (store.pois.some((p) => p.id === categoryId)) {
    return;
  }
  
  store.addPoi({ id: categoryId, label, type: 'category' });
  store.setSlider(categoryId, 30);
  
  await loadDAnchor(categoryId, store.mode);
  await applyCurrentFilter();
}

export async function addCustom(
  lon: number,
  lat: number,
  label: string,
  minutes: number
): Promise<void> {
  const store = useStore.getState();
  const id = customCacheKey(lon, lat);

  if (store.pois.some((p) => p.id === id)) {
    return;
  }

  store.addPoi({ id, label, type: 'custom', lat, lon });
  store.setSlider(id, minutes);
  
  // For custom locations, we'd need to compute dAnchor on the fly or via API
  // For now, skip the dAnchor fetch
  await applyCurrentFilter();
}

export function removePOI(id: string): void {
  const store = useStore.getState();
  store.removePoi(id);
  store.removeSlider(id);
  void applyCurrentFilter();
}

// Climate selections
export function setClimateSelections(labels: string[]): void {
  const store = useStore.getState();
  const next = Array.from(new Set(labels));
  if (sequenceEquals(store.climateSelections, next)) {
    return;
  }
  store.setClimateSelections(next);
  void applyCurrentFilter();
}

export function toggleClimateSelection(label: string): void {
  const store = useStore.getState();
  const current = store.climateSelections;
  const next = current.includes(label)
    ? current.filter((item) => item !== label)
    : [...current, label];
  store.setClimateSelections(next);
  void applyCurrentFilter();
}

export function clearClimateSelections(): void {
  setClimateSelections([]);
}

// Slider management
export function updateSlider(id: string, value: number): void {
  const store = useStore.getState();
  store.setSlider(id, value);
  void applyCurrentFilter();
}

// Pending slider preview updates
let pendingSliderValues: Record<string, number> = {};
let animationFrameId: number | null = null;

// Apply slider preview updates on the next animation frame
const applyPreviewUpdate = () => {
  animationFrameId = null;
  const worker = getMapWorker();
  worker.postMessage({
    type: 'update-preview',
    tempValues: { ...pendingSliderValues }
  });
  pendingSliderValues = {};
};

// Scheduler to apply preview updates without blocking the UI on every tick
const schedulePreviewApply = () => {
  if (animationFrameId) {
    cancelAnimationFrame(animationFrameId);
  }
  animationFrameId = requestAnimationFrame(applyPreviewUpdate);
};

// Update slider and apply filter without persisting to store (for smooth dragging)
export function updateSliderPreview(id: string, value: number): void {
  // Accumulate the latest pending value for the given slider id
  pendingSliderValues[id] = value;
  // Schedule apply to keep UI responsive during drags
  schedulePreviewApply();
}

// Mode management
export async function changePoiMode(id: string, mode: Mode): Promise<void> {
  const store = useStore.getState();
  store.setPoiMode(id, mode);
  
  // Reload dAnchor data for new mode if needed
  if (!store.dAnchorCache[id]?.[mode]) {
    await loadDAnchor(id, mode);
  }
  
  await applyCurrentFilter();
}

export function normalizeMinutes(value: number): number {
  return Math.max(MIN_MINUTES, Math.min(MAX_MINUTES, value));
}

// Filter application
export interface ApplyFilterOptions {
  immediate?: boolean;
}

export async function applyCurrentFilter(
  options: ApplyFilterOptions = {}
): Promise<void> {
  const store = useStore.getState();
  const worker = getMapWorker();

  // The worker needs the full state to recalculate expressions from scratch.
  worker.postMessage({
    type: 'update-state',
    state: {
      pois: store.pois,
      sliders: store.sliders,
      dAnchorCache: store.dAnchorCache,
      poiModes: store.poiModes,
      mode: store.mode
    }
  });
}

export async function restorePersistedFilters(): Promise<void> {
  const store = useStore.getState();
  
  // Load catalog
  await ensureCatalogLoaded();
  
  // Load dAnchor data for all POIs
  for (const poi of store.pois) {
    if (poi.type === 'custom') continue;
    
    const modes: Mode[] = ['drive', 'walk'];
    for (const mode of modes) {
      const hasData = store.dAnchorCache[poi.id]?.[mode];
      if (!hasData) {
        await loadDAnchor(poi.id, mode);
      }
    }
  }
  
  // Apply filters
  await applyCurrentFilter();
}

// Helper function to load dAnchor data
async function loadDAnchor(id: string, mode: Mode): Promise<void> {
  // Ensure catalog is loaded before fetching dAnchor data
  await ensureCatalogLoaded();
  const store = useStore.getState();
  const catalog = store.catalog;

  if (!catalog) {
    console.warn("[loadDAnchor] Catalog unavailable – skipping dAnchor fetch for", id);
    return;
  }

  try {
    const data = await fetchDAnchor(id, mode, catalog);
    store.setDAnchorCache(id, mode, data);
    if (DEBUG) {
      const anchorCount = Object.keys(data).length;
      console.log('✅ [loadDAnchor] Loaded', anchorCount, 'anchors for', id, mode);
    }
  } catch (error) {
    console.error(`❌ [loadDAnchor] Failed to load dAnchor for ${id} (${mode}):`, error);
  }
}

// Cache key for custom locations
export function customCacheKey(lon: number, lat: number): string {
  const format = (value: number) => value.toFixed(6);
  return `custom_${format(lat)}_${format(lon)}`;
}

function buildClimateFilterExpression(labels: string[]): any | null {
  if (!labels.length) return null;
  if (labels.length === 1) {
    return ['==', ['get', 'climate_label'], labels[0]];
  }
  return ['match', ['get', 'climate_label'], labels, true, false];
}

function sequenceEquals(a: readonly string[], b: readonly string[]): boolean {
  if (a === b) return true;
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i += 1) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}
