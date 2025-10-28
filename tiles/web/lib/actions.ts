// Client-side actions for managing POIs, filters, and map interactions
'use client';
import { useStore, type Mode } from './state/store';
import type { Catalog } from './services';
import { fetchCatalog, fetchDAnchor, fetchCustomDAnchor } from './services';
import { getMapController, getMapWorker, onMapControllerReady } from './map/MapController';

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
  
  store.addPoi({ id: brandId, label, type: 'brand', brandIds: [brandId] });
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
  
  store.addPoi({ id: categoryId, label, type: 'category', brandIds: ids.map((value) => String(value)) });
  store.setSlider(categoryId, 30);
  
  await loadDAnchor(categoryId, store.mode);
  await applyCurrentFilter();
}

export async function addCustom(
  lon: number,
  lat: number,
  label: string,
  minutes: number,
  formattedAddress?: string | null
): Promise<void> {
  const store = useStore.getState();
  const id = customCacheKey(lon, lat);

  if (store.pois.some((p) => p.id === id)) {
    return;
  }

  store.addPoi({ id, label, type: 'custom', lat, lon, formattedAddress: formattedAddress ?? null });
  store.setSlider(id, minutes);
  
  // Fetch dAnchor data for custom location
  await loadDAnchorCustom(id, lon, lat, store.mode);
  await applyCurrentFilter();
}

export function removePOI(id: string): void {
  const store = useStore.getState();
  store.removeShowPins(id);
  const controller = getMapController();
  if (controller) {
    controller.hidePinsById(id);
  } else {
    onMapControllerReady(() => {
      getMapController()?.hidePinsById(id);
    });
  }
  store.removePoi(id);
  store.removeSlider(id);
  void applyCurrentFilter();
}

export function setPoiPins(id: string, show: boolean): void {
  const store = useStore.getState();
  const poi = store.pois.find((item) => item.id === id);
  if (!poi) {
    return;
  }

  store.setShowPins(id, show);

  const applyToController = (controller: ReturnType<typeof getMapController>) => {
    if (!controller) return;
    if (show) {
      void controller.showPinsForPoi(poi);
    } else {
      controller.hidePinsForPoi(poi);
    }
  };

  const controller = getMapController();
  if (controller) {
    applyToController(controller);
    return;
  }

  onMapControllerReady(() => {
    const latestPoi = useStore.getState().pois.find((item) => item.id === id);
    const currentController = getMapController();
    if (!latestPoi || !currentController) return;
    if (useStore.getState().showPins[id]) {
      void currentController.showPinsForPoi(latestPoi);
    } else {
      currentController.hidePinsForPoi(latestPoi);
    }
  });
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

// Power corridor avoidance
export function setAvoidPowerLines(enabled: boolean): void {
  const store = useStore.getState();
  if (store.avoidPowerLines === Boolean(enabled)) {
    return;
  }
  store.setAvoidPowerLines(Boolean(enabled));
  void applyCurrentFilter();
}

export function toggleAvoidPowerLines(): void {
  const store = useStore.getState();
  setAvoidPowerLines(!store.avoidPowerLines);
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
  const tempValues = { ...pendingSliderValues };
  pendingSliderValues = {};
  
  if (DEBUG) console.log('[actions] Sending preview to worker:', tempValues);
  const worker = getMapWorker();
  // Send preview values to worker - it will rebuild expressions with temp values
  worker.postMessage({
    type: 'update-preview',
    tempValues
  });
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
    const poi = store.pois.find((p) => p.id === id);
    if (poi?.type === 'custom' && poi.lon != null && poi.lat != null) {
      await loadDAnchorCustom(id, poi.lon, poi.lat, mode);
    } else {
      await loadDAnchor(id, mode);
    }
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
  
  if (DEBUG) console.log('[actions] Sending state to worker, pois:', store.pois.length, 'climate:', store.climateSelections);
  // Send lightweight state update to worker
  worker.postMessage({
    type: 'update-state',
    state: {
      pois: store.pois,
      sliders: store.sliders,
      poiModes: store.poiModes,
      mode: store.mode,
      climateSelections: store.climateSelections,
      avoidPowerLines: store.avoidPowerLines
    }
  });
}


// Helper function to send dAnchor data to worker
function syncDAnchorToWorker(id: string, mode: Mode, data: any): void {
  const worker = getMapWorker();
  worker.postMessage({
    type: 'update-dAnchor',
    id,
    mode,
    data
  });
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
    // Send dAnchor data to worker separately
    syncDAnchorToWorker(id, mode, data);
    if (DEBUG) {
      const anchorCount = Object.keys(data).length;
      console.log('✅ [loadDAnchor] Loaded', anchorCount, 'anchors for', id, mode);
    }
  } catch (error) {
    console.error(`❌ [loadDAnchor] Failed to load dAnchor for ${id} (${mode}):`, error);
  }
}

// Helper function to load dAnchor data for custom locations
async function loadDAnchorCustom(id: string, lon: number, lat: number, mode: Mode): Promise<void> {
  const store = useStore.getState();
  
  // Set loading state
  store.setPoiLoading(id, true);

  try {
    const data = await fetchCustomDAnchor(lon, lat, mode);
    store.setDAnchorCache(id, mode, data);
    // Send dAnchor data to worker separately
    syncDAnchorToWorker(id, mode, data);
    if (DEBUG) {
      const anchorCount = Object.keys(data).length;
      console.log('✅ [loadDAnchorCustom] Loaded', anchorCount, 'anchors for custom location', id, mode);
    }
  } catch (error) {
    console.error(`❌ [loadDAnchorCustom] Failed to load dAnchor for custom ${id} (${mode}):`, error);
  } finally {
    // Clear loading state
    store.setPoiLoading(id, false);
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
