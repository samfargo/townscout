// Controls the MapLibre map and manages drive/walk layer visibility and filters.
import { fetchPoiPoints, type FetchPoiPointsOptions } from "../services";
import type { HoverState, POI } from "../state/store";
import type { FeatureCollection, Point } from "geojson";
import type { LngLatBoundsLike, LngLatLike, Map as MLMap, StyleSpecification } from "maplibre-gl";

export type Mode = "drive" | "walk";

// ==================== LAYER DEFINITIONS ====================

const LAYER_IDS = {
  driveR8: 't_hex_r8_drive',
  driveR7: 't_hex_r7_drive',
  walkR8: 't_hex_r8_walk'
} as const;

type PinFeatureProperties = {
  brand_id?: string;
  name?: string;
  address?: string;
  approx_address?: string;
  poi_id?: string;
  source_type?: string;
};

type PinFeatureCollection = FeatureCollection<Point, PinFeatureProperties>;

type PinRecord = {
  key: string;
  poi: POI;
  sourceId: string;
  pointLayerId: string;
  brands: string[];
   categoryId?: string | null;
  requestId: number;
  cleanup: Array<() => void>;
  isCustom: boolean;
};

function looksLikeCoordinate(value: string): boolean {
  const trimmed = value.trim();
  if (!trimmed) return false;
  if (trimmed.includes('°')) return true;
  const coordRegex = /^[+-]?\d+(\.\d+)?\s*,\s*[+-]?\d+(\.\d+)?$/;
  return coordRegex.test(trimmed);
}

function sanitizeAddress(raw: unknown): string {
  if (typeof raw !== 'string') return '';
  const trimmed = raw.trim();
  if (!trimmed) return '';
  if (looksLikeCoordinate(trimmed)) return '';
  return trimmed;
}

function sanitizeLayerKey(value: string): string {
  return value.replace(/[^a-zA-Z0-9_-]/g, '_');
}

function createBaseStyle(): StyleSpecification {
  return {
    version: 8,
    sources: {
      osm: {
        type: 'raster',
        tiles: [
          'https://a.tile.openstreetmap.org/{z}/{x}/{y}.png',
          'https://b.tile.openstreetmap.org/{z}/{x}/{y}.png',
          'https://c.tile.openstreetmap.org/{z}/{x}/{y}.png'
        ],
        tileSize: 256,
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
      },
      't_hex_r8_drive': {
        type: 'vector',
        url: 'pmtiles:///tiles/t_hex_r8_drive.pmtiles'
      },
      't_hex_r7_drive': {
        type: 'vector',
        url: 'pmtiles:///tiles/t_hex_r7_drive.pmtiles'
      }
    },
    layers: [
      {
        id: 'background',
        type: 'raster',
        source: 'osm',
        minzoom: 0,
        maxzoom: 22
      },
      {
        id: LAYER_IDS.driveR8,
        type: 'fill',
        source: 't_hex_r8_drive',
        'source-layer': 't_hex_r8_drive',
        minzoom: 8,
        maxzoom: 22,
        layout: {
          visibility: 'visible'
        },
        paint: {
          'fill-color': '#10b981',
          'fill-opacity': 0.4
        }
      },
      {
        id: LAYER_IDS.driveR7,
        type: 'fill',
        source: 't_hex_r7_drive',
        'source-layer': 't_hex_r7_drive',
        minzoom: 0,
        maxzoom: 8,
        layout: {
          visibility: 'visible'
        },
        paint: {
          'fill-color': '#10b981',
          'fill-opacity': 0.4
        }
      }
    ]
  };
}

export function getColorForMinutes(minutes: number | null): string {
  if (minutes === null) return '#94a3b8'; // gray for unreachable
  if (minutes <= 10) return '#10b981'; // green
  if (minutes <= 20) return '#f59e0b'; // amber
  if (minutes <= 30) return '#ef4444'; // red
  return '#7c3aed'; // purple for >30 min
}

// ==================== WORKER REGISTRY ====================

let globalWorker: Worker | null = null;

export function getMapWorker(): Worker {
  if (!globalWorker) {
    globalWorker = new Worker(new URL('./map.worker.ts', import.meta.url), {
      type: 'module'
    });
  }
  return globalWorker;
}

// ==================== CONTROLLER REGISTRY ====================

let globalController: MapController | null = null;
let registrationCallbacks: Array<() => void> = [];

export function registerMapController(controller: MapController): void {
  globalController = controller;
  
  // Call any pending callbacks
  const callbacks = [...registrationCallbacks];
  registrationCallbacks = [];
  callbacks.forEach(cb => cb());
}

export function getMapController(): MapController | null {
  return globalController;
}

export function onMapControllerReady(callback: () => void): void {
  if (globalController) {
    callback();
  } else {
    registrationCallbacks.push(callback);
  }
}

// ==================== MAP CONTROLLER ====================

export type ModeExpressionState = {
  expression: any | null;
  maxMinutes: number;
  active: boolean;
};

const HEX_LAYERS = [LAYER_IDS.driveR8, LAYER_IDS.driveR7]; // Walk layers commented out - tiles not available
const MODE_LAYERS: Record<Mode, string[]> = {
  drive: [LAYER_IDS.driveR8, LAYER_IDS.driveR7],
  walk: [] // Walk tiles not available yet
};

let protocolRegistered = false;

// Limit map to the United States (covers Alaska, Hawaii, and territories).
const US_MAX_BOUNDS: LngLatBoundsLike = [
  [-171.791110603, 18.91619],
  [-66.96466, 71.3577635769]
];

export class MapController {
  private map?: MLMap;
  private fallbackMode: Mode = "drive";
  private worker: Worker;
  private lastExpressions: Record<Mode, ModeExpressionState> | null = null;
  private lastExpressionSignature: string | null = null;
  private isDragging = false;
  private pinHoverLayerIds: string[] = [];
  private pinRecords = new Map<string, PinRecord>();
  private moveEndHandler: (() => void) | null = null;

  async init(container: HTMLDivElement) {
    if (this.map) return;

    const [maplibreModule, pmtilesModule] = await Promise.all([
      import("maplibre-gl"),
      import("pmtiles")
    ]);
    
    const maplibre = (maplibreModule as any).default ?? maplibreModule;
    const pmtiles = (pmtilesModule as any).default ?? pmtilesModule;
    const MapCtor = (maplibre as typeof import("maplibre-gl")).Map;
    const NavigationControlCtor = (maplibre as typeof import("maplibre-gl")).NavigationControl;
    const { Protocol } = pmtiles as typeof import("pmtiles");

    if (!protocolRegistered) {
      const protocol = new Protocol();
      const registerProtocol =
        typeof (maplibre as any).addProtocol === "function"
          ? (maplibre as any).addProtocol
          : typeof (maplibre as any).default?.addProtocol === "function"
          ? (maplibre as any).default.addProtocol
          : null;
      if (registerProtocol) {
        registerProtocol("pmtiles", protocol.tile.bind(protocol));
        protocolRegistered = true;
      } else {
        console.warn("MapLibre addProtocol unavailable; PMTiles tiles will not load.");
      }
    }

    const map = new MapCtor({
      container,
      style: createBaseStyle(),
      center: [-98.58, 39.83],
      zoom: 4,
      minZoom: 3,
      renderWorldCopies: false,
      maxBounds: US_MAX_BOUNDS,
      attributionControl: { compact: false },
      hash: false
    });

    map.addControl(new NavigationControlCtor({ visualizePitch: true }), "top-right");

    this.map = map;
    
    // Wait for the style to load (this is much faster than waiting for all tiles)
    await new Promise<void>((resolve) => {
      if (map.isStyleLoaded()) {
        resolve();
      } else {
        map.once("style.load", () => resolve());
      }
    });

    console.log('[MapController] Map initialized and ready!');

    this.moveEndHandler = () => {
      void this.refreshPinsForActiveRecords();
    };
    map.on('moveend', this.moveEndHandler);

    // NOW set up the worker to listen for expression updates
    this.worker = getMapWorker();
    
    // Coalesce worker messages using RAF
    let pendingExpressions: Record<Mode, ModeExpressionState> | null = null;
    let pendingSignature: string | null = null;
    let pendingFallbackMode: Mode = this.fallbackMode;
    let rafId: number | null = null;
    
    const applyPendingExpressions = () => {
      rafId = null;
      if (!pendingExpressions) return;
      const expressions = pendingExpressions;
      const signature = pendingSignature;
      const fallbackMode = pendingFallbackMode;
      pendingExpressions = null;
      pendingSignature = null;
      pendingFallbackMode = this.fallbackMode;
      this.setModeExpressions(expressions, fallbackMode, signature);
    };
    
    this.worker.onmessage = (e) => {
      if (e.data.type === 'expressions-updated') {
        // Coalesce updates to one per frame
        pendingExpressions = e.data.expressions;
        pendingSignature = typeof e.data.signature === 'string' ? e.data.signature : null;
        pendingFallbackMode = e.data.fallbackMode ?? this.fallbackMode;
        if (rafId) {
          cancelAnimationFrame(rafId);
        }
        rafId = requestAnimationFrame(applyPendingExpressions);
      }
    };

    this.showFallback();
  }

  get instance() {
    return this.map;
  }

  private setModeExpressions(
    expressions: Record<Mode, ModeExpressionState>,
    fallbackMode: Mode,
    signature?: string | null
  ) {
    this.fallbackMode = fallbackMode;
    if (!this.map) return; // Safety check

    if (signature && signature === this.lastExpressionSignature) {
      this.lastExpressions = expressions;
      return;
    }
    
    this.lastExpressionSignature = signature ?? null;
    this.lastExpressions = expressions;

    const anyActive = Object.values(expressions).some((entry) => entry.active);

    if (!anyActive) {
      this.showFallback();
      return;
    }

    for (const mode of Object.keys(MODE_LAYERS) as Mode[]) {
      const state = expressions[mode];

      if (!state || !state.active) {
        this.setVisibilityForMode(mode, 'none');
        this.applyOpacityExpressionForMode(mode, null);
        continue;
      }

      this.setVisibilityForMode(mode, 'visible');
      this.applyOpacityExpressionForMode(
        mode,
        state.expression,
        state.maxMinutes
      );
    }
  }

  cameraTo(lon: number, lat: number, minZoom = 13) {
    if (!this.map) return;
    const zoom = Math.max(this.map.getZoom(), minZoom);
    this.map.easeTo({
      center: [lon, lat] as LngLatLike,
      zoom,
      duration: 800
    });
  }

  onHover(callback: (info: HoverState | null) => void) {
    if (!this.map) {
      return () => {};
    }
    
    // Throttle hover queries to avoid competing with slider updates
    let lastQueryTime = 0;
    const THROTTLE_MS = 50; // Query at most every 50ms
    
    const handler = (event: any) => {
      // Skip queries entirely while dragging sliders
      if (this.isDragging) return;
      
      const now = Date.now();
      if (now - lastQueryTime < THROTTLE_MS) return;
      lastQueryTime = now;
      
      const pinHover = this.findPinHover(event.point);
      if (pinHover) {
        callback(pinHover);
        return;
      }

      const feature = this.map?.queryRenderedFeatures(event.point, {
        layers: HEX_LAYERS
      })?.[0];
      if (feature && feature.properties) {
        callback({
          kind: 'hex',
          properties: feature.properties ?? {}
        });
      } else {
        callback(null);
      }
    };
    this.map.on("mousemove", handler);
    return () => {
      this.map?.off("mousemove", handler);
    };
  }

  setDragging(dragging: boolean) {
    this.isDragging = dragging;
  }

  private showFallback() {
    if (!this.map) return;
    for (const mode of Object.keys(MODE_LAYERS) as Mode[]) {
      if (mode === this.fallbackMode) {
        this.setVisibilityForMode(mode, 'visible');
        this.applyOpacityExpressionForMode(mode, null);
      } else {
        this.setVisibilityForMode(mode, 'none');
        this.applyOpacityExpressionForMode(mode, null);
      }
    }
  }

  private setVisibilityForMode(mode: Mode, visibility: 'visible' | 'none') {
    if (!this.map) return;
    for (const layerId of MODE_LAYERS[mode]) {
      if (this.map.getLayer(layerId)) {
        this.map.setLayoutProperty(layerId, 'visibility', visibility);
      }
    }
  }

  private applyOpacityExpressionForMode(
    mode: Mode,
    expression: any | null,
    maxMinutes?: number
  ) {
    if (!this.map) return;

    let opacity: any = 0.4; // Default opacity

    if (expression) {
      // Expression is now a boolean (true if within threshold of at least one POI)
      // Use case to convert boolean to opacity value
      opacity = [
        'case',
        expression,
        0.4, // Opacity when true (within threshold)
        0 // Opacity when false (outside all thresholds)
      ];
    }

    for (const layerId of MODE_LAYERS[mode]) {
      if (this.map.getLayer(layerId)) {
        this.map.setPaintProperty(layerId, 'fill-opacity', opacity);
      }
    }
  }

  private setFallbackVisible(_visible: boolean) {
    // Fallback overlay removed; keep method for backwards compatibility.
  }

  async showPinsForPoi(poi: POI): Promise<void> {
    if (!this.map) return;
    const key = this.getPinKey(poi);
    const sanitized = sanitizeLayerKey(key);
    let record = this.pinRecords.get(key);
    if (!record) {
      record = {
        key,
        poi,
        sourceId: `pins-${sanitized}-source`,
        pointLayerId: `pins-${sanitized}-points`,
        brands: [],
        categoryId: poi.type === 'category' ? poi.id : null,
        requestId: 0,
        cleanup: [],
        isCustom: poi.type === 'custom'
      };
      this.pinRecords.set(key, record);
    } else {
      record.poi = poi;
      record.isCustom = poi.type === 'custom';
      record.categoryId = poi.type === 'category' ? poi.id : null;
    }

    if (poi.type === 'custom') {
      this.renderCustomPin(record);
      return;
    }

    if (poi.type === 'category') {
      record.brands = this.getBrandsForPoi(poi);
      await this.refreshPinsForRecord(record);
      return;
    }

    const brands = this.getBrandsForPoi(poi);
    if (!brands.length) {
      this.hidePinsByKey(key);
      return;
    }

    record.brands = brands;
    record.categoryId = null;
    await this.refreshPinsForRecord(record);
  }

  hidePinsForPoi(poi: POI): void {
    this.hidePinsByKey(this.getPinKey(poi));
  }

  hidePinsById(poiId: string): void {
    for (const [key, record] of this.pinRecords.entries()) {
      if (record.poi.id === poiId) {
        this.hidePinsByKey(key);
      }
    }
  }

  private getPinKey(poi: POI): string {
    return `${poi.type}:${poi.id}`;
  }

  private getBrandsForPoi(poi: POI): string[] {
    if (poi.type === 'brand') {
      return [String(poi.id)];
    }
    if (poi.type === 'category') {
      return (poi.brandIds ?? []).map((value) => String(value));
    }
    return [];
  }

  private hidePinsByKey(key: string): void {
    const record = this.pinRecords.get(key);
    if (!record) return;
    this.removePinLayers(record);
    this.pinRecords.delete(key);
    this.updatePinHoverLayers();
  }

  private async refreshPinsForActiveRecords(): Promise<void> {
    const tasks = Array.from(this.pinRecords.values()).filter((record) => {
      if (record.isCustom) return false;
      if (record.poi.type === "category") return true;
      return record.brands.length > 0;
    });
    await Promise.all(tasks.map((record) => this.refreshPinsForRecord(record)));
  }

  private async refreshPinsForRecord(record: PinRecord): Promise<void> {
    if (!this.map) return;
    const bounds = this.getCurrentBounds();
    const requestId = record.requestId + 1;
    record.requestId = requestId;
    const key = record.key;

    const options: FetchPoiPointsOptions = {};

    if (bounds) {
      options.bounds = bounds;
    }

    if (record.poi.type === 'brand') {
      if (!record.brands.length) {
        this.hidePinsByKey(key);
        return;
      }
      options.brands = record.brands;
    } else if (record.poi.type === 'category') {
      const categoryId = record.categoryId ?? record.poi.id;
      if (!categoryId) {
        this.hidePinsByKey(key);
        return;
      }
      options.categoryId = String(categoryId);
    }

    if (!options.brands && !options.categoryId) {
      this.hidePinsByKey(key);
      return;
    }

    try {
      const geojson = await fetchPoiPoints(options);
      if (record.requestId !== requestId) {
        return;
      }
      const decorated: PinFeatureCollection = {
        type: 'FeatureCollection',
        features: geojson.features.map((feature) => this.decoratePinFeature(feature, record.poi))
      };
      this.removePinLayers(record);
      this.map.addSource(record.sourceId, {
        type: 'geojson',
        data: decorated
      });
      this.map.addLayer({
        id: record.pointLayerId,
        type: 'circle',
        source: record.sourceId,
        paint: {
          'circle-radius': ['interpolate', ['linear'], ['zoom'], 5, 3, 10, 4.5, 14, 6],
          'circle-color': '#7c3aed',
          'circle-opacity': 0.85,
          'circle-stroke-color': '#ffffff',
          'circle-stroke-width': 1
        }
      });
      this.attachPinLayerEvents(record, record.pointLayerId);

      this.updatePinHoverLayers();
    } catch (error) {
      console.error(`[MapController] Failed to load pins for ${record.poi.label}`, error);
    }
  }

  private renderCustomPin(record: PinRecord): void {
    if (!this.map) return;
    const { poi } = record;
    record.brands = [];
    if (typeof poi.lon !== 'number' || typeof poi.lat !== 'number') {
      console.warn('[MapController] Custom POI missing coordinates, skipping pin render.');
      return;
    }
    const sanitizedAddress = sanitizeAddress(poi.formattedAddress);
    const feature: PinFeatureCollection = {
      type: 'FeatureCollection',
      features: [
        {
          type: 'Feature',
          geometry: {
            type: 'Point',
            coordinates: [poi.lon, poi.lat]
          },
          properties: {
            name: poi.label,
            poi_id: poi.id,
            source_type: poi.type
          }
        }
      ]
    };
    if (sanitizedAddress) {
      (feature.features[0].properties as PinFeatureProperties).address = sanitizedAddress;
    }
    this.removePinLayers(record);
    this.map.addSource(record.sourceId, {
      type: 'geojson',
      data: feature
    });
    this.map.addLayer({
      id: record.pointLayerId,
      type: 'circle',
      source: record.sourceId,
      paint: {
        'circle-radius': 6,
        'circle-color': '#ef4444',
        'circle-stroke-color': '#ffffff',
        'circle-stroke-width': 1.2
      }
    });
    this.attachPinLayerEvents(record, record.pointLayerId);
    this.updatePinHoverLayers();
  }

  private decoratePinFeature(feature: any, poi: POI) {
    const clone: any = {
      ...feature,
      properties: {
        ...(feature.properties ?? {})
      }
    };
    clone.properties.poi_id = poi.id;
    clone.properties.source_type = poi.type;
    if (!clone.properties.name || !String(clone.properties.name).trim()) {
      clone.properties.name = poi.label;
    }
    if (feature.geometry?.type === 'Point') {
      const currentAddress = sanitizeAddress(clone.properties.address);
      const approx = sanitizeAddress(clone.properties.approx_address);
      if (currentAddress) {
        clone.properties.address = currentAddress;
      } else if ('address' in clone.properties) {
        delete clone.properties.address;
      }
      if (approx) {
        clone.properties.approx_address = approx;
        if (!currentAddress) {
          clone.properties.address = approx;
        }
      } else if ('approx_address' in clone.properties) {
        delete clone.properties.approx_address;
      }
    }
    return clone;
  }

  private removePinLayers(record: PinRecord): void {
    if (!this.map) return;
    record.cleanup.forEach((dispose) => dispose());
    record.cleanup = [];

    if (this.map.getLayer(record.pointLayerId)) {
      this.map.removeLayer(record.pointLayerId);
    }
    if (this.map.getSource(record.sourceId)) {
      this.map.removeSource(record.sourceId);
    }
  }

  private attachPinLayerEvents(record: PinRecord, layerId?: string) {
    if (!this.map || !layerId) return;
    const enter = () => {
      if (this.map) {
        this.map.getCanvas().style.cursor = 'pointer';
      }
    };
    const leave = () => {
      if (this.map) {
        this.map.getCanvas().style.cursor = '';
      }
    };
    this.map.on('mouseenter', layerId, enter);
    this.map.on('mouseleave', layerId, leave);
    record.cleanup.push(() => {
      this.map?.off('mouseenter', layerId, enter);
      this.map?.off('mouseleave', layerId, leave);
    });
  }

  private updatePinHoverLayers() {
    if (!this.map) {
      this.pinHoverLayerIds = [];
      return;
    }
    const ids: string[] = [];
    for (const record of this.pinRecords.values()) {
      if (this.map.getLayer(record.pointLayerId)) {
        ids.push(record.pointLayerId);
      }
    }
    this.pinHoverLayerIds = ids;
  }

  private findPinHover(point: { x: number; y: number }): HoverState | null {
    if (!this.map || !this.pinHoverLayerIds.length) return null;
    const features = this.map.queryRenderedFeatures([point.x, point.y], { layers: this.pinHoverLayerIds });
    if (!features || !features.length) return null;
    return this.buildPinHoverState(features[0]);
  }

  private buildPinHoverState(feature: any): HoverState | null {
    if (!feature?.geometry || feature.geometry.type !== 'Point') return null;
    const coordinates = feature.geometry.coordinates as [number, number];
    const props = feature.properties ?? {};
    const poiId = typeof props.poi_id === 'string' ? props.poi_id : '';
    const name =
      typeof props.name === 'string' && props.name.trim().length
        ? props.name
        : typeof props.brand_id === 'string'
        ? props.brand_id
        : 'Point of interest';
    const address = this.resolvePinAddress(props, coordinates);
    return {
      kind: 'pin',
      poiId,
      name,
      address,
      coordinates,
      brandId: typeof props.brand_id === 'string' ? props.brand_id : undefined
    };
  }

  private resolvePinAddress(props: Record<string, unknown>, _coordinates: [number, number]): string {
    const candidates = ['address', 'approx_address'];
    for (const key of candidates) {
      const value = props[key];
      const cleaned = sanitizeAddress(typeof value === 'string' ? value : undefined);
      if (cleaned) {
        return cleaned;
      }
    }
    return '';
  }

  private getCurrentBounds():
    | { west: number; south: number; east: number; north: number }
    | null {
    if (!this.map) return null;
    try {
      const bounds = this.map.getBounds();
      return {
        west: bounds.getWest(),
        south: bounds.getSouth(),
        east: bounds.getEast(),
        north: bounds.getNorth()
      };
    } catch {
      return null;
    }
  }

  private clearPins() {
    for (const record of this.pinRecords.values()) {
      this.removePinLayers(record);
    }
    this.pinRecords.clear();
    this.updatePinHoverLayers();
  }

  destroy() {
    if (this.map) {
      if (this.moveEndHandler) {
        this.map.off('moveend', this.moveEndHandler);
        this.moveEndHandler = null;
      }
      this.clearPins();
      this.map.remove();
      this.map = undefined;
    }
    if (this.worker) {
      this.worker.terminate();
    }
    this.lastExpressions = null;
    this.lastExpressionSignature = null;
  }
}
