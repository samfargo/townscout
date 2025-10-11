// Controls the MapLibre map and manages drive/walk layer visibility and filters.
import type { LngLatBoundsLike, LngLatLike, Map as MLMap, StyleSpecification } from "maplibre-gl";

export type Mode = "drive" | "walk";

// ==================== LAYER DEFINITIONS ====================

const LAYER_IDS = {
  driveR8: 't_hex_r8_drive',
  driveR7: 't_hex_r7_drive',
  walkR8: 't_hex_r8_walk'
} as const;

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
  private isDragging = false;

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

    // NOW set up the worker to listen for expression updates
    this.worker = getMapWorker();
    
    // Coalesce worker messages using RAF
    let pendingExpressions: Record<Mode, ModeExpressionState> | null = null;
    let rafId: number | null = null;
    
    const applyPendingExpressions = () => {
      rafId = null;
      if (pendingExpressions) {
        this.setModeExpressions(pendingExpressions, this.fallbackMode);
        pendingExpressions = null;
      }
    };
    
    this.worker.onmessage = (e) => {
      if (e.data.type === 'expressions-updated') {
        // Coalesce updates to one per frame
        pendingExpressions = e.data.expressions;
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

  private expressionsEqual(
    a: ModeExpressionState | null | undefined,
    b: ModeExpressionState | null | undefined
  ): boolean {
    if (a === b) return true;
    if (!a || !b) return false;
    if (a.active !== b.active) return false;
    if (a.maxMinutes !== b.maxMinutes) return false;
    // Deep comparison of expression arrays (simple JSON stringify works for our use case)
    return JSON.stringify(a.expression) === JSON.stringify(b.expression);
  }

  private setModeExpressions(
    expressions: Record<Mode, ModeExpressionState>,
    fallbackMode: Mode
  ) {
    this.fallbackMode = fallbackMode;
    if (!this.map) return; // Safety check

    // Skip if expressions haven't changed
    if (this.lastExpressions) {
      const unchanged = (Object.keys(expressions) as Mode[]).every(mode =>
        this.expressionsEqual(expressions[mode], this.lastExpressions![mode])
      );
      if (unchanged) return;
    }
    
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

  onHover(callback: (props: Record<string, any> | null) => void) {
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
      
      const feature = this.map?.queryRenderedFeatures(event.point, {
        layers: HEX_LAYERS
      })?.[0];
      callback(feature?.properties ?? null);
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

  destroy() {
    if (this.map) {
      this.map.remove();
      this.map = undefined;
    }
    if (this.worker) {
      this.worker.terminate();
    }
  }
}
