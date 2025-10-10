// Controls the MapLibre map and manages drive/walk layer visibility and filters.
import type { LngLatBoundsLike, LngLatLike, Map as MLMap } from "maplibre-gl";
import { createBaseStyle, LAYER_IDS } from "./layers";

export type Mode = "drive" | "walk";

export type ModeFilterState = {
  filter: any | null;
  active: boolean;
};

const HEX_LAYERS = [LAYER_IDS.driveR8, LAYER_IDS.walkR8, LAYER_IDS.driveR7];
const MODE_LAYERS: Record<Mode, string[]> = {
  drive: [LAYER_IDS.driveR8, LAYER_IDS.driveR7],
  walk: [LAYER_IDS.walkR8]
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

    await new Promise<void>((resolve) => {
      map.on("load", () => resolve());
    });

    this.showFallback();
  }

  get instance() {
    return this.map;
  }

  setModeFilters(filters: Record<Mode, ModeFilterState>, fallbackMode: Mode) {
    this.fallbackMode = fallbackMode;
    if (!this.map) return;

    const anyActive = Object.values(filters).some((entry) => entry.active);
    if (!anyActive) {
      this.showFallback();
      return;
    }

    for (const mode of Object.keys(MODE_LAYERS) as Mode[]) {
      const state = filters[mode];
      if (!state || !state.active) {
        this.setVisibilityForMode(mode, "none");
        this.applyFilterForMode(mode, null);
        continue;
      }
      this.setVisibilityForMode(mode, "visible");
      this.applyFilterForMode(mode, state.filter ?? null);
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
    const handler = (event: any) => {
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

  private showFallback() {
    if (!this.map) return;
    for (const mode of Object.keys(MODE_LAYERS) as Mode[]) {
      if (mode === this.fallbackMode) {
        this.setVisibilityForMode(mode, "visible");
        this.applyFilterForMode(mode, null);
      } else {
        this.setVisibilityForMode(mode, "none");
        this.applyFilterForMode(mode, null);
      }
    }
  }

  private setVisibilityForMode(mode: Mode, visibility: "visible" | "none") {
    if (!this.map) return;
    for (const layerId of MODE_LAYERS[mode]) {
      if (this.map.getLayer(layerId)) {
        this.map.setLayoutProperty(layerId, "visibility", visibility);
      }
    }
  }

  private applyFilterForMode(mode: Mode, filter: any | null) {
    if (!this.map) return;
    for (const layerId of MODE_LAYERS[mode]) {
      if (this.map.getLayer(layerId)) {
        this.map.setFilter(layerId, filter as any);
      }
    }
  }

  destroy() {
    if (this.map) {
      this.map.remove();
      this.map = undefined;
    }
  }
}
