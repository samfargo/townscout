// Global application state using Zustand
'use client';
import { create } from 'zustand';
import type { Catalog } from '@/lib/services';

export type Mode = 'drive' | 'walk';

export interface POI {
  id: string;
  label: string;
  type: 'brand' | 'category' | 'custom';
  lat?: number;
  lon?: number;
  brandIds?: string[];
  formattedAddress?: string | null;
}

export type HexHoverState = {
  kind: 'hex';
  properties: Record<string, any>;
};

export type PinHoverState = {
  kind: 'pin';
  poiId: string;
  name: string;
  address: string;
  coordinates: [number, number];
  brandId?: string;
};

export type HoverState = HexHoverState | PinHoverState;

export interface StoreState {
  // Map state
  hover: HoverState | null;
  setHover: (props: HoverState | null) => void;

  // POI filters
  pois: POI[];
  setPois: (pois: POI[]) => void;
  addPoi: (poi: POI) => void;
  removePoi: (id: string) => void;
  showPins: Record<string, boolean>;
  setShowPins: (id: string, value: boolean) => void;
  removeShowPins: (id: string) => void;

  // Climate selections
  climateSelections: string[];
  setClimateSelections: (labels: string[]) => void;

  // Power corridor avoidance
  avoidPowerLines: boolean;
  setAvoidPowerLines: (value: boolean) => void;

  // Travel mode
  mode: Mode;
  setMode: (mode: Mode) => void;
  poiModes: Record<string, Mode>;
  setPoiMode: (id: string, mode: Mode) => void;

  // Slider values (minutes)
  sliders: Record<string, number>;
  setSlider: (id: string, value: number) => void;
  removeSlider: (id: string) => void;

  // dAnchor cache: poi_id -> mode -> anchor_id -> distance_seconds
  dAnchorCache: Record<string, Record<Mode, Record<string, number>>>;
  setDAnchorCache: (id: string, mode: Mode, data: Record<string, number>) => void;

  // Loading state for POIs
  loadingPois: Set<string>;
  setPoiLoading: (id: string, loading: boolean) => void;

  // Catalog
  catalog: Catalog | null;
  setCatalog: (catalog: Catalog) => void;
}

export const useStore = create<StoreState>((set) => ({
  // Map state
  hover: null,
  setHover: (props) => set({ hover: props }),

  // POI filters
  pois: [],
  setPois: (pois) => set({ pois }),
  addPoi: (poi) => set((state) => ({ pois: [...state.pois, poi] })),
  removePoi: (id) => set((state) => ({
    pois: state.pois.filter((p) => p.id !== id)
  })),
  showPins: {},
  setShowPins: (id, value) =>
    set((state) => {
      if (value) {
        return { showPins: { ...state.showPins, [id]: true } };
      }
      if (!(id in state.showPins)) {
        return {};
      }
      const next = { ...state.showPins };
      delete next[id];
      return { showPins: next };
    }),
  removeShowPins: (id) =>
    set((state) => {
      if (!(id in state.showPins)) return {};
      const next = { ...state.showPins };
      delete next[id];
      return { showPins: next };
    }),

  // Climate selections
  climateSelections: [],
  setClimateSelections: (labels) =>
    set(() => ({
      climateSelections: Array.from(new Set(labels))
    })),

  // Power corridor avoidance
  avoidPowerLines: false,
  setAvoidPowerLines: (value) =>
    set(() => ({
      avoidPowerLines: Boolean(value)
    })),

  // Travel mode
  mode: 'drive',
  setMode: (mode) => set({ mode }),
  poiModes: {},
  setPoiMode: (id, mode) => set((state) => ({
    poiModes: { ...state.poiModes, [id]: mode }
  })),

  // Slider values
  sliders: {},
  setSlider: (id, value) => set((state) => ({
    sliders: { ...state.sliders, [id]: value }
  })),
  removeSlider: (id) => set((state) => {
    const { [id]: _, ...rest } = state.sliders;
    return { sliders: rest };
  }),

  // dAnchor cache
  dAnchorCache: {},
  setDAnchorCache: (id, mode, data) => set((state) => ({
    dAnchorCache: {
      ...state.dAnchorCache,
      [id]: {
        ...state.dAnchorCache[id],
        [mode]: data
      }
    }
  })),

  // Loading state
  loadingPois: new Set(),
  setPoiLoading: (id, loading) => set((state) => {
    const next = new Set(state.loadingPois);
    if (loading) {
      next.add(id);
    } else {
      next.delete(id);
    }
    return { loadingPois: next };
  }),

  // Catalog
  catalog: null,
  setCatalog: (catalog) => set({ catalog })
}));
