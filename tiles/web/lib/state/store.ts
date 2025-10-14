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
}

export interface StoreState {
  // Map state
  hover: Record<string, any> | null;
  setHover: (props: Record<string, any> | null) => void;

  // POI filters
  pois: POI[];
  setPois: (pois: POI[]) => void;
  addPoi: (poi: POI) => void;
  removePoi: (id: string) => void;

  // Climate selections
  climateSelections: string[];
  setClimateSelections: (labels: string[]) => void;

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

  // Climate selections
  climateSelections: [],
  setClimateSelections: (labels) =>
    set(() => ({
      climateSelections: Array.from(new Set(labels))
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
