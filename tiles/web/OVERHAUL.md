Please convert the frontend @index.html into the below:

The stack
	•	Next.js (App Router) + TypeScript – file-based routing, SSR/SSG where it helps, but we’ll keep the map strictly client-side.
	•	Radix UI + shadcn/ui + Tailwind – accessible primitives + decent design out of the box.
	•	Zustand – tiny global state without ceremony.
	•	TanStack Query – fetch/caching/invalidations; stop hand-managing fetch and AbortController.
	•	MapLibre GL JS (imperative) – keep it, but isolate it.
	•	deck.gl (optional) – only if you need high-volume pins/heatmaps.
	•	zod (optional) – validate API responses so bugs don’t bubble into UI.

Project layout

app/
  layout.tsx
  page.tsx
  providers.tsx               // QueryClientProvider, Theme, etc.
  (map)/MapCanvas.tsx         // client component for the map
  (sidebar)/Sidebar.tsx
  (sidebar)/FiltersPanel.tsx
  (sidebar)/SearchBox.tsx
  (sidebar)/HoverBox.tsx
  (shared)/ShareButton.tsx
lib/
  map/MapController.ts         // wraps maplibre (imperative)
  map/layers.ts                // style & layer ids
  map/expressions.ts           // pure expression builders
  state/store.ts               // zustand
  state/selectors.ts
  actions/index.ts             // thunk-like: uses store + services + map
  services/api.ts              // getJSON, typed endpoints
  services/catalog.ts
  services/dAnchor.ts
  services/places.ts
  utils/debounce.ts
  utils/number.ts
styles/
  globals.css
  tailwind.css

Key patterns (copy these)

1) Map is isolated behind a controller

Keep React out of mousemove/render loops. Drive the map imperatively.

// lib/map/MapController.ts
import type { Map as MLMap, LngLatLike } from 'maplibre-gl';

export type Mode = 'drive' | 'walk';

export class MapController {
  private map?: MLMap;

  async init(container: HTMLDivElement) {
    const maplibre = await import('maplibre-gl');      // client-only
    const { Map } = maplibre;
    this.map = new Map({
      container,
      style: { /* your style JSON with sources/layers */ } as any,
      center: [-98.58, 39.83],
      zoom: 4
    });
    return new Promise<void>((resolve) => this.map!.on('load', () => resolve()));
  }

  setMode(mode: Mode) {
    if (!this.map) return;
    const vis = (id: string, v: 'visible'|'none') =>
      this.map!.getLayer(id) && this.map!.setLayoutProperty(id, 'visibility', v);
    vis('layer_r7_drive', 'none');
    vis('layer_r8_drive', mode === 'drive' ? 'visible' : 'none');
    vis('layer_r8_walk',  mode === 'walk'  ? 'visible' : 'none');
  }

  setFilter(mode: Mode, filter: any | null) {
    if (!this.map) return;
    if (mode === 'drive') {
      this.map.setFilter('layer_r8_drive', filter);
      this.map.setFilter('layer_r7_drive', filter);
      this.map.setFilter('layer_r8_walk', null);
    } else {
      this.map.setFilter('layer_r8_walk', filter);
      this.map.setFilter('layer_r8_drive', null);
      this.map.setFilter('layer_r7_drive', null);
    }
  }

  cameraTo(lon: number, lat: number, minZoom = 13) {
    if (!this.map) return;
    const z = Math.max(this.map.getZoom(), minZoom);
    this.map.easeTo({ center: [lon, lat] as LngLatLike, zoom: z, duration: 800 });
  }

  onHover(cb: (props: Record<string, any> | null) => void) {
    if (!this.map) return;
    const layers = ['layer_r8_drive', 'layer_r8_walk', 'layer_r7_drive'];
    this.map.on('mousemove', (e) => {
      const f = this.map!.queryRenderedFeatures(e.point, { layers })?.[0];
      cb(f ? (f.properties ?? null) : null);
    });
  }
}

2) A thin React wrapper for the map

Ensure this is a client component and only imports maplibre dynamically.

// app/(map)/MapCanvas.tsx
'use client';
import { useEffect, useRef, useState } from 'react';
import { MapController } from '@/lib/map/MapController';
import { useStore } from '@/lib/state/store';
import { buildCombinedFilter } from '@/lib/map/expressions';

export default function MapCanvas() {
  const divRef = useRef<HTMLDivElement>(null);
  const [ctrl] = useState(() => new MapController());

  const mode    = useStore(s => s.mode);
  const pois    = useStore(s => s.pois);
  const sliders = useStore(s => s.sliders);
  const cache   = useStore(s => s.dAnchorCache);

  useEffect(() => {
    if (!divRef.current) return;
    ctrl.init(divRef.current).then(() => {
      ctrl.setMode(mode);
      // subscribe to hover if you want
    });
    // no cleanup demo; rely on page lifetime
  }, []);

  // recompute and apply filter when relevant state changes
  useEffect(() => {
    const filter = buildCombinedFilter(pois, sliders, cache);
    ctrl.setFilter(mode, filter);
  }, [mode, pois, sliders, cache]);

  return <div ref={divRef} className="h-full w-full" />;
}

3) Global state that’s actually small

Zustand keeps it simple; actions orchestrate services + map controller when needed.

// lib/state/store.ts
import { create } from 'zustand';

export type Mode = 'drive' | 'walk';

export type POI =
  | { type: 'category'; id: string; ids: string[]; label: string }
  | { type: 'brand';    id: string; label: string }
  | { type: 'custom';   id: string; label: string; lon: number; lat: number };

type CacheMap = Record<string, Record<string, number>>;

type State = {
  mode: Mode;
  pois: POI[];
  sliders: Record<string, number>;
  dAnchorCache: CacheMap;
  customCoverageMinutes: Record<string, number>;
  setMode: (m: Mode) => void;
  upsertPOI: (p: POI, defMin?: number) => void;
  setSlider: (id: string, min: number) => void;
  mergeCache: (key: string, data: Record<string, number>) => void;
  setCustomCoverage: (key: string, minutes: number) => void;
  removePOI: (id: string) => void;
};

export const useStore = create<State>((set, get) => ({
  mode: 'drive',
  pois: [],
  sliders: {},
  dAnchorCache: {},
  customCoverageMinutes: {},
  setMode: (mode) => set({ mode }),
  upsertPOI: (p, def = 30) => set(s => ({
    pois: s.pois.some(x => x.id === p.id) ? s.pois.map(x => x.id === p.id ? { ...x, ...p } : x) : [...s.pois, p],
    sliders: { ...s.sliders, [p.id]: s.sliders[p.id] ?? def }
  })),
  setSlider: (id, min) => set(s => ({ sliders: { ...s.sliders, [id]: min } })),
  mergeCache: (key, data) => set(s => ({ dAnchorCache: { ...s.dAnchorCache, [key]: data } })),
  setCustomCoverage: (k, m) => set(s => ({ customCoverageMinutes: { ...s.customCoverageMinutes, [k]: m } })),
  removePOI: (id) => set(s => ({
    pois: s.pois.filter(p => p.id !== id),
    sliders: Object.fromEntries(Object.entries(s.sliders).filter(([k]) => k !== id))
  })),
}));

4) Pure expression builder (unit-test this)

Port your current expression logic here.

// lib/map/expressions.ts
import type { POI } from '@/lib/state/store';

export function minExprForKey(cacheKey: string, maxK = 20) {
  const UNREACH = 65535;
  const literalMap = ['literal', ['var', cacheKey]]; // we’ll inline actual map at call-site
  const terms = [];
  for (let i = 0; i < maxK; i++) {
    const aSec   = ['coalesce', ['get', `a${i}_s`], UNREACH];
    const aidStr = ['to-string', ['get', `a${i}_id`]];
    const bSec   = ['coalesce', ['get', aidStr, literalMap], UNREACH];
    terms.push(['+', aSec, bSec]);
  }
  // NOTE: we replace ['var', cacheKey] with ['literal', cache[cacheKey]] before sending
  return ['min', ...terms];
}

export function buildCombinedFilter(
  pois: POI[],
  sliders: Record<string, number>,
  cache: Record<string, Record<string, number>>
) {
  const clauses: any[] = ['all'];
  for (const p of pois) {
    const key = p.id;
    const m = cache[key];
    if (!m || !Object.keys(m).length) continue;
    const maxSec = (sliders[key] ?? 30) * 60;
    // inline cache literal so maplibre understands it
    const expr = JSON.parse(JSON.stringify(minExprForKey(key))) as any;
    inlineLiteral(expr, key, m);
    clauses.push(['<=', expr, maxSec]);
  }
  return clauses.length === 1 ? null : clauses;
}

function inlineLiteral(node: any, key: string, map: Record<string, number>) {
  if (Array.isArray(node)) {
    for (let i = 0; i < node.length; i++) {
      const v = node[i];
      if (Array.isArray(v) && v[0] === 'literal' && v[1]?.[0] === 'var' && v[1][1] === key) {
        node[i] = ['literal', map];
      } else {
        inlineLiteral(v, key, map);
      }
    }
  }
}

5) Actions that fetch + update cache via TanStack Query

You stop thinking about retries/dedupe.

// lib/services/api.ts
export async function getJSON<T>(url: string): Promise<T> {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`${r.status} ${await r.text()}`);
  return r.json() as Promise<T>;
}

// lib/services/dAnchor.ts
import { getJSON } from './api';

export const fetchCategory = (id: string, mode: 'drive'|'walk') =>
  getJSON<Record<string, number>>(`/api/d_anchor?category=${encodeURIComponent(id)}&mode=${mode}`);

export const fetchBrand = (id: string, mode: 'drive'|'walk') =>
  getJSON<Record<string, number>>(`/api/d_anchor_brand?brand=${encodeURIComponent(id)}&mode=${mode}`);

export const fetchCustom = (lon: number, lat: number, mode: 'drive'|'walk', cutoff: number) =>
  getJSON<Record<string, number>>(`/api/d_anchor_custom?lon=${lon}&lat=${lat}&mode=${mode}&cutoff=${cutoff}&overflow_cutoff=${cutoff}`);

// lib/actions/index.ts
import { useStore } from '@/lib/state/store';
import { fetchBrand, fetchCategory, fetchCustom } from '@/lib/services/dAnchor';

export async function addCategory(id: string, label: string, ids: string[]) {
  const { mode, upsertPOI, mergeCache } = useStore.getState();
  upsertPOI({ type: 'category', id, ids, label });
  const data = await fetchCategory(id, mode);
  mergeCache(id, data);
}

export async function addBrand(id: string, label: string) {
  const { mode, upsertPOI, mergeCache } = useStore.getState();
  upsertPOI({ type: 'brand', id, label });
  const data = await fetchBrand(id, mode);
  mergeCache(id, data);
}

export async function addCustom(lon: number, lat: number, label: string, minutes = 30) {
  const key = `custom@${lon.toFixed(5)},${lat.toFixed(5)}`;
  const { mode, upsertPOI, mergeCache, setCustomCoverage } = useStore.getState();
  upsertPOI({ type: 'custom', id: key, label, lon, lat }, minutes);
  const cutoff = normalizeMinutes(minutes);
  const data = await fetchCustom(lon, lat, mode, cutoff);
  mergeCache(key, data);
  setCustomCoverage(key, cutoff);
}

export function normalizeMinutes(value: number, step=5, min=5, max=240) {
  const clamped = Math.max(min, Math.min(max, value|0));
  return Math.ceil(clamped / step) * step;
}

UI wiring (example)

Use shadcn/ui components for inputs; components dispatch actions and write to store. The map listens via buildCombinedFilter.

// app/(sidebar)/FiltersPanel.tsx
'use client';
import { useStore } from '@/lib/state/store';

export default function FiltersPanel() {
  const pois = useStore(s => s.pois);
  const sliders = useStore(s => s.sliders);
  const setSlider = useStore(s => s.setSlider);
  const removePOI = useStore(s => s.removePOI);

  if (!pois.length) {
    return <div className="rounded-lg border bg-blue-50 p-4 text-blue-700">No filters yet. Add a place type or drop a custom pin.</div>;
  }

  return (
    <div className="space-y-3">
      {pois.map(p => (
        <div key={p.id} className="rounded-xl border bg-white p-4 shadow-sm">
          <div className="flex items-center justify-between">
            <div className="font-semibold">{p.label}</div>
            <button onClick={() => removePOI(p.id)} className="text-sm text-slate-500 hover:text-slate-800">Remove</button>
          </div>
          <div className="mt-3 grid grid-cols-[1fr_auto] items-center gap-3">
            <input
              type="range" min={5} max={240} step={5}
              value={sliders[p.id] ?? 30}
              onChange={(e) => setSlider(p.id, Number(e.target.value))}
              className="w-full"
            />
            <span className="rounded-full bg-slate-100 px-2 py-1 text-sm font-medium">{sliders[p.id] ?? 30} min</span>
          </div>
        </div>
      ))}
    </div>
  );
}

Next.js specifics you shouldn’t mess up
	•	Map component is client-only. Put 'use client' and dynamic import maplibre-gl inside the controller (as shown). Do not import it at module top-level in a server file.
	•	Providers: wrap app/layout.tsx with a providers.tsx that includes QueryClientProvider (TanStack Query) and maybe ThemeProvider.
	•	Code splitting: lazy-load MapCanvas with dynamic(() => import('./MapCanvas'), { ssr: false }) if you want to keep it off the first paint for sidebar-first layouts.

Performance checklist (React flavor)
	•	Keep the map imperative; do not put feature hover data in React state on every mousemove.
	•	Debounce slider updates to ~50ms (you already do this).
	•	Memoize expression building per (poiId, minutes, cacheKeyVersion).
	•	Offload heavy “combine D_anchor” work into a Web Worker if the cache maps get large.
	•	Lazy-load deck.gl only when “Pins” is toggled on.
	•	If pins grow huge, switch to vector tiles for pins and style client-side; stop fetching raw GeoJSON per move.

Design system setup (one command)
	•	Install Tailwind + shadcn: follow shadcn’s Next.js guide (generates components in components/ui/*). You’ll get accessible buttons, dialogs, combobox, etc., instantly better than custom DOM.

What you get with this split
	•	React renders only the sidebar and shell. The map is a black box with a tiny API.
	•	Fetching is reliable, cached, and deduped.
	•	Expressions and URL logic are pure → unit testable.
	•	Design is consistent and accessible without you rebuilding comboboxes.