// Inlined worker for map expression computations.
/* eslint-disable no-restricted-globals */
import { type Mode } from '../state/store';

// ==================== EXPRESSION BUILDING ====================

export type MapExpression = any[];

type AnchorMap = Record<string, number>;

const baseExpressionCache = new WeakMap<AnchorMap, MapExpression>();

function buildBaseExpression(anchorMap: AnchorMap): MapExpression {
  const cached = baseExpressionCache.get(anchorMap);
  if (cached) {
    return cached;
  }

  const UNREACHABLE = 65535;
  const matchArgs: number[] = [];

  for (const anchorId of Object.keys(anchorMap)) {
    const parsedId = parseInt(anchorId, 10);
    const seconds = Number(anchorMap[anchorId]);
    if (!Number.isFinite(parsedId) || !Number.isFinite(seconds)) {
      continue;
    }
    matchArgs.push(parsedId, seconds);
  }

  const matchArgsWithFallback = [...matchArgs, UNREACHABLE];
  const terms: MapExpression[] = [];

  for (let i = 0; i < 20; i += 1) {
    const hexToAnchorSec: MapExpression = [
      'coalesce',
      ['get', `a${i}_s`],
      UNREACHABLE
    ];

    const anchorToPoiSec: MapExpression = [
      'match',
      ['get', `a${i}_id`],
      ...matchArgsWithFallback
    ];

    terms.push(['+', hexToAnchorSec, anchorToPoiSec]);
  }

  const baseExpression: MapExpression = ['min', ...terms];
  baseExpressionCache.set(anchorMap, baseExpression);
  return baseExpression;
}

/**
 * Builds a MapLibre expression that resolves to the minimum travel time in seconds
 * from a feature to any of the anchors defined in the anchorMap.
 */
function buildTravelTimeExpression(anchorMap: AnchorMap): MapExpression {
  return buildBaseExpression(anchorMap);
}

// ==================== WORKER STATE & MESSAGES ====================

export type WorkerState = {
  pois: any[];
  sliders: Record<string, number>;
  poiModes: Record<string, Mode>;
  mode: Mode;
};

export type WorkerMessage = {
  type: 'update-state'; 
  state: WorkerState;
};

export type WorkerPreviewMessage = {
  type: 'update-preview';
  tempValues: Record<string, number>;
};

export type WorkerDAnchorMessage = {
  type: 'update-dAnchor';
  id: string;
  mode: Mode;
  data: any;
};

// --- Worker implementation ---

let state: WorkerState | null = null;
// Store dAnchorCache separately to avoid cloning it on every state update
let dAnchorCache: Record<string, Record<Mode, any>> = {};
// Cache last posted expressions to avoid redundant postMessage calls
let lastPostedExpressions: string | null = null;

function combineWithMin(...conditions: Array<any | null | undefined>): any | null {
  const flattened: any[] = conditions.filter(Boolean);
  if (!flattened.length) return null;
  if (flattened.length === 1) return flattened[0];
  return ['min', ...flattened];
}

function combineWithAny(...conditions: Array<any | null | undefined>): any | null {
  const flattened: any[] = conditions.filter(Boolean);
  if (!flattened.length) return null;
  if (flattened.length === 1) return flattened[0];
  return ['any', ...flattened];
}

// Cache base travel time expressions per POI+mode to avoid rebuilding on preview
const poiExpressionCache = new Map<string, any>();

function getCacheKey(poiId: string, currentMode: Mode): string {
  return `${poiId}:${currentMode}`;
}

function calculateExpressions(tempValues?: Record<string, number>) {
  if (!state) return;

  const { pois, sliders, poiModes, mode } = state;

  const expressions: Record<
    Mode,
    { expression: any | null; maxMinutes: number; active: boolean }
  > = {
    drive: { expression: null, maxMinutes: 0, active: false },
    walk: { expression: null, maxMinutes: 0, active: false }
  };

  const perModeBooleanExpressions: Record<Mode, any[]> = {
    drive: [],
    walk: []
  };

  for (const poi of pois) {
    const currentMode = poiModes[poi.id] || mode;
    const maxMinutes = tempValues?.[poi.id] ?? sliders[poi.id] ?? 30;
    const anchorMap = dAnchorCache[poi.id]?.[currentMode];

    if (!anchorMap || Object.keys(anchorMap).length === 0) {
      continue;
    }

    // Use cached base expression or build it once
    const cacheKey = getCacheKey(poi.id, currentMode);
    let travelTimeExpression = poiExpressionCache.get(cacheKey);
    if (!travelTimeExpression) {
      travelTimeExpression = buildTravelTimeExpression(anchorMap);
      poiExpressionCache.set(cacheKey, travelTimeExpression);
    }

    const maxSeconds = maxMinutes * 60;
    // Create a boolean expression: is travel time <= threshold for this POI?
    // Note: we build a new wrapper expression with the updated threshold,
    // but reuse the expensive base travel time expression
    const booleanExpression = ['<=', travelTimeExpression, maxSeconds];
    perModeBooleanExpressions[currentMode].push(booleanExpression);
    
    expressions[currentMode].maxMinutes = Math.max(
      expressions[currentMode].maxMinutes,
      maxMinutes
    );
  }

  (Object.keys(perModeBooleanExpressions) as Mode[]).forEach((m) => {
    const combined = combineWithAny(...perModeBooleanExpressions[m]);
    if (!combined) return;
    expressions[m].active = true;
    expressions[m].expression = combined;
  });

  // Only post if expressions changed
  const serialized = JSON.stringify(expressions);
  if (serialized !== lastPostedExpressions) {
    lastPostedExpressions = serialized;
    self.postMessage({ type: 'expressions-updated', expressions });
  }
}

// Throttle preview updates using RAF in the worker
let pendingPreviewValues: Record<string, number> | null = null;
let previewRafId: number | null = null;

function schedulePreviewCalculation(tempValues: Record<string, number>) {
  pendingPreviewValues = tempValues;
  
  if (previewRafId !== null) {
    return; // Already scheduled
  }
  
  previewRafId = self.requestAnimationFrame(() => {
    previewRafId = null;
    if (pendingPreviewValues) {
      const values = pendingPreviewValues;
      pendingPreviewValues = null;
      calculateExpressions(values);
    }
  });
}

self.onmessage = (
  e: MessageEvent<WorkerMessage | WorkerPreviewMessage | WorkerDAnchorMessage>
): void => {
  if (e.data.type === 'update-state') {
    state = e.data.state;
    // Cancel any pending preview calculations since we have a full state update
    if (previewRafId !== null) {
      self.cancelAnimationFrame(previewRafId);
      previewRafId = null;
      pendingPreviewValues = null;
    }
    calculateExpressions(); // Recalculate with full state
  } else if (e.data.type === 'update-preview') {
    // Throttle preview updates to one per animation frame
    schedulePreviewCalculation(e.data.tempValues);
  } else if (e.data.type === 'update-dAnchor') {
    // Update dAnchorCache separately
    const { id, mode, data } = e.data;
    if (!dAnchorCache[id]) {
      dAnchorCache[id] = {} as Record<Mode, any>;
    }
    dAnchorCache[id][mode] = data;
    // Clear cached base expression for this POI+mode since data changed
    const cacheKey = getCacheKey(id, mode);
    poiExpressionCache.delete(cacheKey);
  }
};
