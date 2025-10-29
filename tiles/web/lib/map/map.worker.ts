// Inlined worker for map expression computations.
/* eslint-disable no-restricted-globals */
import { type Mode } from '../state/store';

// ==================== EXPRESSION BUILDING ====================

export type MapExpression = any[];

type AnchorMap = Record<string, number>;

const baseExpressionCache = new WeakMap<AnchorMap, MapExpression>();

const MAX_TERMS_PER_HEX = 20;
const UNREACHABLE = 65535;

function buildBaseExpression(anchorMap: AnchorMap): MapExpression {
  const cached = baseExpressionCache.get(anchorMap);
  if (cached) {
    return cached;
  }

  const anchorLiteral: MapExpression = ['literal', anchorMap];
  const terms: MapExpression[] = [];

  for (let i = 0; i < MAX_TERMS_PER_HEX; i += 1) {
    const anchorIdExpression: MapExpression = ['get', `a${i}_id`];
    const hexToAnchorSec: MapExpression = [
      'coalesce',
      ['get', `a${i}_s`],
      UNREACHABLE
    ];

    const anchorToPoiSec: MapExpression = [
      'coalesce',
      [
        'get',
        ['to-string', anchorIdExpression],
        ['var', '__anchor_seconds']
      ],
      UNREACHABLE
    ];

    terms.push(['+', hexToAnchorSec, anchorToPoiSec]);
  }

  const baseExpression: MapExpression = [
    'let',
    '__anchor_seconds',
    anchorLiteral,
    ['min', ...terms]
  ];
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
  climateSelections: string[];
  avoidPowerLines: boolean;
  politicalLeanRange: [number, number] | null;
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

export type WorkerExpressionsUpdatedMessage = {
  type: 'expressions-updated';
  expressions: Record<Mode, { expression: any | null; maxMinutes: number; active: boolean }>;
  fallbackMode: Mode;
  signature: string;
};

// --- Worker implementation ---

let state: WorkerState | null = null;
// Store dAnchorCache separately to avoid cloning it on every state update
let dAnchorCache: Record<string, Record<Mode, any>> = {};
// Cache last posted expressions to avoid redundant postMessage calls
let lastPostedExpressionSignature: string | null = null;

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

function combineWithAll(...conditions: Array<any | null | undefined>): any | null {
  const flattened: any[] = conditions.filter(Boolean);
  if (!flattened.length) return null;
  if (flattened.length === 1) return flattened[0];
  return ['all', ...flattened];
}

// Cache base travel time expressions per POI+mode to avoid rebuilding on preview
const poiExpressionCache = new Map<string, any>();

function getCacheKey(poiId: string, currentMode: Mode): string {
  return `${poiId}:${currentMode}`;
}

function buildClimateFilterExpression(labels: string[]): any | null {
  if (!labels.length) return null;
  if (labels.length === 1) {
    return ['==', ['get', 'climate_label'], labels[0]];
  }
  return ['match', ['get', 'climate_label'], labels, true, false];
}

function buildAvoidPowerLinesExpression(enabled: boolean): any | null {
  if (!enabled) return null;
  return ['==', ['coalesce', ['get', 'near_power_corridor'], false], false];
}

function buildPoliticalLeanFilterExpression(range: [number, number] | null): any | null {
  if (!range) return null;
  const [min, max] = range;
  // Filter: political_lean >= min AND political_lean <= max
  // Handle null values (hexes without political data like water/unpopulated areas)
  return [
    'all',
    ['has', 'political_lean'],
    ['>=', ['get', 'political_lean'], min],
    ['<=', ['get', 'political_lean'], max]
  ];
}

function calculateExpressions(tempValues?: Record<string, number>) {
  if (!state) return;

  const {
    pois,
    sliders,
    poiModes,
    mode,
    climateSelections,
    avoidPowerLines = false,
    politicalLeanRange = null
  } = state;

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
  const baseExpressions: Record<Mode, any | null> = {
    drive: null,
    walk: null
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
    const combined = combineWithAll(...perModeBooleanExpressions[m]);
    baseExpressions[m] = combined;
    if (combined) {
      expressions[m].active = true;
      expressions[m].expression = combined;
    }
  });

  // Add overlay filters
  const climateFilter = buildClimateFilterExpression(climateSelections || []);
  const avoidFilter = buildAvoidPowerLinesExpression(Boolean(avoidPowerLines));
  const politicalFilter = buildPoliticalLeanFilterExpression(politicalLeanRange);

  (Object.keys(expressions) as Mode[]).forEach((m) => {
    const combined = combineWithAll(baseExpressions[m], avoidFilter, climateFilter, politicalFilter);
    if (combined) {
      expressions[m].expression = combined;
      expressions[m].active = true;
    } else {
      expressions[m].expression = null;
      expressions[m].active = false;
    }
  });

  // Only post if expressions changed
  const serializedSignature = JSON.stringify(expressions);
  if (serializedSignature !== lastPostedExpressionSignature) {
    lastPostedExpressionSignature = serializedSignature;
    self.postMessage({
      type: 'expressions-updated',
      expressions,
      fallbackMode: mode,
      signature: serializedSignature
    });
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
