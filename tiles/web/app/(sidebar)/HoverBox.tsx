'use client';
// Shows hover details for the focused map hex.

import React from 'react';

import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { useStore, type Mode } from '@/lib/state/store';
import type { POI } from '@/lib/state/store';

export default function HoverBox() {
  const hover = useStore((state) => state.hover);
  const pois = useStore((state) => state.pois);
  const cache = useStore((state) => state.dAnchorCache);
  const poiModes = useStore((state) => state.poiModes);
  const defaultMode = useStore((state) => state.mode);

  const travelTimes = React.useMemo(() => {
    if (!hover) return [];
    
    return pois.map((poi) => {
      const mode = resolveMode(poi.id, poiModes, defaultMode);
      const anchorMap = cache[poi.id]?.[mode];
      if (!anchorMap || !Object.keys(anchorMap).length) {
        return { label: poi.label, minutes: null, mode };
      }

      // Compute minimum travel time using the same logic as the filter expression
      const minSeconds = computeMinTravelTime(hover, anchorMap);
      const minutes = minSeconds !== null ? Math.round(minSeconds / 60) : null;
      return { label: poi.label, minutes, mode };
    });
  }, [hover, pois, cache, poiModes, defaultMode]);

  return (
    <Card>
      <CardHeader>
        <CardTitle>Hover details</CardTitle>
      </CardHeader>
      <CardContent>
        {!hover && <p className="text-sm text-slate-500">Hover over the map to view details.</p>}
        {hover && travelTimes.length === 0 && (
          <div className="space-y-2">
            <div className="flex items-center justify-between gap-3">
              <dt className="text-xs uppercase tracking-wide text-slate-500">Hex ID</dt>
              <dd className="text-sm font-medium text-slate-800 font-mono text-xs">
                {hover.h3_id || 'N/A'}
              </dd>
            </div>
            <p className="text-sm text-slate-500 mt-3">
              Add filters to see travel times from this hex.
            </p>
          </div>
        )}
        {hover && travelTimes.length > 0 && (
          <dl className="space-y-2">
            <div className="flex items-center justify-between gap-3 pb-2 border-b border-slate-200">
              <dt className="text-xs uppercase tracking-wide text-slate-500">Hex ID</dt>
              <dd className="text-xs font-mono text-slate-600">
                {String(hover.h3_id || 'N/A').slice(-8)}
              </dd>
            </div>
            {travelTimes.map(({ label, minutes, mode }) => (
              <div key={label} className="flex items-center justify-between gap-3">
                <dt className="flex items-center gap-2 text-xs text-slate-600">
                  <span>{label}</span>
                  <span className="rounded-full bg-slate-100 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-slate-600">
                    {mode === 'drive' ? 'Drive' : 'Walk'}
                  </span>
                </dt>
                <dd className="text-sm font-semibold text-slate-800">
                  {minutes !== null ? `${minutes} min` : 'Unreachable'}
                </dd>
              </div>
            ))}
          </dl>
        )}
      </CardContent>
    </Card>
  );
}

function computeMinTravelTime(
  props: Record<string, any>,
  anchorMap: Record<string, number>
): number | null {
  const UNREACHABLE = 65535;
  const maxK = 20;
  let minTime = UNREACHABLE;

  for (let i = 0; i < maxK; i++) {
    const hexToAnchorSec = props[`a${i}_s`];
    const anchorId = props[`a${i}_id`];
    
    if (hexToAnchorSec == null || anchorId == null) continue;
    
    const anchorToDestSec = anchorMap[String(anchorId)] ?? UNREACHABLE;
    const totalSec = hexToAnchorSec + anchorToDestSec;
    
    if (totalSec < minTime) {
      minTime = totalSec;
    }
  }

  return minTime < UNREACHABLE ? minTime : null;
}

function resolveMode(id: string, poiModes: Record<string, Mode>, fallback: Mode): Mode {
  return poiModes[id] ?? fallback;
}
