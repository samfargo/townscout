'use client';
// Shows hover details for the focused map hex.

import React from 'react';

import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Sparkline } from '@/components/ui/sparkline';
import { useStore, type Mode } from '@/lib/state/store';
import type { POI } from '@/lib/state/store';

const MONTHS = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'];

export default function HoverBox() {
  const hover = useStore((state) => state.hover);
  const pois = useStore((state) => state.pois);
  const cache = useStore((state) => state.dAnchorCache);
  const poiModes = useStore((state) => state.poiModes);
  const defaultMode = useStore((state) => state.mode);
  const hexProps = hover?.kind === 'hex' ? hover.properties : null;
  const pinHover = hover?.kind === 'pin' ? hover : null;
  const nearPowerCorridor = Boolean(hexProps?.near_power_corridor);

  const climateSummary = React.useMemo(() => {
    if (!hexProps) return null;
    const label = typeof hexProps.climate_label === 'string' ? hexProps.climate_label : null;
    const summer = decodeQuantized(hexProps.temp_mean_summer_f_q, 0.1);
    const winter = decodeQuantized(hexProps.temp_mean_winter_f_q, 0.1);
    const precip = decodeQuantized(hexProps.ppt_ann_in_q, 0.1);
    
    // Extract monthly data for sparklines
    const monthlyTemp = MONTHS.map((month) => 
      decodeQuantized(hexProps[`temp_mean_${month}_f_q`], 0.1)
    ).filter((val): val is number => val !== null);
    
    const monthlyPrecip = MONTHS.map((month) => 
      decodeQuantized(hexProps[`ppt_${month}_in_q`], 0.1)
    ).filter((val): val is number => val !== null);
    
    if (!label && summer == null && winter == null && precip == null) {
      return null;
    }
    return { 
      label, 
      summer, 
      winter, 
      precip,
      monthlyTemp: monthlyTemp.length === 12 ? monthlyTemp : null,
      monthlyPrecip: monthlyPrecip.length === 12 ? monthlyPrecip : null
    };
  }, [hexProps]);

  const travelTimes = React.useMemo(() => {
    if (!hexProps) return [];
    
    return pois.map((poi) => {
      const mode = resolveMode(poi.id, poiModes, defaultMode);
      const anchorMap = cache[poi.id]?.[mode];
      if (!anchorMap || !Object.keys(anchorMap).length) {
        return { label: poi.label, minutes: null, mode };
      }

      // Compute minimum travel time using the same logic as the filter expression
      const minSeconds = computeMinTravelTime(hexProps, anchorMap);
      const minutes = minSeconds !== null ? Math.round(minSeconds / 60) : null;
      return { label: poi.label, minutes, mode };
    });
  }, [hexProps, pois, cache, poiModes, defaultMode]);

  return (
    <Card className="border-stone-300 bg-[#fbf7ec] p-0 shadow-[0_18px_30px_-26px_rgba(76,54,33,0.22)]">
      <CardHeader className="mb-0 rounded-2xl rounded-b-none border-b border-stone-200 bg-[#f2ebd9] px-4 py-3">
        <CardTitle className="font-serif text-stone-900">Hover details</CardTitle>
      </CardHeader>
      <CardContent className="space-y-3 px-4 pb-4 pt-3 text-sm text-stone-700">
        {!hover && (
          <p className="text-sm text-stone-500">Hover over the map to view details.</p>
        )}
        {pinHover && (
          <div className="space-y-1 rounded-xl border border-stone-200 bg-[#fbf7ec] px-3 py-2">
            <p className="text-[11px] uppercase tracking-wide text-stone-500">Location pin</p>
            <p className="text-sm font-semibold text-stone-800">{pinHover.name}</p>
            {pinHover.address && (
              <p className="text-xs text-stone-600">{pinHover.address}</p>
            )}
          </div>
        )}
        {hexProps && nearPowerCorridor && (
          <div className="rounded-xl border border-amber-300 bg-amber-50 px-3 py-2 text-[11px] text-amber-900">
            <p className="font-semibold uppercase tracking-wide">Near power corridor</p>
            <p className="text-xs text-amber-800">Within 200m of a high-voltage transmission line.</p>
          </div>
        )}
        {hexProps && climateSummary && (
          <div className="space-y-2 rounded-xl border border-stone-200 bg-[#fbf7ec] px-3 py-2">
            <p className="text-[11px] uppercase tracking-wide text-stone-500">Climate typology</p>
            <p className="text-sm font-semibold text-stone-800">
              {climateSummary.label ?? 'Unclassified'}
            </p>
            <dl className="mt-1 grid grid-cols-3 gap-2 text-[11px] text-stone-600">
              <div className="space-y-0.5">
                <dt className="uppercase tracking-wide text-stone-400">Summer avg</dt>
                <dd className="text-sm font-medium text-stone-800">
                  {formatTemperature(climateSummary.summer)}
                </dd>
              </div>
              <div className="space-y-0.5">
                <dt className="uppercase tracking-wide text-stone-400">Winter avg</dt>
                <dd className="text-sm font-medium text-stone-800">
                  {formatTemperature(climateSummary.winter)}
                </dd>
              </div>
              <div className="space-y-0.5">
                <dt className="uppercase tracking-wide text-stone-400">Annual precip</dt>
                <dd className="text-sm font-medium text-stone-800">
                  {formatPrecip(climateSummary.precip)}
                </dd>
              </div>
            </dl>
            {(climateSummary.monthlyTemp || climateSummary.monthlyPrecip) && (
              <div className="mt-2 space-y-1 border-t border-stone-200 pt-2">
                {climateSummary.monthlyTemp && (
                  <div className="space-y-0.5">
                    <p className="text-[10px] uppercase tracking-wide text-stone-400">
                      Monthly Temperature (°F)
                    </p>
                    <Sparkline
                      data={climateSummary.monthlyTemp}
                      width={220}
                      height={28}
                      color="#dc2626"
                      fillColor="#fca5a5"
                      type="line"
                    />
                  </div>
                )}
                {climateSummary.monthlyPrecip && (
                  <div className="mt-1 space-y-0.5">
                    <p className="text-[10px] uppercase tracking-wide text-stone-400">
                      Monthly Precipitation (in)
                    </p>
                    <Sparkline
                      data={climateSummary.monthlyPrecip}
                      width={220}
                      height={28}
                      color="#2563eb"
                      fillColor="#93c5fd"
                      type="bar"
                      showMonthLabels={true}
                    />
                  </div>
                )}
              </div>
            )}
          </div>
        )}
        {hexProps && travelTimes.length === 0 && !climateSummary && (
          <p className="text-sm text-stone-500">
            Add filters to see travel times from this hex.
          </p>
        )}
        {hexProps && travelTimes.length > 0 && (
          <dl className="space-y-2">
            {travelTimes.map(({ label, minutes, mode }) => (
              <div key={label} className="flex items-center justify-between gap-3">
                <dt className="flex items-center gap-2 text-xs text-stone-700">
                  <span>{label}</span>
                  <span className="rounded-full border border-stone-300 bg-[#f7f0de] px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-stone-600">
                    {mode === 'drive' ? 'Drive' : 'Walk'}
                  </span>
                </dt>
                <dd className="text-sm font-semibold text-stone-800">
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

function decodeQuantized(value: unknown, scale: number): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value * scale;
  }
  if (typeof value === 'string') {
    const parsed = Number.parseFloat(value);
    if (Number.isFinite(parsed)) {
      return parsed * scale;
    }
  }
  return null;
}

function formatTemperature(value: number | null): string {
  if (value == null) return '—';
  return `${Math.round(value)}°F`;
}

function formatPrecip(value: number | null): string {
  if (value == null) return '—';
  return `${value.toFixed(0)}″`;
}
