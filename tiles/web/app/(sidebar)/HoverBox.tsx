'use client';
// Shows hover details for the focused map hex.

import React from 'react';

import { Card, CardContent } from '@/components/ui/card';
import { Sparkline } from '@/components/ui/sparkline';
import { useStore, type Mode } from '@/lib/state/store';

const MONTHS = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'];

export default function HoverBox() {
  const hover = useStore((state) => state.hover);
  const pois = useStore((state) => state.pois);
  const cache = useStore((state) => state.dAnchorCache);
  const poiModes = useStore((state) => state.poiModes);
  const defaultMode = useStore((state) => state.mode);
  const hexProps = hover?.kind === 'hex' ? hover.properties : null;
  const pinHover = hover?.kind === 'pin' ? hover : null;
  const hexId = React.useMemo(() => (hexProps ? extractHexId(hexProps) : null), [hexProps]);

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
    <Card className="pointer-events-auto w-fit max-w-[min(240px,calc(100vw-2rem))] border border-stone-200/70 bg-white/95 px-2 pb-2 text-stone-700 shadow-sm shadow-stone-900/10 backdrop-blur-sm">
      <CardContent className="space-y-1.5 p-0 text-[11px] leading-tight">
        <p className="text-[9px] font-semibold uppercase tracking-[0.18em] text-stone-500">Hover details</p>
        {!hover && (
          <p className="text-[10px] text-stone-500">Hover over the map to view details.</p>
        )}
        {pinHover && (
          <div className="space-y-0.5">
            <p className="text-[9px] uppercase tracking-wide text-stone-400">Location pin</p>
            <p className="text-[11px] font-semibold text-stone-800">{pinHover.name}</p>
            {pinHover.address && <p className="text-[10px] text-stone-500">{pinHover.address}</p>}
          </div>
        )}
        {hexProps && hexId && (
          <div className="space-y-0.5">
            <p className="text-[9px] uppercase tracking-wide text-stone-400">Hex ID</p>
            <p className="font-mono text-[10px] font-medium text-stone-700">{hexId}</p>
          </div>
        )}
        {hexProps && climateSummary && (
          <div className="space-y-1">
            <p className="text-[9px] uppercase tracking-wide text-stone-400">Climate typology</p>
            <p className="text-[11px] font-semibold text-stone-800">
              {climateSummary.label ?? 'Unclassified'}
            </p>
            <dl className="grid grid-cols-3 gap-1 text-[9px] text-stone-500">
              <div className="space-y-0.5">
                <dt className="uppercase tracking-wide text-stone-400">Summer</dt>
                <dd className="text-[11px] font-medium text-stone-800">
                  {formatTemperature(climateSummary.summer)}
                </dd>
              </div>
              <div className="space-y-0.5">
                <dt className="uppercase tracking-wide text-stone-400">Winter</dt>
                <dd className="text-[11px] font-medium text-stone-800">
                  {formatTemperature(climateSummary.winter)}
                </dd>
              </div>
              <div className="space-y-0.5">
                <dt className="uppercase tracking-wide text-stone-400">Precip</dt>
                <dd className="text-[11px] font-medium text-stone-800">
                  {formatPrecip(climateSummary.precip)}
                </dd>
              </div>
            </dl>
            {(climateSummary.monthlyTemp || climateSummary.monthlyPrecip) && (
              <div className="space-y-0.5">
                {climateSummary.monthlyTemp && (
                  <div className="space-y-0.5">
                    <p className="text-[9px] uppercase tracking-wide text-stone-400">
                      Monthly Temperature (°F)
                    </p>
                    <Sparkline
                      data={climateSummary.monthlyTemp}
                      width={184}
                      height={20}
                      color="#dc2626"
                      fillColor="#fca5a5"
                      type="line"
                    />
                  </div>
                )}
                {climateSummary.monthlyPrecip && (
                  <div className="space-y-0.5">
                    <p className="text-[9px] uppercase tracking-wide text-stone-400">
                      Monthly Precipitation (in)
                    </p>
                    <Sparkline
                      data={climateSummary.monthlyPrecip}
                      width={184}
                      height={20}
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
          <p className="text-[10px] text-stone-500">Add filters to see travel times from this hex.</p>
        )}
        {hexProps && travelTimes.length > 0 && (
          <dl className="space-y-0.5">
            {travelTimes.map(({ label, minutes, mode }) => (
              <div key={label} className="flex items-center justify-between gap-1">
                <dt className="flex items-center gap-1 text-[9px] text-stone-600">
                  <span>{label}</span>
                  <span className="rounded-full border border-stone-300 bg-white/80 px-1.5 py-0.5 text-[8px] font-semibold uppercase tracking-wide text-stone-500">
                    {mode === 'drive' ? 'Drive' : 'Walk'}
                  </span>
                </dt>
                <dd className="text-[10px] font-semibold text-stone-800">
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

function extractHexId(props: Record<string, any>): string | null {
  const candidates = ['h3_id', 'hex_id', 'hex', 'h3', 'h3_address'];
  for (const key of candidates) {
    if (!(key in props)) continue;
    const raw = props[key];
    if (raw == null) continue;
    if (typeof raw === 'string') {
      const trimmed = raw.trim();
      if (trimmed) return trimmed;
      continue;
    }
    if (typeof raw === 'bigint') {
      return raw.toString(16);
    }
    if (typeof raw === 'number' && Number.isFinite(raw)) {
      const intValue = Math.trunc(raw);
      if (!Number.isSafeInteger(intValue)) {
        return raw.toString();
      }
      return intValue.toString(16);
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
