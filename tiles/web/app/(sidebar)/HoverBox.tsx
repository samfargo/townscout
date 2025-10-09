'use client';
// Shows hover details for the focused map hex.

import React from 'react';

import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { useStore } from '@/lib/state/store';

const KEY_LABELS: Record<string, string> = {
  hex_id: 'Hex ID',
  state: 'State',
  county: 'County',
  population: 'Population',
  travel_minutes: 'Travel Minutes',
  walk_minutes: 'Walk Minutes'
};

export default function HoverBox() {
  const hover = useStore((state) => state.hover);

  const interestingEntries = React.useMemo(() => {
    if (!hover) return [];
    const entries = Object.entries(hover).filter(([key, value]) => {
      if (value == null || value === '') return false;
      if (key in KEY_LABELS) return true;
      if (typeof value === 'number' && /minutes|seconds|pop|score/i.test(key)) return true;
      if (typeof value === 'string' && /label|name/i.test(key)) return true;
      return false;
    });
    return entries.slice(0, 6);
  }, [hover]);

  return (
    <Card>
      <CardHeader>
        <CardTitle>Hover details</CardTitle>
        <p className="text-xs text-slate-500">
          Move your cursor across the map to inspect the underlying hex.
        </p>
      </CardHeader>
      <CardContent>
        {!hover && <p className="text-sm text-slate-500">Hover over the map to view details.</p>}
        {hover && interestingEntries.length === 0 && (
          <p className="text-sm text-slate-500">No descriptive attributes available for this hex.</p>
        )}
        {hover && interestingEntries.length > 0 && (
          <dl className="space-y-2">
            {interestingEntries.map(([key, value]) => (
              <div key={key} className="flex items-center justify-between gap-3">
                <dt className="text-xs uppercase tracking-wide text-slate-500">
                  {KEY_LABELS[key] ?? key.replace(/_/g, ' ')}
                </dt>
                <dd className="text-sm font-medium text-slate-800">
                  {typeof value === 'number' ? formatNumber(value) : String(value)}
                </dd>
              </div>
            ))}
          </dl>
        )}
      </CardContent>
    </Card>
  );
}

function formatNumber(value: number) {
  if (Number.isInteger(value)) {
    return value.toLocaleString();
  }
  return value.toFixed(1);
}
