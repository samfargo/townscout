'use client';

import React from 'react';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Slider } from '@/components/ui/slider';
import { MIN_MINUTES, MAX_MINUTES, MINUTE_STEP, removePOI, updateSlider } from '@/lib/actions';
import { debounce } from '@/lib/utils/debounce';
import { useStore } from '@/lib/state/store';

export default function FiltersPanel() {
  const pois = useStore((state) => state.pois);
  const sliders = useStore((state) => state.sliders);

  const [local, setLocal] = React.useState<Record<string, number>>({});
  const debouncedUpdate = React.useMemo(
    () =>
      debounce((id: string, value: number) => {
        updateSlider(id, value);
      }, 50),
    []
  );

  React.useEffect(() => () => debouncedUpdate.cancel(), [debouncedUpdate]);

  React.useEffect(() => {
    setLocal((prev) => {
      const next = { ...prev };
      for (const key of Object.keys(next)) {
        if (!(key in sliders)) delete next[key];
      }
      return next;
    });
  }, [sliders]);

  if (!pois.length) {
    return (
      <div className="rounded-2xl border border-dashed border-sky-200 bg-sky-50 p-6 text-sm text-sky-700">
        No filters yet. Add a place type or drop a custom pin to start filtering reachable hexes.
      </div>
    );
  }

  const makeValue = (id: string) => local[id] ?? sliders[id] ?? 30;

  return (
    <div className="space-y-3">
      {pois.map((poi) => {
        const sliderValue = makeValue(poi.id);
        return (
          <Card key={poi.id}>
            <CardHeader>
              <div>
                <CardTitle>{poi.label}</CardTitle>
                <p className="text-xs text-slate-500 capitalize">{poi.type}</p>
              </div>
              <Button variant="ghost" size="sm" onClick={() => removePOI(poi.id)}>
                Remove
              </Button>
            </CardHeader>
            <CardContent className="space-y-3">
              <div className="flex items-center justify-between">
                <span className="text-xs uppercase tracking-wide text-slate-500">
                  Max travel time
                </span>
                <Badge variant="muted">{sliderValue} min</Badge>
              </div>
              <Slider
                min={MIN_MINUTES}
                max={MAX_MINUTES}
                step={MINUTE_STEP}
                value={[sliderValue]}
                onValueChange={(values) => {
                  const next = values[0] ?? MIN_MINUTES;
                  setLocal((prev) => ({ ...prev, [poi.id]: next }));
                  debouncedUpdate(poi.id, next);
                }}
                onValueCommit={(values) => {
                  const next = values[0] ?? MIN_MINUTES;
                  debouncedUpdate.cancel();
                  setLocal((prev) => {
                    const copy = { ...prev };
                    delete copy[poi.id];
                    return copy;
                  });
                  updateSlider(poi.id, next);
                }}
              />
            </CardContent>
          </Card>
        );
      })}
    </div>
  );
}
