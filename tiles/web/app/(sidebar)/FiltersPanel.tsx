'use client';
// Manages travel-time sliders for each active POI filter.

import React from 'react';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Slider } from '@/components/ui/slider';
import {
  changePoiMode,
  MIN_MINUTES,
  MAX_MINUTES,
  MINUTE_STEP,
  removePOI,
  updateSlider
} from '@/lib/actions';
import { debounce } from '@/lib/utils/debounce';
import { useStore, type Mode } from '@/lib/state/store';

const SLIDER_DEBOUNCE_MS = 120;

export default function FiltersPanel() {
  const pois = useStore((state) => state.pois);
  const sliders = useStore((state) => state.sliders);
  const poiModes = useStore((state) => state.poiModes);
  const defaultMode = useStore((state) => state.mode);

  const [local, setLocal] = React.useState<Record<string, number>>({});
  const [modePending, setModePending] = React.useState<Record<string, boolean>>({});
  const debouncedUpdate = React.useMemo(
    () =>
      debounce((id: string, value: number) => {
        updateSlider(id, value);
      }, SLIDER_DEBOUNCE_MS),
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

  const makeValue = React.useCallback(
    (id: string) => local[id] ?? sliders[id] ?? 30,
    [local, sliders]
  );

  const handleModeChange = React.useCallback(async (id: string, target: Mode) => {
    setModePending((prev) => ({ ...prev, [id]: true }));
    try {
      await changePoiMode(id, target);
    } catch (error) {
      console.error('Failed to change travel mode', error);
    } finally {
      setModePending((prev) => {
        const copy = { ...prev };
        delete copy[id];
        return copy;
      });
    }
  }, []);

  return (
    <div className="space-y-3">
      {pois.map((poi) => {
        const sliderValue = makeValue(poi.id);
        const currentMode = poiModes[poi.id] ?? defaultMode;
        const pendingModeChange = modePending[poi.id] ?? false;
        return (
          <Card key={poi.id}>
            <CardHeader>
              <div>
                <CardTitle>{poi.label}</CardTitle>
                <p className="text-xs text-slate-500 capitalize">{poi.type}</p>
              </div>
              <Button
                variant="ghost"
                size="icon"
                onClick={() => removePOI(poi.id)}
                aria-label={`Remove ${poi.label}`}
                title="Remove filter"
              >
                <span aria-hidden>X</span>
              </Button>
            </CardHeader>
            <CardContent className="space-y-4">
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
              <div className="flex items-center justify-between">
                <span className="text-xs uppercase tracking-wide text-slate-500">
                  Travel mode
                </span>
                <ModeToggle
                  value={currentMode}
                  disabled={pendingModeChange}
                  onChange={(mode) => {
                    if (mode === currentMode) return;
                    void handleModeChange(poi.id, mode);
                  }}
                />
              </div>
            </CardContent>
          </Card>
        );
      })}
    </div>
  );
}

function ModeToggle({
  value,
  onChange,
  disabled
}: {
  value: Mode;
  onChange: (mode: Mode) => void;
  disabled?: boolean;
}) {
  return (
    <div className="flex items-center gap-2">
      <Button
        size="sm"
        variant={value === 'drive' ? 'default' : 'outline'}
        disabled={disabled || value === 'drive'}
        onClick={() => onChange('drive')}
      >
        Drive
      </Button>
      <Button
        size="sm"
        variant={value === 'walk' ? 'default' : 'outline'}
        disabled={disabled || value === 'walk'}
        onClick={() => onChange('walk')}
      >
        Walk
      </Button>
    </div>
  );
}
