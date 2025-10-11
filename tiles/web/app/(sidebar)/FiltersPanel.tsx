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
  updateSlider,
  updateSliderPreview
} from '@/lib/actions';
import { getMapController } from '@/lib/map/MapController';
import { useStore, type Mode } from '@/lib/state/store';

const antiqueCardClass =
  'border-stone-300 bg-[#fbf7ec] p-0 shadow-[0_18px_32px_-28px_rgba(76,54,33,0.25)]';
const antiqueHeaderClass =
  'mb-0 flex items-center justify-between gap-3 rounded-2xl rounded-b-none border-b border-stone-200 bg-[#f2ebd9] px-4 py-3';
const antiqueContentClass = 'space-y-4 px-4 pb-4 pt-3 text-sm text-stone-700';
const antiqueLabelClass = 'text-xs uppercase tracking-wide text-stone-500';
const antiqueBadgeClass =
  'border border-amber-900 bg-amber-800 text-amber-50 shadow-sm';
const antiqueOutlineButtonClass =
  'border border-stone-300 bg-[#fbf7ec] text-stone-800 shadow-sm transition-transform hover:-translate-y-0.5 hover:bg-[#f2ebd9] focus-visible:ring-amber-700';
const antiqueOutlineActiveClass =
  'border border-amber-900 bg-amber-800 text-amber-50 shadow-sm transition-transform hover:-translate-y-0.5 hover:bg-amber-900 focus-visible:ring-amber-700';

export default function FiltersPanel() {
  const pois = useStore((state) => state.pois);
  const sliders = useStore((state) => state.sliders);
  const poiModes = useStore((state) => state.poiModes);
  const defaultMode = useStore((state) => state.mode);

  const [local, setLocal] = React.useState<Record<string, number>>({});
  const [modePending, setModePending] = React.useState<Record<string, boolean>>({});
  const [isDragging, setIsDragging] = React.useState<Record<string, boolean>>({});

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
          <Card key={poi.id} className={antiqueCardClass}>
            <CardHeader className={antiqueHeaderClass}>
              <div>
                <CardTitle className="font-serif text-stone-900">{poi.label}</CardTitle>
                <p className="text-xs capitalize text-stone-500">{poi.type}</p>
              </div>
              <Button
                variant="ghost"
                size="icon"
                onClick={() => removePOI(poi.id)}
                aria-label={`Remove ${poi.label}`}
                title="Remove filter"
                className="text-amber-900 transition-transform hover:-translate-y-0.5 hover:bg-[#f2ebd9]"
              >
                <span aria-hidden>X</span>
              </Button>
            </CardHeader>
            <CardContent className={antiqueContentClass}>
              <div className="flex items-center justify-between">
                <span className={antiqueLabelClass}>Max travel time</span>
                <Badge variant="muted" className={antiqueBadgeClass}>
                  {sliderValue} min
                </Badge>
              </div>
              <Slider
                min={MIN_MINUTES}
                max={MAX_MINUTES}
                step={MINUTE_STEP}
                value={[sliderValue]}
                onValueChange={(values) => {
                  const next = values[0] ?? MIN_MINUTES;
                  // Signal map controller that dragging started (only once per drag)
                  if (!isDragging[poi.id]) {
                    getMapController()?.setDragging(true);
                    setIsDragging((prev) => ({ ...prev, [poi.id]: true }));
                  }
                  // Update local state immediately for instant UI feedback
                  setLocal((prev) => ({ ...prev, [poi.id]: next }));
                  // Update map via RAF (no localStorage write)
                  updateSliderPreview(poi.id, next);
                }}
                onValueCommit={(values) => {
                  const next = values[0] ?? MIN_MINUTES;
                  // Signal map controller that dragging stopped
                  getMapController()?.setDragging(false);
                  setIsDragging((prev) => {
                    const copy = { ...prev };
                    delete copy[poi.id];
                    return copy;
                  });
                  // Clear local state
                  setLocal((prev) => {
                    const copy = { ...prev };
                    delete copy[poi.id];
                    return copy;
                  });
                  // Persist to store and update map
                  updateSlider(poi.id, next);
                }}
              />
              <div className="flex items-center justify-between">
                <span className={antiqueLabelClass}>Travel mode</span>
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
        variant="outline"
        className={value === 'drive' ? antiqueOutlineActiveClass : antiqueOutlineButtonClass}
        disabled={disabled || value === 'drive'}
        onClick={() => onChange('drive')}
      >
        Drive
      </Button>
      <Button
        size="sm"
        variant="outline"
        className={value === 'walk' ? antiqueOutlineActiveClass : antiqueOutlineButtonClass}
        disabled={disabled || value === 'walk'}
        onClick={() => onChange('walk')}
      >
        Walk
      </Button>
    </div>
  );
}
