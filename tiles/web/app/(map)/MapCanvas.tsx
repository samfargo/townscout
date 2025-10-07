'use client';

import 'maplibre-gl/dist/maplibre-gl.css';

import { useEffect, useRef, useState } from 'react';

import { MapController } from '@/lib/map/MapController';
import { registerMapController } from '@/lib/map/controllerRegistry';
import { buildCombinedFilter } from '@/lib/map/expressions';
import { useStore } from '@/lib/state/store';

export default function MapCanvas() {
  const containerRef = useRef<HTMLDivElement>(null);
  const [controller] = useState(() => new MapController());
  const [ready, setReady] = useState(false);

  const mode = useStore((state) => state.mode);
  const pois = useStore((state) => state.pois);
  const sliders = useStore((state) => state.sliders);
  const cache = useStore((state) => state.dAnchorCache);
  const cacheVersions = useStore((state) => state.cacheVersions);
  const setHover = useStore((state) => state.setHover);

  useEffect(() => {
    if (!containerRef.current) return;
    let cancelled = false;
    let detachHover: (() => void) | undefined;

    controller
      .init(containerRef.current)
      .then(() => {
        if (cancelled) return;
        registerMapController(controller);
        controller.setMode(mode);
        detachHover = controller.onHover((props) => setHover(props));
        setReady(true);
      })
      .catch((err) => {
        console.error('Failed to initialise map', err);
      });

    return () => {
      cancelled = true;
      detachHover?.();
      setHover(null);
    };
  }, [controller, mode, setHover]);

  useEffect(() => {
    if (!ready) return;
    if (!pois.length) {
      controller.setFilter(mode, null);
      return;
    }
    const filter = buildCombinedFilter(pois, sliders, cache, cacheVersions);
    controller.setFilter(mode, filter);
  }, [ready, controller, mode, pois, sliders, cache]);

  return <div ref={containerRef} className="h-full w-full" />;
}
