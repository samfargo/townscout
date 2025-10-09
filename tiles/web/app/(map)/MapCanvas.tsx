'use client';
// Hosts the MapLibre canvas and wires it to global state.

import 'maplibre-gl/dist/maplibre-gl.css';

import { useEffect, useRef, useState } from 'react';

import { applyCurrentFilter } from '@/lib/actions';
import { MapController } from '@/lib/map/MapController';
import { registerMapController } from '@/lib/map/controllerRegistry';
import { useStore } from '@/lib/state/store';

export default function MapCanvas() {
  const containerRef = useRef<HTMLDivElement>(null);
  const [controller] = useState(() => new MapController());
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
        detachHover = controller.onHover((props) => setHover(props));
        applyCurrentFilter({ immediate: true });
      })
      .catch((err) => {
        console.error('Failed to initialise map', err);
      });

    return () => {
      cancelled = true;
      detachHover?.();
      setHover(null);
      // Keep map instance alive; React Strict Mode will remount the component.
    };
  }, [controller, setHover]);

  return <div ref={containerRef} className="h-full w-full" />;
}
