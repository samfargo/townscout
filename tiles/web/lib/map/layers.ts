// MapLibre layer definitions and style configuration
import type { StyleSpecification } from 'maplibre-gl';

export const LAYER_IDS = {
  driveR8: 't_hex_r8_drive',
  driveR7: 't_hex_r7_drive',
  walkR8: 't_hex_r8_walk'
} as const;

export function createBaseStyle(): StyleSpecification {
  return {
    version: 8,
    sources: {
      osm: {
        type: 'raster',
        tiles: [
          'https://a.tile.openstreetmap.org/{z}/{x}/{y}.png',
          'https://b.tile.openstreetmap.org/{z}/{x}/{y}.png',
          'https://c.tile.openstreetmap.org/{z}/{x}/{y}.png'
        ],
        tileSize: 256,
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
      },
      't_hex_r8_drive': {
        type: 'vector',
        url: 'pmtiles:///tiles/t_hex_r8_drive.pmtiles'
      },
      't_hex_r7_drive': {
        type: 'vector',
        url: 'pmtiles:///tiles/t_hex_r7_drive.pmtiles'
      },
      't_hex_r8_walk': {
        type: 'vector',
        url: 'pmtiles:///tiles/t_hex_r8_walk.pmtiles'
      }
    },
    layers: [
      {
        id: 'background',
        type: 'raster',
        source: 'osm',
        minzoom: 0,
        maxzoom: 22
      },
      {
        id: LAYER_IDS.driveR8,
        type: 'fill',
        source: 't_hex_r8_drive',
        'source-layer': 't_hex_r8_drive',
        minzoom: 8,
        maxzoom: 22,
        layout: {
          visibility: 'visible'
        },
        paint: {
          'fill-color': '#10b981',
          'fill-opacity': 0.4
        }
      },
      {
        id: LAYER_IDS.driveR7,
        type: 'fill',
        source: 't_hex_r7_drive',
        'source-layer': 't_hex_r7_drive',
        minzoom: 0,
        maxzoom: 8,
        layout: {
          visibility: 'visible'
        },
        paint: {
          'fill-color': '#10b981',
          'fill-opacity': 0.4
        }
      },
      {
        id: LAYER_IDS.walkR8,
        type: 'fill',
        source: 't_hex_r8_walk',
        'source-layer': 't_hex_r8_walk',
        minzoom: 8,
        maxzoom: 22,
        layout: {
          visibility: 'none'
        },
        paint: {
          'fill-color': '#3b82f6',
          'fill-opacity': 0.4
        }
      }
    ]
  };
}

export function getColorForMinutes(minutes: number | null): string {
  if (minutes === null) return '#94a3b8'; // gray for unreachable
  if (minutes <= 10) return '#10b981'; // green
  if (minutes <= 20) return '#f59e0b'; // amber
  if (minutes <= 30) return '#ef4444'; // red
  return '#7c3aed'; // purple for >30 min
}
