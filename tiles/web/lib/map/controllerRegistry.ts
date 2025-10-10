// Registry for the global MapController instance
import type { MapController } from './MapController';

let globalController: MapController | null = null;

export function registerMapController(controller: MapController): void {
  globalController = controller;
}

export function getMapController(): MapController | null {
  return globalController;
}

