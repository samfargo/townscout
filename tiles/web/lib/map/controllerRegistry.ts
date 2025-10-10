// Registry for the global MapController instance
import type { MapController } from './MapController';

let globalController: MapController | null = null;
let registrationCallbacks: Array<() => void> = [];

export function registerMapController(controller: MapController): void {
  globalController = controller;
  
  // Call any pending callbacks
  const callbacks = [...registrationCallbacks];
  registrationCallbacks = [];
  callbacks.forEach(cb => cb());
}

export function getMapController(): MapController | null {
  return globalController;
}

export function onMapControllerReady(callback: () => void): void {
  if (globalController) {
    callback();
  } else {
    registrationCallbacks.push(callback);
  }
}

