// Proxies the poi_points API to retrieve map pins for brands/categories.
import { createApiProxy, type PoiPinProperties } from "@/lib/services";
import type { FeatureCollection, Point } from "geojson";

export const runtime = "nodejs";

const EMPTY: FeatureCollection<Point, PoiPinProperties> = {
  type: "FeatureCollection",
  features: []
};

export async function GET(request: Request) {
  return createApiProxy(request, {
    endpoint: "/api/poi_points",
    fallback: EMPTY,
    logLabel: "api/poi_points"
  });
}
