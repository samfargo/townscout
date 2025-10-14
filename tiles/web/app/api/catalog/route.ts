// Proxies the catalog API and falls back to an empty payload.
import { createApiProxy } from "@/lib/services";

export const runtime = "nodejs";

const EMPTY = {
  categories: [],
  brands: [],
  catToBrands: {},
  loaded: true
};

function normalizeCatalogResponse(payload: any) {
  // Normalize the response to match the Catalog interface
  return {
    categories: payload.categories || [],
    brands: payload.brands || [],
    catToBrands: payload.catToBrands || payload.cat_to_brands || {},
    loaded: true
  };
}

export async function GET(request: Request) {
  return createApiProxy(request, {
    endpoint: "/api/catalog",
    fallback: EMPTY,
    transform: normalizeCatalogResponse,
    logLabel: "api/catalog"
  });
}
