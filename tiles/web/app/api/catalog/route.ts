// Proxies the catalog API and falls back to an empty payload.
import { NextResponse } from "next/server";

import { resolveApiUrl } from "@/lib/services";

export const runtime = "nodejs";

const EMPTY = {
  categories: [],
  brands: [],
  catToBrands: {},
  loaded: true
};

export async function GET(request: Request) {
  const requestUrl = new URL(request.url);
  const upstreamUrl = resolveApiUrl(`/api/catalog${requestUrl.search}`);

  try {
    const upstream = new URL(upstreamUrl);
    if (upstream.origin === requestUrl.origin && upstream.pathname === requestUrl.pathname) {
      throw new Error("Upstream catalog URL resolves to this Next.js route; aborting to avoid loop.");
    }

    const response = await fetch(upstreamUrl, {
      headers: {
        Accept: "application/json"
      },
      cache: "no-store"
    });

    if (!response.ok) {
      const body = await safeReadBody(response);
      throw new Error(`Upstream catalog request failed: ${response.status} ${response.statusText}: ${body}`);
    }

    const payload = await response.json();
    // Normalize the response to match the Catalog interface
    const normalized = {
      categories: payload.categories || [],
      brands: payload.brands || [],
      catToBrands: payload.catToBrands || payload.cat_to_brands || {},
      loaded: true
    };
    return NextResponse.json(normalized, {
      headers: {
        "cache-control": "no-store"
      }
    });
  } catch (error) {
    console.error("[api/catalog] Falling back to empty catalog", error);
    return NextResponse.json(EMPTY, {
      headers: {
        "cache-control": "no-store"
      }
    });
  }
}

async function safeReadBody(response: Response) {
  try {
    return await response.text();
  } catch {
    return "<no-body>";
  }
}
