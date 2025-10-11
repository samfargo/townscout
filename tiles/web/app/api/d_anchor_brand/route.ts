// Proxies the d_anchor_brand API for brand travel times.
import { NextResponse } from "next/server";

import { resolveApiUrl } from "@/lib/services";

export const runtime = "nodejs";

const EMPTY: Record<string, number> = {};

export async function GET(request: Request) {
  const requestUrl = new URL(request.url);
  const upstreamUrl = resolveApiUrl(`/api/d_anchor_brand${requestUrl.search}`);

  try {
    const upstream = new URL(upstreamUrl);
    if (upstream.origin === requestUrl.origin && upstream.pathname === requestUrl.pathname) {
      throw new Error("Upstream d_anchor_brand URL resolves to this Next.js route; aborting to avoid loop.");
    }

    const response = await fetch(upstreamUrl, {
      headers: {
        Accept: "application/json"
      },
      cache: "no-store"
    });

    if (!response.ok) {
      const body = await safeReadBody(response);
      throw new Error(`Upstream d_anchor_brand request failed: ${response.status} ${response.statusText}: ${body}`);
    }

    const payload = await response.json();
    return NextResponse.json(payload ?? EMPTY, {
      headers: {
        "cache-control": "no-store"
      }
    });
  } catch (error) {
    console.error("[api/d_anchor_brand] Falling back to empty payload", error);
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
