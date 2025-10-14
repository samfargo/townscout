// Proxies the d_anchor API for category travel times.
import { createApiProxy } from "@/lib/services";

export const runtime = "nodejs";

const EMPTY: Record<string, number> = {};

export async function GET(request: Request) {
  return createApiProxy(request, {
    endpoint: "/api/d_anchor",
    fallback: EMPTY,
    logLabel: "api/d_anchor"
  });
}
