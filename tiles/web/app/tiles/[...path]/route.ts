// Streams pmtiles assets with range-aware responses.
import { NextRequest, NextResponse } from "next/server";
import path from "node:path";
import { stat } from "node:fs/promises";
import { createReadStream } from "node:fs";
import { Readable } from "node:stream";

export const runtime = "nodejs";

const ROOT = path.resolve(process.cwd(), "..");

function resolveFile(requested: string[]) {
  const relPath = requested.join("/");
  if (!relPath.endsWith(".pmtiles")) {
    throw new Error("Unsupported file type");
  }
  const filePath = path.resolve(ROOT, relPath);
  if (!filePath.startsWith(ROOT)) {
    throw new Error("Access denied");
  }
  return filePath;
}

function parseRange(rangeHeader: string | null, size: number) {
  if (!rangeHeader) return null;
  const match = /^bytes=(\d*)-(\d*)$/.exec(rangeHeader);
  if (!match) return null;
  let [, startStr, endStr] = match;
  let start = startStr ? Number(startStr) : undefined;
  let end = endStr ? Number(endStr) : undefined;

  if (start === undefined && end === undefined) return null;

  if (start !== undefined && Number.isNaN(start)) return null;
  if (end !== undefined && Number.isNaN(end)) return null;

  if (start === undefined) {
    // suffix-byte-range-spec: last N bytes
    const length = Number(endStr);
    if (Number.isNaN(length)) return null;
    start = Math.max(0, size - length);
    end = size - 1;
  } else {
    end = end === undefined ? size - 1 : Math.min(end, size - 1);
  }

  if (start! > end! || start! >= size) return null;

  return { start: start!, end: end! };
}

async function buildResponse(filePath: string, request: NextRequest, includeBody: boolean) {
  const info = await stat(filePath);
  const range = parseRange(request.headers.get("range"), info.size);
  const headers = new Headers({
    "content-type": "application/octet-stream",
    "cache-control": "public, max-age=31536000, immutable",
    "accept-ranges": "bytes"
  });

  if (!includeBody) {
    headers.set("content-length", info.size.toString());
    return new NextResponse(null, { status: 200, headers });
  }

  if (range) {
    const { start, end } = range;
    const chunkSize = end - start + 1;
    headers.set("content-length", chunkSize.toString());
    headers.set("content-range", `bytes ${start}-${end}/${info.size}`);
    const stream = createReadStream(filePath, { start, end });
    return new NextResponse(Readable.toWeb(stream) as any, { status: 206, headers });
  }

  headers.set("content-length", info.size.toString());
  const stream = createReadStream(filePath);
  return new NextResponse(Readable.toWeb(stream) as any, { status: 200, headers });
}

export async function GET(request: NextRequest, { params }: { params: { path?: string[] } }) {
  try {
    const segments = params.path ?? [];
    if (!segments.length) {
      return NextResponse.json({ error: "Not Found" }, { status: 404 });
    }
    const filePath = resolveFile(segments);
    return await buildResponse(filePath, request, true);
  } catch (error) {
    return NextResponse.json({ error: "Not Found" }, { status: 404 });
  }
}

export async function HEAD(request: NextRequest, { params }: { params: { path?: string[] } }) {
  try {
    const segments = params.path ?? [];
    if (!segments.length) {
      return NextResponse.json({ error: "Not Found" }, { status: 404 });
    }
    const filePath = resolveFile(segments);
    return await buildResponse(filePath, request, false);
  } catch (error) {
    return NextResponse.json({ error: "Not Found" }, { status: 404 });
  }
}
