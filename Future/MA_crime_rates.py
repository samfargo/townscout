#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fast + robust Playwright scraper for MA crime rates.

- Visits: https://ma.beyond2020.com/ma_tops/report/crime-overview/{slug}/2024
- Extracts the text node *immediately after* <strong>Crime Rate:</strong> in the live DOM.
- If missing/slow, falls back to the /print route (usually SSR).
- CSV: tow,slug,crime_rate_per_100k
- Concurrency is throttled and polite to avoid soft rate-limits.
"""

import argparse
import asyncio
import csv
import math
import re
from typing import List, Tuple, Optional

from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError, Page, BrowserContext

BASE_URL_TPL = "https://ma.beyond2020.com/ma_tops/report/crime-overview/{slug}/2024"
PRINT_URL_TPL = BASE_URL_TPL + "/print"
OUT_PATH = "ma_crime_rates.csv"

JS_EXTRACT = r"""
() => {
  const strongs = Array.from(document.querySelectorAll('strong'));
  const target = strongs.find(s => (s.textContent || '').trim().toLowerCase() === 'crime rate:');
  if (!target) return null;

  const nextNonEmpty = (n) => {
    let x = n.nextSibling;
    while (x && x.nodeType === Node.TEXT_NODE && x.textContent.trim() === '') x = x.nextSibling;
    return x;
  };

  let sib = nextNonEmpty(target);
  if (!sib) return null;

  if (sib.nodeType === Node.TEXT_NODE) {
    const t = sib.textContent.trim();
    return t || null;
  }
  if (sib.nodeType === Node.ELEMENT_NODE) {
    let ns = nextNonEmpty(sib);
    if (ns && ns.nodeType === Node.TEXT_NODE) {
      const t = ns.textContent.trim();
      if (t) return t;
    }
    const own = sib.textContent ? sib.textContent.trim() : '';
    return own || null;
  }
  return null;
}
"""

def parse_rate_to_float(text: Optional[str]) -> Optional[float]:
    if not text:
        return None
    t = text.strip().strip('"“”\'')
    # Require a number and "per" to avoid false positives
    if "per" not in t.lower():
        return None
    m = re.search(r"(\d[\d,]*\.?\d*)", t)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", ""))
    except ValueError:
        return None

async def load_towns(path: str) -> List[Tuple[str, str]]:
    towns: List[Tuple[str, str]] = []
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.reader(f)
        for row in r:
            if len(row) >= 2 and row[0].strip():
                towns.append((row[0].strip(), row[1].strip()))
    return towns

async def extract_from_page(page: Page) -> Optional[float]:
    # Wait for any <strong>, then specifically for our label if it appears.
    try:
        await page.wait_for_selector("strong", timeout=4000)
    except PWTimeoutError:
        return None
    txt = await page.evaluate(JS_EXTRACT)
    rate = parse_rate_to_float(txt)
    if rate is not None:
        return rate
    # Give the client one more beat to hydrate
    try:
        await page.wait_for_load_state("networkidle", timeout=3000)
    except PWTimeoutError:
        pass
    txt = await page.evaluate(JS_EXTRACT)
    return parse_rate_to_float(txt)

async def fetch_one(context: BrowserContext, slug: str, name: str, max_retries: int = 2) -> Tuple[str, str, Optional[float]]:
    url = BASE_URL_TPL.format(slug=slug)
    print_url = PRINT_URL_TPL.format(slug=slug)
    page = await context.new_page()
    page.set_default_timeout(8000)
    try:
        for attempt in range(max_retries + 1):
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=15000)
                rate = await extract_from_page(page)
                if rate is not None:
                    return (name, slug, rate)
            except PWTimeoutError:
                pass

            # Try the print view (often SSR and stable)
            try:
                await page.goto(print_url, wait_until="domcontentloaded", timeout=15000)
                rate = await extract_from_page(page)
                if rate is not None:
                    return (name, slug, rate)
            except PWTimeoutError:
                pass

            # Exponential backoff (polite + avoids soft throttles)
            await asyncio.sleep(0.8 * (2 ** attempt))
        return (name, slug, None)
    finally:
        await page.close()

async def run(towns_path: str, concurrency: int):
    towns = await load_towns(towns_path)
    if not towns:
        raise SystemExit("No towns loaded. towns.csv must be: slug,name (no header).")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--disable-dev-shm-usage"])
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
            java_script_enabled=True,
        )

        # Block heavy stuff but allow CSS and XHR.
        async def route_handler(route):
            t = route.request.resource_type
            if t in {"image", "font", "media"}:
                return await route.abort()
            return await route.continue_()
        await context.route("**/*", route_handler)

        sem = asyncio.Semaphore(concurrency)
        results: List[Tuple[str, str, Optional[float]]] = []

        async def worker(slug: str, name: str):
            async with sem:
                row = await fetch_one(context, slug, name)
                print(f"{row[0]} ({row[1]}): {row[2]}")
                results.append(row)

        await asyncio.gather(*(worker(slug, name) for slug, name in towns))

        # Preserve input order
        order = {slug: i for i, (slug, _) in enumerate(towns)}
        results.sort(key=lambda r: order.get(r[1], math.inf))

        with open(OUT_PATH, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["tow", "slug", "crime_rate_per_100k"])
            for name, slug, rate in results:
                w.writerow([name, slug, rate if rate is not None else None])

        await context.close()
        await browser.close()

    print(f"\nDone. Wrote {OUT_PATH}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--towns", required=True, help="CSV (no header): slug,name")
    ap.add_argument("--concurrency", type=int, default=6, help="Parallel tabs (be polite; 6–10 is safe)")
    args = ap.parse_args()
    asyncio.run(run(args.towns, args.concurrency))