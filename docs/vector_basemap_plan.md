# Vector Basemap Migration Plan

## Goal
Replace the raster OpenStreetMap background with a self-hosted vector basemap without disturbing the existing T_hex + MapLibre integration, while keeping compatibility with stock OpenMapTiles (OMT) schemas and Maputnik templates. We still need **actual OMT layers**, but Planetiler gives us a lighter-weight build pipeline than the Docker-heavy OpenMapTiles workflow we attempted earlier.

---

## 1. Why Planetiler is the new pipeline

- **Planetiler + openmaptiles recipe** outputs the same schema (`transportation`, `transportation_name`, `waterway`, `iso_a2`, POI ranks, etc.) that Maputnik templates expect, but it streams straight from OSM PBF to tiles without PostGIS/MVT staging. No schema fork needed.
- **Zero docker-compose**: everything runs as a single JVM process (requires Java 21+ because the provided `planetiler-openmaptiles.jar` is compiled for class-file version 65). This removes the Postgres containers, imposm, and long-lived volumes we no longer want to maintain.
- **Deterministic artifacts**: we can emit MBTiles and/or PMTiles in one command, hash them, and drop the file beside existing `t_hex_*.pmtiles`. Re-running with the same configs gives byte-identical output, which helps when reviewing PRs.

---

## 2. Generate vector tiles with Planetiler

1. **Install prerequisites**  
   - Install a JDK 21 build (`brew install temurin21` or use `sdkman`). Verify `java -version` reports 21+, otherwise `java -jar planetiler-openmaptiles.jar --help` will fail with `UnsupportedClassVersionError`.  
   - Ensure you have 16–32 GB RAM and ~100 GB of free SSD for temporary data when building multi-state extracts. Planetiler streams aggressively but still needs large buffers.

2. **Stage the OSM extract(s)**  
   Place every `.osm.pbf` you care about under `data/osm/` (e.g., `data/osm/massachusetts.osm.pbf`). Planetiler accepts a single file or a directory; the local files keep us in control of data currency.

3. **Run Planetiler**  
   Use the openmaptiles recipe jar that already lives in the repo root:

   ```bash
   export PLANETILER_TMP=/Users/sam/vicinity/tmp/planetiler
   export PLANETILER_CACHE=/Users/sam/vicinity/cache/planetiler
   mkdir -p "$PLANETILER_TMP" "$PLANETILER_CACHE"

   java -Xmx24g -jar planetiler-openmaptiles.jar \
     --osm-path=data/osm/massachusetts.osm.pbf \
     --area=us/massachusetts \
     --tmp=$PLANETILER_TMP \
     --cache=$PLANETILER_CACHE \
     --output=/Users/sam/vicinity/tiles/vicinity_basemap.pmtiles \
     --force
   ```

   - `--area` controls which baked-in bounds Planetiler uses for label languages and background features; match the Geofabrik area of your input.  
   - Planetiler infers the desired container from the `--output` extension (`.pmtiles` vs `.mbtiles`), so you typically only need one flag. If you also want MBTiles, either rerun the job with `--output=/Users/sam/vicinity/tiles/vicinity_basemap.mbtiles` or convert afterward with `pmtiles convert`. Run `java -jar planetiler-openmaptiles.jar --help` after installing Java 21 to see every flag (bounds overrides, zoom limits, drop/keep layers, etc.).
   - Prefer `make vector_basemap` (wraps `scripts/build_vector_basemap.sh`) for repeatable local builds. Override `PLANETILER_OSM`, `PLANETILER_AREA`, `PLANETILER_OUTPUT`, or `PLANETILER_HEAP` at invocation time instead of editing the script:

     ```bash
     PLANETILER_OSM=data/osm/massachusetts.osm.pbf \
     PLANETILER_AREA=us/massachusetts \
     PLANETILER_HEAP=32g \
     make vector_basemap
     ```

4. **Resource expectations**  
   Massachusetts finishes in ~10–15 minutes on an M3 Max with 24 GB heap. Multi-state runs scale roughly linearly with input size because the recipe avoids the heavy SQL post-processing we had before.

5. **Result**  
   You now have `tiles/vicinity_basemap.mbtiles` (optional) and `tiles/vicinity_basemap.pmtiles`, both containing canonical OMT layers ready for MapLibre.

---

## 3. PMTiles handling (if you only produced MBTiles)

Planetiler can already emit PMTiles as shown above. If you ever need to convert a legacy MBTiles file, keep the `pmtiles` CLI flow for parity with existing instructions:

```bash
npx pmtiles convert data/tiles/vicinity_basemap.mbtiles tiles/vicinity_basemap.pmtiles
```

Store the PMTiles output under `/Users/sam/vicinity/tiles/` beside the `t_hex_*.pmtiles` archives so the app can keep using the registered `pmtiles:///` protocol:

```json
"url": "pmtiles:///tiles/vicinity_basemap.pmtiles"
```

---

## 4. Style with Maputnik (two supported workflows)

Maputnik still cannot read `pmtiles://` directly. Use either workflow:

### Option A – Local PMTiles server

1. Serve the PMTiles file:
   ```bash
   cd /Users/sam/vicinity
   npx pmtiles serve tiles/vicinity_basemap.pmtiles
   ```
   This exposes TileJSON + vector tiles at `http://localhost:8080`.

2. In Maputnik, define the source as:
   ```json
   "vicinity-basemap": {
     "type": "vector",
     "tiles": ["http://localhost:8080/{z}/{x}/{y}.pbf"],
     "minzoom": 0,
     "maxzoom": 14
   }
   ```

3. Design the style (Bright/Voyager templates load fine because the schema matches).  
   After exporting, swap the source definition to use `pmtiles:///tiles/vicinity_basemap.pmtiles` for the app.

### Option B – Style against a public OMT endpoint

1. Point Maputnik at `https://maps.tilehosting.com/data/v3.json?key=YOUR_KEY` (or any public OMT-compatible endpoint).  
2. Build the style, export JSON.  
3. Replace the source with the local PMTiles URL before shipping:
   ```json
   "vicinity-basemap": {
     "type": "vector",
     "url": "pmtiles:///tiles/vicinity_basemap.pmtiles",
     "attribution": "© OpenStreetMap contributors"
   }
   ```

This option requires no local tile server; you just rely on schema parity. Maputnik cannot read PMTiles directly, so the swap back to `pmtiles:///` happens after export.
- Stick to OMT-friendly templates (Klokantech Basic, Positron, OSM Bright OMT, or your own vetted styles such as `basic.json`). Random Maputnik gallery styles often assume Mapbox Streets schemas and will surface spurious errors.

---

## 5. Glyphs and sprites

- For a free external option of Maptiler glyphs, use `https://demotiles.maplibre.org/font/{fontstack}/{range}.pbf`.  
- Long-term, host fonts/sprites under `/tiles/web/public/fonts/` and reference them with relative URLs to avoid third-party dependencies.  
- Whatever you choose, ensure the style JSON points to reachable glyphs before deploying; MapLibre will log errors otherwise.
- If your style uses icons, ensure sprite is either a valid public URL or host `/sprites/sprite.json` and `/sprites/sprite.png` locally. MapLibre will not render icons without it.

---

## 6. Wire the style into the frontend (without breaking hex layers)

1. Add the exported style to `tiles/web/public/basemaps/vicinity.json`.
2. Update `createBaseStyle()` so it loads the JSON and merges the existing T_hex sources/layers:

```ts
async function loadMergedStyle() {
  const basemapStyle: StyleSpecification = await fetch('/basemaps/vicinity.json').then((r) => r.json());

  const hexSources = {
    't_hex_r8_drive': { type: 'vector', url: 'pmtiles:///tiles/t_hex_r8_drive.pmtiles' },
    't_hex_r7_drive': { type: 'vector', url: 'pmtiles:///tiles/t_hex_r7_drive.pmtiles' }
  };

  const hexLayers: StyleSpecification['layers'] = [/* existing hex layer definitions */];

  return {
    ...basemapStyle,
    sources: { ...basemapStyle.sources, ...hexSources },
    layers: [...(basemapStyle.layers ?? []), ...hexLayers]
  };
}
```

3. In `MapController.init`, call `loadMergedStyle()` before instantiating MapLibre:

```ts
const style = await loadMergedStyle();
const map = new MapCtor({ container, style, /* existing camera constraints */ });
```

4. Keep the PMTiles protocol registration untouched; the basemap and hex sources all use `pmtiles:///`.

This pattern ensures the basemap is purely JSON-driven, while runtime-generated layers (hex fills, pins) continue to register programmatically.
