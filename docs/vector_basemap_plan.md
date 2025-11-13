# Vector Basemap Migration Plan

## Goal
Replace the raster OpenStreetMap background with a self-hosted vector basemap without disturbing the existing T_hex + MapLibre integration, while keeping compatibility with stock OpenMapTiles (OMT) schemas and Maputnik templates. That means we must emit **actual OMT tiles** and keep the styling workflow unchanged; lightweight “OMT-ish” pipelines (tilemaker) are out unless we completely own the schema and style.

---

## 1. Why the tooling choice matters

- **OpenMapTiles (OMT) pipeline** produces the canonical schema (`transportation`, `transportation_name`, `waterway`, `boundary`, `iso_a2`, consistent `class/subclass`, POI ranks, etc.). Maputnik templates such as Bright/Voyager assume these layers verbatim, so they “just work.” OMT v3 schema matches the common Maputnik templates, provided you select OMT-compatible examples (e.g., Klokantech Basic, Positron, OSM Bright OMT version).
- **Tilemaker + `process-openmaptiles.lua`** only approximates OMT; out of the box it skips `transportation_name`, `iso_a2`, and several other fields/layers, and renames classification enums. You can wire some fields back in, but you are effectively forking the schema and must rewrite the style.
- **Decision**: We want to stay compatible with stock OMT styles and Maputnik templates without custom surgery, so we will run the full OpenMapTiles pipeline. Tilemaker is only viable if we are willing to own a bespoke schema + style and abandon template reuse.

---

## 2. Generate MBTiles using OpenMapTiles

1. **Clone and prepare**  
   ```bash
   git clone https://github.com/openmaptiles/openmaptiles.git
   cd openmaptiles
   ```
   Place the same PBF extracts you already download (e.g., `data/osm/massachusetts.osm.pbf`) into `./data/`.

2. **Resource expectations**  
   `./quickstart.sh` spins up PostGIS, runs imposm imports, vector tile post-processing, and Mapnik rendering helpers. For a single U.S. state expect ~30‑60 minutes and several GB of RAM/disk. Multi-state or national datasets can take hours. Plan to run this on a beefy dev machine or cloud VM; it’s not a “2-minute script on a MacBook Air.” (This work happens offline, so no runtime latency impact for end users.)

3. **Limit geography**  
   - Do not run for the full planet; only the United States. OpenMapTiles will clip vector tiles to the union of all .osm.pbf files in ./data. If you place only US PBFs, the output covers only US + surrounding coastline buffers, which is what we want.

4. **Result**  
   The pipeline writes `./data/tiles.mbtiles` containing the full OMT schema for your region.

---

## 3. Convert to PMTiles for hosting

After `tiles.mbtiles` is ready:

```bash
npx pmtiles convert data/tiles.mbtiles /Users/sam/vicinity/tiles/vicinity_basemap.pmtiles
```

- Keep the PMTiles file alongside existing `t_hex_*.pmtiles` assets so the already-registered `pmtiles` protocol (`MapController.init` in `tiles/web/lib/map/MapController.ts`) can serve it.
- When referencing the basemap inside MapLibre, always use the `pmtiles:///` URL so you benefit from protocol-based range requests:
  ```json
  "url": "pmtiles:///tiles/vicinity_basemap.pmtiles"
  ```

---

## 4. Style with Maputnik (two supported workflows)

Maputnik does **not** understand `pmtiles://` by itself. Use one of the following paths:

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

This option requires no local tile server; you just rely on schema parity.
Maputnik cannot read PMTiles directly. You must use a temporary HTTP tile server or a public OMT endpoint during design.

---

## 5. Glyphs and sprites

- For a free external option of Maptiler glyphs, use `https://demotiles.maplibre.org/font/{fontstack}/{range}.pbf`.  
- Long-term, host fonts/sprites under `/tiles/web/public/fonts/` and reference them with relative URLs to avoid third-party dependencies.  
- Whatever you choose, ensure the style JSON points to reachable glyphs before deploying; MapLibre will log errors otherwise.
- If your style uses icons, ensure sprite is either a valid public URL or host /sprites/sprite.json and /sprites/sprite.png locally. MapLibre will not render icons without it.

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
