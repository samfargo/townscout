// =================================================================
// TownScout: Anchor-Matrix Frontend Logic
// =================================================================

// =================================================================
// 1. Constants & Configuration
// =================================================================
const K_ANCHORS = 4;
const UNREACH_U16 = 65535;
const ACTIVE_DATASET = "massachusetts_drive";

let CONFIG = {
  filters: [],
  dataset: {},
};

let dAnchorCache = new Map(); // Cache for D_anchor data, Map<categoryId, data>

// =================================================================
// 2. MapLibre Initialization
// =================================================================
let map; // Global map object

function initializeMap(tileUrl) {
  const protocol = new pmtiles.Protocol();
  maplibregl.addProtocol("pmtiles", protocol.tile);

  map = new maplibregl.Map({
    container: 'map',
    style: {
      version: 8,
      sources: {
        'osm': {
          type: 'raster',
          tiles: ['https://tile.openstreetmap.org/{z}/{x}/{y}.png'],
          tileSize: 256,
          attribution: '© OpenStreetMap contributors'
        },
        't_hex': {
          type: 'vector',
          url: tileUrl,
          attribution: '© TownScout'
        }
      },
      layers: [
        { id: 'osm-base', type: 'raster', source: 'osm', paint: { 'raster-opacity': 0.7 } },
        {
          id: 't_hex_layer',
          type: 'fill',
          source: 't_hex',
          'source-layer': 'massachusetts_hex_to_anchor_drive', // This may need to be dynamic
          paint: {
            'fill-color': 'rgba(76, 175, 80, 0.9)',
            'fill-outline-color': 'rgba(0, 0, 0, 0.1)',
            'fill-opacity': 0.05, // Start with low opacity, filter will control final
          }
        }
      ]
    },
    center: [-71.06, 42.36], // Boston
    zoom: 10
  });

  map.on('load', onMapLoad);
}

// =================================================================
// 3. Core Logic: Data Fetching & Filtering
// =================================================================

async function loadAppConfig() {
  try {
    const [filtersResponse, manifestResponse] = await Promise.all([
      fetch('/schemas/filters.catalog.json'),
      fetch('/schemas/tiles.manifest.json')
    ]);
    if (!filtersResponse.ok || !manifestResponse.ok) {
      throw new Error('Failed to load configuration files.');
    }
    const filtersCatalog = await filtersResponse.json();
    const tilesManifest = await manifestResponse.json();

    CONFIG.filters = filtersCatalog.filters;
    CONFIG.dataset = tilesManifest.datasets[ACTIVE_DATASET];
    console.log("[Config] Loaded:", CONFIG);
  } catch (error) {
    console.error("Error loading application config:", error);
    document.getElementById('status').textContent = "Error: Could not load app configuration.";
  }
}

async function fetchAllDAnchorData() {
  const requests = CONFIG.filters.map(filter => {
    const url = CONFIG.dataset.d_anchor_api_base.replace('{categoryId}', filter.categoryId);
    return fetch(url).then(res => {
      if (!res.ok) throw new Error(`Failed for category ${filter.categoryId}`);
      return res.json();
    }).then(data => {
      dAnchorCache.set(filter.categoryId, data);
      console.log(`[D_anchor] Loaded for ${filter.name}`);
    });
  });

  try {
    await Promise.all(requests);
  } catch (error) {
    console.error("Error fetching D_anchor data:", error);
    document.getElementById('status').textContent = "Error: Could not load filter data.";
  }
}

function updateMapFilter() {
  const conditions = [];

  for (const filter of CONFIG.filters) {
    if (dAnchorCache.has(filter.categoryId)) {
      const dAnchorData = dAnchorCache.get(filter.categoryId);
      const thresholdSeconds = +document.getElementById(filter.id).value * 60;

      const minOperands = [];
      for (let i = 0; i < K_ANCHORS; i++) {
        const anchorIdExpr = ["get", `a${i}_id`];
        const anchorSecsExpr = ["get", `a${i}_s`];
        const dAnchorLookup = [
          "match",
          anchorIdExpr,
          ...Object.entries(dAnchorData).flatMap(([k, v]) => [parseInt(k), v]),
          UNREACH_U16
        ];
        minOperands.push(["+", anchorSecsExpr, dAnchorLookup]);
      }
      const travelTimeExpression = ["min", ...minOperands];

      conditions.push(["<=", travelTimeExpression, thresholdSeconds]);
    }
  }

  const visibilityExpression = [
    "case",
    ["all", ...conditions],
    0.9, // Visible
    0.05 // Masked
  ];
  
  map.setPaintProperty('t_hex_layer', 'fill-opacity', visibilityExpression);
  console.log("[Filter] Map filter updated.");
}

// =================================================================
// 4. UI Management
// =================================================================

function populateFilterPanel() {
  const panel = document.getElementById('filter-panel-body');
  panel.innerHTML = ''; // Clear existing
  
  for (const filter of CONFIG.filters) {
    const { id, name, params } = filter;
    const row = document.createElement('div');
    row.className = 'row';
    row.innerHTML = `
      <label>${name} ≤ min</label>
      <input id="${id}" type="range" min="${params.min}" max="${params.max}" step="${params.step}" value="${params.initial}">
      <span id="${id}_v">${params.initial}</span>
    `;
    panel.appendChild(row);
  }
}

function initUIEventListeners() {
  CONFIG.filters.forEach(filter => {
    const slider = document.getElementById(filter.id);
    const output = document.getElementById(`${filter.id}_v`);
    
    slider.addEventListener('input', () => {
      output.textContent = slider.value;
    });
    slider.addEventListener('change', () => {
      updateMapFilter();
    });
  });

  document.getElementById('share').addEventListener('click', () => {
    const params = new URLSearchParams();
    CONFIG.filters.forEach(f => params.set(f.id, document.getElementById(f.id).value));
    const url = new URL(window.location.href);
    url.search = params.toString();
    navigator.clipboard.writeText(url.toString()).then(() => {
      const btn = document.getElementById('share');
      const originalText = btn.textContent;
      btn.textContent = 'Copied!';
      setTimeout(() => (btn.textContent = originalText), 1500);
    });
  });
}

function loadStateFromURL() {
  const params = new URLSearchParams(window.location.search);
  CONFIG.filters.forEach(filter => {
    if (params.has(filter.id)) {
      const value = params.get(filter.id);
      document.getElementById(filter.id).value = value;
      document.getElementById(`${filter.id}_v`).textContent = value;
    }
  });
}

// =================================================================
// 5. Application Lifecycle
// =================================================================

async function onMapLoad() {
  console.log("Map loaded.");
  document.getElementById('status').textContent = "Fetching filter data...";
  await fetchAllDAnchorData();
  document.getElementById('status').textContent = "Ready.";
  
  loadStateFromURL();
  updateMapFilter(); // Initial filter application
}

async function main() {
  document.getElementById('status').textContent = "Loading configuration...";
  await loadAppConfig();

  if (CONFIG.dataset.t_hex_url) {
    populateFilterPanel();
    initUIEventListeners();
    initializeMap(CONFIG.dataset.t_hex_url);
  }
}

// Entry point
document.addEventListener('DOMContentLoaded', main);
