# TownScout Architecture Notes

## Frontend-Backend Integration Points

### Static File Serving Strategy
The frontend requires both static assets and data tiles. FastAPI serves multiple static directories:

```python
# In api/app/main.py
app.mount("/static", StaticFiles(directory="tiles/web"), name="static")  # Frontend assets
app.mount("/tiles", StaticFiles(directory="tiles"), name="tiles")        # PMTiles data
```

**Key Insight**: Separate mounts allow different caching policies and access patterns for UI assets vs. map data.

### PMTiles Protocol Integration
The frontend uses a local PMTiles library to handle the `pmtiles://` protocol:

```javascript
// Frontend loads local copy instead of CDN
let protocol = new pmtiles.Protocol();
maplibregl.addProtocol("pmtiles", protocol.tile);

// Tiles referenced with pmtiles:// URLs resolve to /tiles/ static route
const T_HEX_R8_URL = "pmtiles:///tiles/t_hex_r8_drive.pmtiles";
```

**Key Insight**: The `pmtiles://` protocol automatically handles tile requests and caching, while the static route provides the underlying file access.

## Data Pipeline Architecture

### H3 Data Type Preservation
Critical lesson: **Pandas `.iterrows()` corrupts uint64 H3 IDs by converting to float64**.

```python
# WRONG - loses precision
for _, row in df.iterrows():
    h3_val = row['h3_id']  # uint64 → float64 → scientific notation

# CORRECT - preserves types  
for i, row in enumerate(df.itertuples(index=False)):
    h3_col_idx = df.columns.get_loc('h3_id')
    h3_val = row[h3_col_idx]  # Preserves uint64
```

**Key Insight**: Large integer IDs require careful handling in pandas to avoid precision loss.

### H3 Library Version Compatibility
H3 has breaking API changes between versions. Use defensive programming:

```python
# Graceful fallback pattern
try:
    # Try H3 v3 API
    to_boundary = h3.h3_to_geo_boundary
    int_to_str = h3.int_to_string
except Exception:
    try:
        # Try H3 v3 alternate import
        from h3.api.basic_int import h3 as h3v4
        to_boundary = h3v4.h3_to_geo_boundary
    except Exception:
        # Fall back to H3 v4 API
        to_boundary = h3.cell_to_boundary
        int_to_str = h3.int_to_str
```

**Key Insight**: Don't simplify compatibility code even if it seems redundant - different environments have different H3 versions.

### Multi-Resolution Tile Strategy
The frontend uses zoom-level switching between r7 (overview) and r8 (detail) tiles:

```javascript
// Different tile sources for different zoom levels
't_hex_r7': {
  type: 'vector',
  url: "pmtiles:///tiles/t_hex_r7_drive.pmtiles",
  minzoom: 0,
  maxzoom: 8
},
't_hex_r8': {
  type: 'vector', 
  url: "pmtiles:///tiles/t_hex_r8_drive.pmtiles",
  minzoom: 8,
  maxzoom: 14
}
```

**Key Insight**: This provides responsive performance - fewer, larger hexes for overview, more detailed hexes for local views.

## Build Process Dependencies

### Required External Tools
- **tippecanoe**: Converts GeoJSON → MBTiles → PMTiles
- **pmtiles CLI**: Converts MBTiles → PMTiles format
- **curl**: Downloads external JS libraries locally

### Critical Build Order
1. **T_hex computation** → Parquet files with H3 travel times
2. **GeoJSON conversion** → Vector features with H3 geometries  
3. **PMTiles creation** → Compressed map tiles
4. **Static file setup** → Local JS libraries + API routes

**Key Insight**: Each step depends on the previous, but failures often manifest in later steps (e.g., H3 data corruption appears during GeoJSON conversion).

## Development vs Production Considerations

### Local Development Setup
- Local JS library copies (avoid CDN issues)
- FastAPI static file serving (simple deployment)
- Direct file access for tiles (no CDN complexity)

### Production Scaling Points  
- **Static assets**: Move to CDN (CloudFront, etc.)
- **PMTiles**: Serve from object storage (S3) with proper CORS
- **API separation**: Decouple backend services from static serving
- **Caching**: Add Redis for D_anchor API responses

## Error Patterns to Watch For

### Frontend Issues
1. **404s on JS libraries** → Download locally
2. **Blank map with JS errors** → Check tile accessibility
3. **"pmtiles not defined"** → Verify script loading order

### Backend Issues  
1. **Route 404s after changes** → Restart server
2. **CORS errors** → Check allow_origins configuration
3. **File not found** → Verify static mount paths

### Data Issues
1. **Scientific notation in H3 IDs** → Use .itertuples()
2. **H3 function errors** → Check library compatibility  
3. **Invalid H3 cells** → Verify data type preservation

## Future Architecture Improvements

### Pipeline Robustness
- Add data validation at each pipeline step
- Implement checksum verification for large files
- Add retry logic for network-dependent operations

### Performance Optimization
- Lazy-load tile layers based on viewport
- Implement tile caching strategy
- Add compression for API responses

### Scalability Considerations
- Partition data by geographic regions
- Implement incremental tile updates
- Add monitoring for tile request patterns

---

*These notes capture architectural decisions and lessons learned during development. Update as the system evolves.*
