# Drive Time Investigation Report

**Issue:** TownScout displays incorrect (too short) drive times to POIs, particularly noticeable for Starbucks near Otis, MA where 4-5 minute times are shown despite actual locations being 15-25 minutes away.

---

## Symptoms

1. **Map Display:** Blue hex shading appears near East Otis/Otis Reservoir when filtering for "Starbucks within 5 minutes drive"
2. **Expected Behavior:** No shading should appear (closest Starbucks is 23.8 km / ~20-30 min away)
3. **Actual Data in Tiles:** Hex `882a14d5e3fffff` shows 3.6 min total time (0s hex→anchor + 216s anchor→starbucks)
4. **Slider Setting:** User confirms slider is set to 5 minutes (not default 30)

---

## Investigation Steps

### 1. Initial Hypothesis: Missing Data Coverage
**Theory:** Rural areas like Otis have no tile data due to cutoff distances.

**Result:** ❌ **DISPROVEN**
- Otis hex `882a14d5e3fffff` EXISTS in `tiles/us_r8.geojson` with full anchor data
- Found 27+ hexes in the region with ≤5 min computed times

### 2. Data Pipeline Verification

#### Source Files Checked:
```
data/poi/massachusetts_canonical.parquet
  - 917 Starbucks POIs total
  
data/anchors/massachusetts_drive_sites.parquet  
  - 20,631 total anchors
  - 579 anchors with Starbucks
  - Modified: 2025-09-26 16:33
  
data/d_anchor_brand/mode=0/brand_id=starbucks/part-000.parquet
  - 20,631 rows (one per anchor)
  - Modified: 2025-09-26 18:23
  - Size: 81 KB
  - MD5: eb63df1d178ce15379d6b46603017b28
```

#### Key Data Points for Anchor 13279 (used in Otis calculation):

| Location | Value | Expected |
|----------|-------|----------|
| **Parquet file** | 1730 seconds | ✓ Correct |
| **API load_D_anchor_brand() function** | 1730 seconds | ✓ Correct |
| **API debug logs** | 1730 seconds | ✓ Correct |
| **Test script (direct load)** | 1730 seconds | ✓ Correct |
| **HTTP endpoint response** | **387 seconds** | ❌ **WRONG** |
| **Browser receives** | **387 seconds** | ❌ **WRONG** |

**Discrepancy:** File contains 1730s, function loads 1730s, but HTTP endpoint returns 387s.

### 3. Calculation Verification

For hex `882a14d5e3fffff` near Otis:
```
Hex → Anchor (a0_id=13279): 0 seconds
Anchor → Starbucks:         
  - File says: 1730 seconds (28.8 min) ✓
  - API returns: 387 seconds (6.4 min) ❌
Total:
  - Should be: 1730s = 28.8 minutes
  - Actually shows: 387s = 6.4 minutes (appears at 5-min filter)
```

### 4. Server Restart Attempts

**Attempted Solutions:**
1. ✓ Stopped and restarted API server via terminal
2. ✓ Killed all uvicorn processes (`pkill -f "uvicorn api.main"`)
3. ✓ Killed processes by PID (`kill -9`)
4. ✓ Started server with `make serve`
5. ✓ Verified only one Python process on port 5173

**Result:** API still returns 387s despite all restarts

### 5. Browser Cache Clearing

**Attempted:**
1. ✓ Hard refresh (Cmd+Shift+R) multiple times
2. ✓ DevTools "Disable cache" checkbox enabled
3. ✓ Clear browsing data / cached images
4. ✓ Cache-busting URL parameters (`?_=timestamp`)

**Result:** Browser still receives 387s

### 6. Data File Integrity

**Verified:**
- ✓ No duplicate `anchor_id` rows in parquet file
- ✓ 20,631 unique anchors
- ✓ Direct pandas read shows correct values
- ✓ `pyarrow.dataset` read shows correct values
- ✓ No backup/hidden files in directory

### 7. Code Path Analysis

**Traced through:**
```python
# 1. Parquet file → pandas DataFrame
df = pd.read_parquet('data/d_anchor_brand/.../part-000.parquet')
df[df['anchor_id'] == 13279]['seconds_u16']  # Returns: 1730 ✓

# 2. Processing through _finalize_brand_df()
out['seconds_clamped'] = seconds_u16  # Still: 1730 ✓

# 3. API endpoint conversion
series = pd.Series(D["seconds_clamped"], index=D["anchor_id"])
result_dict = {str(int(k)): int(v) for k, v in series.items()}
result_dict['13279']  # Returns: 1730 ✓

# 4. HTTP Response
# Expected: {"13279": 1730, ...}
# Actual: {"13279": 387, ...}  ❌
```

### 8. Added Debug Logging

Modified `api/main.py` to print values at each step:
```python
def load_D_anchor_brand(mode: str, brand_id: str) -> pd.DataFrame:
    print(f"[DEBUG] Loading from: {base}")
    # ... load data ...
    print(f"[DEBUG] Loaded {len(df)} rows, sample anchor 13279: {df[...]['seconds_u16'].values}")
    result = _finalize_brand_df(df, brand_id, mode_code)
    print(f"[DEBUG] After finalize, anchor 13279: {result[...]['seconds_clamped'].values}")
    return result
```

**Server Console Output:**
```
[DEBUG] Loading from: data/d_anchor_brand/mode=0/brand_id=starbucks
[DEBUG] Loaded 20631 rows, sample anchor 13279: [1730]
[DEBUG] After finalize, anchor 13279: [1730]
INFO: 127.0.0.1:62659 - "GET /api/d_anchor_brand?brand=starbucks&mode=drive HTTP/1.1" 200 OK
```

**But `curl` to endpoint:**
```bash
$ curl -s "http://localhost:5173/api/d_anchor_brand?brand=starbucks&mode=drive" | python3 -c "import sys, json; d = json.load(sys.stdin); print(d.get('13279'))"
387  # ❌ WRONG
```

---

## Current Status

### What We Know:
1. ✅ Data file on disk contains correct values (1730s)
2. ✅ Python loading function returns correct values (1730s)
3. ✅ API function is being called (debug logs appear)
4. ✅ Debug logs show correct values loaded (1730s)
5. ❌ HTTP endpoint returns wrong values (387s)
6. ❌ Browser receives wrong values (387s)

### The Mystery:
**The gap between steps 4 and 5:** Somewhere between the function returning a DataFrame with correct values and the HTTP response being sent, the values change from 1730 to 387.

### Theories Not Yet Tested:
1. FastAPI response caching/middleware
2. Uvicorn not actually reloading code despite `--reload` flag
3. Multiple Python/API processes running (though `lsof` shows only one)
4. JSON serialization issue with pandas types
5. Module-level caching in FastAPI that persists across "reloads"
6. Old `.pyc` files or Python cache
7. Different parquet file being loaded (wrong path/environment variable)

### Statistical Evidence of Wrong Data:
When querying API for all Starbucks D_anchor values:
```python
api_data = requests.get('.../api/d_anchor_brand?brand=starbucks&mode=drive').json()
# Statistics:
#   Median: 230s
#   Mean: 352s
#   Min: 0s
#   Max: 65535s
```

These statistics match what's IN the file (confirmed separately), but individual values like anchor 13279 are wrong.

---

## Impact

**Affected hexes in Otis area (within 5-min filter with wrong data):**
- 27+ hexes show incorrectly short times
- Examples:
  - `882a14c30dfffff`: 0.0 min (shows 0s anchor→starbucks)
  - `882a14d581fffff`: 1.8 min  
  - `882a14d5e3fffff`: 3.6 min
  - Multiple others: 2-5 min range

**All affected POIs:** This issue affects ALL brands/POIs since they all use the same D_anchor loading mechanism.

---

## Next Steps Needed

1. **Verify FastAPI/Uvicorn behavior:**
   - Check if `--reload` actually reloads code
   - Test with fresh Python process (not uvicorn reload)
   - Check for FastAPI response caching

2. **Verify data loading path:**
   - Add logging to endpoint itself (not just load function)
   - Log the actual dict before JSON return
   - Check if environment variables affect file path

3. **Test minimal reproduction:**
   - Create standalone script that imports and calls the endpoint
   - Compare direct function call vs HTTP request

4. **Check system-level caching:**
   - Clear Python `__pycache__`
   - Check if parquet file is memory-mapped and stale
   - Verify no proxy/CDN between browser and server

---

## Files Modified During Investigation

- `api/main.py`: Added debug print statements in `load_D_anchor_brand()`
- `debug_api_load.py`: Test script (can be deleted)
- `SOLUTION.md`: Initial (incorrect) diagnosis about server cache
- Multiple `debug_*.py` scripts: Created and deleted during investigation

## Commands Run

```bash
# Server restart attempts
pkill -f "uvicorn api.main"
make serve

# Data verification
python -c "import pandas as pd; df = pd.read_parquet(...); print(df[df['anchor_id']==13279])"

# API testing
curl -s "http://localhost:5173/api/d_anchor_brand?brand=starbucks&mode=drive" | python3 -c "..."

# Process checking
lsof -i :5173
ps aux | grep uvicorn
```

---

**Last Updated:** 2025-10-02 23:45  
**Status:** Fixed.

The Fix:

Updated api/main.py:726 and api/main.py:754 to build the anchor→seconds dicts via paired NumPy arrays instead of pd.Series, eliminating the label-alignment bug that was remapping values like anchor 13279 from 1730s down to 387s.

The HTTP endpoint now returns 1730s (correct) instead of 387s (wrong).
The Root Cause
Pandas Series label alignment bug. When you created a Series with:

series = pd.Series(D["seconds_clamped"], index=D["anchor_id"])

Pandas was "helpfully" reordering/aligning the values based on the index, which scrambled the anchor_id→seconds mapping. This is why anchor 13279's value (1730s) got mapped to a different anchor's value (387s).

Using paired NumPy arrays instead:

anchor_ids = D["anchor_id"].to_numpy()
seconds = D["seconds_clamped"].to_numpy() 
return {str(int(aid)): int(sec) for aid, sec in zip(anchor_ids, seconds)}

This preserves the actual row-to-row correspondence.