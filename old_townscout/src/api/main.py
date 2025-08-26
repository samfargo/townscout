import os
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import redis
from dotenv import load_dotenv

load_dotenv()
app = FastAPI(title="TownScout API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
CITY_CSV = os.getenv("CITY_LOOKUP_CSV", "src/api/city_lookup.csv")
CITIES = pd.read_csv(CITY_CSV)  # columns: name,state,zip,lat,lon,h3

# Preload: for a few common thresholds, precompute Redis sets: key e.g. ts:chipotle:15 contains H3s with <= 15 min
# (Run a separate builder script offline; omitted for brevity.)

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/rank")
def rank(
    chipotle: int = Query(15, ge=1, le=120),
    costco: int = Query(20, ge=1, le=120),
    limit: int = Query(20, ge=1, le=100)
):
    k1 = f"ts:chipotle:{chipotle}"
    k2 = f"ts:costco:{costco}"
    # intersect H3 sets in Redis
    tmpkey = f"tmp:int:{chipotle}:{costco}"
    r.sinterstore(tmpkey, k1, k2)
    cells = list(r.smembers(tmpkey))
    r.delete(tmpkey)

    # Map H3 to cities (left join via precomputed mapping)
    df = CITIES[CITIES["h3"].isin([c.decode() if isinstance(c, bytes) else c for c in cells])]
    # simple score: sum of minutes (lower is better)
    df = df.assign(score=df["chipotle_drive_min"] + df["costco_drive_min"])\
           .sort_values("score").head(limit)
    return df.to_dict(orient="records") 