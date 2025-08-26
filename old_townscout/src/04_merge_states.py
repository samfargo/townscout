import glob, os
import pandas as pd
from src.config import H3_RES_LOW, H3_RES_HIGH

os.makedirs("state_tiles", exist_ok=True)


def merge(res: int):
    files = sorted(glob.glob(f"data/minutes/*_r{res}.parquet"))
    dfs = [pd.read_parquet(f) for f in files if os.path.getsize(f) > 0]
    out = dfs[0]
    for df in dfs[1:]:
        out = out.merge(df, on="h3", how="outer")
    for c in out.columns:
        if c != "h3":
            out[c] = out[c].astype("Int64")
    out = out.sort_values("h3").reset_index(drop=True)
    out.to_parquet(f"state_tiles/us_r{res}.parquet")
    print(f"[ok] state_tiles/us_r{res}.parquet ({len(out)})")


if __name__ == "__main__":
    merge(H3_RES_LOW)
    merge(H3_RES_HIGH) 