import pandas as pd

if __name__ == "__main__":
    for res in [7, 8]:
        df = pd.read_parquet(f"state_tiles/us_r{res}.parquet")
        df.to_csv(f"state_tiles/us_r{res}.csv", index=False)
        print(f"[ok] CSV us_r{res}.csv") 