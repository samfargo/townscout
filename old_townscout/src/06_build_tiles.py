import os, subprocess

GJ7 = "tiles/us_r7.geojson"
GJ8 = "tiles/us_r8.geojson"
MB7 = "tiles/us_r7.mbtiles"
MB8 = "tiles/us_r8.mbtiles"
PM7 = "tiles/us_r7.pmtiles"
PM8 = "tiles/us_r8.pmtiles"

os.makedirs("tiles", exist_ok=True)

def run(cmd):
    print("$", " ".join(cmd)); subprocess.check_call(cmd)

# Clean existing outputs for idempotency
for path in [MB7, MB8, PM7, PM8]:
    if os.path.exists(path):
        os.remove(path)

# Build MBTiles
run(["tippecanoe", "-o", MB7, "-zg", "--drop-densest-as-needed", "--no-feature-limit", "--no-tile-size-limit", "--layer=us_r7", GJ7])
run(["tippecanoe", "-o", MB8, "-zg", "--drop-densest-as-needed", "--no-feature-limit", "--no-tile-size-limit", "--layer=us_r8", GJ8])

# Convert to PMTiles (requires pmtiles CLI in PATH)
run(["pmtiles", "convert", MB7, PM7])
run(["pmtiles", "convert", MB8, PM8]) 