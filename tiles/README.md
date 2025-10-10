# Townscout Tiles

- Vector tiles inherit every column from `state_tiles/us_r{res}.parquet`. Fields ending with `_q` are quantized; divide by 10 to decode temperatures (°F) and precipitation (mm or inches, per suffix).

