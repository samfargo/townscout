"""
Climate data schema and constants.
"""
import pathlib

# PRISM months
MONTHS = (
    "jan", "feb", "mar", "apr", "may", "jun",
    "jul", "aug", "sep", "oct", "nov", "dec",
)

# Default paths
PRISM_DIR_DEFAULT = pathlib.Path("data/climate/prism/normals_1991_2020")
MINUTES_GLOB_DEFAULT = "data/minutes/*_drive_t_hex.parquet"
OUT_PARQUET_DEFAULT = pathlib.Path("out/climate/hex_climate.parquet")

# Quantization scales
TEMP_SCALE = 0.1  # tenths of °F
PPT_MM_SCALE = 0.1  # tenths of millimetres
PPT_IN_SCALE = 0.1  # tenths of inches
MM_PER_INCH = 25.4

# Climate variables
CLIMATE_VARIABLES = ("tmean", "tmin", "tmax", "ppt")

# Output schema columns (after quantization):
# - h3_id (str or int depending on H3 version)
# - res (int32)
# - temp_mean_{month}_f_q (int16) - quantized monthly mean temps in °F
# - temp_mean_ann_f_q (int16) - quantized annual mean temp
# - temp_mean_summer_f_q (int16) - quantized summer mean temp
# - temp_mean_winter_f_q (int16) - quantized winter mean temp
# - temp_max_hot_month_f_q (int16) - quantized hottest month
# - temp_min_cold_month_f_q (int16) - quantized coldest month
# - ppt_{month}_mm_q (uint16) - quantized monthly precip in mm
# - ppt_{month}_in_q (uint16) - quantized monthly precip in inches
# - ppt_ann_mm_q (uint16) - quantized annual precip in mm
# - ppt_ann_in_q (uint16) - quantized annual precip in inches
# - climate_label (str) - human-readable climate classification

