#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JAR_PATH="${ROOT_DIR}/planetiler-openmaptiles.jar"

usage() {
  cat <<'EOF'
build_vector_basemap.sh --osm-path <file|dir> --area <slug> [options]

Required Flags
  --osm-path PATH        Path to an .osm.pbf file or directory containing them.
  --area     SLUG        Planetiler area hint (e.g., us/massachusetts).

Optional Flags
  --output   PATH        Output PMTiles/MBTiles path (default: tiles/vicinity_basemap.pmtiles).
  --heap     SIZE        Java heap size passed to -Xmx (default: 24g).
  --tmp      PATH        Scratch directory (default: $PLANETILER_TMP or tmp/planetiler).
  --cache    PATH        Download cache (default: $PLANETILER_CACHE or cache/planetiler).
  --force                 Pass --force to Planetiler (overwrite outputs).
  --help                  Show this message.

Any additional flags are forwarded directly to Planetiler.
EOF
}

require_arg() {
  local name="$1" value="$2"
  if [[ -z "${value}" ]]; then
    echo "[error] Missing required flag ${name}" >&2
    usage
    exit 1
  fi
}

ensure_java() {
  if ! command -v java >/dev/null 2>&1; then
    echo "[error] java not found. Install JDK 21+ (e.g., brew install temurin21)." >&2
    exit 1
  fi
  local version_raw major_part
  version_raw=$(java -version 2>&1 | awk -F '"' 'NR==1 {print $2}')
  if [[ -z "${version_raw}" ]]; then
    echo "[warn] Unable to parse java -version output; continuing anyway." >&2
    return
  fi
  major_part="${version_raw%%.*}"
  if [[ "${major_part}" == "1" ]]; then
    major_part=$(echo "${version_raw}" | cut -d. -f2)
  fi
  if [[ "${major_part}" =~ ^[0-9]+$ ]] && (( major_part < 21 )); then
    echo "[error] Java ${version_raw} detected; Planetiler jar requires 21+." >&2
    exit 1
  fi
}

OSM_PATH=""
AREA_SLUG=""
OUTPUT_PATH="${ROOT_DIR}/tiles/vicinity_basemap.pmtiles"
JAVA_HEAP="${PLANETILER_HEAP:-24g}"
TMP_DIR="${PLANETILER_TMP:-${ROOT_DIR}/tmp/planetiler}"
CACHE_DIR="${PLANETILER_CACHE:-${ROOT_DIR}/cache/planetiler}"
FORCE_FLAG=""
EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --osm-path)
      OSM_PATH="${2:-}"
      shift 2
      ;;
    --area)
      AREA_SLUG="${2:-}"
      shift 2
      ;;
    --output)
      out_value="${2:-}"
      if [[ -z "${out_value}" ]]; then
        echo "[error] --output requires a path" >&2
        exit 1
      fi
      if [[ "${out_value}" = /* ]]; then
        OUTPUT_PATH="${out_value}"
      else
        OUTPUT_PATH="${ROOT_DIR}/${out_value}"
      fi
      shift 2
      ;;
    --heap)
      JAVA_HEAP="${2:-}"
      shift 2
      ;;
    --tmp)
      TMP_DIR="${2:-}"
      shift 2
      ;;
    --cache)
      CACHE_DIR="${2:-}"
      shift 2
      ;;
    --force)
      FORCE_FLAG="--force"
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    --)
      shift
      EXTRA_ARGS+=("$@")
      break
      ;;
    *)
      EXTRA_ARGS+=("$1")
      shift
      ;;
  esac
done

require_arg "--osm-path" "${OSM_PATH}"
require_arg "--area" "${AREA_SLUG}"

if [[ "${OSM_PATH}" != /* ]]; then
  OSM_PATH="${ROOT_DIR}/${OSM_PATH}"
fi
if [[ ! -f "${JAR_PATH}" ]]; then
  echo "[error] Missing ${JAR_PATH}. Download Planetiler openmaptiles jar." >&2
  exit 1
fi

if [[ ! -e "${OSM_PATH}" ]]; then
  echo "[error] OSM path ${OSM_PATH} not found." >&2
  exit 1
fi

if [[ "${TMP_DIR}" != /* ]]; then
  TMP_DIR="${ROOT_DIR}/${TMP_DIR}"
fi
if [[ "${CACHE_DIR}" != /* ]]; then
  CACHE_DIR="${ROOT_DIR}/${CACHE_DIR}"
fi

ensure_java

mkdir -p "${TMP_DIR}" "${CACHE_DIR}" "$(dirname "${OUTPUT_PATH}")"

echo "[vector-basemap] Building tiles from ${OSM_PATH}"
set -x
java "-Xmx${JAVA_HEAP}" -jar "${JAR_PATH}" \
  --osm-path="${OSM_PATH}" \
  --area="${AREA_SLUG}" \
  --tmp="${TMP_DIR}" \
  --cache="${CACHE_DIR}" \
  --output="${OUTPUT_PATH}" \
  ${FORCE_FLAG} \
  "${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"}"
set +x
echo "[vector-basemap] ✅ Wrote ${OUTPUT_PATH}"
