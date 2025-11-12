#!/usr/bin/env bash
set -euo pipefail

timestamp() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

log() {
  printf '[orchestrator][%s] %s\n' "$(timestamp)" "$*"
}

: "${PROJECT_ID:?set PROJECT_ID}"
: "${ZONE:?set ZONE}"
: "${INSTANCE_NAME:?set INSTANCE_NAME}"
: "${BUCKET:?set BUCKET}"
: "${TARGET:=d_anchor_category}"
: "${SERVICE_ACCOUNT:?set SERVICE_ACCOUNT}"
: "${MACHINE_TYPE:=c4d-highcpu-32}"
: "${BOOT_DISK_SIZE_GB:=200}"
: "${BOOT_DISK_TYPE:=pd-balanced}"
: "${IMAGE_FAMILY:=debian-12}"
: "${IMAGE_PROJECT:=debian-cloud}"
: "${SCOPES:=https://www.googleapis.com/auth/devstorage.read_write,https://www.googleapis.com/auth/logging.write,https://www.googleapis.com/auth/monitoring.write}"

RUN_ID="$(date +%Y%m%d-%H%M%S)"
SRC_TARBALL="vicinity-src-${RUN_ID}.tar.gz"
SRC_PATH="gs://${BUCKET}/src/${SRC_TARBALL}"
RESULTS_PREFIX="gs://${BUCKET}/results/${RUN_ID}"
REPO_ROOT="$(git rev-parse --show-toplevel)"
LOG_ROOT="${REPO_ROOT}/logs/remote_runs"
SERIAL_LOG="${LOG_ROOT}/${RUN_ID}-serial.log"
STARTUP_SCRIPT_PATH="${STARTUP_SCRIPT_PATH:-${REPO_ROOT}/scripts/startup_categories_vm.sh}"

mkdir -p "${LOG_ROOT}"

if [[ ! -f "${STARTUP_SCRIPT_PATH}" ]]; then
  log "missing startup script at ${STARTUP_SCRIPT_PATH}"
  exit 1
fi

log "run=${RUN_ID} target=${TARGET} instance=${INSTANCE_NAME} zone=${ZONE} bucket=${BUCKET}"
log "packaging HEAD $(git -C "${REPO_ROOT}" rev-parse --short HEAD)"

# Create tarball with source code and required data files for categories build
cd "${REPO_ROOT}"

# Create temporary directory for building the tarball
TMP_TAR_DIR="/tmp/vicinity-build-${RUN_ID}"
rm -rf "${TMP_TAR_DIR}"
mkdir -p "${TMP_TAR_DIR}"

# Extract git archive
git archive --format=tar --output "${TMP_TAR_DIR}/base.tar" HEAD
cd "${TMP_TAR_DIR}"
tar -xf base.tar
rm base.tar

# Add the required data files that are gitignored
# Default to massachusetts for now - could be made configurable later
for state in massachusetts; do
    # Add OSM PBF file
    if [[ -f "${REPO_ROOT}/data/osm/${state}.osm.pbf" ]]; then
        mkdir -p "data/osm"
        cp "${REPO_ROOT}/data/osm/${state}.osm.pbf" "data/osm/"
    fi
    # Add POI data
    if [[ -f "${REPO_ROOT}/data/poi/${state}_canonical.parquet" ]]; then
        mkdir -p "data/poi"
        cp "${REPO_ROOT}/data/poi/${state}_canonical.parquet" "data/poi/"
    fi
    # Add anchor data (if it exists)
    if [[ -f "${REPO_ROOT}/data/anchors/${state}_drive_sites.parquet" ]]; then
        mkdir -p "data/anchors"
        cp "${REPO_ROOT}/data/anchors/${state}_drive_sites.parquet" "data/anchors/"
    fi
    if [[ -f "${REPO_ROOT}/data/anchors/${state}_drive_site_id_map.parquet" ]]; then
        mkdir -p "data/anchors"
        cp "${REPO_ROOT}/data/anchors/${state}_drive_site_id_map.parquet" "data/anchors/"
    fi
done

# Create the final compressed tarball
tar -czf "/tmp/${SRC_TARBALL}" .

# Cleanup
cd "${REPO_ROOT}"
rm -rf "${TMP_TAR_DIR}"

log "src tarball size $(du -h "/tmp/${SRC_TARBALL}" | cut -f1) → ${SRC_PATH}"
time gsutil cp "/tmp/${SRC_TARBALL}" "${SRC_PATH}"

instance_exists=0
if gcloud compute instances describe "${INSTANCE_NAME}" --project "${PROJECT_ID}" --zone "${ZONE}" >/dev/null 2>&1; then
  instance_exists=1
fi

if [[ "${instance_exists}" -eq 1 ]]; then
  log "existing instance ${INSTANCE_NAME} detected; deleting so we can recreate a fresh VM"
  gcloud compute instances delete "${INSTANCE_NAME}" \
    --project "${PROJECT_ID}" \
    --zone "${ZONE}" \
    --quiet
fi

log "creating ephemeral instance ${INSTANCE_NAME} machine=${MACHINE_TYPE} disk=${BOOT_DISK_SIZE_GB}GB type=${BOOT_DISK_TYPE}"
gcloud compute instances create "${INSTANCE_NAME}" \
  --project "${PROJECT_ID}" \
  --zone "${ZONE}" \
  --machine-type "${MACHINE_TYPE}" \
  --service-account "${SERVICE_ACCOUNT}" \
  --scopes "${SCOPES}" \
  --image-family "${IMAGE_FAMILY}" \
  --image-project "${IMAGE_PROJECT}" \
  --boot-disk-size "${BOOT_DISK_SIZE_GB}" \
  --boot-disk-type "${BOOT_DISK_TYPE}" \
  --metadata RUN_ID="${RUN_ID}",BUCKET="${BUCKET}",SRC_TARBALL="${SRC_TARBALL}",RESULTS_PREFIX="${RESULTS_PREFIX}",TARGET="${TARGET}" \
  --metadata-from-file startup-script="${STARTUP_SCRIPT_PATH}"
log "instance ${INSTANCE_NAME} created"

INSTANCE_CREATED=1

serial_tail() {
  local next=0
  : >"${SERIAL_LOG}"
  while true; do
    local raw
    if ! raw="$(gcloud compute instances get-serial-port-output "${INSTANCE_NAME}" \
      --project "${PROJECT_ID}" \
      --zone "${ZONE}" \
      --port 1 \
      --start="${next}" 2>&1)"; then
      if grep -qi "was not found" <<<"${raw}"; then
        log "serial tail stopping: instance ${INSTANCE_NAME} not found"
        break
      fi
      sleep 10
      continue
    fi
    if grep -qi "There is no serial port output" <<<"${raw}"; then
      sleep 10
      continue
    fi
    local end_offset
    end_offset="$(printf '%s\n' "${raw}" | sed -n 's/^end: \(.*\)/\1/p' | head -n1)"
    if [[ -z "${end_offset}" ]]; then
      sleep 10
      continue
    fi
    next="${end_offset}"
    local body
    body="$(printf '%s\n' "${raw}" | tail -n +4)"
    if [[ -n "${body}" ]]; then
      while IFS= read -r line; do
        printf '[serial][%s] %s\n' "$(timestamp)" "$line" | tee -a "${SERIAL_LOG}"
      done <<<"${body}"
    fi
    sleep 10
  done
}

serial_tail &
LOG_TAIL_PID=$!

delete_instance() {
  if [[ "${INSTANCE_CREATED:-0}" -eq 1 ]]; then
    log "deleting instance ${INSTANCE_NAME} to drop boot disk"
    gcloud compute instances delete "${INSTANCE_NAME}" \
      --project "${PROJECT_ID}" \
      --zone "${ZONE}" \
      --quiet || log "warning: failed to delete ${INSTANCE_NAME}; please clean up manually"
    INSTANCE_CREATED=0
  fi
}

cleanup() {
  if [[ -n "${LOG_TAIL_PID:-}" ]]; then
    kill "${LOG_TAIL_PID}" >/dev/null 2>&1 || true
    wait "${LOG_TAIL_PID}" 2>/dev/null || true
  fi
  delete_instance
}
trap cleanup EXIT

log "waiting for ${INSTANCE_NAME} to return to TERMINATED (serial log → ${SERIAL_LOG})"
while true; do
  if ! STATUS="$(gcloud compute instances describe "${INSTANCE_NAME}" \
    --project "${PROJECT_ID}" \
    --zone "${ZONE}" \
    --format='value(status)' 2>/dev/null)"; then
    log "instance ${INSTANCE_NAME} no longer present; assuming shutdown complete"
    break
  fi
  [[ "${STATUS}" == "TERMINATED" ]] && break
  log "status=${STATUS} (sleeping 30s)"
  sleep 30
done

LOCAL_RESULTS_DIR="${REPO_ROOT}/data/categories_results/${RUN_ID}"
mkdir -p "${LOCAL_RESULTS_DIR}"
log "syncing results → ${LOCAL_RESULTS_DIR}"
time gsutil -m rsync -r "${RESULTS_PREFIX}" "${LOCAL_RESULTS_DIR}"

# Check for startup errors
if [[ -f "${LOCAL_RESULTS_DIR}/startup_error.txt" ]]; then
  log "⚠️  STARTUP SCRIPT FAILED ⚠️"
  cat "${LOCAL_RESULTS_DIR}/startup_error.txt"
  log "Check serial log at ${SERIAL_LOG} for details"
  if [[ -f "${LOCAL_RESULTS_DIR}/build.log" ]]; then
    log "Partial build.log available:"
    tail -n 50 "${LOCAL_RESULTS_DIR}/build.log"
  fi
  log "❌ Remote run failed - see logs above"
  exit 1
fi

rm -rf "${REPO_ROOT}/data/categories_results/latest"
cp -R "${LOCAL_RESULTS_DIR}" "${REPO_ROOT}/data/categories_results/latest"

if [[ "${TARGET}" == "all" ]]; then
  log "Syncing full pipeline results to local directories..."
  rsync -a "${LOCAL_RESULTS_DIR}/data/anchors/" "${REPO_ROOT}/data/anchors/"
  rsync -a "${LOCAL_RESULTS_DIR}/data/minutes/" "${REPO_ROOT}/data/minutes/"
  rsync -a "${LOCAL_RESULTS_DIR}/tiles/" "${REPO_ROOT}/tiles/"
  rsync -a "${LOCAL_RESULTS_DIR}/data/d_anchor_category/" "${REPO_ROOT}/data/d_anchor_category/"
  rsync -a "${LOCAL_RESULTS_DIR}/data/d_anchor_brand/" "${REPO_ROOT}/data/d_anchor_brand/"
fi

if [[ -f "${LOCAL_RESULTS_DIR}/build.log" ]]; then
  log "tail of remote build.log"
  tail -n 40 "${LOCAL_RESULTS_DIR}/build.log"
fi

log "✅ remote batch complete → ${LOCAL_RESULTS_DIR} (serial log at ${SERIAL_LOG})"
delete_instance
log "done"
