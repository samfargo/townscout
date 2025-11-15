#!/usr/bin/env bash
set -euo pipefail

# Ensure HOME is set (needed for DuckDB and other tools)
export HOME=/root

META="http://metadata.google.internal/computeMetadata/v1/instance/attributes"

timestamp() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

log() {
  printf '[startup][%s] %s\n' "$(timestamp)" "$*"
}

error() {
  printf '[startup][%s][ERROR] %s\n' "$(timestamp)" "$*" >&2
}

QUIET_LOG="/tmp/vicinity-startup-detail.log"
: > "${QUIET_LOG}"
SCRIPT_START_TIME="$(date +%s)"
PHASE_START_TIME="${SCRIPT_START_TIME}"
CURRENT_TELEMETRY_INTERVAL=""
log "Detailed command output will be appended to ${QUIET_LOG}"

duration_since() {
  local start="$1"
  local now
  now="$(date +%s)"
  echo $((now - start))
}

log_duration_from() {
  local label="$1"
  local start="$2"
  if [[ -z "${start}" ]]; then
    log "[timer] ${label} completed (duration unknown)"
    return
  fi
  local elapsed
  elapsed="$(duration_since "${start}")"
  log "[timer] ${label} took ${elapsed}s"
}

VMSTAT_PID=""
MPSTAT_PID=""

telemetry_vmstat_loop() {
  local interval="$1"
  vmstat -n "${interval}" | {
    local header_lines=0
    while read -r line; do
      [[ -z "${line// }" ]] && continue
      if [[ ${header_lines} -lt 2 ]]; then
        ((header_lines++))
        continue
      fi
      set -- $line
      local field_count=$#
      if (( field_count < 17 )); then
        log "[telemetry][vmstat] skipped short line (fields=${field_count}): ${line}"
        continue
      fi
      local runq="$1"
      local blocks="$2"
      local free_mem="$4"
      local idle="${15}"
      local wa="${16}"
      log "[telemetry][vmstat] interval=${interval}s runq=${runq} blocked=${blocks} free=${free_mem} idle=${idle}% wa=${wa}%"
    done
  }
}

telemetry_mpstat_loop() {
  local interval="$1"
  LC_ALL=C mpstat "${interval}" | {
    local header_seen=0
    while read -r line; do
      [[ -z "${line// }" ]] && continue
      if [[ "${line}" == *"CPU"*"%idle"* ]]; then
        header_seen=1
        continue
      fi
      [[ "${line}" == Average:* ]] && continue
      [[ ${header_seen} -eq 0 ]] && continue
      set -- $line
      local timestamp="$1"
      local cpu="$2"
      local usr="${3:-0}"
      local nice="${4:-0}"
      local sys="${5:-0}"
      local iowait="${6:-0}"
      local irq="${7:-0}"
      local soft="${8:-0}"
      local steal="${9:-0}"
      local guest="${10:-0}"
      local gnice="${11:-0}"
      local idle="${12:-0}"
      log "[telemetry][cpu] interval=${interval}s t=${timestamp} cpu=${cpu} usr=${usr}% sys=${sys}% wa=${iowait}% idle=${idle}% irq=${irq}% soft=${soft}% steal=${steal}% guest=${guest}% gnice=${gnice}%"
    done
  }
}

start_monitors() {
  local interval="$1"
  CURRENT_TELEMETRY_INTERVAL="${interval}"
  log "Starting telemetry monitors (interval=${interval}s)"
  if command -v vmstat >/dev/null 2>&1; then
    telemetry_vmstat_loop "${interval}" &
    VMSTAT_PID=$!
  else
    log "vmstat not found; skipping vmstat telemetry"
  fi
  if command -v mpstat >/dev/null 2>&1; then
    telemetry_mpstat_loop "${interval}" &
    MPSTAT_PID=$!
  else
    log "mpstat not found; skipping mpstat telemetry"
  fi
}

stop_monitors() {
  for pid_var in VMSTAT_PID MPSTAT_PID; do
    pid="${!pid_var:-}"
    if [[ -n "${pid}" ]]; then
      kill "${pid}" >/dev/null 2>&1 || true
      wait "${pid}" 2>/dev/null || true
      eval "${pid_var}=''"
    fi
  done
}

restart_monitors() {
  local interval="$1"
  if [[ "${CURRENT_TELEMETRY_INTERVAL}" == "${interval}" ]]; then
    return
  fi
  stop_monitors
  start_monitors "${interval}"
}

log_phase() {
  local label="$1"
  local now
  now="$(date +%s)"
  local elapsed=$((now - PHASE_START_TIME))
  local total=$((now - SCRIPT_START_TIME))
  log "[timer] ${label} took ${elapsed}s (total ${total}s)"
  PHASE_START_TIME="${now}"
}

download_and_extract_source() (
  set -euo pipefail
  local work_dir="/opt/vicinity/work"
  mkdir -p "${work_dir}"
  cd "${work_dir}"
  log "Downloading source tarball..."
  gsutil cp "gs://${BUCKET}/src/${SRC_TARBALL}" .
  log "Extracting source tarball..."
  tar xzf "${SRC_TARBALL}"
  rm -f "${SRC_TARBALL}"
  log "unpacked source into ${work_dir}"
)

install_duckdb_cli() (
  set -euo pipefail
  log "Installing DuckDB CLI..."
  local tmp_zip="/tmp/duckdb.zip"
  DUCKDB_VERSION="v1.1.3"
  wget -q "https://github.com/duckdb/duckdb/releases/download/${DUCKDB_VERSION}/duckdb_cli-linux-amd64.zip" -O "${tmp_zip}"
  unzip -q "${tmp_zip}" -d /tmp
  mv /tmp/duckdb /usr/local/bin/
  chmod +x /usr/local/bin/duckdb
  rm -f "${tmp_zip}"
  export HOME=/root
  mkdir -p /root/.duckdb
  duckdb --version >/dev/null
)

install_rust_toolchain() (
  set -euo pipefail
  log "Installing Rust toolchain..."
  export CARGO_HOME="/root/.cargo"
  export RUSTUP_HOME="/root/.rustup"
  export PATH="${CARGO_HOME}/bin:$PATH"
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
  if [[ ! -f "${CARGO_HOME}/bin/rustc" ]]; then
    log "[timer] rustup failed to produce rustc binary"
    exit 1
  fi
  log "Rust $(${CARGO_HOME}/bin/rustc --version) installed"
)

# Helper function to fetch metadata with retries
fetch_metadata() {
  local key="$1"
  local retries=5
  local count=0
  while [[ $count -lt $retries ]]; do
    if result="$(curl -sf -H "Metadata-Flavor: Google" "${META}/${key}" 2>&1)"; then
      echo "$result"
      return 0
    fi
    count=$((count + 1))
    log "Metadata fetch for ${key} failed (attempt ${count}/${retries}), retrying in 5s..."
    sleep 5
  done
  error "Failed to fetch metadata key ${key} after ${retries} attempts"
  return 1
}

# Helper for optional metadata (no retries, no failure)
fetch_metadata_optional() {
  local key="$1"
  if result="$(curl -sf -H "Metadata-Flavor: Google" "${META}/${key}" 2>/dev/null)"; then
    echo "$result"
  else
    echo ""
  fi
}

# Fetch metadata first, before any redirections
log "Fetching instance metadata..."
RUN_ID="$(fetch_metadata "RUN_ID")"
BUCKET="$(fetch_metadata "BUCKET")"
SRC_TARBALL="$(fetch_metadata "SRC_TARBALL")"
RESULTS_PREFIX="$(fetch_metadata "RESULTS_PREFIX")"
TARGET="$(fetch_metadata "TARGET")"
THREADS_OVERRIDE="$(fetch_metadata_optional "THREADS")"
WORKERS_OVERRIDE="$(fetch_metadata_optional "WORKERS")"
TELEMETRY_INTERVAL_OVERRIDE="$(fetch_metadata_optional "TELEMETRY_INTERVAL")"

# Apply defaults + validation for parallelism knobs
THREADS_VALUE="${THREADS_OVERRIDE:-1}"
if [[ -z "${THREADS_VALUE}" || ! "${THREADS_VALUE}" =~ ^[0-9]+$ || "${THREADS_VALUE}" -lt 1 ]]; then
  THREADS_VALUE=1
fi
WORKERS_VALUE="${WORKERS_OVERRIDE:-32}"
if [[ -z "${WORKERS_VALUE}" || ! "${WORKERS_VALUE}" =~ ^[0-9]+$ || "${WORKERS_VALUE}" -lt 1 ]]; then
  WORKERS_VALUE=32
fi
TELEMETRY_INTERVAL_VALUE="${TELEMETRY_INTERVAL_OVERRIDE:-5}"
if [[ -z "${TELEMETRY_INTERVAL_VALUE}" || ! "${TELEMETRY_INTERVAL_VALUE}" =~ ^[0-9]+$ || "${TELEMETRY_INTERVAL_VALUE}" -lt 1 ]]; then
  TELEMETRY_INTERVAL_VALUE=5
fi
TELEMETRY_BOOTSTRAP_INTERVAL_VALUE=$((TELEMETRY_INTERVAL_VALUE * 6))
if [[ "${TELEMETRY_BOOTSTRAP_INTERVAL_VALUE}" -lt "${TELEMETRY_INTERVAL_VALUE}" ]]; then
  TELEMETRY_BOOTSTRAP_INTERVAL_VALUE="${TELEMETRY_INTERVAL_VALUE}"
fi

log "boot metadata RUN_ID=${RUN_ID} TARGET=${TARGET} SRC=${SRC_TARBALL}"
log "parallelism config threads=${THREADS_VALUE} workers=${WORKERS_VALUE}"
log "telemetry intervals bootstrap=${TELEMETRY_BOOTSTRAP_INTERVAL_VALUE}s fast=${TELEMETRY_INTERVAL_VALUE}s"
start_monitors "${TELEMETRY_BOOTSTRAP_INTERVAL_VALUE}"

# Ensure shutdown always happens, even on error
EXIT_CODE=0
cleanup_and_shutdown() {
  local exit_code=$?
  stop_monitors
  if [[ ${exit_code} -ne 0 ]]; then
    error "Script failed with exit code ${exit_code}"
    EXIT_CODE=${exit_code}
    # Upload any logs we have so far
    if [[ -n "${RESULTS_PREFIX:-}" ]]; then
      # Upload build.log if it exists
      if [[ -f /opt/vicinity/work/build.log ]]; then
        gsutil -m cp /opt/vicinity/work/build.log "${RESULTS_PREFIX}/build.log" 2>/dev/null || true
      fi
      if [[ -f "${QUIET_LOG}" ]]; then
        gsutil -m cp "${QUIET_LOG}" "${RESULTS_PREFIX}/startup_detail.log" 2>/dev/null || true
      fi
      # Create an error marker file with system logs
      {
        echo "Startup script failed at $(timestamp) with exit code ${exit_code}"
        echo "---"
        echo "Last 100 lines of startup script output:"
        journalctl -u google-startup-scripts --no-pager -n 100 2>/dev/null || echo "Could not fetch startup logs"
      } > /tmp/startup_error.txt
      gsutil -m cp /tmp/startup_error.txt "${RESULTS_PREFIX}/startup_error.txt" 2>/dev/null || true
    fi
  fi
  local now
  now="$(date +%s)"
  local total_runtime=$((now - SCRIPT_START_TIME))
  log "[timer] total runtime ${total_runtime}s"
  log "Shutting down VM (exit_code=${EXIT_CODE})"
  shutdown -h now
}
trap cleanup_and_shutdown EXIT

# Fix any broken package installations from previous runs
log "Updating package lists..."
if ! apt-get update -y >>"${QUIET_LOG}" 2>&1; then
  error "apt-get update failed, attempting to fix... (see ${QUIET_LOG})"
  rm -rf /var/lib/apt/lists/*
  apt-get update -y >>"${QUIET_LOG}" 2>&1
fi
log_phase "apt-get update"

# Fix broken dependencies before attempting new installs
apt-get install -y --fix-broken >>"${QUIET_LOG}" 2>&1 || true
apt-get autoremove -y >>"${QUIET_LOG}" 2>&1 || true

log "Installing required packages..."
if ! apt-get install -y python3 python3-pip python3-venv git build-essential pkg-config \
                   libgeos-dev libproj-dev libgdal-dev curl wget unzip procps sysstat >>"${QUIET_LOG}" 2>&1; then
  error "Package installation failed, attempting recovery... (see ${QUIET_LOG})"
  apt-get install -y --fix-broken >>"${QUIET_LOG}" 2>&1
  apt-get install -y python3 python3-pip python3-venv git build-essential pkg-config \
                     libgeos-dev libproj-dev libgdal-dev curl wget unzip procps sysstat >>"${QUIET_LOG}" 2>&1
fi
log_phase "apt-get install base deps"

log "packages installed"

export CARGO_HOME="/root/.cargo"
export RUSTUP_HOME="/root/.rustup"
export PATH="${CARGO_HOME}/bin:$PATH"

log "Launching background setup tasks (DuckDB CLI, repo download, Rust toolchain)..."
DUCKDB_START_TIME="$(date +%s)"
install_duckdb_cli &
DUCKDB_PID=$!

SOURCE_START_TIME="$(date +%s)"
download_and_extract_source &
SOURCE_PID=$!

RUST_START_TIME="$(date +%s)"
install_rust_toolchain >>"${QUIET_LOG}" 2>&1 &
RUST_PID=$!

if ! wait "${SOURCE_PID}"; then
  error "Source download/extract failed"
  exit 1
fi
log_duration_from "Source download/extract" "${SOURCE_START_TIME}"

cd /opt/vicinity/work

PHASE_START_TIME="$(date +%s)"
log "Creating Python virtual environment..."
python3 -m venv .venv
. .venv/bin/activate
if ! pip install --upgrade pip wheel >>"${QUIET_LOG}" 2>&1; then
  error "pip bootstrap failed (see ${QUIET_LOG})"
  exit 1
fi
if ! pip install -r requirements.txt >>"${QUIET_LOG}" 2>&1; then
  error "pip install -r requirements.txt failed (see ${QUIET_LOG})"
  exit 1
fi

log "venv + python deps ready"
log_phase "Python deps install"

if ! wait "${RUST_PID}"; then
  error "Rust toolchain install failed (see ${QUIET_LOG})"
  exit 1
fi
log_duration_from "Rust toolchain install" "${RUST_START_TIME}"

if ! wait "${DUCKDB_PID}"; then
  error "DuckDB CLI install failed"
  exit 1
fi
log_duration_from "DuckDB install" "${DUCKDB_START_TIME}"

log "Building Rust native extension..."
# Ensure Rust is in PATH for make
export PATH="${CARGO_HOME}/bin:$PATH"
if ! make native 2>&1 | tee -a build.log; then
  error "Rust extension build failed"
  exit 1
fi
log "rust extension built"

export THREADS="${THREADS_VALUE}"
export WORKERS="${WORKERS_VALUE}"
export OMP_NUM_THREADS="${THREADS_VALUE}" \
       OPENBLAS_NUM_THREADS="${THREADS_VALUE}" \
       MKL_NUM_THREADS="${THREADS_VALUE}" \
       NUMEXPR_NUM_THREADS="${THREADS_VALUE}" \
       NUMEXPR_MAX_THREADS="${THREADS_VALUE}"

restart_monitors "${TELEMETRY_INTERVAL_VALUE}"

if [[ "${TARGET}" == "all" ]]; then
  log "starting make all"
  if make all 2>&1 | tee -a build.log; then
    log "make all completed successfully"
    log "Uploading results to ${RESULTS_PREFIX}..."
    gsutil -m rsync -r data/anchors "${RESULTS_PREFIX}/data/anchors" || error "Failed to upload anchors"
    gsutil -m rsync -r data/minutes "${RESULTS_PREFIX}/data/minutes" || error "Failed to upload minutes"
    gsutil -m rsync -r tiles "${RESULTS_PREFIX}/tiles" || error "Failed to upload tiles"
    gsutil -m rsync -r data/d_anchor_category "${RESULTS_PREFIX}/data/d_anchor_category" || error "Failed to upload d_anchor_category"
    gsutil -m rsync -r data/d_anchor_brand "${RESULTS_PREFIX}/data/d_anchor_brand" || error "Failed to upload d_anchor_brand"
  else
    error "make all failed"
    EXIT_CODE=1
  fi
  log_phase "make all"
else
  log "starting make d_anchor_category"
  if make d_anchor_category 2>&1 | tee -a build.log; then
    log "make d_anchor_category completed successfully"
    log "Uploading results to ${RESULTS_PREFIX}..."
    gsutil -m rsync -r data/d_anchor_category "${RESULTS_PREFIX}/data/d_anchor_category" || error "Failed to upload d_anchor_category"
  else
    error "make d_anchor_category failed"
    EXIT_CODE=1
  fi
  log_phase "make d_anchor_category"
fi

stop_monitors

# Always upload build log
gsutil -m cp build.log "${RESULTS_PREFIX}/build.log" || true
if [[ -f "${QUIET_LOG}" ]]; then
  gsutil -m cp "${QUIET_LOG}" "${RESULTS_PREFIX}/startup_detail.log" || true
fi
log "uploaded build.log to ${RESULTS_PREFIX}"

log "Build process complete (exit_code=${EXIT_CODE})"
# Trap will handle shutdown
