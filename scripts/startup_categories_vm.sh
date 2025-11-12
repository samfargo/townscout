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

# Fetch metadata first, before any redirections
log "Fetching instance metadata..."
RUN_ID="$(fetch_metadata "RUN_ID")"
BUCKET="$(fetch_metadata "BUCKET")"
SRC_TARBALL="$(fetch_metadata "SRC_TARBALL")"
RESULTS_PREFIX="$(fetch_metadata "RESULTS_PREFIX")"
TARGET="$(fetch_metadata "TARGET")"

log "boot metadata RUN_ID=${RUN_ID} TARGET=${TARGET} SRC=${SRC_TARBALL}"

# Ensure shutdown always happens, even on error
EXIT_CODE=0
cleanup_and_shutdown() {
  local exit_code=$?
  if [[ ${exit_code} -ne 0 ]]; then
    error "Script failed with exit code ${exit_code}"
    EXIT_CODE=${exit_code}
    # Upload any logs we have so far
    if [[ -n "${RESULTS_PREFIX:-}" ]]; then
      # Upload build.log if it exists
      if [[ -f /opt/vicinity/work/build.log ]]; then
        gsutil -m cp /opt/vicinity/work/build.log "${RESULTS_PREFIX}/build.log" 2>/dev/null || true
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
  log "Shutting down VM (exit_code=${EXIT_CODE})"
  shutdown -h now
}
trap cleanup_and_shutdown EXIT

# Fix any broken package installations from previous runs
log "Updating package lists..."
apt-get update -y || {
  error "apt-get update failed, attempting to fix..."
  rm -rf /var/lib/apt/lists/*
  apt-get update -y
}

# Fix broken dependencies before attempting new installs
apt-get install -y --fix-broken || true
apt-get autoremove -y || true

log "Installing required packages..."
apt-get install -y python3 python3-pip python3-venv git build-essential pkg-config \
                   libgeos-dev libproj-dev libgdal-dev curl wget unzip || {
  error "Package installation failed, attempting recovery..."
  apt-get install -y --fix-broken
  apt-get install -y python3 python3-pip python3-venv git build-essential pkg-config \
                     libgeos-dev libproj-dev libgdal-dev curl wget unzip
}

log "packages installed"

# Install DuckDB CLI
log "Installing DuckDB CLI..."
DUCKDB_VERSION="v1.1.3"
wget -q "https://github.com/duckdb/duckdb/releases/download/${DUCKDB_VERSION}/duckdb_cli-linux-amd64.zip" -O /tmp/duckdb.zip
unzip -q /tmp/duckdb.zip -d /tmp
mv /tmp/duckdb /usr/local/bin/
chmod +x /usr/local/bin/duckdb
rm -f /tmp/duckdb.zip
if ! command -v duckdb &> /dev/null; then
  error "duckdb installation failed"
  exit 1
fi
# Set HOME for DuckDB to store extensions/cache
export HOME=/root
mkdir -p /root/.duckdb
log "DuckDB $(duckdb --version) installed"

# Install Rust toolchain for maturin/vicinity_native
log "Installing Rust toolchain..."
export CARGO_HOME="/root/.cargo"
export RUSTUP_HOME="/root/.rustup"
# Pre-add to PATH before installation
export PATH="${CARGO_HOME}/bin:$PATH"
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
# Verify installation
if [[ ! -f "${CARGO_HOME}/bin/rustc" ]]; then
  error "rustc binary not found at ${CARGO_HOME}/bin/rustc after installation"
  ls -la "${CARGO_HOME}/bin/" || error "CARGO_HOME/bin directory doesn't exist"
  exit 1
fi
log "Rust $(${CARGO_HOME}/bin/rustc --version) installed"

mkdir -p /opt/vicinity/work
cd /opt/vicinity/work

log "Downloading source tarball..."
gsutil cp "gs://${BUCKET}/src/${SRC_TARBALL}" .
log "Extracting tarball..."
tar xzf "${SRC_TARBALL}"
rm -f "${SRC_TARBALL}"

log "unpacked source into $(pwd)"

log "Creating Python virtual environment..."
python3 -m venv .venv
. .venv/bin/activate
pip install --upgrade pip wheel
pip install -r requirements.txt

log "venv + python deps ready"

log "Building Rust native extension..."
# Ensure Rust is in PATH for make
export PATH="${CARGO_HOME}/bin:$PATH"
if ! make native 2>&1 | tee -a build.log; then
  error "Rust extension build failed"
  exit 1
fi
log "rust extension built"

export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 NUMEXPR_NUM_THREADS=1
export THREADS=1
export WORKERS=16

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
fi

# Always upload build log
gsutil -m cp build.log "${RESULTS_PREFIX}/build.log" || true
log "uploaded build.log to ${RESULTS_PREFIX}"

log "Build process complete (exit_code=${EXIT_CODE})"
# Trap will handle shutdown
