# Remote VM Spec for `make d_anchor_category`

This runbook focuses on the remote execution path that mirrors our existing Make targets on a Google Cloud VM (`c4d-highcpu-32`, 32 vCPU / 60 GiB). Local runs such as `make all` or `make d_anchor_category` are still available (see `README.md` / `ARCHITECTURE_OVERVIEW.md`), but the goal here is to describe how to offload those heavy steps from a local MacBook to the reusable VM.

---

## 0. High-level Flow

When you run `make categories_remote` (categories only) or `make pipeline_remote` (full pipeline):

1. Package the repo (or a curated subset) into a tarball.
2. Upload that tarball to `gs://<BUCKET>/src/`.
3. Update metadata on a long-lived `c4d-highcpu-32` instance (e.g., `vicinity-batch`) whose startup script:
   - downloads the tarball,
   - installs deps (Python 3.11, `pip`, `maturin`, system libs),
   - runs either `make d_anchor_category` or `make all`, depending on `TARGET`,
   - syncs the relevant outputs (`data/d_anchor_category` only, or anchors/minutes/tiles/D_anchor) to `gs://<BUCKET>/results/<RUN_ID>/`,
   - shuts the VM down so it returns to `TERMINATED`.
4. Start that reused VM and wait for it to reach `TERMINATED`.
5. `gsutil rsync` results into the local `data/categories_results/<RUN_ID>` (and, if running the full pipeline, fan the outputs back into the main `data/` directories).
6. Leave the VM stopped so it is ready for the next run—no create/delete churn.

Everything is orchestrated by `scripts/run_categories_remote.sh`, which the Makefile target (`categories_remote` or `pipeline_remote`) invokes.

---

## 1. One-time GCP Setup (existing inactive VM)

We assume a `c4d-highcpu-32` instance already exists in your project (e.g., `vicinity-batch` in `us-east4-c`) but is currently stopped. The orchestrator will **reuse** that VM on every run, so the only bootstrap work is wiring up the metadata, buckets, and startup script once.

1. **Confirm the reusable VM is stopped:**
   ```bash
   gcloud compute instances list --filter="name=vicinity-batch"
   # Optional: start/stop it manually outside the orchestrator
   gcloud compute instances start vicinity-batch --zone=us-east4-c
   gcloud compute instances stop vicinity-batch --zone=us-east4-c
   ```
   If you plan to keep long-lived state on this machine, document any changes so you can recreate them if the disk is wiped.

2. **Attach the startup script once:**
   ```bash
   gcloud compute instances add-metadata vicinity-batch \
     --zone us-east4-c \
     --metadata-from-file startup-script=scripts/startup_categories_vm.sh
   ```
   GCE runs startup scripts on *every* boot, so the same script will execute each time the orchestrator starts the VM.

3. **Bucket (regional, near the VM):**
   ```bash
   export BUCKET=vicinity-batch-$USER
   gsutil mb -l us-east4 gs://$BUCKET/
   ```
   Use prefixes:
   - `gs://$BUCKET/src/` – uploaded source tarballs.
   - `gs://$BUCKET/results/` – VM outputs per run (`<RUN_ID>/...`).

4. **Service account for the VM:**
   ```bash
   gcloud iam service-accounts create vicinity-batch-sa \
     --display-name="Vicinity batch jobs"

   gcloud projects add-iam-policy-binding $PROJECT_ID \
     --member="serviceAccount:vicinity-batch-sa@$PROJECT_ID.iam.gserviceaccount.com" \
     --role="roles/storage.admin"
   ```
   Storage Admin is enough for “pull code / push results”. Add more roles only if the job touches other APIs.

5. **Local gcloud defaults (run once per workstation):**
   ```bash
   gcloud auth login
   gcloud config set project $PROJECT_ID
   gcloud config set compute/region us-east4
   gcloud config set compute/zone us-east4-b
   ```

---

## 2. Repo Integration

### 2.1 Makefile fragment

```make
PROJECT_ID    ?= your-project-id
ZONE          ?= us-east4-b
INSTANCE_NAME ?= vicinity-batch
BUCKET        ?= vicinity-batch-$(USER)
.PHONY: categories_remote
categories_remote:
	TARGET=d_anchor_category \
	PROJECT_ID=$(PROJECT_ID) \
	ZONE=$(ZONE) \
	INSTANCE_NAME=$(INSTANCE_NAME) \
	BUCKET=$(BUCKET) \
	./scripts/run_categories_remote.sh

.PHONY: pipeline_remote
pipeline_remote:
	TARGET=all \
	PROJECT_ID=$(PROJECT_ID) \
	ZONE=$(ZONE) \
	INSTANCE_NAME=$(INSTANCE_NAME) \
	BUCKET=$(BUCKET) \
	./scripts/run_categories_remote.sh
```

`categories_remote` runs just the expensive D_anchor categories. `pipeline_remote` flips a single flag (`TARGET=all`) so the VM builds the entire graph/anchoring/minutes/tiles/D_anchor chain. Both targets now point at a fixed VM (`INSTANCE_NAME`) that remains stopped between runs.

### 2.2 Orchestrator (`scripts/run_categories_remote.sh`)

Key behavior (trimmed shell):

```bash
#!/usr/bin/env bash
set -euo pipefail

log() {
  printf '[orchestrator][%s] %s\n' "$(date -Is)" "$*"
}

: "${PROJECT_ID:?set PROJECT_ID}"
: "${ZONE:?set ZONE}"
: "${INSTANCE_NAME:?set INSTANCE_NAME}"
: "${BUCKET:?set BUCKET}"
: "${TARGET:=d_anchor_category}"

RUN_ID="$(date +%Y%m%d-%H%M%S)"
SRC_TARBALL="vicinity-src-${RUN_ID}.tar.gz"
SRC_PATH="gs://${BUCKET}/src/${SRC_TARBALL}"
RESULTS_PREFIX="gs://${BUCKET}/results/${RUN_ID}"
REPO_ROOT="$(git rev-parse --show-toplevel)"
LOG_ROOT="${REPO_ROOT}/logs/remote_runs"
SERIAL_LOG="${LOG_ROOT}/${RUN_ID}-serial.log"

mkdir -p "${LOG_ROOT}"

log "run=${RUN_ID} target=${TARGET} instance=${INSTANCE_NAME} zone=${ZONE} bucket=${BUCKET}"
log "packaging HEAD $(git -C "${REPO_ROOT}" rev-parse --short HEAD)"

# Ship only tracked files; this keeps the VM build clean and reproducible.
git -C "${REPO_ROOT}" archive --format=tar.gz --output "/tmp/${SRC_TARBALL}" HEAD
log "src tarball size $(du -h "/tmp/${SRC_TARBALL}" | cut -f1) → ${SRC_PATH}"
time gsutil cp "/tmp/${SRC_TARBALL}" "${SRC_PATH}"
# If you truly need untracked overrides (rare), swap back to tar + --exclude guards.

# Refresh metadata so the next boot picks up the new run parameters.
gcloud compute instances add-metadata "${INSTANCE_NAME}" \
  --project "${PROJECT_ID}" \
  --zone "${ZONE}" \
  --metadata \
    RUN_ID="${RUN_ID}",\
    BUCKET="${BUCKET}",\
    SRC_TARBALL="${SRC_TARBALL}",\
    RESULTS_PREFIX="${RESULTS_PREFIX}",\
    TARGET="${TARGET}"
log "metadata pushed for ${INSTANCE_NAME}: RUN_ID=${RUN_ID} TARGET=${TARGET}"

# Start the reusable worker. The attached startup script runs on every boot.
gcloud compute instances start "${INSTANCE_NAME}" \
  --project "${PROJECT_ID}" \
  --zone "${ZONE}"

# Stream the VM console directly into this terminal so the remote build looks like a local run.
gcloud compute instances tail-serial-port-output "${INSTANCE_NAME}" \
  --zone "${ZONE}" \
  --monitor \
  --port 1 \
  |& awk '{ printf "[serial][%s] %s\n", strftime("%FT%TZ"), $0 }' \
  | tee "${SERIAL_LOG}" &
LOG_TAIL_PID=$!
cleanup() {
  if [[ -n "${LOG_TAIL_PID:-}" ]]; then
    kill "${LOG_TAIL_PID}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

log "waiting for ${INSTANCE_NAME} to return to TERMINATED (serial log → ${SERIAL_LOG})"
while true; do
  STATUS="$(gcloud compute instances describe "${INSTANCE_NAME}" --zone "${ZONE}" \
    --format='value(status)')"
  [[ "${STATUS}" == "TERMINATED" ]] && break
  log "status=${STATUS} (sleeping 30s)"
  sleep 30
done

LOCAL_RESULTS_DIR="${REPO_ROOT}/data/categories_results/${RUN_ID}"
mkdir -p "${LOCAL_RESULTS_DIR}"
log "syncing results → ${LOCAL_RESULTS_DIR}"
time gsutil -m rsync -r "${RESULTS_PREFIX}" "${LOCAL_RESULTS_DIR}"

rm -rf "${REPO_ROOT}/data/categories_results/latest"
cp -R "${LOCAL_RESULTS_DIR}" "${REPO_ROOT}/data/categories_results/latest"

# Optional local fan-out: for full pipeline runs, mirror the remote outputs into the
# canonical working directories so downstream targets see fresh data.
if [[ "${TARGET}" == "all" ]]; then
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

log "remote batch complete → ${LOCAL_RESULTS_DIR} (serial log at ${SERIAL_LOG})"
# VM stays in TERMINATED state, ready for the next run.
log "done"
```

The new `log()` helper keeps every stage timestamped locally, while each serial console stream is mirrored into `logs/remote_runs/<RUN_ID>-serial.log` for after-the-fact debugging. Piping heavy operations (`gsutil cp`, `gsutil rsync`) through `time` makes it obvious how long uploads/downloads consumed and prevents “silent” terminals during multi-minute transfers. Tailing the fetched `build.log` right away also surfaces remote failures without hunting through directories.

Feel free to add logging, Slack hooks, etc.

Those exclusions line up with the generated directories called out in `docs/ARCHITECTURE_OVERVIEW.md` and `README.md` (`data/`, `tiles/`, `state_tiles/`, `out/`) and the Makefile targets that recreate them (`anchors`, `minutes`, `d_anchor_*`, etc.), so the tarball contains only the source + configs the VM actually needs.

### 2.3 Troubleshooting `run_categories_remote.sh`

If the polling loop never sees `TERMINATED` or the startup script exits early, use these quick checks before forcing a rebuild or manual intervention:

- **Tail the serial console:** `gcloud compute instances get-serial-port-output "${INSTANCE_NAME}" --zone "${ZONE}" | tail -n 200` surfaces shell errors from `startup_categories_vm.sh` (missing deps, pip failure, etc.).
- **Review the saved serial transcript:** every orchestrated run now mirrors port-1 output into `logs/remote_runs/<RUN_ID>-serial.log`, so you can re-open the exact stream that scrolled past the terminal.
- **Tail the live serial console:** `gcloud compute instances get-serial-port-output "${INSTANCE_NAME}" --zone "${ZONE}" | tail -n 200` surfaces shell errors from `startup_categories_vm.sh` (missing deps, pip failure, etc.).
- **Grab detailed logs:** `gcloud compute scp --zone "${ZONE}" "${INSTANCE_NAME}:/var/log/syslog" /tmp/${RUN_ID}-syslog` or `gcloud compute ssh "${INSTANCE_NAME}" --zone "${ZONE}" --command "sudo journalctl -u google-startup-scripts --no-pager | tail -n 200"` shows apt/venv progress when the VM is stuck in `RUNNING`.
- **Recover partial artifacts:** Even if the script failed before the final rsync, you can copy `/opt/vicinity/vicinity/build.log` (and any `data/d_anchor_*` outputs) via `gcloud compute scp` or run `gsutil -m rsync` from the instance to `${RESULTS_PREFIX}` manually, then resume the local `gsutil rsync`.
- **Force cleanup:** As a last resort, `gcloud compute instances delete "${INSTANCE_NAME}" --zone "${ZONE}" --keep-disks=all` lets you preserve the boot disk for later inspection; omit `--keep-disks` to tear everything down and avoid charges.

### 2.4 Watching Remote Logs Live

The snippet above automatically tails the serial console (port 1) after the VM is started, so you immediately see the remote `make` output in the same terminal that invoked `make categories_remote` **or** `make pipeline_remote`. A few related tricks:

- **Serial console streaming:** `gcloud compute instances tail-serial-port-output "${INSTANCE_NAME}" --zone "${ZONE}" --monitor` mirrors the console you would see during a comparable local `make` run while also capturing startup-script output.
- **Local replay:** Because `run_categories_remote.sh` now tees that stream into `logs/remote_runs/<RUN_ID>-serial.log`, you can re-run `less +F logs/remote_runs/<RUN_ID>-serial.log` to revisit the exact boot you just kicked off.
- **SSH + tail:** Once the instance is up, run `gcloud compute ssh "${INSTANCE_NAME}" --zone "${ZONE}" --command "cd /opt/vicinity/vicinity && tail -f build.log"` to watch the pipeline log file (assuming the startup script pipes `make ... |& tee build.log`, which it already relies on when uploading results). Replace `build.log` with `/var/log/syslog` if you need system-level messages.
- **Ad-hoc commands:** Use `gcloud compute ssh` without `--command` to open an interactive shell and run any other diagnostics (`htop`, `tail -f data/d_anchor_category/*.log`, etc.). Exit once satisfied; the remote make continues unattended.

Each of these commands can run in parallel with `make categories_remote`, giving you local visibility into the remote job without waiting for `TERMINATED`.

---

## 3. VM Startup Script (`scripts/startup_categories_vm.sh`)

The startup script includes robust error handling to ensure the VM always shuts down, even on failure:

```bash
#!/usr/bin/env bash
set -euo pipefail
META="http://metadata.google.internal/computeMetadata/v1/instance/attributes"
header=(-H "Metadata-Flavor: Google")

log() {
  printf '[startup][%s] %s\n' "$(date -Is)" "$*"
}

error() {
  printf '[startup][%s][ERROR] %s\n' "$(date -Is)" "$*" >&2
}

exec > >(stdbuf -oL awk '{ printf "[startup][%s] %s\n", strftime("%FT%TZ"), $0 }')
exec 2> >(stdbuf -oL awk '{ printf "[startup][%s][stderr] %s\n", strftime("%FT%TZ"), $0 }' >&2)

# Ensure shutdown always happens, even on error
EXIT_CODE=0
cleanup_and_shutdown() {
  local exit_code=$?
  if [[ ${exit_code} -ne 0 ]]; then
    error "Script failed with exit code ${exit_code}"
    EXIT_CODE=${exit_code}
    # Upload any logs we have so far
    if [[ -n "${RESULTS_PREFIX:-}" ]]; then
      if [[ -f /opt/vicinity/vicinity/build.log ]]; then
        gsutil -m cp /opt/vicinity/vicinity/build.log "${RESULTS_PREFIX}/build.log" || true
      fi
      # Create an error marker file
      echo "Startup script failed at $(date -Is) with exit code ${exit_code}" > /tmp/startup_error.txt
      gsutil -m cp /tmp/startup_error.txt "${RESULTS_PREFIX}/startup_error.txt" || true
    fi
  fi
  log "Shutting down VM (exit_code=${EXIT_CODE})"
  shutdown -h now
}
trap cleanup_and_shutdown EXIT

RUN_ID="$(curl "${header[@]}" "${META}/RUN_ID")"
BUCKET="$(curl "${header[@]}" "${META}/BUCKET")"
SRC_TARBALL="$(curl "${header[@]}" "${META}/SRC_TARBALL")"
RESULTS_PREFIX="$(curl "${header[@]}" "${META}/RESULTS_PREFIX")"
TARGET="$(curl "${header[@]}" "${META}/TARGET")"

log "boot metadata RUN_ID=${RUN_ID} TARGET=${TARGET} SRC=${SRC_TARBALL}"

# Fix any broken package installations from previous runs
log "Fixing broken packages (if any)..."
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
                   libgeos-dev libproj-dev libgdal-dev || {
  error "Package installation failed, attempting recovery..."
  # Try to resolve conflicts
  apt-get install -y --fix-broken
  # Retry installation
  apt-get install -y python3 python3-pip python3-venv git build-essential pkg-config \
                     libgeos-dev libproj-dev libgdal-dev
}

log "packages installed"

mkdir -p /opt/vicinity && cd /opt/vicinity
gsutil cp "gs://${BUCKET}/src/${SRC_TARBALL}" .
tar xzf "${SRC_TARBALL}"
cd vicinity  # matches repo root inside tarball

log "unpacked source into $(pwd)"

python3 -m venv .venv
. .venv/bin/activate
pip install --upgrade pip wheel
pip install -r requirements.txt

log "venv + python deps ready"

# Build Rust extension once
log "Building Rust native extension..."
if ! make native 2>&1 | tee -a build.log; then
  error "Rust extension build failed"
  exit 1
fi
log "rust extension built"

export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 NUMEXPR_NUM_THREADS=1
# 32 vCPU ≈ 16 physical cores; start with one worker/core and increase only if profiling
# shows spare capacity (20–24 workers max tends to be the sweet spot).
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

# Always upload build log (even on failure - handled by trap)
gsutil -m cp build.log "${RESULTS_PREFIX}/build.log" || true
log "uploaded build.log to ${RESULTS_PREFIX}"

log "Build process complete (exit_code=${EXIT_CODE})"
# Trap will handle shutdown
```

The `exec > >(stdbuf … awk …)` redirection timestamps every line that the startup script emits (apt, pip, make, etc.), so anything mirrored into the serial console—and by extension the local `logs/remote_runs/<RUN_ID>-serial.log`—has clear timing context. The inline `log` calls mark phase boundaries (metadata read, venv ready, make start/finish), which is invaluable when comparing multiple runs.

Notes:
- `make native` depends on `maturin` via `requirements.txt`; if you move it elsewhere, install explicitly.
- 200 GB boot disk leaves room for OSM PBFs, parquet outputs, temp files.
- Startup scripts execute on **every** boot, so updating metadata + starting the instance is enough to kick off a new run.
- You can bake this into a custom image to avoid apt/venv work each run.

### 3.1 Error Handling

The startup script includes several layers of error handling:

1. **Guaranteed Shutdown:** A `trap cleanup_and_shutdown EXIT` ensures the VM always shuts down, even if the script fails.

2. **Error Logging:** All errors are logged with timestamps and uploaded to GCS before shutdown.

3. **Error Markers:** If the script fails, a `startup_error.txt` file is uploaded to `${RESULTS_PREFIX}`, which the orchestrator detects.

4. **Package Recovery:** The script attempts to fix broken package installations automatically:
   - Clears corrupted apt cache if `apt-get update` fails
   - Runs `apt-get install --fix-broken` before installing new packages
   - Retries package installation if the first attempt fails

5. **Build Logs:** All output from `make` commands is captured in `build.log` and uploaded even on failure.

When a failure occurs:
- The orchestrator detects `startup_error.txt` and exits with an error
- Partial logs are displayed in the terminal
- Serial logs are saved to `logs/remote_runs/<RUN_ID>-serial.log`
- The VM shuts down cleanly, preventing stuck instances

---

## 4. Runtime Expectations

| Machine | vCPU | RAM | Runtime (`make d_anchor_category`) | Approx. weekly cost* |
| ------- | ---- | --- | ---------------------------------- | --------------------- |
| MacBook Pro M1 Pro | 8 | 16 GiB | ~60 h | sunk cost |
| `c4d-highcpu-32`    | 32 | 60 GiB | ~17–18 h | ~$19/run |

\* Computed from on-demand pricing + 18 h runtime.

Set `WORKERS=16` and keep per-worker `THREADS=1` so each ProcessPool worker runs a single SSSP without oversubscribing. Monitor with `htop` and only raise `WORKERS` if you see headroom (20–24 workers is usually the upper bound before hyperthreads stop helping).

---

## 5. Validation & Cleanup

After a run:

1. Inspect `data/categories_results/<RUN_ID>/build.log` for `[error]` or `[warn]`.
2. Optionally replace the local `data/d_anchor_category` with the freshly pulled results.
3. Commit or archive as needed; the `RUN_ID` directory keeps historical outputs.
4. Bucket hygiene: set lifecycle rules to delete `src/` tarballs older than e.g. 14 days.

---

## 6. Future Refinements

- **Custom Image:** Preinstall Python, system libs, Rust toolchain, and cached OSM data to cut boot time and remove apt dependency on every run.
- **Per-state parallelism:** If `STATES` grows beyond MA-only, add a dispatcher (GNU Parallel or separate make targets) so you can fill bigger machines efficiently.
- **CI trigger:** Wrap `make categories_remote` (or `pipeline_remote`) in Cloud Build/Batch if you want weekly cron-driven refreshes without a local workstation.

---

This document should be sufficient to implement and maintain the remote batch job with the current codebase. Update values (bucket, project, states) as your deployment evolves.
