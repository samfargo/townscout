# Remote VM Spec for `make d_anchor_category`

This runbook documents the three execution modes we support:

1. **Full local run** – `make all` (or helper target `pipeline_local`).
2. **Local partial runs** – e.g., `make d_anchor_category` via `categories_local`.
3. **Full remote run** – `make pipeline_remote` (or `categories_remote` for just the heavy batch), which offloads the work to a Google Cloud VM.

Everything below is aligned with the current repo (Python + Rust extension, `Makefile` targets, GCS buckets) and assumes the remote runs use the c4d-highcpu-32 machine (32 vCPU / 60 GiB).

---

## 0. High-level Flow

Local runs (`pipeline_local` / `categories_local`) stay entirely on your workstation using the existing Make targets. The remainder of this spec covers the remote workflow that mirrors those targets on a VM.

When you run `make categories_remote` (categories only) or `make pipeline_remote` (full pipeline):

1. Package the repo (or a curated subset) into a tarball.
2. Upload that tarball to `gs://<BUCKET>/src/`.
3. Create a `c4d-highcpu-32` VM whose startup script:
   - downloads the tarball,
   - installs deps (Python 3.11, `pip`, `maturin`, system libs),
   - runs either `make d_anchor_category` or `make all`, depending on `TARGET`,
   - syncs the relevant outputs (`data/d_anchor_category` only, or anchors/minutes/tiles/D_anchor) to `gs://<BUCKET>/results/<RUN_ID>/`,
   - shuts the VM down.
4. Wait for the VM to reach `TERMINATED`.
5. `gsutil rsync` results into the local `data/categories_results/<RUN_ID>` (and, if running the full pipeline, fan the outputs back into the main data/ directories).
6. Delete the VM (or simply stop it if you reuse a long-lived worker).

Everything is orchestrated by `scripts/run_categories_remote.sh`, which the Makefile target (`categories_remote` or `pipeline_remote`) invokes.

---

## 1. One-time GCP Setup

> Already have a VM (`first-instance`, stopped in `us-east4-c`)? Either treat it as the reusable worker (start it, scp the repo tarball over SSH, run the startup script manually, then shut it down), or bake an image from it and use that image in the orchestration below. The steps here assume you spin up ephemeral instances, but the bucket/service-account pieces still apply if you keep the pre-created machine.

1. **Bucket (regional, near the VM):**
   ```bash
   export BUCKET=vicinity-batch-$USER
   gsutil mb -l us-east4 gs://$BUCKET/
   ```
   Use prefixes:
   - `gs://$BUCKET/src/` – uploaded source tarballs.
   - `gs://$BUCKET/results/` – VM outputs per run (`<RUN_ID>/...`).

2. **Service account for the VM:**
   ```bash
   gcloud iam service-accounts create vicinity-batch-sa \
     --display-name="Vicinity batch jobs"

   gcloud projects add-iam-policy-binding $PROJECT_ID \
     --member="serviceAccount:vicinity-batch-sa@$PROJECT_ID.iam.gserviceaccount.com" \
     --role="roles/storage.admin"
   ```
   Storage Admin is enough for “pull code / push results”. Add more roles only if the job touches other APIs.

3. **Local gcloud defaults (run once per workstation):**
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
PROJECT_ID ?= your-project-id
ZONE       ?= us-east4-b
MACHINE    ?= c4d-highcpu-32
BUCKET     ?= vicinity-batch-$(USER)
.PHONY: categories_remote
categories_remote:
	TARGET=d_anchor_category \
	PROJECT_ID=$(PROJECT_ID) \
	ZONE=$(ZONE) \
	MACHINE=$(MACHINE) \
	BUCKET=$(BUCKET) \
	./scripts/run_categories_remote.sh

.PHONY: pipeline_remote
pipeline_remote:
	TARGET=all \
	PROJECT_ID=$(PROJECT_ID) \
	ZONE=$(ZONE) \
	MACHINE=$(MACHINE) \
	BUCKET=$(BUCKET) \
	./scripts/run_categories_remote.sh

.PHONY: categories_local
categories_local: build/native.stamp
	THREADS=1 WORKERS=32 $(MAKE) d_anchor_category

.PHONY: pipeline_local
pipeline_local:
	$(MAKE) all
```

`categories_remote` keeps the old behavior (just the expensive D_anchor categories). `pipeline_remote` flips a single flag (`TARGET=all`) so the VM builds the entire graph/anchoring/minutes/tiles/D_anchor chain. `categories_local` and `pipeline_local` let you stay on the laptop when iteration speed matters.

### 2.2 Orchestrator (`scripts/run_categories_remote.sh`)

Key behavior (trimmed shell):

```bash
#!/usr/bin/env bash
set -euo pipefail

: "${PROJECT_ID:?set PROJECT_ID}"
: "${ZONE:?set ZONE}"
: "${MACHINE:?set MACHINE}"
: "${BUCKET:?set BUCKET}"

# shellcheck disable=SC2034
: "${TARGET:=d_anchor_category}"

RUN_ID="$(date +%Y%m%d-%H%M%S)"
INSTANCE_NAME="vicinity-categories-${RUN_ID}"
SRC_TARBALL="vicinity-src-${RUN_ID}.tar.gz"
SRC_PATH="gs://${BUCKET}/src/${SRC_TARBALL}"
RESULTS_PREFIX="gs://${BUCKET}/results/${RUN_ID}"
REPO_ROOT="$(git rev-parse --show-toplevel)"
TARGET="${TARGET:-d_anchor_category}"

# Keep the upload lean: skip generated artefacts rebuilt by the Makefile
# (`data/`, `tiles/`, `state_tiles/`, `out/`) plus local env/build dirs.
EXCLUDES=(
  --exclude='.venv'
  --exclude='build'
  --exclude='data'
  --exclude='tiles'
  --exclude='state_tiles'
  --exclude='out'
)
tar czf "/tmp/${SRC_TARBALL}" "${EXCLUDES[@]}" -C "${REPO_ROOT}" .
# Or guarantee "tracked only" content via git archive:
# git -C "${REPO_ROOT}" archive --format=tar.gz --output "/tmp/${SRC_TARBALL}" HEAD
gsutil cp "/tmp/${SRC_TARBALL}" "${SRC_PATH}"

gcloud compute instances create "${INSTANCE_NAME}" \
  --project "${PROJECT_ID}" \
  --zone "${ZONE}" \
  --machine-type "${MACHINE}" \
  --boot-disk-size "200GB" \
  --service-account "vicinity-batch-sa@${PROJECT_ID}.iam.gserviceaccount.com" \
  --scopes "https://www.googleapis.com/auth/cloud-platform" \
  --metadata "RUN_ID=${RUN_ID},BUCKET=${BUCKET},SRC_TARBALL=${SRC_TARBALL},RESULTS_PREFIX=${RESULTS_PREFIX},TARGET=${TARGET}" \
  --metadata-from-file "startup-script=scripts/startup_categories_vm.sh"

# Stream the VM console directly into this terminal so the remote build looks like a local run.
gcloud compute instances tail-serial-port-output "${INSTANCE_NAME}" \
  --zone "${ZONE}" \
  --monitor \
  --port 1 &
LOG_TAIL_PID=$!
cleanup() {
  if [[ -n "${LOG_TAIL_PID:-}" ]]; then
    kill "${LOG_TAIL_PID}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

until gcloud compute instances describe "${INSTANCE_NAME}" --zone "${ZONE}" \
      --format='value(status)' | grep -q TERMINATED; do
  sleep 30
done

LOCAL_RESULTS_DIR="${REPO_ROOT}/data/categories_results/${RUN_ID}"
mkdir -p "${LOCAL_RESULTS_DIR}"
gsutil -m rsync -r "${RESULTS_PREFIX}" "${LOCAL_RESULTS_DIR}"

rm -rf "${REPO_ROOT}/data/categories_results/latest"
cp -R "${LOCAL_RESULTS_DIR}" "${REPO_ROOT}/data/categories_results/latest"

gcloud compute instances delete "${INSTANCE_NAME}" --zone "${ZONE}" --quiet
echo "Remote batch complete → ${LOCAL_RESULTS_DIR}"
```

Feel free to add logging, Slack hooks, etc.

Those exclusions line up with the generated directories called out in `docs/ARCHITECTURE_OVERVIEW.md` and `README.md` (`data/`, `tiles/`, `state_tiles/`, `out/`) and the Makefile targets that recreate them (`anchors`, `minutes`, `d_anchor_*`, etc.), so the tarball contains only the source + configs the VM actually needs.

### 2.3 Troubleshooting `run_categories_remote.sh`

If the polling loop never sees `TERMINATED` or the startup script exits early, use these quick checks before deleting the VM:

- **Tail the serial console:** `gcloud compute instances get-serial-port-output "${INSTANCE_NAME}" --zone "${ZONE}" | tail -n 200` surfaces shell errors from `startup_categories_vm.sh` (missing deps, pip failure, etc.).
- **Grab detailed logs:** `gcloud compute scp --zone "${ZONE}" "${INSTANCE_NAME}:/var/log/syslog" /tmp/${RUN_ID}-syslog` or `gcloud compute ssh "${INSTANCE_NAME}" --zone "${ZONE}" --command "sudo journalctl -u google-startup-scripts --no-pager | tail -n 200"` shows apt/venv progress when the VM is stuck in `RUNNING`.
- **Recover partial artifacts:** Even if the script failed before the final rsync, you can copy `/opt/vicinity/vicinity/build.log` (and any `data/d_anchor_*` outputs) via `gcloud compute scp` or run `gsutil -m rsync` from the instance to `${RESULTS_PREFIX}` manually, then resume the local `gsutil rsync`.
- **Force cleanup:** Once you have the logs, `gcloud compute instances delete "${INSTANCE_NAME}" --zone "${ZONE}" --keep-disks=all` lets you preserve the boot disk for later inspection; omit `--keep-disks` to tear everything down and avoid charges.

### 2.4 Watching Remote Logs Live

The snippet above automatically tails the serial console (port 1) after the VM is created, so you immediately see the remote `make` output in the same terminal that invoked `make categories_remote` **or** `make pipeline_remote`. A few related tricks:

- **Serial console streaming:** `gcloud compute instances tail-serial-port-output "${INSTANCE_NAME}" --zone "${ZONE}" --monitor` follows the same console that prints during `make pipeline_local`, including startup-script output.
- **SSH + tail:** Once the instance is up, run `gcloud compute ssh "${INSTANCE_NAME}" --zone "${ZONE}" --command "cd /opt/vicinity/vicinity && tail -f build.log"` to watch the pipeline log file (assuming the startup script pipes `make ... |& tee build.log`, which it already relies on when uploading results). Replace `build.log` with `/var/log/syslog` if you need system-level messages.
- **Ad-hoc commands:** Use `gcloud compute ssh` without `--command` to open an interactive shell and run any other diagnostics (`htop`, `tail -f data/d_anchor_category/*.log`, etc.). Exit once satisfied; the remote make continues unattended.

Each of these commands can run in parallel with `make categories_remote`, giving you local visibility into the remote job without waiting for `TERMINATED`.

---

## 3. VM Startup Script (`scripts/startup_categories_vm.sh`)

```bash
#!/usr/bin/env bash
set -euo pipefail
META="http://metadata.google.internal/computeMetadata/v1/instance/attributes"
header=(-H "Metadata-Flavor: Google")

RUN_ID="$(curl "${header[@]}" "${META}/RUN_ID")"
BUCKET="$(curl "${header[@]}" "${META}/BUCKET")"
SRC_TARBALL="$(curl "${header[@]}" "${META}/SRC_TARBALL")"
RESULTS_PREFIX="$(curl "${header[@]}" "${META}/RESULTS_PREFIX")"
TARGET="$(curl "${header[@]}" "${META}/TARGET")"

apt-get update -y
apt-get install -y python3 python3-pip python3-venv git build-essential pkg-config \
                   libgeos-dev libproj-dev libgdal-dev

mkdir -p /opt/vicinity && cd /opt/vicinity
gsutil cp "gs://${BUCKET}/src/${SRC_TARBALL}" .
tar xzf "${SRC_TARBALL}"
cd vicinity  # matches repo root inside tarball

python3 -m venv .venv
. .venv/bin/activate
pip install --upgrade pip wheel
pip install -r requirements.txt

# Build Rust extension once
make native

export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 NUMEXPR_NUM_THREADS=1
export THREADS=1 WORKERS=32  # 32 workers fully occupy c4d-highcpu-32

if [[ "${TARGET}" == "all" ]]; then
  make all
  gsutil -m rsync -r data/anchors "${RESULTS_PREFIX}/data/anchors"
  gsutil -m rsync -r data/minutes "${RESULTS_PREFIX}/data/minutes"
  gsutil -m rsync -r tiles "${RESULTS_PREFIX}/tiles"
  gsutil -m rsync -r data/d_anchor_category "${RESULTS_PREFIX}/data/d_anchor_category"
  gsutil -m rsync -r data/d_anchor_brand "${RESULTS_PREFIX}/data/d_anchor_brand"
else
  make d_anchor_category
  gsutil -m rsync -r data/d_anchor_category "${RESULTS_PREFIX}/data/d_anchor_category"
fi

gsutil -m cp build.log "${RESULTS_PREFIX}/build.log" || true

shutdown -h now
```

Notes:
- `make native` depends on `maturin` via `requirements.txt`; if you move it elsewhere, install explicitly.
- 200 GB boot disk leaves room for OSM PBFs, parquet outputs, temp files.
- You can bake this into a custom image to avoid apt/venv work each run.

---

## 4. Runtime Expectations

| Machine | vCPU | RAM | Runtime (`make d_anchor_category`) | Approx. weekly cost* |
| ------- | ---- | --- | ---------------------------------- | --------------------- |
| MacBook Pro M1 Pro | 8 | 16 GiB | ~60 h | sunk cost |
| `c4d-highcpu-32`    | 32 | 60 GiB | ~17–18 h | ~$19/run |

\* Computed from on-demand pricing + 18 h runtime.

Set `WORKERS=32` and keep per-worker `THREADS=1` so each ProcessPool worker runs a single SSSP without oversubscribing. Monitor with `htop` if you tweak values.

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
