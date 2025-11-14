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

Everything is orchestrated by `scripts/run_remote.sh`, which the Makefile target (`categories_remote` or `pipeline_remote`) invokes.

---

## 1. One-time GCP Setup (existing inactive VM)

We assume a `c4d-highcpu-32` instance already exists in your project (e.g., `vicinity-batch` in `us-east4-c`) but is currently stopped. The orchestrator will **reuse** that VM on every run, so the only bootstrap work is wiring up the metadata, buckets, and startup script once.

1. **Confirm the reusable VM is stopped:** Use the GCE console or the equivalent `gcloud compute` calls to verify the instance is TERMINATED, optionally starting and stopping it to be sure the lifecycle behaves. Document any stateful tweaks so they can be recreated if the disk ever needs to be rebuilt.

2. **Attach the startup script once:** Add `scripts/startup_categories_vm.sh` as instance metadata so it runs automatically on every boot that the orchestrator triggers.

3. **Bucket (regional, near the VM):** Create a regional bucket in `us-east4` (for example `vicinity-batch-$USER`) with `src/` reserved for uploaded tarballs and `results/` reserved for per-run artifacts.

4. **Service account for the VM:** Create a dedicated service account such as `vicinity-batch-sa` and grant Storage Admin so the VM can download source and upload results; only add extra permissions if the workload uses additional APIs.

5. **Local gcloud defaults (run once per workstation):** Authenticate, then set project, region, and zone defaults so the helper script can rely on consistent settings.

---

## 2. Repo Integration

### 2.1 Makefile fragment

The Makefile exposes two orchestration targets. `categories_remote` sets `TARGET=d_anchor_category`, while `pipeline_remote` sets `TARGET=all`. Both targets pass the same core variables—`PROJECT_ID`, `ZONE`, `INSTANCE_NAME`, and `BUCKET`—into `scripts/run_remote.sh`, ensuring every run reuses the same stopped VM and bucket without recreating infrastructure.

### 2.2 Orchestrator (`scripts/run_remote.sh`)

Key behavior:

- Validates the required environment variables, stamps each run with a unique `RUN_ID`, and writes logs under `logs/remote_runs/<RUN_ID>-serial.log`.
- Archives tracked files only, uploads the tarball to `gs://$BUCKET/src/`, and updates instance metadata with the run parameters so the next boot knows what to execute.
- Starts the reusable VM, tails the serial console in real time, and waits for the instance to return to the TERMINATED state before proceeding.
- Rsyncs `gs://$BUCKET/results/<RUN_ID>` into `data/categories_results/<RUN_ID>`, refreshes the `latest` symlink/folder, and—when the full pipeline runs—fans outputs back into the canonical local directories.
- Shows the tail of the remote `build.log` immediately so failures surface without digging through directories.

Feel free to add logging, Slack hooks, etc.

Those exclusions line up with the generated directories called out in `docs/ARCHITECTURE_OVERVIEW.md` and `README.md` (`data/`, `tiles/`, `state_tiles/`, `out/`) and the Makefile targets that recreate them (`anchors`, `minutes`, `d_anchor_*`, etc.), so the tarball contains only the source + configs the VM actually needs.

### 2.3 Troubleshooting `run_remote.sh`

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
- **Local replay:** Because `run_remote.sh` now tees that stream into `logs/remote_runs/<RUN_ID>-serial.log`, you can re-run `less +F logs/remote_runs/<RUN_ID>-serial.log` to revisit the exact boot you just kicked off.
- **SSH + tail:** Once the instance is up, run `gcloud compute ssh "${INSTANCE_NAME}" --zone "${ZONE}" --command "cd /opt/vicinity/vicinity && tail -f build.log"` to watch the pipeline log file (assuming the startup script pipes `make ... |& tee build.log`, which it already relies on when uploading results). Replace `build.log` with `/var/log/syslog` if you need system-level messages.
- **Ad-hoc commands:** Use `gcloud compute ssh` without `--command` to open an interactive shell and run any other diagnostics (`htop`, `tail -f data/d_anchor_category/*.log`, etc.). Exit once satisfied; the remote make continues unattended.

Each of these commands can run in parallel with `make categories_remote`, giving you local visibility into the remote job without waiting for `TERMINATED`.

Every boot also spawns lightweight telemetry loops (`vmstat`/`mpstat`) whose output is prefixed with `[vmstat]` / `[mpstat]` before being mirrored to the serial console and saved log. That means you automatically capture CPU utilization and `%wa` samples for both `categories_remote` and `pipeline_remote` without opening an SSH session; just search the serial transcript for those tags. `[timer]` lines clock the major phases (apt, pip, Rust build, make target, total runtime) so you can compare runs at a glance. Verbose dependency steps (apt/pip/rustup) are muted from the serial stream; the full transcript is uploaded as `${RESULTS_PREFIX}/startup_detail.log` and pulled down to `data/categories_results/<RUN_ID>/startup_detail.log`.

---

## 3. VM Startup Script (`scripts/startup_categories_vm.sh`)

The startup script is a full bootstrapper: it reads metadata for the current run, timestamps every line of output, fixes broken apt state if needed, installs system packages, downloads the uploaded tarball into `/opt/vicinity`, recreates the virtual environment, installs Python dependencies, builds the Rust extension, and runs either `make all` or `make d_anchor_category`. Single-threaded math libraries keep resource usage predictable, while environment variables (notably `WORKERS`) scale with the VM size. Each major phase logs explicit markers so the serial console and saved logs show clear boundaries, and every run uploads its `build.log` plus the relevant result directories before powering off. Notes:
- `make native` depends on `maturin` from `requirements.txt`, so keep that requirement in sync if the build location changes.
- A 200 GB boot disk leaves headroom for OSM inputs, intermediate parquet files, and temporary artifacts.
- Because startup scripts execute on every boot, updating metadata + starting the instance is all it takes to kick off a new run.
- Consider baking the dependencies into a custom image to avoid repeating apt/venv work if boot time ever becomes painful.

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
| `c4d-highcpu-96`    | 96 | 180 GiB | ~8–10 h  | ~$56/run |

\* Computed from on-demand pricing + 18 h runtime.

Tuning guidance for high-core runs:
- Keep `THREADS=1` (enforced via `OMP_NUM_THREADS`, `MKL_NUM_THREADS`, `OPENBLAS_NUM_THREADS`, `NUMEXPR_{NUM,MAX}_THREADS`) so each worker drives a single SSSP.
- Start with `WORKERS = ⌊vCPU / 3⌋` (32 on c4d-highcpu-96). This keeps plenty of memory headroom (~2.5–3 GiB RSS per worker while writing parquet).
- During `make d_anchor_category`, monitor:
  - `htop` (if you SSH in) or the `[mpstat]` stream in the serial log for overall CPU% (target ≥90 % when CPU-bound),
  - `[vmstat]` rows (emitted by the scripted `vmstat 1`) for `wa` (keep <5–10 %; higher means PD throughput is throttling),
  - `free -h` / `ps` for remaining RAM (stop if <20–25 % free).
- If the telemetry firehose is too chatty, set `TELEMETRY_INTERVAL` (Makefile/run_remote knob) to a higher value so the background `vmstat`/`mpstat` samplers emit less frequently.
- If CPU <70 % and `wa` stays low, rerun with +4–8 workers; stop increasing once `wa` spikes or memory drops below the safe headroom.
- Record wall-clock runtime, average CPU%, and average `wa` from each run (serial logs already timestamp every phase) so speedups are measurable—expect ~1.8–2.1× vs the c4d-highcpu-32 baseline when I/O keeps up.

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
