# Persistent sessions

A session keeps the VM's **filesystem state, working directory and configuration** across restarts. One session id =
one isolated session = one private GitHub repository holding its snapshots as Release assets. The Docker image itself is
never stored.

> Status: implemented and tested against fakes (GitHub Releases mock, fake `docker`, Worker tests with a mocked GitHub).
> It has **not** been run against real GitHub or a deployed Worker; the Docker layer has had a first real-Docker check (see the end). See "Verification status" at the end.

Added later (round 2): a host-side **command history with replay**, **background snapshot upload**, **deep snapshots**, a planned
**handoff to a fresh VM after about 5 hours**, and **held commands** during that handoff. Each has its own section below.

## What Claude sees

| Tool | Purpose |
|---|---|
| `start_runner(session, container?)` | `session="fresh"` creates a session and returns its id. `session=<id>` restores it. `container` picks the Docker image for a **new** session (default `ubuntu:24.04`); an existing session keeps its image. Safe to call repeatedly: it never starts a second VM, and it reports progress while the VM boots or restores. |
| `stop_runner(force?)` | Saves (verified), shuts the session down, then cancels the GitHub run. Refuses while a command runs unless `force=true`. Repeat the call to follow a long save. |
| `save_session(wait_seconds?, mode?)` | Save now. Large saves continue in the background; call again to follow. `mode="deep"` also records package manifests, image `VOLUME` data and runtime config (see "Deep snapshots"). |
| `bash_history(job_id?, limit?)` | The runner's command history, newest first, including commands that were refused. With `job_id`: one entry in full (command and end of output). Also lists commands the Worker is holding (see "Handoff"). |
| `list_sessions()` | Known sessions, image, last snapshot, whether one is running, and what GitHub currently runs. |

`bash`, `bash_status`, `bash_cancel`, `remote_upload` and `remote_download` keep working. In a session, `bash` runs **inside the
session container**. Without `start_runner`, a plain VM starts exactly as before and keeps nothing.
`bash` takes commands up to **60 000 characters** and, instead of `command`, a `replay_job_id` (a `job_id` from `bash` or
`bash_history`, or a `q-...` id for a held command) so a long command never has to be sent twice.

Only **one VM exists at a time**. "Multiple sessions" means multiple stored sessions that can each be restored; starting
one while another runs is refused with an instruction to call `stop_runner` first (nothing is changed or discarded).

## What is saved

`docker diff` lists what changed relative to the image; those entries (plus a list of deleted paths) are taken from
`docker export` and stored with the working directory and the session configuration (image, id). On restore the image is
pulled, a fresh container is created, and the changes are applied. Excluded: `/proc`, `/sys`, `/dev`, `/tmp/vm-jobs`,
`/etc/hosts|hostname|resolv.conf|mtab`, `/.dockerenv`, and Docker's `--init` mountpoint (`/usr/sbin/docker-init`, `/sbin/docker-init`).

**Not saved:** running processes, open sockets, memory, anything outside the container (the host VM). Background servers
must be started again after a restore.

Saves happen on `save_session`, on `stop_runner`, at the planned handoff and when keepalive asks for a final save (idle or
lifetime limit). **There are no periodic autosaves:** the loop that used to make them is deliberately switched off in
`SessionManager.tick`, so work since the last explicit save is lost if the VM dies without a graceful stop.
While a snapshot is *captured* the container is paused and commands are refused with a clear message; the capture goes to
local scratch space, and the **upload runs in the background** (see below).

## Storage format and integrity

* Repository `vmsession-<id>` under `RELAY_OWNER`, **private** (the Worker refuses to use a repo that is not private).
* A snapshot is a draft release `snap-uploading-<epoch>-<seq>` with `chunk-00001.bin, ...` (default 1 GiB, GitHub's asset
  limit is 2 GiB) and `manifest.json` (per-chunk size and SHA-256, whole-snapshot SHA-256, image, workdir, sequence).
* Every chunk is hashed while it is written; after upload GitHub's reported size and digest are compared with the local
  ones (download-and-hash fallback if no digest is returned). The whole snapshot is verified again on restore, chunk by
  chunk and as a whole.
* **Commit = one rename** of the release to `snap-<seq>`. Nothing older is deleted before that succeeds. A failed or killed
  save leaves the previous valid snapshot restorable. Obsolete snapshots and leftover drafts are removed afterwards (also at
  the next boot, which cleans drafts from a run that was killed mid-upload).
* The token never appears on a command line and is never sent to a CDN host (redirects are followed by hand and the
  `Authorization` header is dropped when the host changes, as in `relay.sh`).

### The 40 GB cap

`DEFAULT_CAP_BYTES = 40_000_000_000` per session repository. The previous snapshot is kept until the new one is verified,
so by default **previous + new must fit** in 40 GB: replacing a snapshot in place limits one snapshot to roughly **20 GB**
(a first save can use the whole 40 GB). Constructing the store with `cap_includes_previous=False` lets the *new* snapshot use
up to 40 GB on its own, at the price that the repository may briefly hold both snapshots (up to ~80 GB) until the old one is
removed after the commit. That is the trade-off between a strict 40 GB ceiling and being able to replace a large snapshot in
place; the default is the strict one.
A save that would exceed the cap fails before uploading and the previous snapshot is untouched. The runner also keeps
512 MB of disk free and stages chunks in `VM_SNAPSHOT_SCRATCH` (the workflow uses `/mnt/vm-scratch` if present).

## Concurrency and recovery

* **Epoch fencing.** Each start of a session bumps its epoch in the Worker. Before committing, a runner asks the Worker;
  a runner whose epoch is stale, whose start failed, or whose sequence number does not increase is refused. A replaced
  runner therefore cannot overwrite its successor's snapshot, and a runner whose restore failed never saves over the
  snapshot it could not read.
* **Start slot.** `RunnerCoordinator.requestDispatch()` stays the single gate: exactly one caller dispatches; others are
  rejected immediately, never queued.
* **Graceful shutdown.** `keepalive.sh`, on idle or near the 6 h limit (session VMs stop 30 min earlier), writes
  `/tmp/vm-stopping`; the agent saves and writes `/tmp/vm-saved` then `/tmp/vm-stop-now`; keepalive waits up to 25 min.
  (After `VM_HANDOFF_SECONDS` it writes `/tmp/vm-handoff` instead: see "Handoff".)
  `stop_runner` uses `POST /session/shutdown` and cancels the run only after the agent reports `stopped`.
* **Hard kill / lost VM.** The next `start_runner` restores the last committed snapshot. If `stop_runner` finds the VM
  dead or a shutdown hung > 30 min it cancels the run and says which snapshot will be restored.

## Background upload

A save has two parts. The **capture** (container paused) writes the snapshot to local scratch space; once it is done the
container runs again and the **upload** continues on its own thread. Only a snapshot that was uploaded, verified and
committed counts as saved: `/session/save` answers `200 saved` only then, `202 uploading` while the upload runs and `500` if it
failed (the previous snapshot stays valid and the session stays marked as having unsaved changes). One upload at a time; a
shutdown waits for one in flight. `/status` shows `uploading`, `captured_seq`, `durable_seq` and `upload_error`.

## Command history and replay

The agent keeps a SQLite history **on the host** (`~/.vm-agent/history.db`, mode 0600, outside the directory the container can
see). Every `POST /jobs` is recorded, **including refused ones** (busy, restoring, capturing, handoff), each with its own id.
`GET /history` and `GET /history/{id}` read it (commands are shortened in the list). A job can be run again with
`{replay_of: <id>}`; the agent looks the command up and applies the normal validation. Limits: 60 000 characters per command,
60 000 characters of output kept for a finished job (20 000 for a running one). The history is exported into every snapshot
as `.vm-snapshot/extra/history.sqlite` and merged back on restore; names under `/.vm-snapshot` planted from inside the
container are dropped when a snapshot is applied.

## Deep snapshots

A normal snapshot already keeps installed packages (their files and package databases), caches, dotfiles and configuration,
because they all live on the container filesystem that `docker diff` captures. **Deep** (`save_session mode="deep"`, and the
final save of a handoff) adds what that cannot carry: the image reference and digest, the container's runtime configuration
(user, working directory, environment **without** secret-looking names), package manifests (dpkg, apt-manual, apk, pip, npm -g,
cargo, gem) and the contents of the image's `VOLUME` directories (up to 96 MB each, 8 at most; larger ones are reported as
skipped). Restoring a deep snapshot restores the volumes and **compares the manifests with the restored container; drift is
reported, never repaired** (nothing is reinstalled). Shell variables exported in one command never persist: every command is
a new shell, only the working directory carries over.

## Handoff after about 5 hours

A hosted-runner job ends after 6 hours. At `VM_HANDOFF_SECONDS` (default 18000, session VMs only; ignored if 0 or not earlier
than the early session stop) keepalive writes `vm-handoff`. The agent then:

1. refuses new commands with **503 `handoff`** (also while it waits),
2. lets a running command finish for up to 10 minutes, then stops it,
3. makes a **deep** save (skipped only if nothing changed and the last snapshot was already deep),
4. tells the Worker (`handoff` event, then `stopped` with `handoff: true` and `saved`), and ends the run through the usual flags.

The Worker records the handoff on the session. It does **not** use a timer: the replacement starts on the **next tool call that
needs a VM**. That call clears the old registration, cancels the finished old run, and starts the same session with a deep
restore (the claim says `deep: true`). If nobody calls, nothing starts and the session simply stays saved. If the final save
failed, the replacement restores the last good snapshot and `list_sessions` says the save did not complete. `stop_runner`
cancels a pending handoff ("stay stopped"); a handoff nobody picked up for 12 hours is forgotten.

### Held commands

A `bash` command that arrives while the VM is being handed over (503 `handoff`, or the replacement is still booting) is **not
run**. If it was sent as `command`, the Worker keeps it under a `q-<16 hex>` id (at most 20, 60 000 characters each, 12 hours)
and says so. **Nothing runs by itself:** the client runs it with `bash replay_job_id="q-..."` once the VM is ready; the id is
dropped when the runner accepts it, and stays if it is refused again. A held command only runs in the session it was held for.
`bash_history` lists held commands.

## Failed commands (D1): every `bash` call that did not run has an id

A `bash` command can fail to start for reasons that have nothing to do with the command: no VM yet, the VM's tunnel is gone,
the runner is busy, restoring, **capturing a snapshot (container paused)**, stopping, handing over, or the runner's reply was
lost. The Worker never loses such a command. It saves it in a D1 table (binding `DB`, table `failed_commands`) and the reply says
`It was saved as ext-<uuid>`. Run it later with `bash replay_job_id="ext-<uuid>"`:

* **Any session, or a plain VM.** The table is a plain key-value store: the id is not tied to a session or a runner. The Worker
  sends the saved text as an ordinary command (it adds no `replay_of`, so an older agent works too).
* **Nothing runs by itself**, and a replay does **not** delete the row: it can be run again and again until it is pruned.
* **If the replay is refused as well**, the reply says `It stays saved as ext-...`: the same id, no second copy.
* **Outcome unknown** (both submit attempts got no answer): saved with `outcome = unknown` and the reply says it **MAY have run**,
  so check before replaying.
* **A command that did start** is in the runner's own history (`bash_history`, `job_id`), as before. That history lives on the VM
  and is saved only with a snapshot, so a command that was *accepted* and then lost with the VM is not in D1.
* `bash_history` lists the newest saved commands (also when no runner exists) and `bash_history job_id=ext-...` shows one in full.
* Limits: 60 000 characters per command. If the database is missing or unavailable the reply says so; a handoff then falls back
  to the older per-session `q-` queue. Old `q-` ids stay valid for their 12 hours. Commands may contain secrets: do not put any in.

**Pruning, without counting.** D1 bills *rows scanned* and *rows written*, so `COUNT(*)` over a 1 000-row table would cost about
1 000 rows read on every call. The Worker never counts. `seq` is the table's integer primary key, only the **oldest** rows are
ever deleted (so the next number is always max+1), and the number comes back free as `last_row_id` from the insert. When it reaches
**1 000** (then 1 300, 1 600, ...) the Worker runs one range delete, `DELETE ... WHERE seq <= seq - 700`: the oldest **300** go and
**700** stay. The table never holds more than 1 000 rows. A save reads nothing; a delete costs only the rows it removes. If a prune
fails the save still succeeds and the next prune (300 inserts later) removes everything older, so the table is at most 1 300 rows
in that case. Deleting one row per insert would write the same number of rows in total but needs one extra query per call.

**Setup (once per environment).**
1. `npx wrangler d1 create remote-linux-vm-failed-commands` (and `-staging` for staging); put the `database_id` in the
   `d1_databases` entry of `wrangler.jsonc` (the committed value is a placeholder). `wrangler.local.jsonc` needs the same entry.
2. `npx wrangler d1 migrations apply remote-linux-vm-db --remote` (add `--env staging` for staging).
3. Deploy the Worker. Without the binding the feature is off and `bash` says that a failed command could not be saved.

## Starting a VM no longer needs a person

Before dispatching, the Worker looks at GitHub. A run that is provably stuck (older than the 15-minute start window, or
belonging to a registered runner whose tunnel is gone / unhealthy past the grace period) is **cancelled by the Worker**
(cancel, then force-cancel) and a new VM is started. A young run, or one whose age GitHub did not report, is left alone
and the reply says it is probably still booting and to retry; it never tells anyone to cancel manually.

## Configuration

No new secrets. Uses `GITHUB_PAT` (Actions read/write), `RELAY_CLASSIC_PAT` (create the private repo), `RELAY_APP_ID`,
`RELAY_INSTALLATION_ID`, `RELAY_APP_PRIVATE_KEY` (per-session token, `contents: write` on that one repository, checked to
see exactly one repository before release) and `RELAY_OWNER`. The workflow needs the repository variable `VM_BASE_URL`.
Wrangler: the `SESSIONS` Durable Object binding and migration `v3-sessions` (already in `wrangler.jsonc`).
Agent env: `VM_SNAPSHOT_CHUNK_BYTES`, `VM_SNAPSHOT_SCRATCH`, `VM_BASE_URL`. Never set `VM_SNAPSHOT_TEST`.
keepalive env: `VM_HANDOFF_SECONDS` (default 18000), plus the existing `VM_MAX_SECONDS`, `VM_SESSION_RESERVE_SECONDS`,
`VM_SAVE_WAIT_SECONDS`. The workflow copies **every** `vm_*.py` next to the agent (`test_workflow_files.py` fails if one is
missing: `vm_history` is imported unconditionally).

## Limits

* One VM at a time. A relay transfer into a session goes through a host staging directory and then `docker cp`, so the
  sender's `RESULT: OK` covers only the first hop; check the VM job with `bash_status`.
* Images must be public or already pullable by the runner (no private-registry credentials).
* No `delete_session`: delete the `vmsession-<id>` repository by hand.
* Repository tokens are not revoked at shutdown; they expire (about 1 hour) and are scoped to one repository.
* **Image-declared `VOLUME` directories are not part of a normal snapshot.** `docker export`/`diff` exclude volume contents, so in a
  real-Docker experiment (session 7) a file written under an image's `VOLUME /data` was gone after capture and restore, while
  files in `/workspace` and `/srv` survived. A **deep** save does capture them (up to 96 MB per volume); a normal save still does
  not and says nothing. Keep session data outside any VOLUME path, or save deep. (The deep volume path itself has only been
  run against the fake docker.)
* No periodic autosave (see "What is saved"). The handoff replacement starts only when a tool call needs a VM.
* The job workdir `/workspace` is created `root:root 755`; an image whose default `USER` is not root cannot write there
  unless the session user is root (seen with `USER ubuntu`; session 8, not yet fixed).

## Verification status

Ran and passed: Python 228 tests, 6 skipped (the opt-in real-Docker tests, `VM_REAL_DOCKER=1`; the >1 GiB one also needs `VM_REAL_DOCKER_LARGE=1`) (snapshot store, container, session manager, agent, keepalive shell tests with real
subprocesses), Worker 184 tests (including the failed-commands store against a local D1 simulation, not real D1), `tsc --noEmit`; the workflow's claim step was executed with a fake `curl`.
**Run against real Docker 28.0.4 (overlay2) on a hosted-runner VM:** a `SessionContainer` capture -> wipe -> restore
cycle (`test_real_docker.py`): unicode/space paths, symlinks, hardlinks (incl. to a base-image file), owner/mode bits, empty
dirs, modified and deleted base files/dirs, 3+ chunks, container resumed after `pause`; alpine/python-slim shell detection; capture while a process is writing (consistent prefix);
a ~1.5 GiB snapshot through capture/apply at the default chunk size; and cross-file atomicity (a lockstep writer with an
incompressible filler between its two files). The atomicity test is mutation-checked: it passes with `pause` and fails 5/5 with
`pause` disabled, so it does prove the pause.
**Never run:** real GitHub draft-release behaviour (the >1 GiB snapshot was moved only through a local container, not GitHub), the workflow on a hosted
runner, a Cloudflare deploy.
`relay_test.sh`: the original `mock2.py` was missing from the upload, so section 12 (put/get against a mock GitHub) could not run.
A **reconstructed** `mock2.py` (written in session 7 from what the section asserts and what `relay.sh` sends; NOT the original) now
lets it run: full script 136 passed / 0 failed. Mutation-checked against `relay.sh` copies (no Accept header, token forwarded to the CDN host,
digest check skipped: each is caught). It shows `relay.sh` is consistent with that model of the Releases API, not that it matches real GitHub.
