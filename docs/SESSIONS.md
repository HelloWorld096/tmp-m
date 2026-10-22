# Persistent sessions

A session keeps the VM's **filesystem state, working directory and configuration** across restarts. One session id =
one isolated session = one private GitHub repository holding its snapshots as Release assets. The Docker image itself is
never stored.

> Status: implemented and tested against fakes (GitHub Releases mock, fake `docker`, Worker tests with a mocked GitHub).
> It has **not** been run against real GitHub or a deployed Worker; the Docker layer has had a first real-Docker check (see the end). See "Verification status" at the end.

## What Claude sees

| Tool | Purpose |
|---|---|
| `start_runner(session, container?)` | `session="fresh"` creates a session and returns its id. `session=<id>` restores it. `container` picks the Docker image for a **new** session (default `ubuntu:24.04`); an existing session keeps its image. Safe to call repeatedly: it never starts a second VM, and it reports progress while the VM boots or restores. |
| `stop_runner(force?)` | Saves (verified), shuts the session down, then cancels the GitHub run. Refuses while a command runs unless `force=true`. Repeat the call to follow a long save. |
| `save_session(wait_seconds?)` | Save now. Large saves continue in the background; call again to follow. |
| `list_sessions()` | Known sessions, image, last snapshot, whether one is running, and what GitHub currently runs. |

`bash`, `bash_status`, `bash_cancel`, `remote_upload` and `remote_download` keep working. In a session, `bash` runs **inside the
session container**. Without `start_runner`, a plain VM starts exactly as before and keeps nothing.

Only **one VM exists at a time**. "Multiple sessions" means multiple stored sessions that can each be restored; starting
one while another runs is refused with an instruction to call `stop_runner` first (nothing is changed or discarded).

## What is saved

`docker diff` lists what changed relative to the image; those entries (plus a list of deleted paths) are taken from
`docker export` and stored with the working directory and the session configuration (image, id). On restore the image is
pulled, a fresh container is created, and the changes are applied. Excluded: `/proc`, `/sys`, `/dev`, `/tmp/vm-jobs`,
`/etc/hosts|hostname|resolv.conf|mtab`, `/.dockerenv`, and Docker's `--init` mountpoint (`/usr/sbin/docker-init`, `/sbin/docker-init`).

**Not saved:** running processes, open sockets, memory, anything outside the container (the host VM). Background servers
must be started again after a restore.

Saves happen: after an idle period when something changed (60 s idle, at most every 10 min), on `save_session`, and on
`stop_runner`. While a snapshot is captured the container is paused and commands are refused with a clear message.

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
  `stop_runner` uses `POST /session/shutdown` and cancels the run only after the agent reports `stopped`.
* **Hard kill / lost VM.** The next `start_runner` restores the last committed snapshot. If `stop_runner` finds the VM
  dead or a shutdown hung > 30 min it cancels the run and says which snapshot will be restored.

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

## Limits

* One VM at a time. A relay transfer into a session goes through a host staging directory and then `docker cp`, so the
  sender's `RESULT: OK` covers only the first hop; check the VM job with `bash_status`.
* Images must be public or already pullable by the runner (no private-registry credentials).
* No `delete_session`: delete the `vmsession-<id>` repository by hand.
* Repository tokens are not revoked at shutdown; they expire (about 1 hour) and are scoped to one repository.
* **Image-declared `VOLUME` directories are not saved.** `docker export`/`diff` exclude volume contents, so in a real-Docker
  experiment (session 7) a file written under an image's `VOLUME /data` was gone after capture and restore, while files in
  `/workspace` and `/srv` survived. Nothing reports this at save time. Keep session data outside any VOLUME path of the image.
* The job workdir `/workspace` is created `root:root 755`; an image whose default `USER` is not root cannot write there
  unless the session user is root (seen with `USER ubuntu`; session 8, not yet fixed).

## Verification status

Ran and passed: Python 181 tests (+6 opt-in real-Docker tests, `VM_REAL_DOCKER=1`; the >1 GiB one also needs `VM_REAL_DOCKER_LARGE=1`) (snapshot store, container, session manager, agent, keepalive shell tests with real
subprocesses), Worker 145 tests, `tsc --noEmit`; the workflow's claim step was executed with a fake `curl`.
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
