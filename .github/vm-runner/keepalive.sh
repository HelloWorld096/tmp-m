#!/usr/bin/env bash
# Holds the workflow run open while the VM is useful, and ends it when it is not:
#
#   * idle:  no request and no running job for VM_IDLE_SECONDS (default 60 min); or
#   * limit: the VM has been up for VM_MAX_SECONDS (default 6 h, GitHub's limit for a hosted-runner job). Always, even mid-job; or
#   * dead:  the agent or the tunnel process has been down for VM_DEAD_CHECKS consecutive checks
#            AND nothing has touched the activity file for VM_DEAD_STALE_SECONDS.
#
# Why "dead" matters: the workflow uses one concurrency group with cancel-in-progress=false, so a
# replacement VM cannot start until this run ends. Without this check a crashed agent would block
# every replacement for up to the idle timeout. The staleness condition is what keeps a *busy* VM
# safe: a running job touches the activity file every ~10 s, so a slow /health caused by a CPU-bound
# build can never get a live build killed.
#
# All knobs are environment variables so tests can run it in seconds.
set -uo pipefail

ACTIVITY_FILE="${VM_ACTIVITY_FILE:-/tmp/vm-activity}"
IDLE_LIMIT="${VM_IDLE_SECONDS:-3600}"
MAX_RUNTIME="${VM_MAX_SECONDS:-21600}"
# The workflow writes the epoch second at which the run began, so setup time counts against the limit.
# Without that file (e.g. in tests) the clock starts when this script does.
START_FILE="${VM_START_FILE:-/tmp/vm-started}"
POLL="${VM_POLL_SECONDS:-15}"
HEALTH_URL="${VM_AGENT_HEALTH_URL:-http://127.0.0.1:8000/health}"
TUNNEL_PID_FILE="${VM_TUNNEL_PID_FILE:-/tmp/tunnel.pid}"
DEAD_CHECKS="${VM_DEAD_CHECKS:-4}"
DEAD_STALE="${VM_DEAD_STALE_SECONDS:-120}"

started=$(cat "$START_FILE" 2>/dev/null || true)
case "$started" in ''|*[!0-9]*) started=$(date +%s) ;; esac

dead=0
while true; do
  now=$(date +%s)
  if [ $((now - started)) -ge "$MAX_RUNTIME" ]; then
    echo "Maximum VM lifetime reached."
    exit 0
  fi
  # mtime, not contents: a half-written file can never crash this loop.
  last=$(stat -c %Y "$ACTIVITY_FILE" 2>/dev/null || echo "$now")
  age=$((now - last))
  if [ "$age" -ge "$IDLE_LIMIT" ]; then
    echo "Idle timeout reached."
    exit 0
  fi

  healthy=1
  curl -fsS --max-time 5 "$HEALTH_URL" >/dev/null 2>&1 || healthy=0
  pid=$(cat "$TUNNEL_PID_FILE" 2>/dev/null || true)
  if [ -z "$pid" ] || ! kill -0 "$pid" 2>/dev/null; then healthy=0; fi

  if [ "$healthy" = 1 ]; then dead=0; else dead=$((dead + 1)); fi
  if [ "$dead" -ge "$DEAD_CHECKS" ] && [ "$age" -ge "$DEAD_STALE" ]; then
    echo "Agent or tunnel is down and the VM is idle; ending this run so a replacement can start."
    exit 0
  fi
  sleep "$POLL"
done
