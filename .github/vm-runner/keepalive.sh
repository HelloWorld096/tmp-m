#!/usr/bin/env bash
# Holds the workflow run open while the VM is useful, and ends it when it is not:
#
#   * idle:  no request and no running job for VM_IDLE_SECONDS (default 60 min); or
#   * limit: the VM has been up for VM_MAX_SECONDS (default 6 h, GitHub's limit for a hosted-runner job). Always, even mid-job; or
#   * dead:  the agent or the tunnel process has been down for VM_DEAD_CHECKS consecutive checks
#            AND nothing has touched the activity file for VM_DEAD_STALE_SECONDS.
#
# Why "dead" matters: only one VM run may exist at a time (the Worker will not dispatch while a run is
# active, and the workflow's first step refuses a second one), so a replacement cannot start until this
# run ends. Without this check a crashed agent would block every replacement for up to the idle timeout. The staleness condition is what keeps a *busy* VM
# safe: a running job touches the activity file every ~10 s, so a slow /health caused by a CPU-bound
# build can never get a live build killed.
#
# Persistent sessions: when the agent has attached a session (it creates vm-session-active), idle or the lifetime
# limit no longer end the run at once. keepalive writes vm-stopping, which makes the agent do a final verified save,
# and waits up to VM_SAVE_WAIT_SECONDS for the agent's vm-saved flag. The lifetime limit is moved earlier by
# VM_SESSION_RESERVE_SECONDS so the save finishes before GitHub's hard 6 h cut-off (which cannot be waited out).
# The agent's vm-stop-now flag (written after any shutdown, or when this runner was superseded) ends the run at
# any time. A dead agent still ends the run immediately: nothing is left that could save.
#
# Handoff: a session VM that has been up for VM_HANDOFF_SECONDS (default 5 h) writes vm-handoff instead of vm-stopping. The
# agent then refuses new jobs, lets a running job finish (bounded), makes a deep save and reports "handoff" so the Worker can
# start the replacement VM from that snapshot. From there it is the same wait as for vm-stopping (vm-saved / vm-stop-now /
# VM_SAVE_WAIT_SECONDS / hard limit). Handoff is for session VMs only; a plain VM never writes it.
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
FLAGS_DIR="${VM_FLAGS_DIR:-/tmp}"                      # same directory the agent's SessionManager writes its flags to
SAVE_WAIT="${VM_SAVE_WAIT_SECONDS:-1500}"              # 25 min for the final save
SESSION_RESERVE="${VM_SESSION_RESERVE_SECONDS:-1800}"  # session VMs start stopping this long before MAX_RUNTIME
HANDOFF_AFTER="${VM_HANDOFF_SECONDS:-18000}"            # session VMs hand over to a replacement after this long (5 h)

started=$(cat "$START_FILE" 2>/dev/null || true)
case "$started" in ''|*[!0-9]*) started=$(date +%s) ;; esac

# A session VM stops earlier, to leave time for the final save. Never less than half the lifetime (misconfiguration guard).
SESSION_LIMIT=$((MAX_RUNTIME - SESSION_RESERVE))
if [ "$SESSION_LIMIT" -lt $((MAX_RUNTIME / 2)) ]; then SESSION_LIMIT=$((MAX_RUNTIME / 2)); fi
# A handoff only makes sense before the early session stop; at or after it (or with 0) it is switched off.
if [ "$HANDOFF_AFTER" -le 0 ] || [ "$HANDOFF_AFTER" -ge "$SESSION_LIMIT" ]; then HANDOFF_AFTER=0; fi

dead=0
stopping_since=0
while true; do
  now=$(date +%s)
  # The agent finished (or gave up) and asks the run to end.
  if [ -e "$FLAGS_DIR/vm-stop-now" ]; then
    echo "The agent asked the run to end ($(cat "$FLAGS_DIR/vm-stop-now" 2>/dev/null | head -c 40))."
    exit 0
  fi
  session=0
  if [ -e "$FLAGS_DIR/vm-session-active" ]; then session=1; fi
  # Hard limit: always, even mid-job and even while a final save is running (GitHub would cut the job anyway).
  if [ $((now - started)) -ge "$MAX_RUNTIME" ]; then
    echo "Maximum VM lifetime reached."
    exit 0
  fi
  if [ "$stopping_since" -gt 0 ]; then
    if [ -e "$FLAGS_DIR/vm-saved" ]; then
      echo "Final session save finished ($(cat "$FLAGS_DIR/vm-saved" 2>/dev/null | head -c 20))."
      exit 0
    fi
    if [ $((now - stopping_since)) -ge "$SAVE_WAIT" ]; then
      echo "The final session save did not finish within ${SAVE_WAIT}s; ending the run. The last verified snapshot stays valid."
      exit 0
    fi
  else
    reason=""
    limit="$MAX_RUNTIME"
    if [ "$session" = 1 ]; then limit="$SESSION_LIMIT"; fi
    if [ $((now - started)) -ge "$limit" ]; then reason="Maximum VM lifetime reached."; fi
    # mtime, not contents: a half-written file can never crash this loop.
    last=$(stat -c %Y "$ACTIVITY_FILE" 2>/dev/null || echo "$now")
    age=$((now - last))
    if [ -z "$reason" ] && [ "$age" -ge "$IDLE_LIMIT" ]; then reason="Idle timeout reached."; fi
    handoff=0
    if [ -z "$reason" ] && [ "$session" = 1 ] && [ "$HANDOFF_AFTER" -gt 0 ] && [ $((now - started)) -ge "$HANDOFF_AFTER" ]; then
      reason="Handoff time reached."; handoff=1
    fi
    if [ -n "$reason" ]; then
      if [ "$session" = 1 ]; then
        flag=vm-stopping
        if [ "$handoff" = 1 ]; then flag=vm-handoff; fi
        echo "$reason Asking the session to save before the run ends."
        printf '%s' "$reason" > "$FLAGS_DIR/$flag" || { echo "Could not write the stop flag; ending the run."; exit 0; }
        stopping_since=$now
      else
        echo "$reason"
        exit 0
      fi
    fi
  fi
  last=$(stat -c %Y "$ACTIVITY_FILE" 2>/dev/null || echo "$now")
  age=$((now - last))

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
