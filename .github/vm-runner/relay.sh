#!/usr/bin/env bash
# relay.sh v2 - move one file between two machines through a temporary private GitHub repo.
#
#   relay.sh send OWNER/REPO FILE [--detach]    sender side (file -> chunks -> repo -> wait for verdict -> burn)
#   relay.sh recv OWNER/REPO DEST [--force] [--detach]
#                                               receiver side (wait for `ready` -> clone -> verify -> verdict -> burn)
#   relay.sh put  OWNER/REPO FILE [--detach]    REVERSE ROAD, Runner side: upload FILE as a draft-release asset (<2 GiB, no chunking)
#   relay.sh get  OWNER/REPO DEST [--force] [--detach]
#                                               REVERSE ROAD, Sandbox side: wait for the asset, download, verify, burn
#   relay.sh wait STATEFILE [SECONDS]           wait for a --detach job (default 240 s, under the 300 s sandbox limit)
#   relay.sh burn OWNER/REPO                    delete the repo and revoke the token by hand
#
# Token: env RELAY_TOKEN, or RELAY_TOKEN_FILE=/path (mode 600). It is NEVER taken from argv, and it
# reaches git through GIT_CONFIG_* environment variables, so it does not show up in `ps`.
#
# Exit codes: 0 ok | 1 usage/precondition | 2 transfer failed | 3 integrity mismatch
#             4 timeout | 5 repo unreachable / vanished, outcome unknown | 6 not enough disk
#             7 detached job died | 8 verified transfer but cleanup incomplete | 124 `wait` timed out while the job is still running | 130 interrupted
#
# Tunables (env): RELAY_CHUNK=45M RELAY_BATCH=4 RELAY_POLL=5 RELAY_GRACE=60 RELAY_WAIT=1800
#   RELAY_RETRIES=4 RELAY_BACKOFF=2 RELAY_MAX_BYTES=1073741824 RELAY_ALLOW_LARGE=0
#   RELAY_BURN_ON_FAIL=1 RELAY_CONNECT_WAIT=60 RELAY_MAX_ASSET=2147483647 RELAY_UPLOADS=https://uploads.github.com RELAY_TMP=/tmp RELAY_STATE=/path
# Test hooks (honoured ONLY when RELAY_TEST=1, because they decide where the bearer token is sent):
#   RELAY_GIT_BASE (file:// base), RELAY_API (mock API), RELAY_UPLOADS (mock uploads), RELAY_TEST_CORRUPT=before_sums|after_sums
set -uo pipefail

E_USAGE=1; E_XFER=2; E_MISMATCH=3; E_TIMEOUT=4; E_GONE=5; E_DISK=6; E_DIED=7; E_CLEANUP=8
# These three endpoints decide where the bearer token is sent. In production they are fixed; an environment
# variable can only redirect them under the explicit test flag, so nothing that merely controls the environment
# of a transfer can point the token at another host.
GIT_BASE=https://github.com; API=https://api.github.com; UPLOADS=https://uploads.github.com; CORRUPT=""
if [ "${RELAY_TEST:-}" = 1 ]; then
  GIT_BASE=${RELAY_GIT_BASE:-$GIT_BASE}; API=${RELAY_API:-$API}; UPLOADS=${RELAY_UPLOADS:-$UPLOADS}; CORRUPT=${RELAY_TEST_CORRUPT:-}
fi
CHUNK=${RELAY_CHUNK:-45M}          # 45 MiB: safely under GitHub's 50 MB warning, far under the 100 MB wall
BATCH=${RELAY_BATCH:-4}
POLL=${RELAY_POLL:-5}; GRACE=${RELAY_GRACE:-60}; WAIT=${RELAY_WAIT:-1800}
RETRIES=${RELAY_RETRIES:-4}
MAX_BYTES=${RELAY_MAX_BYTES:-1073741824}   # one repo ~1 GB; sharding is NOT implemented
TMPBASE=${RELAY_TMP:-${TMPDIR:-/tmp}}
STATE=${RELAY_STATE:-}
SELF=$(realpath -- "$0")

role=""; repo=""; path=""; token=""; url=""; work=""; partial=""
REL_ID=""; PUSHED=0; BURNED=0; FORCE=0; DETACH=0; BURN_RC=0; BURN_DELETE_UNVERIFIED=0
size=""; sha=""; DONE_ERR=""; PROBE_OUT=""

scrub() { local m="$*"; [ -n "$token" ] && m=${m//"$token"/'***'}; printf '%s' "$m"; }
log()   { printf '[relay %s] %s\n' "$(date +%H:%M:%S)" "$(scrub "$*")" >&2; }
state() {
  [ -n "$STATE" ] || return 0
  local m; m=$(scrub "${3:-}"); m=${m//$'\n'/ }
  printf 'state=%s code=%s pid=%s msg=%s\n' "$1" "${2:-0}" "$$" "$m" > "$STATE.tmp" && mv -f "$STATE.tmp" "$STATE"
}
die() { local c=$1; shift; log "ERROR: $*"; state failed "$c" "$*"; exit "$c"; }

usage() { sed -n '2,16p' "$SELF" | sed 's/^# \{0,1\}//' >&2; exit $E_USAGE; }

cleanup() {
  local rc=$?; trap - EXIT; cd / 2>/dev/null
  [ -n "$work" ] && rm -rf -- "$work"
  [ -n "$partial" ] && rm -f -- "$partial"
  if [ "$rc" -ne 0 ] && [ "$role" = put ] && [ -n "$REL_ID" ]; then
    ajson DELETE "/repos/$repo/releases/$REL_ID"; log "put failed; removed the half-made draft release (HTTP $AJ_CODE) so a retry is possible"
  fi
  if [ "$rc" -ne 0 ] && [ "$role" = send ] && [ "$PUSHED" = 1 ] && [ "$BURNED" = 0 ] \
     && [ "${RELAY_BURN_ON_FAIL:-1}" = 1 ]; then
    log "send failed after data was pushed; burning the repo"; burn
  fi
  exit "$rc"
}
trap cleanup EXIT
trap 'log interrupted; state failed 130 interrupted; exit 130' INT TERM

# ---------------------------------------------------------------- helpers
retry() {  # retry CMD...  (exponential backoff, RETRIES attempts)
  local n=1 d=${RELAY_BACKOFF:-2}
  until "$@"; do
    [ "$n" -ge "$RETRIES" ] && return 1
    log "attempt $n/$RETRIES failed (${1##*/} ${2:-}); retrying in ${d}s"
    sleep "$d"; n=$((n+1)); d=$((d*2 > 30 ? 30 : d*2))
  done
}
gq() {  # run quietly; on failure log only the last line of output
  local out
  if out=$("$@" 2>&1); then return 0; fi
  log "failed: ${*:1:3}: ${out##*$'\n'}"; return 1
}
to_bytes() { numfmt --from=iec -- "$1" 2>/dev/null; }
avail_bytes() { df -Pk -- "$1" 2>/dev/null | awk 'NR==2{print $4*1024}'; }
probe() { PROBE_OUT=$(git ls-remote "$url" 2>&1); return $?; }   # rc 0 even for an empty repo

api() {  # api METHOD PATH -> prints HTTP status (000 = network failure). Token goes via stdin, not argv.
  printf 'header = "Authorization: Bearer %s"\n' "$token" |
    curl -sS -o /dev/null -w '%{http_code}' --max-time 30 -K - \
      -H 'Accept: application/vnd.github+json' -X "$1" "$API$2" 2>/dev/null || true
}

# Delete the repo FIRST, then revoke the token (a revoked token can no longer delete).
burn() {
  BURNED=1; BURN_RC=0; BURN_DELETE_UNVERIFIED=0
  local c
  case "$GIT_BASE" in
    file://*) rm -rf -- "${GIT_BASE#file://}/$repo.git"; log "repo removed (file mode)"
              [ "$API" != "https://api.github.com" ] || return 0 ;;
    *) c=$(api DELETE "/repos/$repo")
       case "$c" in
         204) log "repo deleted" ;;
         404) log "repo already gone (peer burned it)" ;;
         401) log "WARN: delete got 401: token already revoked/expired; repository deletion is unverified"; BURN_DELETE_UNVERIFIED=1 ;;
         403) log "WARN: delete forbidden (token lacks Administration:write?). Repo may still exist."; BURN_RC=1 ;;
         *)   log "WARN: delete returned HTTP $c. Repo may still exist."; BURN_RC=1 ;;
       esac ;;
  esac
  c=$(api DELETE /installation/token)
  case "$c" in
    204) log "token revoked" ;;
    401) log "token already invalid" ;;
    *)   log "WARN: token revoke returned HTTP $c"; BURN_RC=1 ;;
  esac
  return "$BURN_RC"
}

# ---------------------------------------------------------------- verdicts
verdict() {  # verdict STATUS REASON SHA BYTES  -> publishes refs/heads/done from a scratch repo
  local d rc=0; d=$(mktemp -d "$TMPBASE/relay-v.XXXXXX") || return 1
  ( cd "$d" && git init -q -b done &&
    python3 -c 'import json,sys;print(json.dumps({"status":sys.argv[1],"reason":sys.argv[2],"sha256":sys.argv[3],"bytes":int(sys.argv[4])}))' "$@" > result.json &&
    git add result.json && git commit -qm verdict &&
    retry gq git push -q "$url" HEAD:refs/heads/done ) || rc=1
  rm -rf -- "$d"
  [ "$rc" = 0 ] || log "WARN: could not publish the verdict"
  return "$rc"
}
rfail() {  # receiver failure after the repo is reachable: tell the sender, wait the grace, burn, exit
  local code=$1 st=$2; shift 2
  log "FAILED: $*"; verdict "$st" "$*" "" 0; state failed "$code" "$*"
  sleep "$GRACE"; burn; exit "$code"
}

handle_verdict() {  # sender: read the receiver's verdict, burn, exit
  local res st rsha rbytes reason pre=0; [ -z "$sha" ] && pre=1   # pre=1: seen at preflight, before we sent anything
  retry gq git fetch -q origin refs/heads/done:refs/relay/done || die $([ "$pre" = 1 ] && echo $E_USAGE || echo $E_XFER) "could not fetch the verdict"
  res=$(git show refs/relay/done:result.json 2>/dev/null) || die $([ "$pre" = 1 ] && echo $E_USAGE || echo $E_XFER) "repo carries a 'done' ref without a valid verdict; refusing to use it"
  IFS='|' read -r st rsha rbytes reason < <(printf '%s' "$res" | python3 -c '
import json,sys
d=json.load(sys.stdin)
f=lambda k:str(d.get(k,"")).replace("|","/").replace("\n"," ")
print("|".join([f("status"),f("sha256"),f("bytes"),f("reason")]))' 2>/dev/null)
  [ -n "$st" ] || die $([ "$pre" = 1 ] && echo $E_USAGE || echo $E_XFER) "unparseable verdict: $res"
  burn
  if [ "$pre" = 1 ]; then die $E_USAGE "repo already carries a verdict ($st: $reason); it is stale or the receiver failed early"; fi
  case "$st" in
    OK) if [ "$rsha" = "$sha" ] && [ "$rbytes" = "$size" ]; then
          log "verified: receiver hash == sender hash ($size bytes)"; state ok 0 "$size bytes verified"; exit 0
        fi
        die $E_MISMATCH "receiver said OK but hash/size differ (sender $sha/$size, receiver $rsha/$rbytes)" ;;
    MISMATCH) die $E_MISMATCH "receiver: $reason" ;;
    *)        die $E_XFER "receiver failed: $reason" ;;
  esac
}
check_done() {  # 1 = no verdict yet, 2 = repo error (DONE_ERR set); exits if a verdict exists
  local out rc
  out=$(git ls-remote --exit-code "$url" refs/heads/done 2>&1); rc=$?
  [ "$rc" -eq 2 ] && return 1
  [ "$rc" -ne 0 ] && { DONE_ERR=$out; return 2; }
  handle_verdict
}

# ---------------------------------------------------------------- sender
flip_byte() { python3 -c 'import sys
f=open(sys.argv[1],"r+b"); b=f.read(1); f.seek(0); f.write(bytes([b[0]^0xff])); f.close()' "$1"; }

do_send() {
  role=send
  local file cbytes need avail i n total
  file=$(realpath -m -- "$path")
  [ -e "$file" ] || die $E_USAGE "no such file: $file"
  [ -d "$file" ] && die $E_USAGE "$file is a directory; pack it first: tar -cf out.tar -C parent dir"
  [ -f "$file" ] || die $E_USAGE "not a regular file: $file"
  [ -r "$file" ] || die $E_USAGE "not readable: $file"
  size=$(stat -c %s -- "$file") || die $E_USAGE "cannot stat $file"
  if [ "$size" -gt "$MAX_BYTES" ] && [ "${RELAY_ALLOW_LARGE:-0}" != 1 ]; then
    die $E_USAGE "$size bytes exceeds RELAY_MAX_BYTES=$MAX_BYTES (one repo ~1 GB; sharding is not implemented). Set RELAY_ALLOW_LARGE=1 to try anyway."
  fi
  cbytes=$(to_bytes "$CHUNK") || die $E_USAGE "bad RELAY_CHUNK '$CHUNK'"
  { [ "$cbytes" -ge 1 ] && [ "$cbytes" -le $((95*1024*1024)) ]; } || die $E_USAGE "RELAY_CHUNK must be 1 byte..95 MiB (GitHub rejects files over 100 MB)"
  mkdir -p -- "$TMPBASE" || die $E_USAGE "cannot create RELAY_TMP"
  work=$(mktemp -d "$TMPBASE/relay.XXXXXX") || die $E_USAGE "cannot create work dir"
  need=$(( size*21/10 + 64*1024*1024 )); avail=$(avail_bytes "$work")
  [ "${avail:-0}" -ge "$need" ] || die $E_DISK "need ~$need bytes of scratch space in $TMPBASE (chunks + git objects), have ${avail:-0}"
  cd "$work" || die $E_USAGE "cannot enter work dir"
  { git init -q -b main && git remote add origin "$url"; } || die $E_USAGE "git init failed"

  state running 0 "preflight"
  retry probe || die $E_GONE "cannot reach $repo (wrong name? deleted? token expired/revoked/not scoped to it?): ${PROBE_OUT##*$'\n'}"
  if [ -n "$PROBE_OUT" ]; then check_done; die $E_USAGE "repo is not empty; refusing to touch it"; fi

  state running 0 "hashing"; log "hashing $size bytes"
  sha=$(sha256sum -- "$file" | cut -d' ' -f1)
  [[ "$sha" =~ ^[0-9a-f]{64}$ ]] || die $E_XFER "could not hash $file"
  state running 0 "splitting"
  split -b "$cbytes" -d -a 5 -- "$file" part_ || die $E_XFER "split failed (disk full?)"
  local parts; mapfile -t parts < <(find . -maxdepth 1 -type f -name 'part_*' -printf '%f\n' | sort)
  n=${#parts[@]}
  total=0; [ "$n" -gt 0 ] && total=$(find . -maxdepth 1 -type f -name 'part_*' -printf '%s\n' | awk '{s+=$1}END{print s+0}')
  [ "$total" = "$size" ] || die $E_XFER "file changed while it was being split (expected $size bytes, got $total)"
  if [ "$CORRUPT" = before_sums ] && [ "$n" -gt 0 ]; then flip_byte "${parts[0]}"; fi
  if [ "$n" -gt 0 ]; then sha256sum -- "${parts[@]}" > SHA256SUMS; else : > SHA256SUMS; fi
  if [ "$CORRUPT" = after_sums ] && [ "$n" -gt 0 ]; then flip_byte "${parts[0]}"; fi

  log "pushing $n chunk(s) of <= $cbytes bytes"
  PUSHED=1
  for ((i=0; i<n; i+=BATCH)); do
    check_done; [ $? -eq 2 ] && log "WARN: verdict poll failed during upload (continuing)"
    state running 0 "pushing chunks $((i+1))-$((i+BATCH>n ? n : i+BATCH)) of $n"
    { git add -- "${parts[@]:i:BATCH}" && git commit -qm "batch $i"; } || die $E_XFER "git commit failed"
    retry gq git push -q origin main || die $E_XFER "push failed after $RETRIES attempts"
  done
  python3 -c 'import json,sys;print(json.dumps({"version":2,"name":sys.argv[1],"bytes":int(sys.argv[2]),"sha256":sys.argv[3],"chunks":int(sys.argv[4])}))' \
    "$(basename -- "$file")" "$size" "$sha" "$n" > manifest.json
  { git add manifest.json SHA256SUMS && git commit -qm ready; } || die $E_XFER "git commit failed"
  retry gq git push -q origin HEAD:refs/heads/ready || die $E_XFER "could not publish the ready marker"
  log "ready pushed; waiting for the receiver's verdict"
  state running 0 "waiting for verdict"

  local end=$((SECONDS+WAIT)) errs=0 rc
  while [ "$SECONDS" -lt "$end" ]; do
    check_done; rc=$?
    if [ "$rc" -eq 2 ]; then
      errs=$((errs+1)); log "poll error ($errs/5): ${DONE_ERR##*$'\n'}"
      [ "$errs" -ge 5 ] && die $E_GONE "repo unreachable and no verdict seen. Outcome UNKNOWN: verify the hash on the destination."
    else errs=0; fi
    sleep "$POLL"
  done
  die $E_TIMEOUT "no verdict within ${WAIT}s"
}

# ---------------------------------------------------------------- receiver
do_recv() {
  role=recv
  local dest isdir=0 end errs rc out found
  dest=$(realpath -m -- "$path")
  [ -d "$dest" ] && isdir=1; case "$path" in */) isdir=1 ;; esac
  mkdir -p -- "$TMPBASE" || die $E_USAGE "cannot create RELAY_TMP"
  work=$(mktemp -d "$TMPBASE/relay.XXXXXX") || die $E_USAGE "cannot create work dir"
  cd "$work" || die $E_USAGE "cannot enter work dir"

  state running 0 "connecting"
  end=$((SECONDS+${RELAY_CONNECT_WAIT:-60}))
  until probe; do
    [ "$SECONDS" -ge "$end" ] && die $E_GONE "cannot reach $repo: ${PROBE_OUT##*$'\n'}"
    log "repo not reachable yet; retrying"; sleep "$POLL"
  done
  case "$PROBE_OUT" in *refs/heads/done*) die $E_USAGE "repo already carries a verdict; refusing to reuse it" ;; esac

  # Fail EARLY on a bad destination, and tell the sender so it does not wait for nothing.
  if [ "$isdir" = 0 ]; then
    mkdir -p -- "$(dirname -- "$dest")" 2>/dev/null || rfail $E_XFER FAILED "cannot create destination directory for $dest"
    if [ -e "$dest" ] && [ "$FORCE" != 1 ]; then rfail $E_XFER FAILED "destination $dest exists (use --force)"; fi
  else
    mkdir -p -- "$dest" 2>/dev/null || rfail $E_XFER FAILED "cannot create destination directory $dest"
  fi

  state running 0 "waiting for ready"; log "waiting for the sender's ready marker"
  end=$((SECONDS+WAIT)); errs=0; found=0
  while [ "$SECONDS" -lt "$end" ]; do
    out=$(git ls-remote --exit-code "$url" refs/heads/ready 2>&1); rc=$?
    case $rc in
      0) found=1; break ;;
      2) errs=0 ;;
      *) errs=$((errs+1)); log "poll error ($errs/5): ${out##*$'\n'}"
         [ "$errs" -ge 5 ] && die $E_GONE "repo became unreachable while waiting (deleted? token revoked?)" ;;
    esac
    sleep "$POLL"
  done
  [ "$found" = 1 ] || die $E_TIMEOUT "sender never signalled ready within ${WAIT}s"

  state running 0 "cloning"
  clone_once() { rm -rf -- repo; git clone -q --branch ready --single-branch --no-tags "$url" repo; }
  retry gq clone_once || rfail $E_XFER FAILED "clone failed after $RETRIES attempts (disk full?)"
  cd repo || rfail $E_XFER FAILED "clone produced no directory"

  local want_sha="" want_bytes want_chunks name
  read -r want_sha want_bytes want_chunks name < <(python3 -c '
import json,re,sys
m=json.load(sys.stdin)
sha=m["sha256"]; b=int(m["bytes"]); c=int(m["chunks"])
assert re.fullmatch(r"[0-9a-f]{64}",sha) and b>=0 and c>=0
name=re.sub(r"[^A-Za-z0-9._-]","_",str(m.get("name","")))[:200].lstrip(".") or "payload.bin"
print(sha,b,c,name)' < manifest.json 2>/dev/null)
  [ -n "$want_sha" ] || rfail $E_XFER FAILED "manifest.json missing or invalid"
  [ "$isdir" = 1 ] && dest="$dest/$name"
  if [ "$isdir" = 1 ] && [ -e "$dest" ] && [ "$FORCE" != 1 ]; then rfail $E_XFER FAILED "destination $dest exists (use --force)"; fi

  local parts; mapfile -t parts < <(find . -maxdepth 1 -type f -name 'part_*' -printf '%f\n' | sort)
  [ "${#parts[@]}" = "$want_chunks" ] || rfail $E_MISMATCH MISMATCH "expected $want_chunks chunks, found ${#parts[@]}"
  if [ "$want_chunks" -gt 0 ]; then
    out=$(sha256sum -c SHA256SUMS 2>&1 | grep -v ': OK$' | head -n 3 | tr '\n' ' ')
    [ -z "$out" ] || rfail $E_MISMATCH MISMATCH "chunk checksum failure: $out"
  fi
  local davail; davail=$(avail_bytes "$(dirname -- "$dest")")
  [ "${davail:-0}" -ge $((want_bytes + 1048576)) ] || rfail $E_DISK FAILED "not enough disk at destination: need $want_bytes bytes, have ${davail:-0}"

  state running 0 "joining"
  partial="$dest.relay-partial.$$"
  if [ "$want_chunks" -gt 0 ]; then cat -- "${parts[@]}" > "$partial"; else : > "$partial"; fi || rfail $E_XFER FAILED "join failed (disk full?)"
  local got gotbytes
  got=$(sha256sum -- "$partial" | cut -d' ' -f1); gotbytes=$(stat -c %s -- "$partial")
  if [ "$got" != "$want_sha" ] || [ "$gotbytes" != "$want_bytes" ]; then
    rm -f -- "$partial"; partial=""
    rfail $E_MISMATCH MISMATCH "joined file differs from the sender's hash (got $got/$gotbytes, want $want_sha/$want_bytes)"
  fi
  mv -f -- "$partial" "$dest" || rfail $E_XFER FAILED "could not move the file into place"
  partial=""
  log "OK: $dest ($gotbytes bytes, sha256 $got)"
  verdict OK "ok" "$got" "$gotbytes"
  state ok 0 "$dest $gotbytes bytes verified"
  sleep "$GRACE"
  burn
  if [ "$BURN_RC" -ne 0 ]; then
    state cleanup_incomplete "$E_CLEANUP" "destination verified ($gotbytes bytes sha256:$got) but repository/token cleanup is incomplete"
    log "ERROR: transfer verified, but cleanup failed; run remote_transfer_cleanup with the recorded repo/token"
    exit "$E_CLEANUP"
  fi
  exit 0
}


# ---------------------------------------------------------------- reverse road (release assets)
AJ_CODE=""; AJ_BODY=""
ajson() {  # ajson METHOD PATH [JSON] -> sets AJ_CODE / AJ_BODY. Token via stdin, never argv.
  local out
  out=$(printf 'header = "Authorization: Bearer %s"\n' "$token" |
    curl -sS --max-time 60 -K - -H 'Accept: application/vnd.github+json' -X "$1" ${3:+-d "$3"} \
      -w '\n%{http_code}' "$API$2" 2>/dev/null) || true
  AJ_CODE=${out##*$'\n'}; AJ_BODY=${out%$'\n'*}; [[ "$AJ_CODE" =~ ^[0-9]{3}$ ]] || AJ_CODE=000
}
# prints "id|size|digest|name" of the first fully uploaded asset, or nothing
first_asset() { printf '%s' "$AJ_BODY" | python3 -c '
import json,sys
try: rels=json.load(sys.stdin)
except Exception: sys.exit(0)
for r in rels:
    for a in r.get("assets",[]):
        if a.get("state")=="uploaded":
            import re; print("|".join([str(a["id"]),str(a["size"]),str(a.get("digest") or ""),re.sub(r"[^A-Za-z0-9._-]","_",a["name"])])); sys.exit(0)'; }

do_put() {
  role=put
  local file asize name up_code
  file=$(realpath -m -- "$path")
  [ -e "$file" ] || die $E_USAGE "no such file: $file"
  [ -d "$file" ] && die $E_USAGE "$file is a directory; pack it first: tar -cf out.tar -C parent dir"
  { [ -f "$file" ] && [ -r "$file" ]; } || die $E_USAGE "not a readable regular file: $file"
  size=$(stat -c %s -- "$file") || die $E_USAGE "cannot stat $file"
  [ "$size" -le "${RELAY_MAX_ASSET:-2147483647}" ] || die $E_USAGE "$size bytes exceeds the 2 GiB release-asset limit; use send/recv (chunked) instead"
  name=$(basename -- "$file" | sed 's/[^A-Za-z0-9._-]/_/g'); name=${name#.}; [ -n "$name" ] || name=payload.bin

  state running 0 "preflight"
  ajson GET "/repos/$repo/releases"
  case "$AJ_CODE" in
    200) ;;
    401|403|404) die $E_GONE "cannot use $repo (HTTP $AJ_CODE): wrong name, deleted, or token expired/revoked/not scoped to it" ;;
    *)   die $E_GONE "GitHub API unreachable or erroring (HTTP $AJ_CODE)" ;;
  esac
  # Parse, don't string-compare: real GitHub pretty-prints an empty list as "[", a blank line, "]".
  [ "$(printf '%s' "$AJ_BODY" | python3 -c 'import json,sys;print(len(json.load(sys.stdin)))' 2>/dev/null)" = 0 ] \
    || die $E_USAGE "repo already has releases (or the reply was not a JSON list); refusing to touch it"

  state running 0 "hashing"; log "hashing $size bytes"
  sha=$(sha256sum -- "$file" | cut -d' ' -f1); [[ "$sha" =~ ^[0-9a-f]{64}$ ]] || die $E_XFER "could not hash $file"

  state running 0 "creating draft release"
  ajson POST "/repos/$repo/releases" "{\"tag_name\":\"relay-$(date +%s)\",\"name\":\"relay\",\"draft\":true}"
  [ "$AJ_CODE" = 201 ] || die $E_XFER "could not create the draft release (HTTP $AJ_CODE): $(scrub "${AJ_BODY:0:200}")"
  PUSHED=1
  local rid; rid=$(printf '%s' "$AJ_BODY" | python3 -c 'import json,sys;print(json.load(sys.stdin)["id"])' 2>/dev/null)
  [[ "$rid" =~ ^[0-9]+$ ]] || die $E_XFER "release id missing from the API reply"
  REL_ID=$rid

  state running 0 "uploading $size bytes"; log "uploading $size bytes as $name (streamed, not buffered)"
  work=$(mktemp -d "$TMPBASE/relay.XXXXXX") || die $E_USAGE "cannot create work dir"
  # -T streams from disk. (--data-binary @file buffers the whole file in RAM and failed at 1 GiB.)
  up_code=$(printf 'header = "Authorization: Bearer %s"\n' "$token" |
    curl -sS -K - -X POST -H 'Accept: application/vnd.github+json' -H 'Content-Type: application/octet-stream' \
      --max-time "${RELAY_UPLOAD_TIMEOUT:-1800}" -T "$file" -o "$work/asset.json" -w '%{http_code}' \
      "$UPLOADS/repos/$repo/releases/$rid/assets?name=$name" 2>"$work/curl.err") || true
  [ "$up_code" = 201 ] || die $E_XFER "upload failed (HTTP ${up_code:-000}): $(scrub "$(head -c 200 "$work/curl.err")") $(scrub "$(head -c 200 "$work/asset.json" 2>/dev/null)")"
  local aid astate adigest
  IFS='|' read -r aid asize astate adigest < <(python3 -c '
import json,sys
d=json.load(open(sys.argv[1])); print("|".join([str(d.get("id","")),str(d.get("size","")),str(d.get("state","")),str(d.get("digest") or "")]))' "$work/asset.json" 2>/dev/null)
  [ "$astate" = uploaded ] || die $E_XFER "asset state is '$astate', not 'uploaded'"
  [ "$asize" = "$size" ]   || die $E_MISMATCH "GitHub stored $asize bytes, expected $size"
  if [ -n "$adigest" ] && [ "$adigest" != "sha256:$sha" ]; then die $E_MISMATCH "GitHub digest $adigest differs from local sha256:$sha"; fi
  [ -n "$adigest" ] || log "WARN: GitHub returned no digest; only the size was checked on this side"
  log "OK: asset $aid ($asize bytes, sha256 $sha) is waiting for the Sandbox"
  state ok 0 "asset $aid $asize bytes sha256:$sha"
  echo "ASSET id=$aid bytes=$asize sha256=$sha"
  exit 0
}

do_get() {
  role=get
  local dest isdir=0 end errs rc line aid asz adig aname partial_dir code
  dest=$(realpath -m -- "$path")
  [ -d "$dest" ] && isdir=1; case "$path" in */) isdir=1 ;; esac
  if [ "$isdir" = 0 ]; then
    mkdir -p -- "$(dirname -- "$dest")" 2>/dev/null || die $E_XFER "cannot create destination directory for $dest"
    [ ! -e "$dest" ] || [ "$FORCE" = 1 ] || die $E_XFER "destination $dest exists (use --force)"
  else mkdir -p -- "$dest" 2>/dev/null || die $E_XFER "cannot create destination directory $dest"; fi

  state running 0 "waiting for the asset"; log "waiting for an uploaded asset in $repo"
  end=$((SECONDS+WAIT)); errs=0; line=""
  while [ "$SECONDS" -lt "$end" ]; do
    ajson GET "/repos/$repo/releases"
    case "$AJ_CODE" in
      200) errs=0; line=$(first_asset); [ -n "$line" ] && break ;;
      *)   errs=$((errs+1)); log "poll error ($errs/5): HTTP $AJ_CODE"
           [ "$errs" -ge 5 ] && die $E_GONE "repo unreachable (HTTP $AJ_CODE): deleted? token revoked or expired?" ;;
    esac
    sleep "$POLL"
  done
  [ -n "$line" ] || die $E_TIMEOUT "no asset appeared within ${WAIT}s"
  IFS='|' read -r aid asz adig aname <<< "$line"
  aname=$(printf '%s' "$aname" | sed 's/[^A-Za-z0-9._-]/_/g'); aname=${aname#.}; [ -n "$aname" ] || aname=payload.bin
  [ "$isdir" = 1 ] && dest="$dest/$aname"
  if [ "$isdir" = 1 ] && [ -e "$dest" ] && [ "$FORCE" != 1 ]; then die $E_XFER "destination $dest exists (use --force)"; fi
  local davail; davail=$(avail_bytes "$(dirname -- "$dest")")
  [ "${davail:-0}" -ge $((asz + 1048576)) ] || die $E_DISK "need $asz bytes at the destination, have ${davail:-0}"

  state running 0 "downloading $asz bytes"; log "downloading asset $aid ($asz bytes)"
  partial="$dest.relay-partial.$$"
  # The Accept header is essential: without it GitHub answers 200 with JSON metadata instead of the file.
  # curl drops the Authorization header when the 302 leaves api.github.com, so the token never reaches the CDN host.
  code=$(printf 'header = "Authorization: Bearer %s"\n' "$token" |
    curl -sS -L -K - --max-time "${RELAY_DOWNLOAD_TIMEOUT:-1800}" -H 'Accept: application/octet-stream' \
      -o "$partial" -w '%{http_code}' "$API/repos/$repo/releases/assets/$aid" 2>/dev/null) || true
  [ "$code" = 200 ] || { rm -f -- "$partial"; partial=""; die $E_XFER "download failed (HTTP ${code:-000}); repo kept for a retry"; }
  local gotbytes got; gotbytes=$(stat -c %s -- "$partial"); got=$(sha256sum -- "$partial" | cut -d' ' -f1)
  if [ "$gotbytes" != "$asz" ]; then
    rm -f -- "$partial"; partial=""; die $E_MISMATCH "got $gotbytes bytes, expected $asz (a JSON reply instead of the file? truncated?); repo kept for a retry"
  fi
  if [ -n "$adig" ]; then
    [ "$adig" = "sha256:$got" ] || { rm -f -- "$partial"; partial=""; die $E_MISMATCH "sha256 mismatch: GitHub says $adig, got sha256:$got; repo kept for a retry"; }
  else log "WARN: GitHub gave no digest; only size verified. Compare the hash with the sender's."; fi
  mv -f -- "$partial" "$dest" || die $E_XFER "could not move the file into place"
  partial=""
  log "OK: $dest ($gotbytes bytes, sha256 $got)"
  burn
  if [ "$BURN_RC" -ne 0 ] || [ "$BURN_DELETE_UNVERIFIED" = 1 ]; then
    state cleanup_incomplete "$E_CLEANUP" "transfer verified ($gotbytes bytes sha256:$got) but repository/token cleanup is incomplete"
    log "ERROR: transfer verified, but cleanup failed; run remote_transfer_cleanup with the recorded repo/token"
    exit "$E_CLEANUP"
  fi
  state ok 0 "$dest $gotbytes bytes sha256:$got"
  exit 0
}

# ---------------------------------------------------------------- detached jobs
do_wait() {
  local st=${1:-} line stt code pid end=$((SECONDS+${2:-240}))
  { [ -n "$st" ] && [ -f "$st" ]; } || { echo "no such state file: $st" >&2; return 1; }
  while :; do
    line=$(cat "$st"); stt=${line#state=}; stt=${stt%% *}
    code=${line#*code=}; code=${code%% *}; pid=${line#*pid=}; pid=${pid%% *}
    case "$stt" in
      ok)     echo "$line"; return 0 ;;
      failed) echo "$line"; return "${code:-2}" ;;
      running)
        if [ "${pid:-0}" -gt 0 ] && ! kill -0 "$pid" 2>/dev/null; then
          line=$(cat "$st"); case "$line" in state=running*) echo "$line (process $pid is gone)"; return $E_DIED ;; esac
          continue
        fi ;;
    esac
    [ "$SECONDS" -ge "$end" ] && { echo "$line"; return 124; }
    sleep 1
  done
}

# ---------------------------------------------------------------- main
main() {
  local cmd=${1:-}; [ $# -gt 0 ] && shift
  case "$cmd" in
    wait) do_wait "$@"; exit $? ;;
    send|recv|put|get|burn) ;;
    *) usage ;;
  esac
  local pos=() a
  for a in "$@"; do
    case "$a" in
      --detach) DETACH=1 ;; --force) FORCE=1 ;; -h|--help) usage ;;
      -*) die $E_USAGE "unknown option $a" ;;
      *) pos+=("$a") ;;
    esac
  done
  repo=${pos[0]:-}; path=${pos[1]:-}
  [[ "$repo" =~ ^[A-Za-z0-9._-]+/[A-Za-z0-9._-]+$ ]] || die $E_USAGE "repo must look like OWNER/REPO"
  if [ "$cmd" != burn ] && [ -z "$path" ]; then die $E_USAGE "missing FILE/DEST argument"; fi
  if [ -z "${RELAY_TOKEN:-}" ] && [ -n "${RELAY_TOKEN_FILE:-}" ]; then
    [ -r "$RELAY_TOKEN_FILE" ] || die $E_USAGE "cannot read RELAY_TOKEN_FILE"
    RELAY_TOKEN=$(tr -d '[:space:]' < "$RELAY_TOKEN_FILE")
  fi
  token=${RELAY_TOKEN:-}
  [ -n "$token" ] || die $E_USAGE "no token: set RELAY_TOKEN or RELAY_TOKEN_FILE (never pass it as an argument)"
  local t; for t in git python3 sha256sum split curl base64 numfmt realpath stat df awk find mktemp; do
    command -v "$t" >/dev/null 2>&1 || die $E_USAGE "required tool missing: $t"
  done
  url="$GIT_BASE/$repo.git"
  [[ "$POLL" =~ ^[0-9]+$ && "$GRACE" =~ ^[0-9]+$ && "$WAIT" =~ ^[0-9]+$ && "$RETRIES" =~ ^[0-9]+$ && "$BATCH" =~ ^[1-9][0-9]*$ ]] \
    || die $E_USAGE "RELAY_POLL/GRACE/WAIT/RETRIES must be integers and RELAY_BATCH >= 1"
  [ "$GRACE" -gt "$POLL" ] || log "WARN: RELAY_GRACE should exceed RELAY_POLL or the sender may miss the verdict"

  # Token reaches git through the environment, never through argv.
  local auth; auth="Authorization: Basic $(printf 'x-access-token:%s' "$token" | base64 -w0)"
  export GIT_TERMINAL_PROMPT=0 GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_COUNT=8
  export GIT_CONFIG_KEY_0=http.extraheader  GIT_CONFIG_VALUE_0="$auth"
  export GIT_CONFIG_KEY_1=user.name         GIT_CONFIG_VALUE_1=relay
  export GIT_CONFIG_KEY_2=user.email        GIT_CONFIG_VALUE_2=relay@localhost
  export GIT_CONFIG_KEY_3=core.compression  GIT_CONFIG_VALUE_3=0
  export GIT_CONFIG_KEY_4=pack.window       GIT_CONFIG_VALUE_4=0
  export GIT_CONFIG_KEY_5=pack.depth        GIT_CONFIG_VALUE_5=0
  export GIT_CONFIG_KEY_6=http.lowSpeedLimit GIT_CONFIG_VALUE_6=1000
  export GIT_CONFIG_KEY_7=http.lowSpeedTime  GIT_CONFIG_VALUE_7=120

  if [ "$cmd" = burn ]; then burn; exit $?; fi

  if [ "$DETACH" = 1 ] && [ "${RELAY_DETACHED:-0}" != 1 ]; then
    STATE=${RELAY_STATE:-$TMPBASE/relay-${repo//\//_}.state}
    mkdir -p -- "$TMPBASE"; local logf=${STATE%.state}.log
    printf 'state=running code=0 pid=0 msg=queued\n' > "$STATE"
    local extra=(); [ "$FORCE" = 1 ] && extra=(--force)
    RELAY_DETACHED=1 RELAY_STATE="$STATE" nohup setsid "$SELF" "$cmd" "$repo" "$path" ${extra[@]+"${extra[@]}"} > "$logf" 2>&1 < /dev/null &
    echo "state=$STATE log=$logf"; trap - EXIT; exit 0
  fi

  case "$cmd" in send) do_send ;; recv) do_recv ;; put) do_put ;; get) do_get ;; esac
}
main "$@"
