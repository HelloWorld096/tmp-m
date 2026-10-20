#!/usr/bin/env bash
# relay_test.sh - local test-suite for relay.sh (git over file://, GitHub API over a mock server).
# It proves the logic. It does NOT prove GitHub itself: that needs a real minted token.
set -u
HERE=$(cd "$(dirname "$0")" && pwd); R="$HERE/relay.sh"
T=$(mktemp -d); MOCKPID=""
M2=""
trap '[ -n "$MOCKPID" ] && kill $MOCKPID 2>/dev/null; [ -n "$M2" ] && kill $M2 2>/dev/null; rm -rf "$T"' EXIT
export RELAY_TEST=1 RELAY_GIT_BASE="file://$T/remote" RELAY_TMP="$T/tmp" RELAY_POLL=1 RELAY_GRACE=2 \
       RELAY_BACKOFF=0 RELAY_CHUNK=1M RELAY_WAIT=60 RELAY_CONNECT_WAIT=3 RELAY_TOKEN=SECRET-TOKEN-0123456789
mkdir -p "$T/remote/o" "$T/tmp" "$T/in" "$T/out" "$T/shim"
pass=0; failn=0
ok()   { pass=$((pass+1)); printf '  ok   %s\n' "$1"; }
bad()  { failn=$((failn+1)); printf '  FAIL %s  (%s)\n' "$1" "${2:-}"; }
eq()   { [ "$2" = "$3" ] && ok "$1" || bad "$1" "got '$2', want '$3'"; }
yes()  { if eval "$2"; then ok "$1"; else bad "$1" "$2"; fi; }
newrepo() { git init --bare -q -b main "$T/remote/o/$1.git"; echo "o/$1"; }
gone()    { [ ! -d "$T/remote/o/$1.git" ]; }
rnd()     { head -c "$2" /dev/urandom > "$1"; }
sh256()   { sha256sum "$1" | cut -d' ' -f1; }
leftovers() { ls "$T/tmp" 2>/dev/null | grep -v '\.\(state\|log\)$' | wc -l; }
# transfer FILE DEST REPO [recv-args]  -> sets SRC / RRC
transfer() {
  local f=$1 d=$2 r=$3; shift 3
  "$R" recv "$r" "$d" "$@" > "$T/recv.log" 2>&1 & local rp=$!
  "$R" send "$r" "$f" > "$T/send.log" 2>&1; SRC=$?
  wait $rp; RRC=$?
}

echo "== 1. round trips: edge-case sizes and names"
for spec in "0:empty" "1:one-byte" "1048576:exact-chunk" "1048577:chunk-plus-one" "5767181:multi-chunk"; do
  n=${spec%%:*}; label=${spec#*:}; f="$T/in/odd name ($label).bin"; rnd "$f" "$n"
  r=$(newrepo "rt$n"); d="$T/out/rt-$label.bin"
  transfer "$f" "$d" "$r"
  eq "$label: sender exit 0" "$SRC" 0; eq "$label: receiver exit 0" "$RRC" 0
  yes "$label: bytes identical" "cmp -s '$f' '$d'"
  yes "$label: repo burned" "gone rt$n"
done
yes "no scratch dirs left behind" "[ \$(leftovers) -eq 0 ]"

echo "== 2. receiver first / sender first / directory destination"
f="$T/in/my file.bin"; rnd "$f" 2500000; r=$(newrepo order)
"$R" send "$r" "$f" > "$T/send.log" 2>&1 & sp=$!; sleep 3
"$R" recv "$r" "$T/out/late.bin" > "$T/recv.log" 2>&1; rrc=$?; wait $sp; src=$?
eq "sender-first: both exit 0" "$src$rrc" "00"; yes "sender-first: identical" "cmp -s '$f' '$T/out/late.bin'"
r=$(newrepo dirdest); mkdir -p "$T/out/dd"; transfer "$f" "$T/out/dd/" "$r"
eq "dir dest: exit codes" "$SRC$RRC" "00"; yes "dir dest: file named from manifest, sanitised" "cmp -s '$f' '$T/out/dd/my_file.bin'"
r=$(newrepo deepdest); transfer "$f" "$T/out/a/b/c/x.bin" "$r"
yes "missing parent dirs are created" "cmp -s '$f' '$T/out/a/b/c/x.bin'"

echo "== 3. destination exists"
r=$(newrepo exists); echo old > "$T/out/exist.bin"; transfer "$f" "$T/out/exist.bin" "$r"
eq "no --force: receiver exit 2" "$RRC" 2; eq "no --force: sender told, exit 2" "$SRC" 2
yes "no --force: sender log carries the receiver's REASON" "grep -q 'exists' '$T/send.log'"
eq "no --force: old file untouched" "$(cat "$T/out/exist.bin")" old
r=$(newrepo force); transfer "$f" "$T/out/exist.bin" "$r" --force
eq "--force: exit codes" "$SRC$RRC" "00"; yes "--force: replaced" "cmp -s '$f' '$T/out/exist.bin'"

echo "== 4. corruption is caught"
for mode in before_sums after_sums; do
  r=$(newrepo "c$mode"); d="$T/out/corrupt-$mode.bin"
  RELAY_TEST_CORRUPT=$mode "$R" recv "$r" "$d" > "$T/recv.log" 2>&1 & rp=$!
  RELAY_TEST_CORRUPT=$mode "$R" send "$r" "$f" > "$T/send.log" 2>&1; src=$?; wait $rp; rrc=$?
  eq "$mode: sender exit 3" "$src" 3; eq "$mode: receiver exit 3" "$rrc" 3
  yes "$mode: no corrupt file delivered" "[ ! -e '$d' ] && ! ls '$d'.relay-partial.* >/dev/null 2>&1"
  yes "$mode: sender log names the corrupt chunk (reason not lost)" "grep -qE 'checksum failure|differs' '$T/send.log'"
  yes "$mode: repo burned" "gone c$mode"
done

echo "== 5. sender precondition failures"
r=$(newrepo pre)
"$R" send "$r" "$T/in/nope" >/dev/null 2>&1; eq "missing file -> 1" $? 1
"$R" send "$r" "$T/in" >/dev/null 2>&1;      eq "directory -> 1" $? 1
RELAY_CHUNK=96M "$R" send "$r" "$f" >/dev/null 2>&1; eq "chunk >= 96M rejected -> 1" $? 1
RELAY_CHUNK=banana "$R" send "$r" "$f" >/dev/null 2>&1; eq "garbage chunk -> 1" $? 1
RELAY_MAX_BYTES=100 "$R" send "$r" "$f" >/dev/null 2>&1; eq "over size cap -> 1" $? 1
RELAY_POLL=abc "$R" send "$r" "$f" >/dev/null 2>&1; eq "non-numeric tunable -> 1" $? 1
"$R" send "not-a-repo" "$f" >/dev/null 2>&1; eq "bad repo name -> 1" $? 1
env -u RELAY_TOKEN "$R" send "$r" "$f" >/dev/null 2>&1; eq "no token -> 1" $? 1
yes "preconditions did not burn the repo" "[ -d '$T/remote/o/pre.git' ]"
"$R" send "o/doesnotexist" "$f" >/dev/null 2>&1; eq "unreachable repo -> 5" $? 5
r=$(newrepo full); ( cd "$T" && rm -rf seed && git init -q -b main seed && cd seed && echo x > x && git add x && git -c user.name=a -c user.email=a@b commit -qm x && git push -q "$T/remote/o/full.git" main )
"$R" send "$r" "$f" >/dev/null 2>&1; eq "non-empty repo -> 1" $? 1
yes "non-empty repo is NOT burned (not ours)" "[ -d '$T/remote/o/full.git' ]"
mkdir -p "$T/stale.git"; r=$(newrepo stale)
git -C "$T/remote/o/stale.git" -c user.name=a -c user.email=a@b hash-object -w --stdin </dev/null >/dev/null
( cd "$T/seed" && git push -q "$T/remote/o/stale.git" main:refs/heads/done )
"$R" send "$r" "$f" >/dev/null 2>&1; eq "stale verdict in repo -> 1" $? 1

echo "== 6. receiver failure modes"
r=$(newrepo rw); RELAY_WAIT=3 "$R" recv "$r" "$T/out/never.bin" >/dev/null 2>&1; eq "no sender -> timeout 4" $? 4
"$R" recv "o/doesnotexist" "$T/out/never.bin" >/dev/null 2>&1; eq "unreachable -> 5" $? 5
r=$(newrepo vanish); "$R" recv "$r" "$T/out/never.bin" >/dev/null 2>&1 & rp=$!; sleep 2; rm -rf "$T/remote/o/vanish.git"; wait $rp
eq "repo vanishes while waiting -> 5" $? 5
r=$(newrepo doneexists); ( cd "$T/seed" && git push -q "$T/remote/o/doneexists.git" main:refs/heads/done )
"$R" recv "$r" "$T/out/never.bin" >/dev/null 2>&1; eq "repo already has verdict -> 1" $? 1

echo "== 7. sender timeout burns the repo it filled"
r=$(newrepo stuck); RELAY_WAIT=3 "$R" send "$r" "$f" >/dev/null 2>&1; eq "no receiver -> timeout 4" $? 4
yes "timed-out repo burned" "gone stuck"
r=$(newrepo stuck2); RELAY_WAIT=3 RELAY_BURN_ON_FAIL=0 "$R" send "$r" "$f" >/dev/null 2>&1
yes "RELAY_BURN_ON_FAIL=0 leaves it" "[ -d '$T/remote/o/stuck2.git' ]"
yes "no scratch dirs left after failures" "[ \$(leftovers) -eq 0 ]"

echo "== 8. flaky network: shimmed git"
cat > "$T/shim/git" <<EOF
#!/usr/bin/env bash
echo "git \$*" >> "\$SHIM_LOG"
if [ -n "\${SHIM_FAIL_PUSH:-}" ] && [ "\$1" = push ]; then
  c=\$(cat "\$SHIM_COUNT" 2>/dev/null || echo 0)
  if [ "\$c" -lt "\$SHIM_FAIL_PUSH" ]; then echo \$((c+1)) > "\$SHIM_COUNT"; echo "fatal: simulated network error" >&2; exit 128; fi
fi
if [ -n "\${SHIM_LEAK:-}" ] && [ "\$1" = ls-remote ]; then echo "fatal: auth failed for \$RELAY_TOKEN" >&2; exit 128; fi
exec $(command -v git) "\$@"
EOF
chmod +x "$T/shim/git"; export SHIM_LOG="$T/argv.log" SHIM_COUNT="$T/count"; : > "$SHIM_LOG"
r=$(newrepo flaky); echo 0 > "$SHIM_COUNT"
"$R" recv "$r" "$T/out/flaky.bin" > "$T/recv.log" 2>&1 & rp=$!
PATH="$T/shim:$PATH" SHIM_FAIL_PUSH=2 "$R" send "$r" "$f" > "$T/send.log" 2>&1; src=$?; wait $rp; rrc=$?
eq "2 push failures are retried: exit codes" "$src$rrc" "00"; yes "flaky: identical" "cmp -s '$f' '$T/out/flaky.bin'"
yes "flaky: retries were logged" "grep -q 'retrying' '$T/send.log'"
r=$(newrepo hopeless); echo 0 > "$SHIM_COUNT"
PATH="$T/shim:$PATH" SHIM_FAIL_PUSH=99 "$R" send "$r" "$f" > "$T/send.log" 2>&1; eq "permanent push failure -> 2" $? 2
yes "hopeless repo burned" "gone hopeless"

echo "== 9. the token never leaks"
r=$(newrepo leak); : > "$SHIM_LOG"
"$R" recv "$r" "$T/out/leak.bin" > "$T/recv.log" 2>&1 & rp=$!
PATH="$T/shim:$PATH" "$R" send "$r" "$f" > "$T/send.log" 2>&1; wait $rp
yes "token absent from every git argv" "! grep -q SECRET-TOKEN '$SHIM_LOG'"
yes "token absent from logs" "! grep -q SECRET-TOKEN '$T/send.log' '$T/recv.log'"
r=$(newrepo leak2); "$R" recv "$r" "$T/out/never.bin" >/dev/null 2>&1 & rp=$!; sleep 1
yes "token absent from ps while running" "! ps -eo args | grep -v grep | grep -q SECRET-TOKEN"
kill $rp 2>/dev/null; wait $rp 2>/dev/null
r=$(newrepo leak3); PATH="$T/shim:$PATH" SHIM_LEAK=1 "$R" send "$r" "$f" > "$T/leak.log" 2>&1
eq "git error echoing the token: exit 5" $? 5; yes "error output is scrubbed" "! grep -q SECRET-TOKEN '$T/leak.log' && grep -q '\\*\\*\\*' '$T/leak.log'"
printf 'FILE-TOKEN-777\n' > "$T/tok"; chmod 600 "$T/tok"; r=$(newrepo tokfile)
( export RELAY_TOKEN_FILE="$T/tok"; unset RELAY_TOKEN; transfer "$f" "$T/out/tokfile.bin" "$r"; exit $((SRC+RRC)) ); eq "RELAY_TOKEN_FILE works" $? 0

echo "== 10. detached jobs"
r=$(newrepo det); export RELAY_STATE="$T/tmp/det.state"
"$R" recv "$r" "$T/out/det.bin" > "$T/recv.log" 2>&1 & rp=$!
start=$SECONDS; "$R" send "$r" "$f" --detach > "$T/detach.out" 2>&1; drc=$?; el=$((SECONDS-start))
eq "--detach returns at once with 0" "$drc" 0; yes "--detach returned in < 3 s" "[ $el -lt 3 ]"
"$R" wait "$RELAY_STATE" 30 > "$T/wait.out" 2>&1; eq "wait -> 0 when job succeeds" $? 0; wait $rp
yes "detached transfer identical" "cmp -s '$f' '$T/out/det.bin'"
r=$(newrepo det2); export RELAY_STATE="$T/tmp/det2.state"
RELAY_WAIT=60 "$R" send "$r" "$f" --detach >/dev/null 2>&1
"$R" wait "$RELAY_STATE" 3 >/dev/null 2>&1; eq "wait -> 124 while still running" $? 124
pid=$(sed -n 's/.*pid=\([0-9]*\).*/\1/p' "$RELAY_STATE"); kill -9 "$pid" 2>/dev/null; sleep 1
"$R" wait "$RELAY_STATE" 5 >/dev/null 2>&1; eq "wait -> 7 when the job was killed" $? 7
"$R" wait "$T/nope.state" 1 >/dev/null 2>&1; eq "wait on missing state -> 1" $? 1
r=$(newrepo det3); export RELAY_STATE="$T/tmp/det3.state"; RELAY_WAIT=2 "$R" send "$r" "$f" --detach >/dev/null 2>&1
"$R" wait "$RELAY_STATE" 30 >/dev/null 2>&1; eq "wait -> job's own failure code (4)" $? 4
unset RELAY_STATE

echo "== 11. burn / revoke against a mock GitHub API"
cat > "$T/mock.py" <<'EOF'
import http.server, sys
d = sys.argv[1]
class H(http.server.BaseHTTPRequestHandler):
    def do_DELETE(self):
        open(d + '/req.log', 'a').write("DELETE %s auth=%s\n" % (self.path, self.headers.get('Authorization')))
        name = 'revoke_code' if self.path == '/installation/token' else 'delete_code'
        try: code = int(open(d + '/' + name).read())
        except Exception: code = 204
        self.send_response(code); self.send_header('Content-Length', '0'); self.end_headers()
    def log_message(self, *a): pass
s = http.server.HTTPServer(('127.0.0.1', 0), H)
open(d + '/port', 'w').write(str(s.server_address[1])); s.serve_forever()
EOF
mkdir -p "$T/mock"; python3 "$T/mock.py" "$T/mock" & MOCKPID=$!; sleep 1
export RELAY_API="http://127.0.0.1:$(cat "$T/mock/port")"
burnit() { echo "$1" > "$T/mock/delete_code"; echo "$2" > "$T/mock/revoke_code"; : > "$T/mock/req.log"; env -u RELAY_GIT_BASE "$R" burn o/x >/dev/null 2>&1; }
burnit 204 204; eq "burn 204/204 -> 0" $? 0
eq "delete first, then revoke" "$(cut -d' ' -f1,2 "$T/mock/req.log" | tr '\n' '|')" "DELETE /repos/o/x|DELETE /installation/token|"
yes "bearer token sent, not in URL" "grep -q 'auth=Bearer SECRET-TOKEN-0123456789' '$T/mock/req.log' && ! grep -q 'SECRET.*HTTP' '$T/mock/req.log'"
burnit 404 401; eq "already gone + already revoked -> 0" $? 0
burnit 401 401; eq "401 on delete is tolerated (loudly) -> 0" $? 0
burnit 403 204; eq "403 on delete (no Administration) -> 1" $? 1
yes "403 still revokes the token" "grep -q 'installation/token' '$T/mock/req.log'"
burnit 500 204; eq "500 on delete -> 1" $? 1
kill $MOCKPID; wait $MOCKPID 2>/dev/null; MOCKPID=""
env -u RELAY_GIT_BASE "$R" burn o/x >/dev/null 2>&1; eq "API unreachable -> 1" $? 1
export RELAY_API="http://127.0.0.1:1"; : > "$T/mock/req.log"
r=$(newrepo apidown); transfer "$f" "$T/out/apidown.bin" "$r"
eq "receiver reports cleanup incomplete when token revoke fails" "$SRC$RRC" "08"
yes "destination bytes still verified despite cleanup failure" "cmp -s '$f' '$T/out/apidown.bin'"
# Without RELAY_TEST=1 the endpoint overrides are ignored: the token must never reach a host chosen by the environment.
: > "$T/mock/req.log"
env -u RELAY_TEST -u RELAY_GIT_BASE https_proxy=http://127.0.0.1:1 HTTPS_PROXY=http://127.0.0.1:1 RELAY_RETRIES=1 \
  RELAY_API="http://127.0.0.1:$(cat "$T/mock/port")" RELAY_UPLOADS="http://127.0.0.1:$(cat "$T/mock/port")" "$R" burn o/x >/dev/null 2>&1
eq "endpoint overrides ignored without RELAY_TEST (mock saw 0 requests)" "$(wc -l < "$T/mock/req.log" | tr -d ' ')" 0
unset RELAY_API

echo "== 12. reverse road: put / get against a mock GitHub (API host, uploads host, separate CDN host)"
mkdir -p "$T/m2"; python3 "$HERE/mock2.py" "$T/m2" & M2=$!; sleep 1
P=$(cat "$T/m2/port"); export RELAY_API="http://127.0.0.1:$P" RELAY_UPLOADS="http://127.0.0.1:$P"
ctl() { curl -s "http://127.0.0.1:$P/_ctl?$1" >/dev/null; }
RQ="$T/m2/req.log"; BL="$T/m2/blob.log"; RR=o/rev
rv="$T/in/rev.bin"; rnd "$rv" 20000000; rvsha=$(sh256 "$rv")
ctl reset=1; base_left=$(leftovers)   # a kill -9'd detached job (section 10) legitimately leaves one
"$R" put $RR "$rv" > "$T/put.log" 2>&1; eq "put: exit 0" $? 0
yes "put: prints the ASSET line with the right hash" "grep -q '^ASSET id=99 bytes=20000000 sha256=$rvsha' '$T/put.log'"
yes "put: bearer header sent, token never in a URL" "grep -q 'auth=Bearer SECRET' '$RQ' && ! grep -E '^(GET|POST|DELETE) [^ ]*SECRET' '$RQ'"
"$R" get $RR "$T/out/rev.bin" > "$T/get.log" 2>&1; eq "get: exit 0" $? 0
yes "put->get: identical" "cmp -s '$rv' '$T/out/rev.bin'"
yes "get: asked for octet-stream (not JSON metadata)" "grep -q 'accept=application/octet-stream' '$RQ'"
yes "get: token NOT forwarded to the CDN host" "[ \$(grep -c . '$BL') -ge 1 ] && ! grep -q SECRET '$BL' && grep -q 'auth=None' '$BL'"
yes "get: burned (token revoked) after success" "grep -q 'DELETE /installation/token' '$RQ'"
yes "put/get: token absent from logs" "! grep -q SECRET-TOKEN '$T/put.log' '$T/get.log'"
yes "put+get left no scratch dirs behind" "[ \$(leftovers) -eq \$base_left ]"

ctl "reset=1&preload=$rv"; "$R" put $RR "$rv" >/dev/null 2>&1; eq "put: repo already has releases -> 1" $? 1
ctl "reset=1&mode=up=liedigest"; "$R" put $RR "$rv" >/dev/null 2>&1; eq "put: GitHub digest disagrees -> 3" $? 3
yes "put: half-made release deleted so a retry is possible" "grep -q 'DELETE /repos/o/rev/releases/7' '$RQ'"
ctl "reset=1&mode=up=500"; "$R" put $RR "$rv" >/dev/null 2>&1; eq "put: upload 500 -> 2" $? 2
yes "put: release deleted after a failed upload" "grep -q 'DELETE /repos/o/rev/releases/7' '$RQ'"
ctl reset=1; RELAY_MAX_ASSET=100 "$R" put $RR "$rv" >/dev/null 2>&1; eq "put: over the asset cap -> 1" $? 1
"$R" put $RR "$T/in/nope" >/dev/null 2>&1; eq "put: missing file -> 1" $? 1
"$R" put $RR "$T/in" >/dev/null 2>&1; eq "put: directory -> 1" $? 1
ctl "reset=1&mode=api=404"; "$R" put $RR "$rv" >/dev/null 2>&1; eq "put: repo/API 404 -> 5" $? 5

for m in corrupt truncate json; do
  ctl "reset=1&preload=$rv&mode=dl=$m"; rm -f "$T/out/g-$m.bin"
  "$R" get $RR "$T/out/g-$m.bin" >/dev/null 2>&1; eq "get: dl=$m -> 3" $? 3
  yes "get: dl=$m delivers nothing, leaves no partial" "[ ! -e '$T/out/g-$m.bin' ] && ! ls '$T/out'/g-$m.bin.relay-partial.* >/dev/null 2>&1"
  yes "get: dl=$m does NOT burn (kept for retry)" "! grep -q 'installation/token' '$RQ'"
done
ctl reset=1; RELAY_WAIT=3 "$R" get $RR "$T/out/never.bin" >/dev/null 2>&1; eq "get: no asset ever -> timeout 4" $? 4
ctl "reset=1&mode=api=401"; "$R" get $RR "$T/out/never.bin" >/dev/null 2>&1; eq "get: expired/revoked token -> 5" $? 5
ctl "reset=1&preload=$rv"; echo old > "$T/out/gx.bin"; "$R" get $RR "$T/out/gx.bin" >/dev/null 2>&1; eq "get: dest exists -> 2" $? 2
eq "get: old file untouched" "$(cat "$T/out/gx.bin")" old
ctl "reset=1&preload=$rv"; "$R" get $RR "$T/out/gx.bin" --force >/dev/null 2>&1; eq "get --force -> 0" $? 0
yes "get --force replaced it" "cmp -s '$rv' '$T/out/gx.bin'"
ctl "reset=1&preload=$rv"; mkdir -p "$T/out/gd"; "$R" get $RR "$T/out/gd/" >/dev/null 2>&1; eq "get dir dest -> 0" $? 0
yes "get dir dest: named from the asset" "cmp -s '$rv' '$T/out/gd/payload.bin'"
ctl "reset=1&mode=up=nodigest"; "$R" put $RR "$rv" > "$T/put2.log" 2>&1; eq "put: no digest from GitHub still succeeds" $? 0
yes "put: warns that only size was checked" "grep -q 'no digest' '$T/put2.log'"
"$R" get $RR "$T/out/nodig.bin" > "$T/get2.log" 2>&1; eq "get: no digest -> size-only success" $? 0
yes "get: warns and delivers identical bytes" "grep -q 'no digest' '$T/get2.log' && cmp -s '$rv' '$T/out/nodig.bin'"
ctl "reset=1&preload=$rv"; export RELAY_STATE="$T/tmp/rr.state"; "$R" get $RR "$T/out/det-get.bin" --detach >/dev/null 2>&1
"$R" wait "$RELAY_STATE" 30 >/dev/null 2>&1; eq "get --detach + wait -> 0" $? 0; unset RELAY_STATE
yes "detached get delivered identical bytes" "cmp -s '$rv' '$T/out/det-get.bin'"
kill $M2 2>/dev/null; wait $M2 2>/dev/null; M2=""; unset RELAY_API RELAY_UPLOADS

echo
printf 'RESULT: %d passed, %d failed\n' "$pass" "$failn"
[ "$failn" -eq 0 ]
