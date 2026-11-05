#!/usr/bin/env bash
# Tests the download path of scripts/lint-workflows.sh against a local
# HTTP server (#897): an HTTP error must die with the download message
# before any hash is read, and a download of wrong bytes must still
# fail with the pin message unchanged. The pins, the directory and the
# URL base are the script's own override points; the script runs whole,
# twice, and its verdict is read from its output.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
work=$(mktemp -d)
trap 'kill $server 2>/dev/null || true; rm -rf "$work"' EXIT

printf 'not a tarball, just fixture bytes\n' > "$work/body.bin"

# The fixture server: /missing answers 404, everything else the body.
python3 - "$work" <<'PY' &
import http.server, sys

work = sys.argv[1]

class S(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if "missing" in self.path:
            self.send_error(404)
            return
        body = open(f"{work}/body.bin", "rb").read()
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass

srv = http.server.HTTPServer(("127.0.0.1", 0), S)
open(f"{work}/port.txt", "w").write(f"port={srv.server_port}\n")
srv.serve_forever()
PY
server=$!

port=""
for _ in $(seq 1 100); do
  port=$(sed -n 's/^port=//p' "$work/port.txt" 2> /dev/null || true)
  [ -n "$port" ] && break
  sleep 0.05
done
[ -n "$port" ] || { echo "test-lint-workflows: the fixture server did not start" >&2; exit 1; }

# Run the script against the fixture, with the pin and the download
# directory overridden. The PATH drops the machine's actionlint (CI's
# runner has none): the download branch must run for the test to test
# it. The workflows under the repo lint fine, so a run that cleared
# download and pin would carry on to actionlint - which the fixture is
# not: a healthy path's verdict is 1, from actionlint, never 0.
run() {
  local dir=$1 path=$2
  PATH=$(getconf PATH) ACTIONLINT_URL_BASE="http://127.0.0.1:$port$path" \
    ACTIONLINT_DIR=$dir \
    "$here/lint-workflows.sh" > "$work/out" 2> "$work/err"
}

# 1) an HTTP error: the download message, with the URL's status path,
#    and no pin text before it.
if run "$work/d1" /missing; then
  echo "test-lint-workflows: the 404 download did not fail" >&2
  exit 1
fi
grep -q 'the actionlint download failed' "$work/err" || {
  echo "test-lint-workflows: the 404 said: $(cat "$work/err")" >&2
  exit 1
}
grep -q 'not the pinned' "$work/err" && {
  echo "test-lint-workflows: the 404 read as a pin mismatch" >&2
  exit 1
}

# 2) bytes that are not the real release: the pin message, unchanged,
# against the real pin (no pin override: an env var that replaced the
# pin would turn the supply-chain check into a setting).
if run "$work/d2" /ok; then
  echo "test-lint-workflows: wrong bytes did not fail the pin" >&2
  exit 1
fi
grep -q 'is not the pinned' "$work/err" || {
  echo "test-lint-workflows: the pin said: $(cat "$work/err")" >&2
  exit 1
}

echo "test-lint-workflows: ok"
