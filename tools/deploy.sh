#!/bin/bash
# Builds, tests and swaps the binary in place, then restarts the service with a plain
# kill (Restart=always brings it back). Run as the service's user, from the repo.
set -euo pipefail
cd "$(dirname "$0")/.."
SERVICE=${SERVICE:-radar-hub}

go vet ./...
go test -count=1 ./...
go build -o radar-hub.new .
mv radar-hub.new radar-hub

pid=$(systemctl show -p MainPID --value "$SERVICE")
if [ "${pid:-0}" = "0" ]; then
    echo "$SERVICE is not running; binary updated only."
    exit 0
fi
kill "$pid"
for _ in $(seq 20); do
    sleep 0.5
    new=$(systemctl show -p MainPID --value "$SERVICE")
    if [ "${new:-0}" != "0" ] && [ "$new" != "$pid" ] && systemctl is-active --quiet "$SERVICE"; then
        echo "$SERVICE restarted (pid $pid -> $new)."
        exit 0
    fi
done
echo "$SERVICE did not come back: journalctl -u $SERVICE -n 30" >&2
exit 1
