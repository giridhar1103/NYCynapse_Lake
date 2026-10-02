#!/usr/bin/env bash
# Install the systemd units for the lake and enable every timer.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
install -m 0644 "$here"/systemd/*.service "$here"/systemd/*.timer /etc/systemd/system/
systemctl daemon-reload
for t in "$here"/systemd/*.timer; do
    systemctl enable --now "$(basename "$t")"
done
systemctl list-timers 'nyc-lake-*' --no-pager
