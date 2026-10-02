#!/usr/bin/env bash
# Install the systemd units for the lake, enable every timer and start the live pollers.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
install -m 0644 "$here"/systemd/*.service "$here"/systemd/*.timer /etc/systemd/system/
systemctl daemon-reload
for t in "$here"/systemd/*.timer; do
    systemctl enable --now "$(basename "$t")"
done
for poller in subway_realtime citibike_live; do
    systemctl enable "nyc-lake-poll@${poller}.service"
    systemctl restart "nyc-lake-poll@${poller}.service"
done
systemctl list-timers 'nyc-lake-*' --no-pager
