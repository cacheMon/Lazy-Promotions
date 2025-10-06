#!/usr/bin/env bash
# disable_hyperthread.sh — disable/enable SMT via /sys/devices/system/cpu/smt/control
# Usage:
#   sudo ./disable_hyperthread.sh off
#   sudo ./disable_hyperthread.sh on
#   ./disable_hyperthread.sh status
set -euo pipefail

CTL="/sys/devices/system/cpu/smt/control"

need_root() {
  if [[ $EUID -ne 0 ]]; then
    echo "Must run as root (use sudo)." >&2
    exit 1
  fi
}

print_status() {
  if [[ -f "$CTL" ]]; then
    echo "Current SMT control state: $(cat "$CTL")"
  else
    echo "SMT control file not found. Your kernel or CPU may not support SMT control."
  fi
}

disable_smt() {
  need_root
  [[ -f "$CTL" ]] || { echo "SMT control file not found. Cannot disable SMT." >&2; exit 1; }
  echo "Disabling SMT..."
  echo off > "$CTL"
  echo "Done. State: $(cat "$CTL")"
}

enable_smt() {
  need_root
  [[ -f "$CTL" ]] || { echo "SMT control file not found. Cannot enable SMT." >&2; exit 1; }
  echo "Enabling SMT..."
  echo on > "$CTL"
  echo "Done. State: $(cat "$CTL")"
}

cmd="${1:-status}"
case "$cmd" in
  status) print_status ;;
  off|disable) disable_smt ;;
  on|enable) enable_smt ;;
  *) echo "Usage: $0 {status|off|on}" >&2; exit 2 ;;
esac
