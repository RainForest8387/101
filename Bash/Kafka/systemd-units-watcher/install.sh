#!/bin/bash
#
# install.sh — установка watchdog'а и таймера на целевой хост.
#
# Кладёт скрипт в /opt/systemd-units-watcher/, юниты — в /etc/systemd/system/,
# делает daemon-reload и включает таймер.
#
# Использование:
#   sudo ./install.sh                 # установить и включить таймер
#   sudo ./install.sh --no-enable     # установить, но таймер не включать
#   sudo ./install.sh --uninstall     # снять таймер и удалить файлы
#
set -euo pipefail

PREFIX="${PREFIX:-/opt/systemd-units-watcher}"
UNIT_DIR="${UNIT_DIR:-/etc/systemd/system}"
SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENABLE=1
UNINSTALL=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-enable) ENABLE=0; shift ;;
    --uninstall) UNINSTALL=1; shift ;;
    -h|--help)   sed -n '2,/^set -euo/p' "$0" | sed 's/^# \{0,1\}//; $d'; exit 0 ;;
    *) echo "Неизвестный параметр: $1" >&2; exit 2 ;;
  esac
done

[[ "$(id -u)" -eq 0 ]] || { echo "Нужны права root: sudo $0" >&2; exit 2; }

if [[ $UNINSTALL -eq 1 ]]; then
  systemctl disable --now systemd-units-watcher.timer 2>/dev/null || true
  rm -f "$UNIT_DIR/systemd-units-watcher.timer" "$UNIT_DIR/systemd-units-watcher.service"
  rm -rf "$PREFIX"
  systemctl daemon-reload
  echo "Удалено."
  exit 0
fi

install -d -m 0755 "$PREFIX"
install -m 0755 "$SRC_DIR/systemd-units-watcher.sh" "$PREFIX/systemd-units-watcher.sh"
install -m 0644 "$SRC_DIR/systemd/systemd-units-watcher.service" "$UNIT_DIR/systemd-units-watcher.service"
install -m 0644 "$SRC_DIR/systemd/systemd-units-watcher.timer"   "$UNIT_DIR/systemd-units-watcher.timer"

systemctl daemon-reload

if [[ $ENABLE -eq 1 ]]; then
  systemctl enable --now systemd-units-watcher.timer
  echo
  systemctl list-timers systemd-units-watcher.timer --no-pager
else
  echo "Файлы установлены. Включить: systemctl enable --now systemd-units-watcher.timer"
fi
