#!/usr/bin/env bash
#
# trace_smev4.sh — трассировка маршрута до всех хостов ЦОД СМЭВ4.
#
# Использует mtr (приоритет) либо traceroute.
#
# Использование:
#   ./trace_smev4.sh                     # авто-режим (TCP/ICMP)
#   ./trace_smev4.sh --mode icmp         # всё через ICMP
#   ./trace_smev4.sh --mode tcp          # всё через TCP-SYN (порт по группе)
#   ./trace_smev4.sh --mode udp          # всё через UDP (на профильный порт)
#   ./trace_smev4.sh --auth              # включить auth-серверы (по домену)
#   ./trace_smev4.sh -c 20               # число проб на хост (mtr), по умолч. 10
#   ./trace_smev4.sh -m 20               # макс. число хопов, по умолч. 30
#   ./trace_smev4.sh --tool traceroute   # принудительно traceroute
#   ./trace_smev4.sh -o report.txt       # сохранить вывод в файл
#
# Замечание: TCP/ICMP/UDP-трассировка требует прав root (raw-сокеты).


set -u

COUNT=10
MAXHOPS=30
TOOL=""
MODE="auto"          # auto | icmp | tcp | udp
INCLUDE_AUTH=0
OUTFILE=""

BROKER_PORT=6651     # открытый порт брокеров для TCP/UDP-проб
AUTH_PORT=443
NTP_PORT=123

# ---------- разбор аргументов ----------
while [[ $# -gt 0 ]]; do
  case "$1" in
    -c|--count)   COUNT="$2"; shift 2 ;;
    -m|--maxhops) MAXHOPS="$2"; shift 2 ;;
    --mode)       MODE="$2"; shift 2 ;;
    --tool)       TOOL="$2"; shift 2 ;;
    --auth)       INCLUDE_AUTH=1; shift ;;
    -o|--output)  OUTFILE="$2"; shift 2 ;;
    -h|--help)
      grep '^#' "$0" | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *) echo "Неизвестный аргумент: $1" >&2; exit 2 ;;
  esac
done

case "$MODE" in
  auto|icmp|tcp|udp) ;;
  *) echo "Неизвестный режим: $MODE (допустимо: auto, icmp, tcp, udp)" >&2; exit 2 ;;
esac

# ---------- цели ----------
BROKER_IPS=(
  "109.207.15.27"
  "109.207.15.59"
  "109.207.15.155"
  "109.207.15.187"
)
NTP_IPS=(
  "109.207.15.28"
  "109.207.15.60"
  "109.207.15.156"
  "109.207.15.188"
)
AUTH_HOSTS=(
  "podd1.gosuslugi.ru"
  "podd2.gosuslugi.ru"
  "podd3.gosuslugi.ru"
  "podd4.gosuslugi.ru"
  "podd1-cross.gosuslugi.ru"
  "podd2-cross.gosuslugi.ru"
  "podd3-cross.gosuslugi.ru"
  "podd4-cross.gosuslugi.ru"
)

# ---------- выбор инструмента ----------
if [[ -z "$TOOL" ]]; then
  if command -v mtr >/dev/null 2>&1; then
    TOOL="mtr"
  elif command -v traceroute >/dev/null 2>&1; then
    TOOL="traceroute"
  else
    echo "Ошибка: не найдены ни mtr, ни traceroute." >&2
    echo "Установите: apt install mtr-tiny traceroute | yum install mtr traceroute | brew install mtr" >&2
    exit 3
  fi
else
  if ! command -v "$TOOL" >/dev/null 2>&1; then
    echo "Ошибка: инструмент '$TOOL' не найден в PATH." >&2
    exit 3
  fi
fi

[[ $EUID -ne 0 ]] && echo "ПРЕДУПРЕЖДЕНИЕ: запуск не от root — TCP/ICMP-пробы могут не работать, используйте sudo." >&2

# ---------- вывод ----------
emit() {
  if [[ -n "$OUTFILE" ]]; then printf '%s\n' "$*" | tee -a "$OUTFILE"
  else printf '%s\n' "$*"; fi
}

# run_trace <target> <effective_mode> <port>
run_trace() {
  local target="$1" emode="$2" port="$3" out
  if [[ "$TOOL" == "mtr" ]]; then
    local margs=(-r -w -b -c "$COUNT" -m "$MAXHOPS")
    case "$emode" in
      icmp) ;;                                   # ICMP — режим mtr по умолчанию
      tcp)  margs+=(-T -P "$port") ;;
      udp)  margs+=(-u -P "$port") ;;
    esac
    out=$(mtr "${margs[@]}" "$target" 2>&1)
  else
    local targs=(-m "$MAXHOPS" -w 2 -q 1)
    case "$emode" in
      icmp) targs+=(-I) ;;
      tcp)  targs+=(-T -p "$port") ;;
      udp)  targs+=(-U -p "$port") ;;
    esac
    out=$(traceroute "${targs[@]}" "$target" 2>&1)
  fi
  emit "$out"
}

# определить фактический режим и порт для группы
eff_mode() { # <group>
  if [[ "$MODE" != "auto" ]]; then echo "$MODE"; return; fi
  case "$1" in
    broker) echo "tcp" ;;
    auth)   echo "tcp" ;;
    ntp)    echo "icmp" ;;
  esac
}
grp_port() { # <group>
  case "$1" in
    broker) echo "$BROKER_PORT" ;;
    auth)   echo "$AUTH_PORT" ;;
    ntp)    echo "$NTP_PORT" ;;
  esac
}

# ---------- запуск ----------
[[ -n "$OUTFILE" ]] && : > "$OUTFILE"

emit "############################################################"
emit "# Трассировка маршрутов ЦОД СМЭВ4"
emit "# Инструмент: $TOOL | режим: $MODE | дата: $(date '+%Y-%m-%d %H:%M:%S')"
emit "############################################################"

m=$(eff_mode broker); p=$(grp_port broker)
emit ""
emit "===== Брокеры (режим: $m, порт: $p) ====="
for ip in "${BROKER_IPS[@]}"; do
  emit ""; emit ">>> $ip"
  run_trace "$ip" "$m" "$p"
done

m=$(eff_mode ntp); p=$(grp_port ntp)
emit ""
emit "===== NTP-серверы (режим: $m) ====="
for ip in "${NTP_IPS[@]}"; do
  emit ""; emit ">>> $ip"
  run_trace "$ip" "$m" "$p"
done

if [[ "$INCLUDE_AUTH" -eq 1 ]]; then
  m=$(eff_mode auth); p=$(grp_port auth)
  emit ""
  emit "===== Серверы аутентификации (по домену, режим: $m, порт: $p) ====="
  for h in "${AUTH_HOSTS[@]}"; do
    emit ""; emit ">>> $h"
    run_trace "$h" "$m" "$p"
  done
fi

emit ""
emit "Готово."
[[ -n "$OUTFILE" ]] && echo "Отчёт сохранён: $OUTFILE"
