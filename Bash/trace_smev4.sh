#!/usr/bin/env bash
#
# trace_smev4.sh — трассировка маршрута до всех хостов ЦОД СМЭВ4.
#
# Использует mtr (приоритет) либо traceroute. Проходит по всем уникальным
# адресам из конфига: брокеры, NTP-серверы и (опционально) серверы
# аутентификации по доменным именам.
#
# Использование:
#   ./trace_smev4.sh                 # трассировка всех IP (брокеры + NTP)
#   ./trace_smev4.sh --auth          # включить и auth-серверы (по домену)
#   ./trace_smev4.sh -c 20           # число проб на хост (mtr), по умолчанию 10
#   ./trace_smev4.sh -m 20           # макс. число хопов, по умолчанию 30
#   ./trace_smev4.sh --tool traceroute  # принудительно использовать traceroute
#   ./trace_smev4.sh -o report.txt   # сохранить вывод в файл
#

set -u

COUNT=10        # проб на хост (mtr -c)
MAXHOPS=30      # макс. хопов
TOOL=""         # auto
INCLUDE_AUTH=0
OUTFILE=""

# ---------- разбор аргументов ----------
while [[ $# -gt 0 ]]; do
  case "$1" in
    -c|--count)   COUNT="$2"; shift 2 ;;
    -m|--maxhops) MAXHOPS="$2"; shift 2 ;;
    --tool)       TOOL="$2"; shift 2 ;;
    --auth)       INCLUDE_AUTH=1; shift ;;
    -o|--output)  OUTFILE="$2"; shift 2 ;;
    -h|--help)
      grep '^#' "$0" | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *) echo "Неизвестный аргумент: $1" >&2; exit 2 ;;
  esac
done

# ---------- цели ----------
# IP брокеров
BROKER_IPS=(
  "109.207.15.27"
  "109.207.15.59"
  "109.207.15.155"
  "109.207.15.187"
)

# NTP-серверы
NTP_IPS=(
  "109.207.15.28"
  "109.207.15.60"
  "109.207.15.156"
  "109.207.15.188"
)

# Серверы аутентификации
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
    echo "Установите: apt install mtr-tiny  |  yum install mtr  |  brew install mtr" >&2
    exit 3
  fi
else
  if ! command -v "$TOOL" >/dev/null 2>&1; then
    echo "Ошибка: инструмент '$TOOL' не найден в PATH." >&2
    exit 3
  fi
fi

# ---------- вывод (экран + опц. файл) ----------
emit() {
  if [[ -n "$OUTFILE" ]]; then
    printf '%s\n' "$*" | tee -a "$OUTFILE"
  else
    printf '%s\n' "$*"
  fi
}

run_trace() {
  local target="$1"
  local out
  if [[ "$TOOL" == "mtr" ]]; then
    # -r отчёт, -w широкий, -b показать IP и имя, -c число проб, -m хопы
    out=$(mtr -r -w -b -c "$COUNT" -m "$MAXHOPS" "$target" 2>&1)
  else
    # traceroute: -m хопы, -w таймаут ожидания хопа
    out=$(traceroute -m "$MAXHOPS" -w 2 "$target" 2>&1)
  fi
  if [[ -n "$OUTFILE" ]]; then
    printf '%s\n' "$out" | tee -a "$OUTFILE"
  else
    printf '%s\n' "$out"
  fi
}

# ---------- запуск ----------
[[ -n "$OUTFILE" ]] && : > "$OUTFILE"

emit "############################################################"
emit "# Трассировка маршрутов ЦОД СМЭВ4"
emit "# Инструмент: $TOOL | дата: $(date '+%Y-%m-%d %H:%M:%S')"
emit "############################################################"

emit ""
emit "===== Брокеры ====="
for ip in "${BROKER_IPS[@]}"; do
  emit ""
  emit ">>> $ip"
  run_trace "$ip"
done

emit ""
emit "===== NTP-серверы ====="
for ip in "${NTP_IPS[@]}"; do
  emit ""
  emit ">>> $ip"
  run_trace "$ip"
done

if [[ "$INCLUDE_AUTH" -eq 1 ]]; then
  emit ""
  emit "===== Серверы аутентификации (по домену) ====="
  for h in "${AUTH_HOSTS[@]}"; do
    emit ""
    emit ">>> $h"
    run_trace "$h"
  done
fi

emit ""
emit "Готово."
[[ -n "$OUTFILE" ]] && echo "Отчёт сохранён: $OUTFILE"
