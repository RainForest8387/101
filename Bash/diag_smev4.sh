#!/usr/bin/env bash
#
# diag_smev4.sh — единая диагностика доступности ЦОД СМЭВ4.
#
# Логика:
#   1) Для каждого адреса выполняется проверка доступности:
#        - брокеры  -> TCP-connect на конкретный host:port;
#        - auth     -> HTTPS-запрос (по доменному имени, как требует конфиг);
#        - NTP      -> UDP/NTP-проба.
#   2) Трассировка (traceroute/mtr) запускается ТОЛЬКО для недоступных адресов,
#      проба идёт на порт/протокол, который не ответил:
#        - брокер   -> TCP-SYN на его уникальный порт;
#        - auth     -> TCP-SYN на 443;
#        - NTP      -> ICMP.
#
# Каждый брокер имеет собственный порт — проверяется и трассируется
# именно пара host:port (109.207.15.27:6651 и 109.207.15.27:6652 — отдельно).
#
# Использование:
#   ./diag_smev4.sh                  # проверка + трассировка недоступных
#   ./diag_smev4.sh --trace-all      # трассировать все, а не только упавшие
#   ./diag_smev4.sh --no-trace       # только проверка доступности
#   ./diag_smev4.sh -t 3             # таймаут проверки (сек), по умолч. 5
#   ./diag_smev4.sh -m 30            # макс. хопов трассировки
#   ./diag_smev4.sh --no-color       # без цвета
#   ./diag_smev4.sh -o report.txt    # сохранить полный отчёт в файл
#
# Для TCP/ICMP-трассировки нужны права root
# Код возврата: 0 — всё доступно; 1 — есть недоступные адреса.

set -u

TIMEOUT=5
MAXHOPS=30
TRACE_MODE="failed"   # failed | all | none
USE_COLOR=1
OUTFILE=""

# ---------- разбор аргументов ----------
while [[ $# -gt 0 ]]; do
  case "$1" in
    -t|--timeout) TIMEOUT="$2"; shift 2 ;;
    -m|--maxhops) MAXHOPS="$2"; shift 2 ;;
    --trace-all)  TRACE_MODE="all"; shift ;;
    --no-trace)   TRACE_MODE="none"; shift ;;
    --no-color)   USE_COLOR=0; shift ;;
    -o|--output)  OUTFILE="$2"; shift 2 ;;
    -h|--help)    grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "Неизвестный аргумент: $1" >&2; exit 2 ;;
  esac
done

# ---------- цвета ----------
if [[ "$USE_COLOR" -eq 1 && -t 1 ]]; then
  C_OK=$'\033[32m'; C_FAIL=$'\033[31m'; C_HDR=$'\033[1;36m'; C_DIM=$'\033[2m'; C_RST=$'\033[0m'
else
  C_OK=''; C_FAIL=''; C_HDR=''; C_DIM=''; C_RST=''
fi
OK_MARK="${C_OK}OK${C_RST}"; FAIL_MARK="${C_FAIL}FAIL${C_RST}"

PASS=0; FAIL=0

# ---------- конфигурация ----------
# Брокеры: "host:port  описание"
BROKERS=(
  # NODE1 broker-addresses
  "109.207.15.27:6651  NODE1 broker"
  "109.207.15.27:6652  NODE1 broker"
  "109.207.15.59:6651  NODE1 broker"
  "109.207.15.59:6652  NODE1 broker"
  # NODE1 broker-cross-addresses
  "109.207.15.155:6667 NODE1 broker-cross"
  "109.207.15.155:6668 NODE1 broker-cross"
  "109.207.15.187:6667 NODE1 broker-cross"
  "109.207.15.187:6668 NODE1 broker-cross"
  # NODE2 broker-addresses
  "109.207.15.155:6651 NODE2 broker"
  "109.207.15.155:6652 NODE2 broker"
  "109.207.15.187:6651 NODE2 broker"
  "109.207.15.187:6652 NODE2 broker"
  # NODE2 broker-cross-addresses
  "109.207.15.27:6667  NODE2 broker-cross"
  "109.207.15.27:6668  NODE2 broker-cross"
  "109.207.15.59:6667  NODE2 broker-cross"
  "109.207.15.59:6668  NODE2 broker-cross"
)

# Серверы аутентификации (только по доменному имени!)
AUTH_URLS=(
  "https://podd1.gosuslugi.ru:443/auth"
  "https://podd2.gosuslugi.ru:443/auth"
  "https://podd3-cross.gosuslugi.ru:443/auth"
  "https://podd4-cross.gosuslugi.ru:443/auth"
  "https://podd3.gosuslugi.ru:443/auth"
  "https://podd4.gosuslugi.ru:443/auth"
  "https://podd1-cross.gosuslugi.ru:443/auth"
  "https://podd2-cross.gosuslugi.ru:443/auth"
)

# NTP-серверы (UDP 123)
NTP_HOSTS=(
  "109.207.15.28"
  "109.207.15.60"
  "109.207.15.156"
  "109.207.15.188"
)

# ---------- инструмент трассировки ----------
TRACER=""
if command -v mtr >/dev/null 2>&1; then TRACER="mtr"
elif command -v traceroute >/dev/null 2>&1; then TRACER="traceroute"; fi

if [[ "$TRACE_MODE" != "none" && -z "$TRACER" ]]; then
  echo "ПРЕДУПРЕЖДЕНИЕ: mtr/traceroute не найдены — трассировка отключена." >&2
  echo "Установка: apt install mtr-tiny traceroute | yum install mtr traceroute" >&2
  TRACE_MODE="none"
fi
if [[ "$TRACE_MODE" != "none" && $EUID -ne 0 ]]; then
  echo "ПРЕДУПРЕЖДЕНИЕ: не root — TCP/ICMP-трассировка может не работать, используйте sudo." >&2
fi

# ---------- вывод ----------
emit() {
  if [[ -n "$OUTFILE" ]]; then printf '%s\n' "$*" | tee -a "$OUTFILE"
  else printf '%s\n' "$*"; fi
}

# ---------- проверки доступности ----------
check_tcp() {  # host port
  local host="$1" port="$2"
  if command -v nc >/dev/null 2>&1; then
    nc -z -w "$TIMEOUT" "$host" "$port" >/dev/null 2>&1
  else
    timeout "$TIMEOUT" bash -c "exec 3<>/dev/tcp/$host/$port" >/dev/null 2>&1
  fi
}
check_https() {  # url
  local code
  code=$(curl -s -k -o /dev/null -w '%{http_code}' \
         --connect-timeout "$TIMEOUT" --max-time $((TIMEOUT*3)) "$1" 2>/dev/null)
  [[ -n "$code" && "$code" != "000" ]]
}
check_ntp() {  # host
  local host="$1"
  if command -v ntpdate >/dev/null 2>&1; then
    ntpdate -q -t "$TIMEOUT" "$host" >/dev/null 2>&1 && return 0
  fi
  if command -v sntp >/dev/null 2>&1; then
    sntp -t "$TIMEOUT" "$host" >/dev/null 2>&1 && return 0
  fi
  if command -v nc >/dev/null 2>&1; then
    printf '\x1b%.0s' {1..48} | nc -u -w "$TIMEOUT" "$host" 123 2>/dev/null | grep -q . && return 0
  fi
  return 2
}

# ---------- трассировка ----------
# do_trace <target> <mode: tcp|icmp|udp> <port>
do_trace() {
  local target="$1" mode="$2" port="$3" out
  [[ "$TRACE_MODE" == "none" || -z "$TRACER" ]] && return
  emit "${C_DIM}--- трассировка $target ($mode${port:+:$port}) ---${C_RST}"
  if [[ "$TRACER" == "mtr" ]]; then
    local a=(-r -w -b -c 5 -m "$MAXHOPS")
    case "$mode" in tcp) a+=(-T -P "$port");; udp) a+=(-u -P "$port");; esac
    out=$(mtr "${a[@]}" "$target" 2>&1)
  else
    local a=(-m "$MAXHOPS" -w 2 -q 1)
    case "$mode" in icmp) a+=(-I);; tcp) a+=(-T -p "$port");; udp) a+=(-U -p "$port");; esac
    out=$(traceroute "${a[@]}" "$target" 2>&1)
  fi
  emit "$out"
  emit ""
}

# ---------- запуск ----------
[[ -n "$OUTFILE" ]] && : > "$OUTFILE"
emit "############################################################"
emit "# Диагностика ЦОД СМЭВ4  ($(date '+%Y-%m-%d %H:%M:%S'))"
emit "# Проверка + трассировка [$TRACE_MODE] | таймаут ${TIMEOUT}s | tracer: ${TRACER:-нет}"
emit "############################################################"

# --- Брокеры ---
emit ""
emit "${C_HDR}===== Брокеры (TCP host:port) =====${C_RST}"
for entry in "${BROKERS[@]}"; do
  hp="${entry%% *}"; desc="${entry#* }"; desc="${desc#"${desc%%[![:space:]]*}"}"
  host="${hp%%:*}"; port="${hp##*:}"
  if check_tcp "$host" "$port"; then
    emit "  [$OK_MARK]   $(printf '%-22s' "$hp") $desc"; ((PASS++))
    [[ "$TRACE_MODE" == "all" ]] && do_trace "$host" tcp "$port"
  else
    emit "  [$FAIL_MARK] $(printf '%-22s' "$hp") $desc"; ((FAIL++))
    [[ "$TRACE_MODE" != "none" ]] && do_trace "$host" tcp "$port"
  fi
done

# --- Auth ---
emit ""
emit "${C_HDR}===== Серверы аутентификации (HTTPS, по доменному имени) =====${C_RST}"
for url in "${AUTH_URLS[@]}"; do
  # хост из URL для трассировки
  hostport="${url#https://}"; hostport="${hostport%%/*}"; ahost="${hostport%%:*}"
  if check_https "$url"; then
    emit "  [$OK_MARK]   $url"; ((PASS++))
    [[ "$TRACE_MODE" == "all" ]] && do_trace "$ahost" tcp 443
  else
    emit "  [$FAIL_MARK] $url"; ((FAIL++))
    [[ "$TRACE_MODE" != "none" ]] && do_trace "$ahost" tcp 443
  fi
done

# --- NTP ---
emit ""
emit "${C_HDR}===== NTP-серверы (UDP 123) =====${C_RST}"
for host in "${NTP_HOSTS[@]}"; do
  check_ntp "$host"; rc=$?
  if [[ $rc -eq 0 ]]; then
    emit "  [$OK_MARK]   $host"; ((PASS++))
    [[ "$TRACE_MODE" == "all" ]] && do_trace "$host" icmp ""
  elif [[ $rc -eq 2 ]]; then
    emit "  [SKIP] $host (нет ntpdate/sntp/nc для проверки)"
  else
    emit "  [$FAIL_MARK] $host"; ((FAIL++))
    [[ "$TRACE_MODE" != "none" ]] && do_trace "$host" icmp ""
  fi
done

# --- Итог ---
emit ""
emit "${C_HDR}===== Итог =====${C_RST}"
emit "  Доступно: ${C_OK}${PASS}${C_RST}   Недоступно: ${C_FAIL}${FAIL}${C_RST}"
[[ -n "$OUTFILE" ]] && echo "Отчёт сохранён: $OUTFILE"

[[ "$FAIL" -eq 0 ]] && exit 0 || exit 1
