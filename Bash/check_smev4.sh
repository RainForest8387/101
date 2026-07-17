#!/usr/bin/env bash
#
# check_smev4.sh — проверка сетевой доступности всех хостов/портов ЦОД СМЭВ4.
#
# Проверяет:
#   - TCP-порты брокеров (broker-addresses / broker-cross-addresses);
#   - HTTPS-доступность серверов аутентификации (auth-server);
#   - NTP-серверы (UDP 123).
#
# Использование:
#   ./check_smev4.sh              # проверить всё
#   ./check_smev4.sh -t 3         # задать таймаут (сек), по умолчанию 5
#   ./check_smev4.sh --no-color   # без цветного вывода
#
# Код возврата: 0 — все проверки успешны; 1 — есть недоступные хосты/порты.

set -u

TIMEOUT=5
USE_COLOR=1

# ---------- разбор аргументов ----------
while [[ $# -gt 0 ]]; do
  case "$1" in
    -t|--timeout) TIMEOUT="$2"; shift 2 ;;
    --no-color)   USE_COLOR=0; shift ;;
    -h|--help)
      grep '^#' "$0" | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *) echo "Неизвестный аргумент: $1" >&2; exit 2 ;;
  esac
done

# ---------- цвета ----------
if [[ "$USE_COLOR" -eq 1 && -t 1 ]]; then
  C_OK=$'\033[32m'; C_FAIL=$'\033[31m'; C_HDR=$'\033[1;36m'; C_RST=$'\033[0m'
else
  C_OK=''; C_FAIL=''; C_HDR=''; C_RST=''
fi

OK_MARK="${C_OK}OK${C_RST}"
FAIL_MARK="${C_FAIL}FAIL${C_RST}"

PASS=0
FAIL=0

# ---------- конфигурация (из настроек ЦОД СМЭВ4) ----------

# TCP: "IP:ПОРТ  описание"
TCP_TARGETS=(
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

# HTTPS auth-server: URL
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

# NTP: IP (UDP 123)
NTP_HOSTS=(
  "109.207.15.28"
  "109.207.15.60"
  "109.207.15.156"
  "109.207.15.188"
)

# ---------- проверка TCP-порта ----------
# Приоритет: nc -> bash /dev/tcp
check_tcp() {
  local host="$1" port="$2"
  if command -v nc >/dev/null 2>&1; then
    nc -z -w "$TIMEOUT" "$host" "$port" >/dev/null 2>&1
    return $?
  fi
  # запасной вариант на чистом bash
  timeout "$TIMEOUT" bash -c "exec 3<>/dev/tcp/$host/$port" >/dev/null 2>&1
}

# ---------- проверка HTTPS ----------
check_https() {
  local url="$1"
  # -k: не проверять сертификат (важно только наличие соединения/ответа)
  # принимаем любой HTTP-код как признак доступности сервиса
  local code
  code=$(curl -s -k -o /dev/null -w '%{http_code}' \
         --connect-timeout "$TIMEOUT" --max-time $((TIMEOUT * 3)) "$url" 2>/dev/null)
  [[ -n "$code" && "$code" != "000" ]]
}

# ---------- проверка NTP (UDP 123) ----------
check_ntp() {
  local host="$1"
  if command -v ntpdate >/dev/null 2>&1; then
    ntpdate -q -t "$TIMEOUT" "$host" >/dev/null 2>&1 && return 0
  fi
  if command -v sntp >/dev/null 2>&1; then
    sntp -t "$TIMEOUT" "$host" >/dev/null 2>&1 && return 0
  fi
  if command -v nc >/dev/null 2>&1; then
    # UDP-проба: посылаем NTP-запрос и ждём ответ
    printf '\x1b%.0s' {1..48} | \
      nc -u -w "$TIMEOUT" "$host" 123 2>/dev/null | grep -q . && return 0
  fi
  return 2  # нет инструмента для проверки
}

# ---------- запуск проверок ----------
printf '%s=== Проверка TCP-портов брокеров ===%s\n' "$C_HDR" "$C_RST"
for entry in "${TCP_TARGETS[@]}"; do
  hp="${entry%% *}"; desc="${entry#* }"; desc="${desc#"${desc%%[![:space:]]*}"}"
  host="${hp%%:*}"; port="${hp##*:}"
  if check_tcp "$host" "$port"; then
    printf '  [%s]   %-22s %s\n' "$OK_MARK"   "$hp" "$desc"; ((PASS++))
  else
    printf '  [%s] %-22s %s\n' "$FAIL_MARK" "$hp" "$desc"; ((FAIL++))
  fi
done

printf '\n%s=== Проверка серверов аутентификации (HTTPS) ===%s\n' "$C_HDR" "$C_RST"
for url in "${AUTH_URLS[@]}"; do
  if check_https "$url"; then
    printf '  [%s]   %s\n' "$OK_MARK"   "$url"; ((PASS++))
  else
    printf '  [%s] %s\n' "$FAIL_MARK" "$url"; ((FAIL++))
  fi
done

printf '\n%s=== Проверка NTP-серверов (UDP 123) ===%s\n' "$C_HDR" "$C_RST"
for host in "${NTP_HOSTS[@]}"; do
  check_ntp "$host"; rc=$?
  if [[ $rc -eq 0 ]]; then
    printf '  [%s]   %s\n' "$OK_MARK" "$host"; ((PASS++))
  elif [[ $rc -eq 2 ]]; then
    printf '  [%s] %s (нет ntpdate/sntp/nc для проверки)\n' "SKIP" "$host"
  else
    printf '  [%s] %s\n' "$FAIL_MARK" "$host"; ((FAIL++))
  fi
done

# ---------- итог ----------
printf '\n%s=== Итог ===%s\n' "$C_HDR" "$C_RST"
printf '  Успешно: %s%d%s   Ошибок: %s%d%s   Таймаут: %ss\n' \
  "$C_OK" "$PASS" "$C_RST" "$C_FAIL" "$FAIL" "$C_RST" "$TIMEOUT"

[[ "$FAIL" -eq 0 ]] && exit 0 || exit 1
