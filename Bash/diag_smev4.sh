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
#      проба идёт на тот же порт/протокол, который не ответил:
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
#   ./diag_smev4.sh --no-install     # не доустанавливать ntpdate автоматически
#   ./diag_smev4.sh -o report.txt    # сохранить полный отчёт в файл
#
# Если ntpdate не установлен, скрипт попытается установить его автоматически
# (apt/dnf/yum/zypper/pacman, при необходимости через sudo).
# Для TCP/ICMP-трассировки и автоустановки пакетов нужны права root
# Код возврата: 0 — всё доступно; 1 — есть недоступные адреса.

set -u

# Поиск бинарника по имени с перебором стандартных каталогов (в т.ч. sbin,
# которых часто нет в PATH у обычного пользователя). Печатает абсолютный путь.
find_bin() {  # name -> абсолютный путь или пусто
  local n="$1" p d
  p=$(command -v "$n" 2>/dev/null) && [[ -x "$p" ]] && { printf '%s' "$p"; return 0; }
  for d in /usr/local/sbin /usr/sbin /sbin /usr/local/bin /usr/bin /bin; do
    [[ -x "$d/$n" ]] && { printf '%s' "$d/$n"; return 0; }
  done
  return 1
}

TIMEOUT=5
MAXHOPS=30
TRACE_MODE="failed"   # failed | all | none
USE_COLOR=1
OUTFILE=""
NO_INSTALL=0          # 1 = не пытаться доустанавливать ntpdate

# ---------- разбор аргументов ----------
while [[ $# -gt 0 ]]; do
  case "$1" in
    -t|--timeout) TIMEOUT="$2"; shift 2 ;;
    -m|--maxhops) MAXHOPS="$2"; shift 2 ;;
    --trace-all)  TRACE_MODE="all"; shift ;;
    --no-trace)   TRACE_MODE="none"; shift ;;
    --no-color)   USE_COLOR=0; shift ;;
    --no-install) NO_INSTALL=1; shift ;;
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
# TRACER — тип (mtr|traceroute), TRACER_BIN — абсолютный путь к нему.
TRACER=""; TRACER_BIN=""
if TRACER_BIN=$(find_bin mtr); then TRACER="mtr"
elif TRACER_BIN=$(find_bin traceroute); then TRACER="traceroute"; fi

if [[ "$TRACE_MODE" != "none" && -z "$TRACER" ]]; then
  echo "ПРЕДУПРЕЖДЕНИЕ: mtr/traceroute не найдены — трассировка отключена." >&2
  echo "Установка: apt install mtr-tiny traceroute | yum install mtr traceroute" >&2
  TRACE_MODE="none"
fi
if [[ "$TRACE_MODE" != "none" && $EUID -ne 0 ]]; then
  echo "ПРЕДУПРЕЖДЕНИЕ: не root — TCP/ICMP-трассировка может не работать, используйте sudo." >&2
fi

# ---------- вывод ----------
# На экран — как есть (с цветом), в файл — со снятыми ANSI-кодами.
emit() {
  printf '%s\n' "$*"
  if [[ -n "$OUTFILE" ]]; then
    printf '%s\n' "$*" | sed -E $'s/\033\\[[0-9;]*m//g' >> "$OUTFILE"
  fi
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
resolve_host() {  # host -> печатает IP в stdout, код 0 если удалось
  local host="$1" ip=""
  if command -v getent >/dev/null 2>&1; then
    ip=$(getent ahosts "$host" 2>/dev/null | awk '/STREAM|RAW|DGRAM/{print $1; exit} {print $1; exit}')
  fi
  if [[ -z "$ip" ]] && command -v host >/dev/null 2>&1; then
    ip=$(host "$host" 2>/dev/null | awk '/has address/{print $NF; exit}')
  fi
  if [[ -z "$ip" ]] && command -v nslookup >/dev/null 2>&1; then
    ip=$(nslookup "$host" 2>/dev/null | awk '/^Address: /{print $2; exit}')
  fi
  if [[ -z "$ip" ]] && command -v python3 >/dev/null 2>&1; then
    ip=$(python3 -c "import socket,sys; print(socket.gethostbyname(sys.argv[1]))" "$host" 2>/dev/null)
  fi
  [[ -n "$ip" ]] && { printf '%s' "$ip"; return 0; } || return 1
}

# Проверка наличия ntpdate; при отсутствии — попытка автоустановки.
# Возвращает 0, если ntpdate доступен после проверки/установки.
ensure_ntpdate() {
  find_bin ntpdate >/dev/null 2>&1 && return 0
  [[ "$NO_INSTALL" -eq 1 ]] && return 1

  # Определяем менеджер пакетов
  local pm="" install=""
  if   command -v apt-get >/dev/null 2>&1; then pm="apt-get"; install="apt-get install -y ntpdate"
  elif command -v apt     >/dev/null 2>&1; then pm="apt";     install="apt install -y ntpdate"
  elif command -v dnf     >/dev/null 2>&1; then pm="dnf";     install="dnf install -y ntpdate"
  elif command -v yum     >/dev/null 2>&1; then pm="yum";     install="yum install -y ntpdate"
  elif command -v zypper  >/dev/null 2>&1; then pm="zypper";  install="zypper --non-interactive install ntpdate"
  elif command -v pacman  >/dev/null 2>&1; then pm="pacman";  install="pacman -Sy --noconfirm ntp"
  else
    emit "  ${C_DIM}ntpdate не найден, менеджер пакетов не определён — установка невозможна.${C_RST}"
    return 1
  fi

  # Нужны права root
  local sudo_cmd=""
  if [[ $EUID -ne 0 ]]; then
    if command -v sudo >/dev/null 2>&1; then sudo_cmd="sudo"
    else
      emit "  ${C_DIM}ntpdate не установлен; нет прав root для установки ($pm). Запустите под root или: $install${C_RST}"
      return 1
    fi
  fi

  emit "  ${C_DIM}ntpdate не найден — устанавливаю пакет ($pm)...${C_RST}"
  # Для apt обновим индексы (тихо), ошибки не критичны
  if [[ "$pm" == "apt" || "$pm" == "apt-get" ]]; then
    $sudo_cmd $pm update >/dev/null 2>&1
  fi
  $sudo_cmd $install >/dev/null 2>&1
  local bin
  if bin=$(find_bin ntpdate); then
    emit "  ${C_DIM}ntpdate успешно установлен ($bin).${C_RST}"
    return 0
  fi
  emit "  ${C_DIM}Не удалось установить ntpdate автоматически. Установите вручную: $install${C_RST}"
  return 1
}

# Локальный IP интерфейса, через который идёт маршрут к сети СМЭВ.
get_local_ip() {  # target
  local target="$1" ip=""
  if command -v ip >/dev/null 2>&1; then
    ip=$(ip route get "$target" 2>/dev/null | grep -oP 'src \K[0-9.]+' | head -1)
  fi
  [[ -z "$ip" ]] && command -v hostname >/dev/null 2>&1 && ip=$(hostname -I 2>/dev/null | awk '{print $1}')
  printf '%s' "$ip"
}

check_ntp() {  # host
  local host="$1" bin
  if bin=$(find_bin ntpdate); then
    "$bin" -q -t "$TIMEOUT" "$host" >/dev/null 2>&1 && return 0
  fi
  if bin=$(find_bin sntp); then
    "$bin" -t "$TIMEOUT" "$host" >/dev/null 2>&1 && return 0
  fi
  if bin=$(find_bin nc); then
    printf '\x1b%.0s' {1..48} | "$bin" -u -w "$TIMEOUT" "$host" 123 2>/dev/null | grep -q . && return 0
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
    out=$("$TRACER_BIN" "${a[@]}" "$target" 2>&1)
  else
    local a=(-m "$MAXHOPS" -w 2 -q 1)
    case "$mode" in icmp) a+=(-I);; tcp) a+=(-T -p "$port");; udp) a+=(-U -p "$port");; esac
    out=$("$TRACER_BIN" "${a[@]}" "$target" 2>&1)
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

# --- Сведения о клиенте (для заявок оператору СМЭВ) ---
LOCAL_IP=$(get_local_ip "${BROKERS[0]%%:*}")
emit ""
emit "${C_HDR}===== Адреса этого хоста =====${C_RST}"
emit "  Локальный IP (маршрут к СМЭВ): ${LOCAL_IP:-не определён}"

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
emit "${C_HDR}===== Серверы аутентификации (HTTPS, по fqdn) =====${C_RST}"
for url in "${AUTH_URLS[@]}"; do
  # хост из URL для трассировки
  hostport="${url#https://}"; hostport="${hostport%%/*}"; ahost="${hostport%%:*}"
  # предварительная проверка DNS: без резолва нет смысла в HTTPS/трассировке
  if ! aip=$(resolve_host "$ahost"); then
    emit "  [${C_FAIL}DNS FAIL${C_RST}] $url  ${C_DIM}(имя не резолвиться в IP — ошибка DNS)${C_RST}"
    ((FAIL++))
    continue
  fi
  if check_https "$url"; then
    emit "  [$OK_MARK]   $url  ${C_DIM}($ahost -> $aip)${C_RST}"; ((PASS++))
    [[ "$TRACE_MODE" == "all" ]] && do_trace "$ahost" tcp 443
  else
    emit "  [$FAIL_MARK] $url  ${C_DIM}($ahost -> $aip)${C_RST}"; ((FAIL++))
    [[ "$TRACE_MODE" != "none" ]] && do_trace "$ahost" tcp 443
  fi
done

# --- NTP ---
emit ""
emit "${C_HDR}===== NTP-серверы (UDP 123) =====${C_RST}"
ensure_ntpdate   # проверить/доустановить ntpdate перед проверкой
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
