#!/bin/bash
#
# repartition_topics.sh — пересоздание Kafka-топиков по паттерну с новым числом партиций.
#
# Что делает:
#   1. Находит топики, имя которых целиком соответствует паттерну (напр. 'drub.*').
#   2. Для каждого топика проверяет, что он пустой (0 сообщений во всех партициях).
#   3. Пустые — удаляет и создаёт заново с заданным числом партиций.
#   4. Непустые — по умолчанию пропускает (защита от потери данных);
#      с флагом --force — удаляет и непустые и пустые и пересоздаёт (ДАННЫЕ БУДУТ ПОТЕРЯНЫ!).
#
# ВАЖНО: по умолчанию скрипт работает в режиме DRY-RUN — только показывает план и
# ничего не меняет. Чтобы реально удалить и пересоздать топики, нужен флаг --apply.
#
# Ход выполнения дублируется в файл отчёта report_<ГГГГ-ММ-ДД>_<ЧЧ-ММ-СС>.log
# в текущем каталоге (путь меняется через --report / env REPORT_DIR, отключается --no-report).
#
# Уменьшить число партиций штатным alter нельзя — только delete + create,
# поэтому топик обязан быть пустым.
#
# Удаление топика в Kafka асинхронное: --delete лишь помечает топик, а метаданные и
# сегменты чистятся фоном. Скрипт ждёт исчезновения топика до --delete-wait секунд, а
# всё, что не успело, повторно проверяет и создаёт в конце (раздел "Отложенные топики").
# Если топик не исчезает вообще — типично включён auto.create.topics.enable и живой
# клиент пересоздаёт топик сразу после удаления: остановите клиентов и повторите.
#
# Подключение к брокеру: SASL_SSL / SCRAM-SHA-256 (готовый -c client.properties
# либо генерация временного конфига из KAFKA_USER/KAFKA_PASSWORD — см. ниже).
#
# Использование:
#   ./repartition_topics.sh -p 'drub.*'                  # dry-run (по умолчанию): только план
#   KAFKA_USER=svc KAFKA_PASSWORD=secret ./repartition_topics.sh -p 'drub.*' # пример запуска с переменными окружения
#   ./repartition_topics.sh -p 'drub.*' -c client.properties --partitions 1 --apply
#   ./repartition_topics.sh -p 'drub.*' -r 3 -y --apply    # RF=3, без подтверждения
#   ./repartition_topics.sh -p 'drub.*' -C retention.ms=604800000 -C cleanup.policy=compact --apply
#   ./repartition_topics.sh -p 'drub.*' --force --apply    # пересоздать даже непустые топики (потеря данных!)
#
# Параметры:
#   -p, --pattern PAT           ERE-паттерн имени топика (обязателен). Матчится целиком: ^PAT$
#   -N, --partitions N          новое число партиций (по умолчанию 1)
#   -r, --replication-factor N  RF нового топика (по умолчанию — дефолт брокера)
#   -C, --config KEY=VALUE      конфиг нового топика (можно повторять): retention.ms,
#                               cleanup.policy, retention.bytes, segment.ms, ... (иначе дефолтное значение брокера)
#   -b, --bootstrap-server      адрес брокера host:port (или env BOOTSTRAP)
#   -c, --command-config FILE   client.properties c SASL/SSL (или env CFG)
#   -f, --force                 пересоздавать и НЕПУСТЫЕ топики (потеря данных); env FORCE=1
#   -a, --apply                 выполнить изменения (без него — dry-run)
#   -d, --dry-run               ничего не менять (режим по умолчанию, флаг оставлен для явности)
#   -y, --yes                   не спрашивать подтверждение
#       --delete-wait SEC       сколько секунд ждать реального удаления топика (по умолчанию 300;
#                               env DELETE_WAIT). Удаление в Kafka асинхронное — при большом числе
#                               топиков/партиций или занятом брокере увеличьте значение.
#       --report FILE           путь к файлу отчёта (по умолчанию report_<дата>_<время>.log)
#       --no-report             не писать файл отчёта
#       --no-color              без цветного вывода
#   -h, --help                  эта справка
#
# Переменные окружения (подключение по SASL_SSL / SCRAM-SHA-256):
#   BOOTSTRAP                   = адрес брокера host:port, если не задан -b
#   CFG                         = путь к готовому client.properties, если не задан -c
#   KAFKA_USER, KAFKA_PASSWORD  = учётка SCRAM; если -c/CFG не заданы, генерируется
#                                 временный client.properties (security.protocol=SASL_SSL,
#                                 sasl.mechanism=SCRAM-SHA-256)
#   SSL_TRUSTSTORE_LOCATION     = truststore c CA брокера (иначе — системный CA JVM)
#   SSL_TRUSTSTORE_PASSWORD     = пароль truststore
#   SSL_TRUSTSTORE_TYPE         = тип truststore (JKS/PKCS12), необязательно
#   SSL_ENDPOINT_ID_ALGO        = ssl.endpoint.identification.algorithm (пусто = без проверки hostname)
#   KAFKA_HOME                  = каталог установки Kafka (иначе kafka-topics.sh из PATH)
#   REPORT_DIR                  = каталог для файла отчёта (по умолчанию текущий)
#   DELETE_WAIT                 = таймаут ожидания удаления топика, сек (по умолчанию 300)
#
# Конфиги пересоздаваемых топиков (всё необязательно, приоритет: -C > TOPIC_CONFIGS > именованные):
#   RETENTION_MS, RETENTION_BYTES, CLEANUP_POLICY, SEGMENT_MS, SEGMENT_BYTES,
#   MIN_INSYNC_REPLICAS, MAX_MESSAGE_BYTES, COMPRESSION_TYPE  — именованные переменные
#   TOPIC_CONFIGS = "retention.ms=... cleanup.policy=..."     — произвольный список key=value через пробел
#   (для любых ключей, которых нет среди именованных, использовать -C key=value или TOPIC_CONFIGS)
#
# Код возврата: 0 — успех (или нечего делать); 1 — были ошибки/пропуски непустых топиков.

set -uo pipefail

# ---------- значения по умолчанию ----------
PATTERN=""
PARTITIONS=1
REPL_FACTOR=""
BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"
CFG="${CFG:-}"
DRY_RUN=1               # 1 — безопасный режим по умолчанию; сбрасывается флагом --apply
ASSUME_YES=0
REPORT_ENABLED=1
REPORT_DIR="${REPORT_DIR:-.}"
REPORT_FILE=""          # заполняется автоматически: report_<дата>_<время>.log
LOG_FIFO=""
LOG_PID=""
FORCE="${FORCE:-0}"     # 1 — пересоздавать даже непустые топики (потеря данных)
USE_COLOR=1
DELETE_WAIT="${DELETE_WAIT:-300}"   # сколько СЕКУНД ждать фактического удаления топика
PENDING=()              # ждут второго круга: удаление отправлено, топик ещё виден
PENDING_IDS=()          # TopicId этих топиков ДО удаления (для диагностики)
NOT_CREATED=()          # удалены и так и не созданы — ДАННЫХ/ТОПИКА НЕТ, создать вручную
NOT_DELETED=()          # удаление не сработало — топик остался как был
RAW_CONFIGS=()         # значения флагов -C/--config (обрабатываются после определения хелперов)

# ---------- краткая справка ----------
usage() {
  cat <<'EOF'
repartition_topics.sh — пересоздание Kafka-топиков по паттерну с новым числом партиций.

Использование: repartition_topics.sh -p PATTERN [опции]

Опции:
  -p, --pattern PAT           ERE-паттерн имени топика (обязателен), матчится целиком: ^PAT$
  -N, --partitions N          новое число партиций (по умолчанию 1)
  -r, --replication-factor N  RF нового топика (по умолчанию — дефолт брокера)
  -C, --config KEY=VALUE      конфиг нового топика, можно повторять (retention.ms, cleanup.policy, ...)
  -b, --bootstrap-server ADDR адрес брокера host:port (или env BOOTSTRAP)
  -c, --command-config FILE   client.properties с SASL/SSL (или env CFG)
  -f, --force                 пересоздавать и НЕПУСТЫЕ топики (потеря данных)
  -a, --apply                 РЕАЛЬНО выполнить изменения (без него — dry-run)
  -d, --dry-run               ничего не менять (режим по умолчанию)
  -y, --yes                   не спрашивать подтверждение
      --delete-wait SEC       ждать удаления топика SEC секунд (по умолчанию 300)
      --report FILE           путь к файлу отчёта (по умолчанию report_<дата>_<время>.log)
      --no-report             не писать файл отчёта
      --no-color              без цветного вывода
  -h, --help                  эта справка

По умолчанию — DRY-RUN: скрипт только показывает план. Изменения вносит только --apply.
Ход выполнения дублируется в файл report_<ГГГГ-ММ-ДД>_<ЧЧ-ММ-СС>.log (см. --report / REPORT_DIR).

Примеры:
  ./repartition_topics.sh -p 'drub.*'                                        # dry-run: только план
  ./repartition_topics.sh -p 'drub.fl.cert.*'                                # ERE, не glob: нужен '.*', а не '*'
  KAFKA_USER=svc KAFKA_PASSWORD=secret ./repartition_topics.sh -p 'drub.*' --apply
  ./repartition_topics.sh -p 'drub.*' -c client.properties --partitions 1 --apply
  ./repartition_topics.sh -p 'drub.*' -r 3 -y --apply
  ./repartition_topics.sh -p 'drub.*' -C retention.ms=604800000 -C cleanup.policy=compact --apply
  ./repartition_topics.sh -p 'drub.*' --force --apply
  ./repartition_topics.sh -p 'drub.*' --report /var/log/kafka/repart.log --apply

Подробное описание, переменные окружения и коды возврата — в комментариях в начале файла.
EOF
}

# ---------- разбор аргументов ----------
ORIG_ARGS=("$@")
while [[ $# -gt 0 ]]; do
  case "$1" in
    -p|--pattern)             PATTERN="$2"; shift 2 ;;
    -N|--partitions)          PARTITIONS="$2"; shift 2 ;;
    -r|--replication-factor)  REPL_FACTOR="$2"; shift 2 ;;
    -C|--config)              RAW_CONFIGS+=("$2"); shift 2 ;;
    -b|--bootstrap-server)    BOOTSTRAP="$2"; shift 2 ;;
    -c|--command-config)      CFG="$2"; shift 2 ;;
    -f|--force)               FORCE=1; shift ;;
    -a|--apply)               DRY_RUN=0; shift ;;
    -d|--dry-run)             DRY_RUN=1; shift ;;
    -y|--yes)                 ASSUME_YES=1; shift ;;
    --delete-wait)            DELETE_WAIT="$2"; shift 2 ;;
    --report)                 REPORT_FILE="$2"; shift 2 ;;
    --no-report)              REPORT_ENABLED=0; shift ;;
    --no-color)               USE_COLOR=0; shift ;;
    -h|--help)                usage; exit 0 ;;
    *) echo "Неизвестный аргумент: $1" >&2; exit 2 ;;
  esac
done

# ---------- цвета и хелперы вывода ----------
if [[ "$USE_COLOR" -eq 1 && -t 1 ]]; then
  C_OK=$'\033[1;32m'; C_BAD=$'\033[1;31m'; C_WARN=$'\033[1;33m'; C_HDR=$'\033[1;36m'; C_RST=$'\033[0m'
else
  C_OK=''; C_BAD=''; C_WARN=''; C_HDR=''; C_RST=''
fi
ok()   { printf '%s  [ OK ] %s%s\n' "$C_OK"   "$*" "$C_RST"; }
bad()  { printf '%s  [FAIL] %s%s\n' "$C_BAD"  "$*" "$C_RST"; }
warn() { printf '%s  [WARN] %s%s\n' "$C_WARN" "$*" "$C_RST"; }
info() { printf '    %s\n' "$*"; }
step() { printf '\n%s== %s ==%s\n' "$C_HDR" "$*" "$C_RST"; }

# ---------- запасной сток вывода ----------
# На некоторых хостах /dev/null имеет неверные права или подменён обычным файлом,
# и тогда ЛЮБОЙ редирект в него падает с "Отказано в доступе" — например, проверка
# `command -v ... >/dev/null` начинает возвращать "не найдено". Подстилаем временный файл.
TMP_NULL=""
if [[ -c /dev/null && -w /dev/null ]]; then
  DEVNULL=/dev/null
else
  TMP_NULL="$(mktemp "${TMPDIR:-/tmp}/repartition-null.XXXXXX")" || TMP_NULL=""
  DEVNULL="${TMP_NULL:-/dev/null}"
  warn "/dev/null недоступен для записи — вывод глушится через $DEVNULL"
  warn "Почините хост: sudo chmod 666 /dev/null (или пересоздайте: mknod /dev/null c 1 3)"
fi

# ---------- отчёт о выполнении ----------
# Весь вывод (stdout+stderr) идёт и на консоль, и в файл отчёта. Схема: скрипт пишет
# в FIFO, фоновый tee раздаёт поток на консоль и в файл; на выходе поток закрывается,
# tee дожидается конца, и из отчёта убираются ANSI-последовательности.
start_report() {
  [[ "$REPORT_ENABLED" -eq 1 ]] || return 0
  local ts dir
  ts="$(date '+%Y-%m-%d_%H-%M-%S')"
  [[ -z "$REPORT_FILE" ]] && REPORT_FILE="${REPORT_DIR%/}/report_${ts}.log"
  dir="$(dirname "$REPORT_FILE")"
  if ! mkdir -p "$dir" 2>"$DEVNULL" || ! : > "$REPORT_FILE" 2>"$DEVNULL"; then
    warn "Не удалось создать файл отчёта '$REPORT_FILE' — работаем без отчёта."
    REPORT_FILE=""; return 0
  fi
  {
    echo "# repartition_topics.sh — отчёт о выполнении"
    echo "# Запуск:      $(date '+%Y-%m-%d %H:%M:%S %Z')"
    echo "# Хост:        $(hostname 2>"$DEVNULL")   пользователь: $(id -un 2>"$DEVNULL")"
    echo "# Каталог:     $PWD"
    echo "# Команда:     $0 ${ORIG_ARGS[*]:-}"
    if [[ "$DRY_RUN" -eq 1 ]]; then
      echo "# Режим:       DRY-RUN (изменения не вносятся)"
    else
      echo "# Режим:       APPLY (топики будут удалены и пересозданы)"
    fi
    echo
  } >> "$REPORT_FILE"

  LOG_FIFO="$(mktemp -u "${TMPDIR:-/tmp}/repartition-log.XXXXXX")"
  if ! mkfifo "$LOG_FIFO" 2>"$DEVNULL"; then
    warn "mkfifo недоступен — отчёт будет неполным (только заголовок): $REPORT_FILE"
    LOG_FIFO=""; return 0
  fi
  exec 3>&1                                   # 3 — настоящая консоль
  tee -a "$REPORT_FILE" < "$LOG_FIFO" >&3 &
  LOG_PID=$!
  exec > "$LOG_FIFO" 2>&1                     # весь дальнейший вывод — через tee
  return 0
}

finish_report() {
  [[ -n "$LOG_FIFO" ]] || return 0
  exec 1>&3 2>&3 3>&-                         # вернуть вывод на консоль, закрыть FIFO
  wait "$LOG_PID" 2>"$DEVNULL"
  rm -f "$LOG_FIFO"; LOG_FIFO=""
  local tmp                                    # вычистить ANSI-цвета из отчёта
  if tmp="$(mktemp "${TMPDIR:-/tmp}/repartition-rep.XXXXXX")"; then
    if sed "s/$(printf '\033')\[[0-9;]*m//g" "$REPORT_FILE" > "$tmp" 2>"$DEVNULL"; then
      cat "$tmp" > "$REPORT_FILE"
    fi
    rm -f "$tmp"
  fi
  printf '%sОтчёт сохранён: %s%s\n' "$C_HDR" "$REPORT_FILE" "$C_RST"
  return 0
}

# ---------- уборка временных файлов ----------
TMP_CFG=""
cleanup() {
  finish_report
  [[ -n "$TMP_CFG"  ]] && rm -f "$TMP_CFG"
  [[ -n "$TMP_NULL" ]] && rm -f "$TMP_NULL"
  return 0
}
trap cleanup EXIT

start_report

# ---------- проверка входных данных ----------
[[ -z "$PATTERN" ]] && { bad "Не задан -p/--pattern"; echo "Подсказка: $0 --help" >&2; exit 2; }
if ! [[ "$PARTITIONS" =~ ^[1-9][0-9]*$ ]]; then
  bad "Некорректное число партиций: '$PARTITIONS' (нужно целое > 0)"; exit 2
fi
if [[ -n "$REPL_FACTOR" ]] && ! [[ "$REPL_FACTOR" =~ ^[1-9][0-9]*$ ]]; then
  bad "Некорректный replication-factor: '$REPL_FACTOR'"; exit 2
fi
if [[ -n "$CFG" && ! -r "$CFG" ]]; then
  bad "command-config недоступен для чтения: $CFG"; exit 2
fi
if ! [[ "$DELETE_WAIT" =~ ^[0-9]+$ ]]; then
  bad "Некорректный --delete-wait: '$DELETE_WAIT' (нужно целое число секунд)"; exit 2
fi

# ---------- сбор конфигов топика (дедуп по ключу, приоритет: -C > TOPIC_CONFIGS > именованные) ----------
# Параллельные массивы (совместимо с bash 3.2 — без associative arrays).
CONFIG_KEYS=()
CONFIG_VALS=()
add_config() {  # add_config "key=value" — добавить/перезаписать по ключу
  local kv="$1" key val i
  if [[ "$kv" != *=* ]]; then bad "Некорректный конфиг (нужно key=value): '$kv'"; exit 2; fi
  key="${kv%%=*}"; val="${kv#*=}"
  if [[ -z "$key" || ! "$key" =~ ^[A-Za-z0-9._-]+$ ]]; then bad "Некорректный ключ конфига: '$key'"; exit 2; fi
  for i in ${CONFIG_KEYS[@]+"${!CONFIG_KEYS[@]}"}; do
    if [[ "${CONFIG_KEYS[$i]}" == "$key" ]]; then CONFIG_VALS[$i]="$val"; return; fi
  done
  CONFIG_KEYS+=("$key"); CONFIG_VALS+=("$val")
}
# 1) именованные переменные (низший приоритет)
[[ -n "${CLEANUP_POLICY:-}"      ]] && add_config "cleanup.policy=$CLEANUP_POLICY"
[[ -n "${RETENTION_MS:-}"        ]] && add_config "retention.ms=$RETENTION_MS"
[[ -n "${RETENTION_BYTES:-}"     ]] && add_config "retention.bytes=$RETENTION_BYTES"
[[ -n "${SEGMENT_MS:-}"          ]] && add_config "segment.ms=$SEGMENT_MS"
[[ -n "${SEGMENT_BYTES:-}"       ]] && add_config "segment.bytes=$SEGMENT_BYTES"
[[ -n "${MIN_INSYNC_REPLICAS:-}" ]] && add_config "min.insync.replicas=$MIN_INSYNC_REPLICAS"
[[ -n "${MAX_MESSAGE_BYTES:-}"   ]] && add_config "max.message.bytes=$MAX_MESSAGE_BYTES"
[[ -n "${COMPRESSION_TYPE:-}"    ]] && add_config "compression.type=$COMPRESSION_TYPE"
# 2) произвольный список TOPIC_CONFIGS="k=v k=v" (значения без пробелов; запятые внутри значения — ок)
if [[ -n "${TOPIC_CONFIGS:-}" ]]; then
  read -r -a _tc <<< "$TOPIC_CONFIGS"
  for kv in "${_tc[@]}"; do add_config "$kv"; done
fi
# 3) флаги -C/--config (высший приоритет)
for kv in ${RAW_CONFIGS[@]+"${RAW_CONFIGS[@]}"}; do add_config "$kv"; done

# строка-сводка для вывода в план
CONFIG_SUMMARY=""
for i in ${CONFIG_KEYS[@]+"${!CONFIG_KEYS[@]}"}; do
  CONFIG_SUMMARY+="${CONFIG_KEYS[$i]}=${CONFIG_VALS[$i]} "
done

# ---------- поиск kafka-утилит ----------
find_tool() {  # find_tool <basename> -> печатает путь или пусто
  local name="$1" path
  if [[ -n "${KAFKA_HOME:-}" && -x "$KAFKA_HOME/bin/$name" ]]; then
    printf '%s' "$KAFKA_HOME/bin/$name"
    return 0
  fi
  # без редиректов: command -v молчит, если не нашёл (не зависит от прав на /dev/null)
  path="$(command -v "$name")" && printf '%s' "$path"
  return 0
}
KAFKA_TOPICS="$(find_tool kafka-topics.sh)"
KAFKA_OFFSETS="$(find_tool kafka-get-offsets.sh)"     # есть в Kafka >= 3.0
KAFKA_RUN_CLASS="$(find_tool kafka-run-class.sh)"      # запасной путь для подсчёта offset'ов
[[ -z "$KAFKA_TOPICS" ]] && { bad "kafka-topics.sh не найден (задайте KAFKA_HOME или PATH)"; exit 2; }

# ---------- аутентификация: SASL_SSL / SCRAM-SHA-256 ----------
# Если готовый command-config не передан, но заданы KAFKA_USER/KAFKA_PASSWORD —
# генерируем временный client.properties (права 600, удаляется на выходе).
if [[ -z "$CFG" ]]; then
  if [[ -n "${KAFKA_USER:-}" && -n "${KAFKA_PASSWORD:-}" ]]; then
    if [[ "$KAFKA_PASSWORD" == *'"'* ]]; then
      bad "Пароль SCRAM содержит символ '\"' — задайте client.properties через -c вручную."; exit 2
    fi
    umask 077
    TMP_CFG="$(mktemp "${TMPDIR:-/tmp}/kafka-scram.XXXXXX.properties")" || { bad "mktemp не удался"; exit 2; }
    CFG="$TMP_CFG"
    {
      echo "security.protocol=SASL_SSL"
      echo "sasl.mechanism=SCRAM-SHA-256"
      printf 'sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="%s" password="%s";\n' \
             "$KAFKA_USER" "$KAFKA_PASSWORD"
      [[ -n "${SSL_TRUSTSTORE_LOCATION:-}" ]] && echo "ssl.truststore.location=${SSL_TRUSTSTORE_LOCATION}"
      [[ -n "${SSL_TRUSTSTORE_PASSWORD:-}" ]] && echo "ssl.truststore.password=${SSL_TRUSTSTORE_PASSWORD}"
      [[ -n "${SSL_TRUSTSTORE_TYPE:-}"     ]] && echo "ssl.truststore.type=${SSL_TRUSTSTORE_TYPE}"
      # Пустое значение = отключить проверку hostname; переменная не задана = дефолт брокера
      [[ -n "${SSL_ENDPOINT_ID_ALGO+x}"    ]] && echo "ssl.endpoint.identification.algorithm=${SSL_ENDPOINT_ID_ALGO}"
    } > "$CFG"
  else
    bad "Для SASL_SSL/SCRAM-SHA-256 нужен либо -c/--command-config, либо KAFKA_USER и KAFKA_PASSWORD."
    info "Пример: KAFKA_USER=svc KAFKA_PASSWORD=secret $0 -p '$PATTERN'"
    exit 2
  fi
fi

# ---------- общие аргументы подключения ----------
CONN=(--bootstrap-server "$BOOTSTRAP" --command-config "$CFG")

# ---------- функции работы с топиками ----------

# list_all_topics -> список всех топиков (по одному в строке)
list_all_topics() {
  "$KAFKA_TOPICS" "${CONN[@]}" --list 2>"$DEVNULL"
}

# topic_partitions <topic> -> текущее число партиций (или пусто)
topic_partitions() {
  "$KAFKA_TOPICS" "${CONN[@]}" --describe --topic "$1" 2>"$DEVNULL" \
    | awk -F'PartitionCount:[[:space:]]*' 'NF>1{split($2,a,"[[:space:]]");print a[1];exit}'
}

# topic_id <topic> -> TopicId из --describe (или пусто, если топика нет)
topic_id() {
  "$KAFKA_TOPICS" "${CONN[@]}" --describe --topic "$1" 2>"$DEVNULL" \
    | awk -F'TopicId:[[:space:]]*' 'NF>1{split($2,a,"[[:space:]]");print a[1];exit}'
}

# offsets_sum <topic> <time(-1|-2)> -> сумма offset'ов по всем партициям
offsets_sum() {
  local topic="$1" time="$2"
  if [[ -n "$KAFKA_OFFSETS" ]]; then
    "$KAFKA_OFFSETS" "${CONN[@]}" --topic "$topic" --time "$time" 2>"$DEVNULL" \
      | awk -F: '{s+=$3} END{print s+0}'
  elif [[ -n "$KAFKA_RUN_CLASS" ]]; then
    local args=(--bootstrap-server "$BOOTSTRAP")
    [[ -n "$CFG" ]] && args+=(--command-config "$CFG")
    "$KAFKA_RUN_CLASS" kafka.tools.GetOffsetShell "${args[@]}" \
      --topic "$topic" --time "$time" 2>"$DEVNULL" \
      | awk -F: '{s+=$3} END{print s+0}'
  else
    echo "ERR"
  fi
}

# message_count <topic> -> число сообщений (latest - earliest) или ERR
message_count() {
  local topic="$1" latest earliest
  latest="$(offsets_sum "$topic" -1)"
  earliest="$(offsets_sum "$topic" -2)"
  [[ "$latest" == "ERR" || "$earliest" == "ERR" ]] && { echo "ERR"; return; }
  echo $(( latest - earliest ))
}

# wait_topic_gone <topic> [секунд] -> ждёт исчезновения топика из метаданных.
# Удаление в Kafka асинхронное: --delete лишь помечает топик, реальное удаление
# сегментов и очистка метаданных занимают время (тем больше, чем больше топиков
# и партиций удаляется одновременно). Ждём по реальным часам: каждый опрос — это
# запуск JVM (kafka-topics.sh --list), который сам по себе занимает секунды.
wait_topic_gone() {
  local topic="$1" limit="${2:-$DELETE_WAIT}" deadline interval=2
  deadline=$(( $(date +%s) + limit ))
  while :; do
    list_all_topics | grep -qx -- "$topic" || return 0
    [[ "$(date +%s)" -ge "$deadline" ]] && return 1
    sleep "$interval"
    [[ "$interval" -lt 5 ]] && interval=$((interval+1))
  done
}

# create_topic <topic> -> создаёт топик с целевыми параметрами; 0 — успех
create_topic() {
  local topic="$1" new_parts i
  local create_args=("${CONN[@]}" --create --topic "$topic" --partitions "$PARTITIONS")
  [[ -n "$REPL_FACTOR" ]] && create_args+=(--replication-factor "$REPL_FACTOR")
  for i in ${CONFIG_KEYS[@]+"${!CONFIG_KEYS[@]}"}; do
    create_args+=(--config "${CONFIG_KEYS[$i]}=${CONFIG_VALS[$i]}")
  done
  if ! "$KAFKA_TOPICS" "${create_args[@]}" 2>&1 | sed 's/^/    /'; then
    bad "Ошибка создания '$topic'."; return 1
  fi
  new_parts="$(topic_partitions "$topic")"
  if [[ "$new_parts" == "$PARTITIONS" ]]; then
    ok "Создан заново: партиций = $new_parts."; return 0
  fi
  bad "Создан, но партиций=$new_parts (ожидалось $PARTITIONS)."; return 1
}

# diagnose_stuck <topic> <topic_id_до_удаления> -> 0, если топика сейчас нет (можно создавать);
# 1, если топик на месте — с объяснением, почему удаление не сработало.
diagnose_stuck() {
  local topic="$1" old_id="$2" new_id
  new_id="$(topic_id "$topic")"
  if [[ -z "$new_id" ]]; then
    info "Топик в метаданных больше не виден — удаление всё-таки завершилось."
    return 0
  fi
  if [[ -n "$old_id" && "$new_id" != "$old_id" ]]; then
    bad "'$topic' был удалён и СРАЗУ ПЕРЕСОЗДАН клиентом (TopicId $old_id -> $new_id)."
    info "Так бывает при auto.create.topics.enable=true и живом продюсере/консьюмере:"
    info "клиент создаёт топик заново с брокерским num.partitions."
    info "Остановите клиентов этого топика (или запретите им CREATE через ACL) и повторите."
  else
    bad "'$topic' НЕ УДАЛЁН брокером (TopicId прежний: ${new_id:-?}) — топик остался как был."
    info "Проверьте: право DeleteTopics у пользователя из command-config;"
    info "delete.topic.enable на брокере; оффлайн/несинхронные реплики топика"
    info "(kafka-topics.sh --describe --topic '$topic' --unavailable-partitions)."
  fi
  return 1
}

# ---------- 1. поиск топиков по паттерну ----------
step "1. Поиск топиков по паттерну: ^${PATTERN}$"
info "Брокер: $BOOTSTRAP${CFG:+   command-config: $CFG}"

ALL="$(list_all_topics)"
if [[ -z "$ALL" ]]; then
  bad "Не удалось получить список топиков (проверьте адрес брокера и аутентификацию)"; exit 1
fi

MATCHED=()
while IFS= read -r _t; do [[ -n "$_t" ]] && MATCHED+=("$_t"); done \
  < <(printf '%s\n' "$ALL" | grep -E "^(${PATTERN})$" | sort)
if [[ "${#MATCHED[@]}" -eq 0 ]]; then
  warn "Ни один топик не подходит под паттерн — делать нечего."
  # Частая ошибка: паттерн пишут как glob ('cert*'), а это ERE, где '*' повторяет
  # предыдущий символ. Подсказываем исправленный вариант и похожие топики.
  if [[ "$PATTERN" =~ [^.]\* ]]; then
    fixed="$(printf '%s' "$PATTERN" | sed -E 's/([^.])\*/\1.*/g')"
    n="$(printf '%s\n' "$ALL" | grep -cE "^(${fixed})$")"
    warn "Паттерн разбирается как ERE, а не как glob: '*' повторяет предыдущий символ,"
    warn "то есть 'cert*' — это 'cer' + любое число 't', а не 'всё, что начинается с cert'."
    info "Возможно, вы имели в виду:  -p '$fixed'   (подходящих топиков: $n)"
  fi
  literal="$(printf '%s' "$PATTERN" | sed -E 's/[][(){}*+?^$|\\].*//')"
  if [[ -n "$literal" ]]; then
    similar="$(printf '%s\n' "$ALL" | grep -F -- "$literal" | head -10)"
    if [[ -n "$similar" ]]; then
      info "Топики, содержащие '$literal':"
      while IFS= read -r _t; do info "- $_t"; done <<< "$similar"
    fi
  fi
  exit 0
fi
ok "Найдено топиков: ${#MATCHED[@]}"
for t in "${MATCHED[@]}"; do info "- $t"; done
info "Целевые партиции: $PARTITIONS${REPL_FACTOR:+   RF: $REPL_FACTOR}"
[[ -n "$CONFIG_SUMMARY" ]] && info "Конфиги нового топика: ${CONFIG_SUMMARY% }" \
                          || info "Конфиги нового топика: (дефолты брокера)"
if [[ "$FORCE" -eq 1 ]]; then
  info "Непустые топики: ПЕРЕСОЗДАВАТЬ (--force — данные будут потеряны)"
else
  info "Непустые топики: пропускать (безопасный режим)"
fi
if [[ "$DRY_RUN" -eq 1 ]]; then
  info "Режим: DRY-RUN (по умолчанию) — ничего не меняем, только план"
else
  info "Режим: APPLY — топики будут удалены и пересозданы"
fi
[[ -n "$REPORT_FILE" ]] && info "Отчёт: $REPORT_FILE"

# ---------- 2. подтверждение ----------
if [[ "$DRY_RUN" -eq 0 && "$ASSUME_YES" -eq 0 ]]; then
  if [[ "$FORCE" -eq 1 ]]; then
    printf '\n%sВНИМАНИЕ: режим --force — НЕПУСТЫЕ топики будут удалены ВМЕСТЕ С ДАННЫМИ%s\n' "$C_BAD" "$C_RST"
    printf '%s(партиций: %s%s). Отмена невозможна.%s\n' "$C_BAD" "$PARTITIONS" "${REPL_FACTOR:+, RF: $REPL_FACTOR}" "$C_RST"
    read -r -p "Для подтверждения введите слово yes: " ans
    info "Ответ оператора: '${ans}'"
    [[ "$ans" == "yes" ]] || { echo "Отменено."; exit 0; }
  else
    printf '\n%sБудут удалены и пересозданы ПУСТЫЕ топики выше (партиций: %s%s).%s\n' \
      "$C_WARN" "$PARTITIONS" "${REPL_FACTOR:+, RF: $REPL_FACTOR}" "$C_RST"
    read -r -p "Продолжить? [y/N] " ans
    info "Ответ оператора: '${ans}'"
    [[ "$ans" =~ ^[Yy]$ ]] || { echo "Отменено."; exit 0; }
  fi
fi

# ---------- 3. обработка ----------
RECREATED=0; SKIPPED=0; FAILED=0

for topic in "${MATCHED[@]}"; do
  step "Топик: $topic"

  cur_parts="$(topic_partitions "$topic")"
  info "Текущее число партиций: ${cur_parts:-?}  ->  целевое: $PARTITIONS"

  cnt="$(message_count "$topic")"
  if [[ "$cnt" == "ERR" ]]; then
    bad "Не удалось посчитать сообщения (нет kafka-get-offsets.sh / kafka-run-class.sh) — пропуск."
    FAILED=$((FAILED+1)); continue
  fi

  if [[ "$cnt" -ne 0 ]]; then
    if [[ "$FORCE" -eq 1 ]]; then
      warn "Топик НЕ пуст: $cnt сообщ. — режим --force: данные будут УДАЛЕНЫ."
    else
      warn "Топик НЕ пуст: $cnt сообщ. — пропуск (для пересоздания используйте --force)."
      SKIPPED=$((SKIPPED+1)); continue
    fi
  else
    ok "Топик пуст (0 сообщений)."
  fi

  if [[ "$DRY_RUN" -eq 1 ]]; then
    info "[dry-run] удалил бы и пересоздал '$topic' с $PARTITIONS партициями${CONFIG_SUMMARY:+, конфиги: ${CONFIG_SUMMARY% }}."
    RECREATED=$((RECREATED+1)); continue
  fi

  # удаление
  old_id="$(topic_id "$topic")"
  [[ -n "$old_id" ]] && info "TopicId до удаления: $old_id"
  if ! "$KAFKA_TOPICS" "${CONN[@]}" --delete --topic "$topic" 2>&1 | sed 's/^/    /'; then
    bad "Ошибка удаления '$topic' — пропуск (см. сообщение брокера выше)."
    NOT_DELETED+=("$topic"); FAILED=$((FAILED+1)); continue
  fi
  if ! wait_topic_gone "$topic"; then
    warn "Ещё виден в метаданных через ${DELETE_WAIT}с — создание ОТЛОЖЕНО (повторим в конце)."
    PENDING+=("$topic"); PENDING_IDS+=("$old_id"); continue
  fi
  ok "Удалён."

  # создание
  if create_topic "$topic"; then RECREATED=$((RECREATED+1)); else FAILED=$((FAILED+1)); fi
done

# ---------- 4. отложенные топики (удаление ещё не доехало) ----------
# Топик уже удалён командой --delete, поэтому бросать его нельзя: без создания
# он просто исчезнет. Ждём второй круг и создаём всё, что успело удалиться.
if [[ "${#PENDING[@]}" -gt 0 ]]; then
  step "Отложенные топики: ${#PENDING[@]} (ждём завершения удаления)"
  info "Удаление в Kafka асинхронное — даём каждому топику ещё до ${DELETE_WAIT}с."
  for i in ${PENDING[@]+"${!PENDING[@]}"}; do
    topic="${PENDING[$i]}"; old_id="${PENDING_IDS[$i]}"
    if wait_topic_gone "$topic" || diagnose_stuck "$topic" "$old_id"; then
      ok "'$topic': удаление завершено — создаём."
      if create_topic "$topic"; then RECREATED=$((RECREATED+1)); else NOT_CREATED+=("$topic"); FAILED=$((FAILED+1)); fi
    else
      NOT_DELETED+=("$topic"); FAILED=$((FAILED+1))
    fi
  done
fi

# ---------- итог ----------
step "Итог"
info "Пересоздано: $RECREATED   Пропущено (непустые): $SKIPPED   Ошибок: $FAILED"
if [[ "${#NOT_DELETED[@]}" -gt 0 ]]; then
  warn "НЕ УДАЛЕНЫ (остались с прежним числом партиций, потери данных нет) — ${#NOT_DELETED[@]}:"
  for topic in ${NOT_DELETED[@]+"${NOT_DELETED[@]}"}; do info "- $topic"; done
  info "Разберитесь с причиной (права DeleteTopics / auto.create.topics.enable / реплики) и повторите."
fi
if [[ "${#NOT_CREATED[@]}" -gt 0 ]]; then
  bad "УДАЛЕНЫ, НО НЕ СОЗДАНЫ — ${#NOT_CREATED[@]} топик(ов), создайте вручную:"
  for topic in ${NOT_CREATED[@]+"${NOT_CREATED[@]}"}; do
    info "$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP --command-config $CFG --create --topic $topic --partitions $PARTITIONS${REPL_FACTOR:+ --replication-factor $REPL_FACTOR}"
  done
fi
if [[ "$DRY_RUN" -eq 1 ]]; then
  printf '%sРежим dry-run (по умолчанию) — изменения не вносились.%s\n' "$C_HDR" "$C_RST"
  printf '%sДля реального выполнения повторите ту же команду с флагом --apply.%s\n' "$C_HDR" "$C_RST"
fi

[[ "$FAILED" -eq 0 && "$SKIPPED" -eq 0 ]] && exit 0 || exit 1
