#!/usr/bin/env bash
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
# Уменьшить число партиций штатным alter нельзя — только delete + create,
# поэтому топик обязан быть пустым.
#
# Подключение к брокеру: SASL_SSL / SCRAM-SHA-256 (готовый -c client.properties
# либо генерация временного конфига из KAFKA_USER/KAFKA_PASSWORD — см. ниже).
#
# Использование:
#   KAFKA_USER=svc KAFKA_PASSWORD=secret ./repartition_topics.sh -p 'drub.*'   # -> 1 партиция
#   ./repartition_topics.sh -p 'drub.*' -c client.properties --partitions 1
#   ./repartition_topics.sh -p 'drub.*' -d               # dry-run: только показать план
#   ./repartition_topics.sh -p 'drub.*' -r 3 -y          # RF=3, без подтверждения
#   ./repartition_topics.sh -p 'drub.*' -C retention.ms=604800000 -C cleanup.policy=compact # если требуется задать недефлтные параметры топиков
#   ./repartition_topics.sh -p 'drub.*' --force            # пересоздать даже непустые топки (потеря данных)
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
#   -d, --dry-run               ничего не менять, только показать, что будет сделано
#   -y, --yes                   не спрашивать подтверждение
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
DRY_RUN=0
ASSUME_YES=0
FORCE="${FORCE:-0}"     # 1 — пересоздавать даже непустые топики (потеря данных)
USE_COLOR=1
DELETE_WAIT=60          # сколько секунд ждать фактического удаления топика
RAW_CONFIGS=()         # значения флагов -C/--config (обрабатываются после определения хелперов)

# ---------- разбор аргументов ----------
while [[ $# -gt 0 ]]; do
  case "$1" in
    -p|--pattern)             PATTERN="$2"; shift 2 ;;
    -N|--partitions)          PARTITIONS="$2"; shift 2 ;;
    -r|--replication-factor)  REPL_FACTOR="$2"; shift 2 ;;
    -C|--config)              RAW_CONFIGS+=("$2"); shift 2 ;;
    -b|--bootstrap-server)    BOOTSTRAP="$2"; shift 2 ;;
    -c|--command-config)      CFG="$2"; shift 2 ;;
    -f|--force)               FORCE=1; shift ;;
    -d|--dry-run)             DRY_RUN=1; shift ;;
    -y|--yes)                 ASSUME_YES=1; shift ;;
    --no-color)               USE_COLOR=0; shift ;;
    -h|--help)                grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
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
  local name="$1"
  if [[ -n "${KAFKA_HOME:-}" && -x "$KAFKA_HOME/bin/$name" ]]; then
    printf '%s' "$KAFKA_HOME/bin/$name"
  elif command -v "$name" >/dev/null 2>&1; then
    command -v "$name"
  fi
}
KAFKA_TOPICS="$(find_tool kafka-topics.sh)"
KAFKA_OFFSETS="$(find_tool kafka-get-offsets.sh)"     # есть в Kafka >= 3.0
KAFKA_RUN_CLASS="$(find_tool kafka-run-class.sh)"      # запасной путь для подсчёта offset'ов
[[ -z "$KAFKA_TOPICS" ]] && { bad "kafka-topics.sh не найден (задайте KAFKA_HOME или PATH)"; exit 2; }

# ---------- аутентификация: SASL_SSL / SCRAM-SHA-256 ----------
# Если готовый command-config не передан, но заданы KAFKA_USER/KAFKA_PASSWORD —
# генерируем временный client.properties (права 600, удаляется на выходе).
TMP_CFG=""
cleanup() { [[ -n "$TMP_CFG" ]] && rm -f "$TMP_CFG"; }
trap cleanup EXIT

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
  "$KAFKA_TOPICS" "${CONN[@]}" --list 2>/dev/null
}

# topic_partitions <topic> -> текущее число партиций (или пусто)
topic_partitions() {
  "$KAFKA_TOPICS" "${CONN[@]}" --describe --topic "$1" 2>/dev/null \
    | awk -F'PartitionCount:[[:space:]]*' 'NF>1{split($2,a,"[[:space:]]");print a[1];exit}'
}

# offsets_sum <topic> <time(-1|-2)> -> сумма offset'ов по всем партициям
offsets_sum() {
  local topic="$1" time="$2"
  if [[ -n "$KAFKA_OFFSETS" ]]; then
    "$KAFKA_OFFSETS" "${CONN[@]}" --topic "$topic" --time "$time" 2>/dev/null \
      | awk -F: '{s+=$3} END{print s+0}'
  elif [[ -n "$KAFKA_RUN_CLASS" ]]; then
    local args=(--bootstrap-server "$BOOTSTRAP")
    [[ -n "$CFG" ]] && args+=(--command-config "$CFG")
    "$KAFKA_RUN_CLASS" kafka.tools.GetOffsetShell "${args[@]}" \
      --topic "$topic" --time "$time" 2>/dev/null \
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

# wait_topic_gone <topic> -> ждёт исчезновения топика из --list
wait_topic_gone() {
  local topic="$1" i
  for (( i=0; i<DELETE_WAIT; i++ )); do
    if ! list_all_topics | grep -qx -- "$topic"; then return 0; fi
    sleep 1
  done
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

# ---------- 2. подтверждение ----------
if [[ "$DRY_RUN" -eq 0 && "$ASSUME_YES" -eq 0 ]]; then
  if [[ "$FORCE" -eq 1 ]]; then
    printf '\n%sВНИМАНИЕ: режим --force — НЕПУСТЫЕ топики будут удалены ВМЕСТЕ С ДАННЫМИ%s\n' "$C_BAD" "$C_RST"
    printf '%s(партиций: %s%s). Отмена невозможна.%s\n' "$C_BAD" "$PARTITIONS" "${REPL_FACTOR:+, RF: $REPL_FACTOR}" "$C_RST"
    read -r -p "Для подтверждения введите слово yes: " ans
    [[ "$ans" == "yes" ]] || { echo "Отменено."; exit 0; }
  else
    printf '\n%sБудут удалены и пересозданы ПУСТЫЕ топики выше (партиций: %s%s).%s\n' \
      "$C_WARN" "$PARTITIONS" "${REPL_FACTOR:+, RF: $REPL_FACTOR}" "$C_RST"
    read -r -p "Продолжить? [y/N] " ans
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
  if ! "$KAFKA_TOPICS" "${CONN[@]}" --delete --topic "$topic" 2>&1 | sed 's/^/    /'; then
    bad "Ошибка удаления '$topic' — пропуск."; FAILED=$((FAILED+1)); continue
  fi
  if ! wait_topic_gone "$topic"; then
    bad "Топик '$topic' не исчез за ${DELETE_WAIT}с — пропуск создания (проверьте вручную)."
    FAILED=$((FAILED+1)); continue
  fi
  ok "Удалён."

  # создание
  create_args=("${CONN[@]}" --create --topic "$topic" --partitions "$PARTITIONS")
  [[ -n "$REPL_FACTOR" ]] && create_args+=(--replication-factor "$REPL_FACTOR")
  for i in ${CONFIG_KEYS[@]+"${!CONFIG_KEYS[@]}"}; do
    create_args+=(--config "${CONFIG_KEYS[$i]}=${CONFIG_VALS[$i]}")
  done
  if "$KAFKA_TOPICS" "${create_args[@]}" 2>&1 | sed 's/^/    /'; then
    new_parts="$(topic_partitions "$topic")"
    if [[ "$new_parts" == "$PARTITIONS" ]]; then
      ok "Создан заново: партиций = $new_parts."
      RECREATED=$((RECREATED+1))
    else
      bad "Создан, но партиций=$new_parts (ожидалось $PARTITIONS)."; FAILED=$((FAILED+1))
    fi
  else
    bad "Ошибка создания '$topic'."; FAILED=$((FAILED+1))
  fi
done

# ---------- итог ----------
step "Итог"
info "Пересоздано: $RECREATED   Пропущено (непустые): $SKIPPED   Ошибок: $FAILED"
if [[ "$DRY_RUN" -eq 1 ]]; then
  printf '%sРежим dry-run — изменения не вносились.%s\n' "$C_HDR" "$C_RST"
fi

[[ "$FAILED" -eq 0 && "$SKIPPED" -eq 0 ]] && exit 0 || exit 1
