#!/bin/bash
#
# kafka-set-infinite-retention.sh
#
# Массово выставляет бесконечное время хранения (retention.ms=-1, retention.bytes=-1)
# для уже существующих топиков Kafka. Данные в топиках не трогаются.
#
# Возможности:
#   - dry-run (по умолчанию ничего не меняется, пока не указан --apply)
#   - бэкап текущих значений + автогенерация rollback-скрипта
#   - фильтры по include/exclude regexp
#   - пропуск внутренних (служебных) топиков
#   - опционально: только топики, в которых реально есть данные
#   - предупреждение про compacted-топики
#   - работа с защищённым кластером через --command-config
#
set -euo pipefail

# ------------------------------- параметры ----------------------------------
BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"
KAFKA_BIN="${KAFKA_BIN:-}"          # каталог bin дистрибутива Kafka, если не в PATH
COMMAND_CONFIG=""                   # файл свойств клиента (SASL/SSL)
APPLY=0                             # 0 = dry-run
ONLY_NONEMPTY=0                     # менять только топики, где есть данные
INCLUDE_INTERNAL=0                  # включать служебные топики (__consumer_offsets и т.п.)
SET_BYTES=1                         # выставлять также retention.bytes=-1
SET_LOCAL=0                         # local.retention.ms=-1 (tiered storage, KIP-405)
INCLUDE_REGEX='.*'
EXCLUDE_REGEX='^$'
BACKUP_DIR="./kafka-retention-backup-$(date +%Y%m%d-%H%M%S)"

usage() {
  cat <<'EOF'
Использование:
  kafka-set-infinite-retention.sh --bootstrap-server host:9092 [опции]

Опции:
  -b, --bootstrap-server HOST:PORT   Адрес брокера (или переменная BOOTSTRAP)
      --command-config FILE          Файл свойств клиента (SASL/SSL)
      --kafka-bin DIR                Каталог bin дистрибутива Kafka
      --apply                        Реально применить изменения (без него — dry-run)
      --include REGEX                Обрабатывать только топики, подходящие под regexp
      --exclude REGEX                Пропустить топики, подходящие под regexp
      --include-internal             Не пропускать служебные топики (_*, __*)
      --only-nonempty                Менять только топики, где есть данные
      --no-bytes                     Не трогать retention.bytes
      --local-retention              Также выставить local.retention.ms=-1 (tiered storage)
      --backup-dir DIR               Каталог для бэкапа и rollback-скрипта
  -h, --help, help                   Показать эту справку
                                     (запуск без аргументов делает то же самое)

Примеры:
  # посмотреть, что будет сделано
  ./kafka-set-infinite-retention.sh -b kafka-1:9092

  # применить ко всем неслужебным топикам
  ./kafka-set-infinite-retention.sh -b kafka-1:9092 --apply

  # только топики с префиксом data. и только непустые, кластер с SASL
  ./kafka-set-infinite-retention.sh -b kafka-1:9092 \
      --command-config /etc/kafka/client.properties \
      --include '^data\.' --only-nonempty --apply
EOF
}

# запуск без аргументов -> показать справку
if [[ $# -eq 0 ]]; then
  usage
  exit 0
fi

# проверка, что у опции есть значение
need_val() {
  if [[ $# -lt 2 || -z "${2:-}" || "${2:0:1}" == "-" ]]; then
    echo "ОШИБКА: опция $1 требует значение." >&2
    echo "Запустите с --help для справки." >&2
    exit 1
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -b|--bootstrap-server) need_val "$@"; BOOTSTRAP="$2"; shift 2 ;;
    --command-config)      need_val "$@"; COMMAND_CONFIG="$2"; shift 2 ;;
    --kafka-bin)           need_val "$@"; KAFKA_BIN="$2"; shift 2 ;;
    --apply)               APPLY=1; shift ;;
    --include)             need_val "$@"; INCLUDE_REGEX="$2"; shift 2 ;;
    --exclude)             need_val "$@"; EXCLUDE_REGEX="$2"; shift 2 ;;
    --include-internal)    INCLUDE_INTERNAL=1; shift ;;
    --only-nonempty)       ONLY_NONEMPTY=1; shift ;;
    --no-bytes)            SET_BYTES=0; shift ;;
    --local-retention)     SET_LOCAL=1; shift ;;
    --backup-dir)          need_val "$@"; BACKUP_DIR="$2"; shift 2 ;;
    -h|-\?|--help|help)    usage; exit 0 ;;
    *)
      echo "Неизвестный аргумент: $1" >&2
      echo "Запустите с --help для справки." >&2
      exit 1 ;;
  esac
done

# --------------------------- поиск утилит Kafka ------------------------------
find_cmd() {
  local base="$1"
  if [[ -n "$KAFKA_BIN" ]]; then
    [[ -x "$KAFKA_BIN/$base.sh" ]] && { echo "$KAFKA_BIN/$base.sh"; return; }
    [[ -x "$KAFKA_BIN/$base"    ]] && { echo "$KAFKA_BIN/$base";    return; }
  fi
  command -v "$base.sh" 2>/dev/null && return
  command -v "$base"    2>/dev/null && return
  echo ""
}

KTOPICS="$(find_cmd kafka-topics)"
KCONFIGS="$(find_cmd kafka-configs)"
KOFFSETS="$(find_cmd kafka-get-offsets)"
KRUNCLASS="$(find_cmd kafka-run-class)"

if [[ -z "$KTOPICS" || -z "$KCONFIGS" ]]; then
  echo "ОШИБКА: не найдены kafka-topics/kafka-configs. Укажите --kafka-bin <dir>." >&2
  exit 1
fi

CC_ARGS=()
[[ -n "$COMMAND_CONFIG" ]] && CC_ARGS=(--command-config "$COMMAND_CONFIG")

# ------------------------------ подготовка -----------------------------------
mkdir -p "$BACKUP_DIR"
ROLLBACK="$BACKUP_DIR/rollback.sh"
{
  echo '#!/bin/bash'
  echo '# Автосгенерированный откат изменений retention.'
  echo 'set -euo pipefail'
} > "$ROLLBACK"
chmod +x "$ROLLBACK"

NEW_CONFIG="retention.ms=-1"
[[ $SET_BYTES -eq 1 ]] && NEW_CONFIG="$NEW_CONFIG,retention.bytes=-1"
[[ $SET_LOCAL -eq 1 ]] && NEW_CONFIG="$NEW_CONFIG,local.retention.ms=-1"

echo "Брокер:        $BOOTSTRAP"
echo "Новый конфиг:  $NEW_CONFIG"
echo "Режим:         $([[ $APPLY -eq 1 ]] && echo 'ПРИМЕНЕНИЕ' || echo 'DRY-RUN (изменений не будет)')"
echo "Бэкап:         $BACKUP_DIR"
echo

# ------------------------------ список топиков -------------------------------
mapfile -t ALL_TOPICS < <("$KTOPICS" --bootstrap-server "$BOOTSTRAP" "${CC_ARGS[@]}" --list | sed '/^$/d' | sort)

if [[ ${#ALL_TOPICS[@]} -eq 0 ]]; then
  echo "Топиков не найдено — проверьте адрес брокера и права доступа." >&2
  exit 1
fi

topic_has_data() {
  local t="$1" earliest latest
  if [[ -n "$KOFFSETS" ]]; then
    earliest=$("$KOFFSETS" --bootstrap-server "$BOOTSTRAP" "${CC_ARGS[@]}" \
                 --topic "$t" --time earliest 2>/dev/null | awk -F: '{s+=$3} END{print s+0}')
    latest=$(  "$KOFFSETS" --bootstrap-server "$BOOTSTRAP" "${CC_ARGS[@]}" \
                 --topic "$t" --time latest   2>/dev/null | awk -F: '{s+=$3} END{print s+0}')
  elif [[ -n "$KRUNCLASS" ]]; then
    earliest=$("$KRUNCLASS" kafka.tools.GetOffsetShell --bootstrap-server "$BOOTSTRAP" \
                 --topic "$t" --time -2 2>/dev/null | awk -F: '{s+=$3} END{print s+0}')
    latest=$(  "$KRUNCLASS" kafka.tools.GetOffsetShell --bootstrap-server "$BOOTSTRAP" \
                 --topic "$t" --time -1 2>/dev/null | awk -F: '{s+=$3} END{print s+0}')
  else
    return 0   # не смогли проверить — считаем, что данные есть
  fi
  [[ "${latest:-0}" -gt "${earliest:-0}" ]]
}

# ------------------------------ основной цикл --------------------------------
CHANGED=0; SKIPPED=0; FAILED=0
declare -a TOUCHED=()

for T in "${ALL_TOPICS[@]}"; do
  if [[ $INCLUDE_INTERNAL -eq 0 && "$T" == _* ]]; then
    echo "[skip] $T — служебный топик"; SKIPPED=$((SKIPPED + 1)); continue
  fi
  if ! [[ "$T" =~ $INCLUDE_REGEX ]]; then
    echo "[skip] $T — не подходит под --include"; SKIPPED=$((SKIPPED + 1)); continue
  fi
  if [[ "$T" =~ $EXCLUDE_REGEX ]]; then
    echo "[skip] $T — подходит под --exclude"; SKIPPED=$((SKIPPED + 1)); continue
  fi
  if [[ $ONLY_NONEMPTY -eq 1 ]] && ! topic_has_data "$T"; then
    echo "[skip] $T — нет данных"; SKIPPED=$((SKIPPED + 1)); continue
  fi

  # текущая конфигурация -> бэкап
  CUR="$("$KCONFIGS" --bootstrap-server "$BOOTSTRAP" "${CC_ARGS[@]}" \
          --entity-type topics --entity-name "$T" --describe 2>/dev/null || true)"
  printf '%s\n' "$CUR" > "$BACKUP_DIR/${T//\//_}.conf"

  # предупреждение про compaction: retention.ms там не управляет удалением
  POLICY="$(grep -oE '(^|[[:space:]])cleanup\.policy=[a-z,]+' <<<"$CUR" | head -1 | cut -d= -f2 || true)"
  if [[ "$POLICY" == "compact" ]]; then
    echo "[warn] $T — cleanup.policy=compact, retention.ms не влияет на удаление (compaction продолжится)"
  fi

  # строки отката
  {
    echo "# --- $T ---"
    for KEY in retention.ms retention.bytes local.retention.ms; do
      [[ "$NEW_CONFIG" == *"$KEY="* ]] || continue
      OLD="$(grep -oE "(^|[[:space:]])${KEY//./\\.}=[-0-9]+" <<<"$CUR" | head -1 | cut -d= -f2 || true)"
      if [[ -n "$OLD" ]]; then
        echo "$KCONFIGS --bootstrap-server '$BOOTSTRAP' ${CC_ARGS[*]} --entity-type topics --entity-name '$T' --alter --add-config '$KEY=$OLD'"
      else
        echo "$KCONFIGS --bootstrap-server '$BOOTSTRAP' ${CC_ARGS[*]} --entity-type topics --entity-name '$T' --alter --delete-config '$KEY'"
      fi
    done
  } >> "$ROLLBACK"

  if [[ $APPLY -eq 0 ]]; then
    echo "[dry-run] $T <- $NEW_CONFIG"
    CHANGED=$((CHANGED + 1)); TOUCHED+=("$T"); continue
  fi

  if "$KCONFIGS" --bootstrap-server "$BOOTSTRAP" "${CC_ARGS[@]}" \
       --entity-type topics --entity-name "$T" --alter --add-config "$NEW_CONFIG" >/dev/null 2>&1; then
    echo "[ok]   $T <- $NEW_CONFIG"
    CHANGED=$((CHANGED + 1)); TOUCHED+=("$T")
  else
    echo "[FAIL] $T — не удалось изменить конфигурацию" >&2
    FAILED=$((FAILED + 1))
  fi
done

# ------------------------------- проверка ------------------------------------
if [[ $APPLY -eq 1 && ${#TOUCHED[@]} -gt 0 ]]; then
  echo
  echo "Проверка применённых значений:"
  for T in "${TOUCHED[@]}"; do
    OUT="$("$KCONFIGS" --bootstrap-server "$BOOTSTRAP" "${CC_ARGS[@]}" \
            --entity-type topics --entity-name "$T" --describe 2>/dev/null || true)"
    RMS="$(grep -oE '(^|[[:space:]])retention\.ms=[-0-9]+' <<<"$OUT" | head -1 | cut -d= -f2 || true)"
    printf '  %-50s retention.ms=%s\n' "$T" "${RMS:-<не задан>}"
  done
fi

echo
PROCESSED=$((CHANGED + SKIPPED + FAILED))
echo "Итого: изменено=$CHANGED, пропущено=$SKIPPED, ошибок=$FAILED"
echo "Обработано $PROCESSED из ${#ALL_TOPICS[@]} топиков в кластере"
if [[ $PROCESSED -ne ${#ALL_TOPICS[@]} ]]; then
  echo "ВНИМАНИЕ: обработаны не все топики — цикл завершился преждевременно!" >&2
fi
echo "Бэкап конфигураций: $BACKUP_DIR"
echo "Скрипт отката:      $ROLLBACK"
[[ $APPLY -eq 0 ]] && echo "Это был DRY-RUN. Для применения добавьте --apply."
exit 0
