#!/bin/bash
#
# systemd-units-watcher.sh — проверка и автоподъём systemd-юнитов Kafka-стека.
#
# Что делает:
#   1. Обходит список юнитов в порядке зависимостей (по умолчанию:
#      zookeeper.service -> kafka.service -> schema-registry.service).
#   2. Для каждого юнита смотрит `systemctl is-active`.
#      - active            — ничего не делает;
#      - activating        — ждёт, пока юнит доедет до active (см. --timeout);
#      - inactive / failed — выполняет `systemctl start` и ждёт перехода в active;
#      - masked / отсутствует — не трогает, пишет предупреждение (это ручное решение админа).
#   3. Перед стартом следующего юнита выдерживает паузу --settle секунд,
#      чтобы предыдущий успел открыть свой порт (Kafka не поднимется без ZooKeeper).
#   4. Если юнит был в failed — перед стартом делает `systemctl reset-failed`,
#      иначе start может упереться в start-limit (StartLimitBurst).
#
# ВАЖНО: по умолчанию скрипт работает в режиме DRY-RUN — только показывает, что
# сделал бы, и ничего не запускает. Для реальных действий нужен флаг --apply.
# В systemd-юните (systemd-units-watcher.service) --apply уже прописан.
#
# Весь вывод идёт в stdout/stderr, поэтому при запуске через systemd он
# автоматически попадает в journald: journalctl -u systemd-units-watcher.service
# Дополнительно можно писать файл лога: --log /var/log/systemd-units-watcher.log
#
# Требуются права root (systemctl start). Если скрипт запущен не от root, но в
# системе есть sudo — команды будут выполняться через sudo -n (без пароля).
#
# Использование:
#   ./systemd-units-watcher.sh                                  # dry-run: только проверка
#   ./systemd-units-watcher.sh --apply                          # проверить и поднять упавшее
#   ./systemd-units-watcher.sh --apply -u zookeeper.service -u kafka.service
#   ./systemd-units-watcher.sh --apply --settle 20 --timeout 180
#   WATCH_UNITS="zookeeper.service kafka.service" ./systemd-units-watcher.sh --apply
#
# Параметры:
#   -u, --unit NAME       юнит для проверки (можно повторять; задаёт порядок запуска).
#                         Суффикс .service можно не писать. Перебивает список по умолчанию.
#   -a, --apply           выполнять действия (без него — dry-run)
#   -d, --dry-run         ничего не менять (режим по умолчанию, флаг для явности)
#   -t, --timeout SEC     сколько ждать перехода юнита в active после старта (по умолчанию 120)
#   -s, --settle SEC      пауза после успешного старта юнита перед следующим (по умолчанию 15)
#   -r, --retries N       сколько раз повторить `systemctl start` при неудаче (по умолчанию 2)
#       --no-reset-failed не делать `systemctl reset-failed` перед стартом failed-юнита
#   -l, --log FILE        дублировать вывод в файл
#       --no-color        без цветного вывода
#   -h, --help            эта справка
#
# Коды возврата:
#   0 — все юниты в active (или были успешно подняты)
#   1 — хотя бы один юнит не удалось поднять
#   2 — ошибка в параметрах запуска / нет systemctl
#
set -uo pipefail

# ------------------------------- параметры ----------------------------------
DEFAULT_UNITS=(zookeeper.service kafka.service schema-registry.service)
UNITS=()
APPLY=0
TIMEOUT="${TIMEOUT:-120}"          # ожидание active после старта, сек
SETTLE="${SETTLE:-15}"             # пауза между стартами юнитов, сек
RETRIES="${RETRIES:-2}"            # доп. попытки systemctl start
RESET_FAILED=1
LOG_FILE="${LOG_FILE:-}"
USE_COLOR=1

usage() { sed -n '2,/^set -uo/p' "$0" | sed 's/^# \{0,1\}//; $d'; }

# ------------------------------ разбор аргументов ---------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    -u|--unit)         UNITS+=("$2"); shift 2 ;;
    -a|--apply)        APPLY=1; shift ;;
    -d|--dry-run)      APPLY=0; shift ;;
    -t|--timeout)      TIMEOUT="$2"; shift 2 ;;
    -s|--settle)       SETTLE="$2"; shift 2 ;;
    -r|--retries)      RETRIES="$2"; shift 2 ;;
    --no-reset-failed) RESET_FAILED=0; shift ;;
    -l|--log)          LOG_FILE="$2"; shift 2 ;;
    --no-color)        USE_COLOR=0; shift ;;
    -h|--help)         usage; exit 0 ;;
    *) echo "Неизвестный параметр: $1 (см. --help)" >&2; exit 2 ;;
  esac
done

# Список юнитов: -u ... > env UNITS > значение по умолчанию
if [[ ${#UNITS[@]} -eq 0 ]]; then
  if [[ -n "${WATCH_UNITS:-}" ]]; then
    read -r -a UNITS <<<"$WATCH_UNITS"
  else
    UNITS=("${DEFAULT_UNITS[@]}")
  fi
fi
# Дописываем .service там, где суффикс не указан
for i in "${!UNITS[@]}"; do
  [[ "${UNITS[$i]}" == *.* ]] || UNITS[$i]="${UNITS[$i]}.service"
done

for v in TIMEOUT SETTLE RETRIES; do
  [[ "${!v}" =~ ^[0-9]+$ ]] || { echo "$v должен быть целым числом, получено: ${!v}" >&2; exit 2; }
done

# --------------------------------- вывод ------------------------------------
if [[ $USE_COLOR -eq 1 && -t 1 ]]; then
  C_RED=$'\033[31m'; C_GRN=$'\033[32m'; C_YEL=$'\033[33m'; C_DIM=$'\033[2m'; C_0=$'\033[0m'
else
  C_RED=''; C_GRN=''; C_YEL=''; C_DIM=''; C_0=''
fi

log() {  # log <цвет> <уровень> <текст...>
  local color="$1" level="$2"; shift 2
  local line; line="$(date '+%Y-%m-%d %H:%M:%S') [$level] $*"
  printf '%s%s%s\n' "$color" "$line" "$C_0"
  [[ -n "$LOG_FILE" ]] && printf '%s\n' "$line" >>"$LOG_FILE"
  return 0
}
info() { log ""       "INFO" "$@"; }
ok()   { log "$C_GRN" "OK"   "$@"; }
warn() { log "$C_YEL" "WARN" "$@"; }
err()  { log "$C_RED" "ERROR" "$@" ; }
dim()  { log "$C_DIM" "INFO" "$@"; }

if [[ -n "$LOG_FILE" ]]; then
  mkdir -p "$(dirname "$LOG_FILE")" 2>/dev/null
  touch "$LOG_FILE" 2>/dev/null || { echo "Не могу писать в лог-файл: $LOG_FILE" >&2; exit 2; }
fi

# ------------------------------ окружение -----------------------------------
command -v systemctl >/dev/null 2>&1 || { err "systemctl не найден — это не systemd-система"; exit 2; }

# Привилегии: root напрямую, иначе пробуем беспарольный sudo
SUDO=()
if [[ "$(id -u)" -ne 0 ]]; then
  if command -v sudo >/dev/null 2>&1 && sudo -n true 2>/dev/null; then
    SUDO=(sudo -n)
    dim "Запуск не от root — управляющие команды пойдут через sudo -n"
  else
    warn "Запуск не от root и беспарольный sudo недоступен: 'systemctl start' скорее всего не сработает"
  fi
fi

# ${SUDO[@]+...} — чтобы пустой массив не ронял скрипт под set -u в bash 3.x
sctl()  { ${SUDO[@]+"${SUDO[@]}"} systemctl "$@"; }             # изменяющие команды
sctlq() { systemctl "$@" 2>/dev/null; }                        # опрос состояния, без sudo

unit_exists() { sctlq cat -- "$1" >/dev/null; }

# Ждём, пока юнит станет active; 0 — дождались, 1 — нет
wait_active() {
  local unit="$1" deadline=$(( SECONDS + TIMEOUT )) state
  while :; do
    state="$(sctlq is-active -- "$unit")"
    [[ "$state" == "active" ]] && return 0
    # failed/inactive после старта — дальше ждать нечего
    [[ "$state" == "failed" ]] && return 1
    (( SECONDS >= deadline )) && return 1
    sleep 2
  done
}

# Короткая выжимка из журнала — чтобы причина падения была видна в отчёте
show_failure() {
  local unit="$1" l
  sctlq status --no-pager --lines=0 -- "$unit" | sed -n '1,6p' | while IFS= read -r l; do dim "    $l"; done
  if command -v journalctl >/dev/null 2>&1; then
    dim "    --- последние строки журнала $unit ---"
    journalctl --no-pager -n 15 -o cat -u "$unit" 2>/dev/null \
      | while IFS= read -r l; do dim "    | $l"; done
  fi
}

# ------------------------------- основной цикл ------------------------------
[[ $APPLY -eq 1 ]] || warn "Режим DRY-RUN: ничего не запускается. Для реальных действий добавьте --apply"
info "Проверяю юниты: ${UNITS[*]}"

FAILED=()      # не удалось поднять
STARTED=()     # подняты этим запуском
SKIPPED=()     # masked / отсутствуют

for unit in "${UNITS[@]}"; do
  if ! unit_exists "$unit"; then
    warn "$unit — юнит не найден в системе, пропускаю"
    SKIPPED+=("$unit"); continue
  fi

  load_state="$(sctlq show -p LoadState --value -- "$unit")"
  if [[ "$load_state" == "masked" ]]; then
    warn "$unit — юнит masked, намеренно не трогаю (снять: systemctl unmask $unit)"
    SKIPPED+=("$unit"); continue
  fi

  state="$(sctlq is-active -- "$unit")"

  case "$state" in
    active)
      ok "$unit — active, действий не требуется"
      continue
      ;;
    activating|reloading)
      info "$unit — состояние '$state', жду до ${TIMEOUT}s перехода в active"
      if [[ $APPLY -eq 0 ]]; then
        dim "  [dry-run] дождался бы завершения запуска"
        continue
      fi
      if wait_active "$unit"; then
        ok "$unit — дошёл до active"
      else
        err "$unit — не перешёл в active за ${TIMEOUT}s (сейчас: $(sctlq is-active -- "$unit"))"
        show_failure "$unit"
        FAILED+=("$unit")
      fi
      continue
      ;;
    *)
      warn "$unit — состояние '$state', требуется запуск"
      ;;
  esac

  if [[ $APPLY -eq 0 ]]; then
    dim "  [dry-run] выполнил бы: systemctl start $unit (и ждал бы active до ${TIMEOUT}s)"
    continue
  fi

  # failed-юнит без reset-failed может не стартовать из-за StartLimitBurst
  if [[ "$state" == "failed" && $RESET_FAILED -eq 1 ]]; then
    info "  $unit — сбрасываю состояние failed (systemctl reset-failed)"
    sctl reset-failed -- "$unit" >/dev/null 2>&1
  fi

  started=0
  for (( attempt = 1; attempt <= RETRIES + 1; attempt++ )); do
    info "  $unit — запуск, попытка $attempt из $(( RETRIES + 1 ))"
    sctl start -- "$unit" >/dev/null 2>&1
    if wait_active "$unit"; then
      ok "$unit — запущен и активен"
      started=1
      break
    fi
    warn "  $unit — попытка $attempt неудачна (состояние: $(sctlq is-active -- "$unit"))"
    [[ "$(sctlq is-active -- "$unit")" == "failed" ]] && [[ $RESET_FAILED -eq 1 ]] \
      && sctl reset-failed -- "$unit" >/dev/null 2>&1
    (( attempt <= RETRIES )) && sleep 5
  done

  if [[ $started -eq 1 ]]; then
    STARTED+=("$unit")
    # пауза, чтобы следующий в цепочке юнит увидел уже готовый сервис
    if [[ "$unit" != "${UNITS[$(( ${#UNITS[@]} - 1 ))]}" && $SETTLE -gt 0 ]]; then
      dim "  Пауза ${SETTLE}s перед следующим юнитом"
      sleep "$SETTLE"
    fi
  else
    err "$unit — запустить не удалось"
    show_failure "$unit"
    FAILED+=("$unit")
  fi
done

# ---------------------------------- итог ------------------------------------
echo
info "Итог: запущено ${#STARTED[@]}, не удалось ${#FAILED[@]}, пропущено ${#SKIPPED[@]}"
[[ ${#STARTED[@]} -gt 0 ]] && ok   "Подняты: ${STARTED[*]}"
[[ ${#SKIPPED[@]} -gt 0 ]] && warn "Пропущены (masked/отсутствуют): ${SKIPPED[*]}"
if [[ ${#FAILED[@]} -gt 0 ]]; then
  err "Не удалось поднять: ${FAILED[*]}"
  exit 1
fi
exit 0
