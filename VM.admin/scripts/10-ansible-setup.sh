#!/usr/bin/env bash
#
# Установка Ansible в изолированный venv пользователя.
# Запускается ВНУТРИ Linux: WSL-дистрибутив (вариант A) или jump host (вариант B).
# Права root нужны только для доустановки python3-venv, если его нет.
#
#   ./10-ansible-setup.sh                          # онлайн, версия подбирается автоматически
#   ./10-ansible-setup.sh --core-version 2.16      # явная ветка ansible-core
#   ./10-ansible-setup.sh --offline-dir ./wheels   # закрытый контур, из локальных wheel
#   ./10-ansible-setup.sh --venv ~/.venvs/ansible --project ~/ansible
#
set -euo pipefail

VENV="${HOME}/.venvs/ansible"
PROJECT="${HOME}/ansible"
CORE_VERSION=""
OFFLINE_DIR=""
SKIP_COLLECTIONS=0
SKIP_SSH_KEY=0

log()  { printf '\033[36m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[33m[!]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[31m[x]\033[0m %s\n' "$*" >&2; exit 1; }

while [[ $# -gt 0 ]]; do
    case "$1" in
        --venv)          VENV="$2"; shift 2 ;;
        --project)       PROJECT="$2"; shift 2 ;;
        --core-version)  CORE_VERSION="$2"; shift 2 ;;
        --offline-dir)   OFFLINE_DIR="$(cd "$2" && pwd)"; shift 2 ;;
        --no-collections) SKIP_COLLECTIONS=1; shift ;;
        --no-ssh-key)    SKIP_SSH_KEY=1; shift ;;
        -h|--help)       sed -n '2,11p' "$0"; exit 0 ;;
        *)               die "неизвестный аргумент: $1" ;;
    esac
done

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# ------------------------------------------------------------------ окружение
log "Окружение"
. /etc/os-release 2>/dev/null || true
echo "    дистрибутив : ${PRETTY_NAME:-неизвестно}"
if grep -qi microsoft /proc/version 2>/dev/null; then
    echo "    среда       : WSL ($(wslpath -w / 2>/dev/null || echo 'версия не определена'))"
fi

command -v python3 >/dev/null || die "python3 не найден"
PY_FULL="$(python3 -c 'import sys; print("%d.%d.%d" % sys.version_info[:3])')"
PY_MINOR="$(python3 -c 'import sys; print(sys.version_info[1])')"
PY_MAJOR="$(python3 -c 'import sys; print(sys.version_info[0])')"
echo "    python3     : ${PY_FULL}"
[[ "$PY_MAJOR" -eq 3 ]] || die "нужен Python 3"

# Подбор ветки ansible-core по Python на control node.
# Ограничение сверху задаёт Python на ЦЕЛЕВЫХ хостах, см. README (таблица совместимости).
if [[ -z "$CORE_VERSION" ]]; then
    if   [[ "$PY_MINOR" -ge 10 ]]; then CORE_VERSION="2.17"
    elif [[ "$PY_MINOR" -eq 9  ]]; then CORE_VERSION="2.16"
    elif [[ "$PY_MINOR" -eq 8  ]]; then CORE_VERSION="2.13"
    else die "Python ${PY_FULL} слишком старый для современного ansible-core"
    fi
fi
log "Ставим ansible-core ~= ${CORE_VERSION}.0"

# ------------------------------------------------------------------ venv
if ! python3 -c 'import venv' 2>/dev/null; then
    warn "модуль venv отсутствует, ставлю python3-venv (нужен sudo)"
    if command -v apt-get >/dev/null; then
        sudo apt-get update && sudo apt-get install -y python3-venv python3-pip
    else
        die "поставьте пакет python3-venv вручную и повторите"
    fi
fi

if [[ ! -x "${VENV}/bin/python" ]]; then
    log "Создаю venv: ${VENV}"
    python3 -m venv "$VENV"
else
    log "venv уже существует: ${VENV}"
fi

PIP_ARGS=(--disable-pip-version-check)
if [[ -n "$OFFLINE_DIR" ]]; then
    log "Офлайн-режим, источник пакетов: ${OFFLINE_DIR}"
    PIP_ARGS+=(--no-index --find-links "$OFFLINE_DIR")
fi

"${VENV}/bin/python" -m pip install "${PIP_ARGS[@]}" --upgrade pip wheel
"${VENV}/bin/python" -m pip install "${PIP_ARGS[@]}" \
    "ansible-core~=${CORE_VERSION}.0" \
    passlib \
    jmespath

log "Установлено:"
"${VENV}/bin/ansible" --version | sed 's/^/    /'

# ------------------------------------------------------------------ коллекции
if [[ "$SKIP_COLLECTIONS" -eq 0 ]]; then
    log "Коллекции Ansible Galaxy"
    if [[ -n "$OFFLINE_DIR" ]] && compgen -G "${OFFLINE_DIR}/*.tar.gz" >/dev/null; then
        for tarball in "${OFFLINE_DIR}"/*-*.tar.gz; do
            "${VENV}/bin/ansible-galaxy" collection install "$tarball" || warn "не встала: $tarball"
        done
    elif [[ -n "$OFFLINE_DIR" ]]; then
        warn "в ${OFFLINE_DIR} нет архивов коллекций, пропускаю"
    else
        "${VENV}/bin/ansible-galaxy" collection install \
            ansible.posix community.general ansible.utils || warn "коллекции не установились, проверьте прокси"
    fi
fi

# ------------------------------------------------------------------ проект
log "Каталог проекта: ${PROJECT}"
mkdir -p "${PROJECT}"/{inventory,group_vars,host_vars,playbooks,roles,logs,.facts}

for f in ansible.cfg inventory/astra.ini group_vars/all.yml \
         playbooks/smoke.yml playbooks/astra-inventory.yml playbooks/example-baseline.yml; do
    src="${REPO_DIR}/ansible/${f}"
    dst="${PROJECT}/${f}"
    if [[ -f "$src" && ! -f "$dst" ]]; then
        mkdir -p "$(dirname "$dst")"
        cp "$src" "$dst"
        echo "    + ${f}"
    elif [[ -f "$dst" ]]; then
        echo "    = ${f} (уже есть, не трогаю)"
    fi
done

# ------------------------------------------------------------------ SSH-ключ
if [[ "$SKIP_SSH_KEY" -eq 0 ]]; then
    KEY="${HOME}/.ssh/id_ed25519"
    if [[ ! -f "$KEY" ]]; then
        log "Генерирую SSH-ключ ${KEY}"
        mkdir -p "${HOME}/.ssh" && chmod 700 "${HOME}/.ssh"
        ssh-keygen -t ed25519 -a 100 -f "$KEY" -C "ansible-$(whoami)@$(hostname)" -N ''
    else
        log "SSH-ключ уже есть: ${KEY}"
    fi
    chmod 600 "$KEY"; chmod 644 "${KEY}.pub"
    echo "    публичная часть (раздать на целевые хосты):"
    sed 's/^/    /' "${KEY}.pub"
fi

# ------------------------------------------------------------------ PATH
RC="${HOME}/.bashrc"
MARK='# >>> ansible venv >>>'
if ! grep -qF "$MARK" "$RC" 2>/dev/null; then
    log "Прописываю venv в ${RC}"
    cat >> "$RC" <<RCEOF

${MARK}
export PATH="${VENV}/bin:\$PATH"
export ANSIBLE_CONFIG="${PROJECT}/ansible.cfg"
# <<< ansible venv <<<
RCEOF
fi

cat <<DONE

Готово.

    venv     : ${VENV}
    проект   : ${PROJECT}
    конфиг   : ${PROJECT}/ansible.cfg

Дальше:
    exec bash -l                                   # подхватить PATH
    vi ${PROJECT}/inventory/astra.ini              # вписать реальные хосты
    ssh-copy-id -i ~/.ssh/id_ed25519.pub user@host # раздать ключ (по одному разу на хост)
    ansible all -m ping                            # первая ad-hoc команда
DONE
