#!/usr/bin/env bash
#
# Сборка офлайн-комплекта для закрытого контура.
# Запускается на машине С ИНТЕРНЕТОМ (любой Linux, можно контейнер),
# результат переносится на рабочую станцию и ставится через:
#     ./10-ansible-setup.sh --offline-dir <каталог>
#
#   ./20-offline-bundle.sh                                   # ansible-core 2.17 под Python 3.11
#   ./20-offline-bundle.sh --core-version 2.16 --python 3.9  # под Debian 11 / старый venv
#   ./20-offline-bundle.sh --out /tmp/bundle --archive
#
set -euo pipefail

CORE_VERSION="2.17"
PY_VERSION="3.11"
PLATFORM="manylinux2014_x86_64"
OUT="$(pwd)/ansible-offline-bundle"
ARCHIVE=0

COLLECTIONS=(ansible.posix community.general ansible.utils)

log() { printf '\033[36m==>\033[0m %s\n' "$*"; }
die() { printf '\033[31m[x]\033[0m %s\n' "$*" >&2; exit 1; }

while [[ $# -gt 0 ]]; do
    case "$1" in
        --core-version) CORE_VERSION="$2"; shift 2 ;;
        --python)       PY_VERSION="$2"; shift 2 ;;
        --platform)     PLATFORM="$2"; shift 2 ;;
        --out)          OUT="$2"; shift 2 ;;
        --archive)      ARCHIVE=1; shift ;;
        -h|--help)      sed -n '2,11p' "$0"; exit 0 ;;
        *)              die "неизвестный аргумент: $1" ;;
    esac
done

command -v python3 >/dev/null || die "нужен python3"
mkdir -p "$OUT"
OUT="$(cd "$OUT" && pwd)"

log "Комплект: ansible-core ~= ${CORE_VERSION}.0, целевой Python ${PY_VERSION}, платформа ${PLATFORM}"
log "Каталог: ${OUT}"

# ------------------------------------------------------------------- колёса
# --only-binary=:all: обязателен вместе с --platform/--python-version:
# pip не умеет собирать sdist под чужую платформу.
python3 -m pip download \
    --dest "$OUT" \
    --only-binary=:all: \
    --platform "$PLATFORM" \
    --python-version "$PY_VERSION" \
    "ansible-core~=${CORE_VERSION}.0" \
    passlib \
    jmespath \
    pip \
    wheel

# ------------------------------------------------------------------- коллекции
if command -v ansible-galaxy >/dev/null; then
    log "Скачиваю коллекции"
    ansible-galaxy collection download "${COLLECTIONS[@]}" -p "${OUT}/collections"
    # requirements.yml из download-каталога пригодится на целевой машине
    cp "${OUT}/collections/"*.tar.gz "$OUT" 2>/dev/null || true
else
    log "ansible-galaxy недоступен, коллекции пропущены"
    log "Поставьте ansible-core локально и перезапустите, если коллекции нужны"
fi

# ------------------------------------------------------------------- манифест
{
    echo "# Офлайн-комплект Ansible"
    echo "Собран: $(date -Is)"
    echo "Хост сборки: $(uname -srm)"
    echo "ansible-core: ~=${CORE_VERSION}.0"
    echo "Python: ${PY_VERSION}"
    echo "Платформа: ${PLATFORM}"
    echo
    echo "## Содержимое"
    ( cd "$OUT" && ls -1 *.whl *.tar.gz 2>/dev/null )
} > "${OUT}/MANIFEST.txt"

log "Проверка целостности"
( cd "$OUT" && sha256sum *.whl *.tar.gz 2>/dev/null > SHA256SUMS ) || true

if [[ "$ARCHIVE" -eq 1 ]]; then
    TARBALL="${OUT}.tar.gz"
    log "Упаковка в ${TARBALL}"
    tar czf "$TARBALL" -C "$(dirname "$OUT")" "$(basename "$OUT")"
    sha256sum "$TARBALL"
fi

cat <<DONE

Готово. Перенесите каталог на рабочую станцию и выполните:

    ./10-ansible-setup.sh --offline-dir /путь/к/$(basename "$OUT") --core-version ${CORE_VERSION}

Проверить комплект перед установкой:
    sha256sum -c SHA256SUMS
DONE
