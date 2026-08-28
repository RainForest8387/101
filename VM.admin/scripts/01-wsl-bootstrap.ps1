<#
.SYNOPSIS
    Установка WSL-дистрибутива под обычным пользователем (без прав администратора).

.DESCRIPTION
    Работает только если подсистема WSL уже включена администраторами
    (проверьте scripts\00-check-workstation.ps1).

    Два режима:
      1) Онлайн — `wsl --install -d <Distro> --no-launch`. Дистрибутив ставится
         в профиль пользователя, повышение прав не требуется.
      2) Офлайн — `wsl --import` из заранее подготовленного rootfs-tar.
         Так же можно завезти собственный образ Astra Linux, чтобы control node
         и целевые серверы были одной ОС.

.PARAMETER Distro
    Имя дистрибутива из `wsl --list --online`. По умолчанию Debian.

.PARAMETER Name
    Имя регистрируемого экземпляра WSL. По умолчанию совпадает с Distro.

.PARAMETER RootfsTar
    Путь к rootfs.tar[.gz] для офлайн-режима (wsl --import).

.PARAMETER InstallRoot
    Куда положить VHDX экземпляра. По умолчанию %LOCALAPPDATA%\WSL\<Name>.

.PARAMETER WslVersion
    2 (по умолчанию) или 1. WSL1 не требует платформы виртуальной машины.

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File .\01-wsl-bootstrap.ps1
.EXAMPLE
    powershell -ExecutionPolicy Bypass -File .\01-wsl-bootstrap.ps1 -Name astra18 -RootfsTar D:\images\astra-1.8-rootfs.tar
#>

[CmdletBinding()]
param(
    [string] $Distro      = 'Debian',
    [string] $Name        = '',
    [string] $RootfsTar   = '',
    [string] $InstallRoot = '',
    [ValidateSet(1, 2)]
    [int]    $WslVersion  = 2
)

$ErrorActionPreference = 'Stop'
if (-not $Name)        { $Name = $Distro }
if (-not $InstallRoot) { $InstallRoot = Join-Path $env:LOCALAPPDATA "WSL\$Name" }

function Invoke-Wsl {
    param([string[]] $WslArgs)
    $out = & wsl.exe @WslArgs 2>&1
    $text = ($out | Out-String) -replace "`0", ''
    if ($text.Trim()) { Write-Host $text.Trim() }
    return $LASTEXITCODE
}

if (-not (Get-Command wsl.exe -ErrorAction SilentlyContinue)) {
    Write-Host 'wsl.exe не найден. Подсистема WSL не включена — нужен разовый запрос к администраторам.' -ForegroundColor Red
    Write-Host 'Команды для них смотрите в README, раздел "Что просить у админов".' -ForegroundColor Red
    exit 1
}

$existing = ((& wsl.exe --list --quiet 2>&1 | Out-String) -replace "`0", '') -split "`r?`n" |
            ForEach-Object { $_.Trim() } | Where-Object { $_ }

if ($existing -contains $Name) {
    Write-Host "Экземпляр '$Name' уже зарегистрирован. Ничего не делаю." -ForegroundColor Green
    exit 0
}

if ($RootfsTar) {
    # ---------------------------------------------------------- офлайн-импорт
    if (-not (Test-Path $RootfsTar)) { throw "Файл не найден: $RootfsTar" }
    New-Item -ItemType Directory -Path $InstallRoot -Force | Out-Null
    Write-Host "Импорт $RootfsTar -> $InstallRoot (имя: $Name, WSL$WslVersion)" -ForegroundColor Cyan
    $code = Invoke-Wsl @('--import', $Name, $InstallRoot, $RootfsTar, '--version', "$WslVersion")
    if ($code -ne 0) { throw "wsl --import завершился с кодом $code" }
}
else {
    # ---------------------------------------------------------------- онлайн
    Write-Host "Установка дистрибутива '$Distro' из каталога WSL..." -ForegroundColor Cyan
    Write-Host 'Если команда потребует повышения прав — используйте офлайн-режим (-RootfsTar).' -ForegroundColor DarkGray
    $code = Invoke-Wsl @('--install', '-d', $Distro, '--no-launch')
    if ($code -ne 0) {
        Write-Host ''
        Write-Host 'Не удалось поставить дистрибутив из каталога.' -ForegroundColor Yellow
        Write-Host 'Запасные пути:' -ForegroundColor Yellow
        Write-Host '  * поставить Debian/Ubuntu из Microsoft Store (обычно доступно пользователю);'
        Write-Host '  * получить rootfs.tar на машине с интернетом и повторить с ключом -RootfsTar;'
        Write-Host '  * перейти к варианту B (control node на Linux-хосте).'
        exit $code
    }
}

# ------------------------------------------------------- первичная инициализация
Write-Host ''
Write-Host "Инициализация '$Name'..." -ForegroundColor Cyan
Invoke-Wsl @('-d', $Name, '--', 'true') | Out-Null

$user = ($env:USERNAME -replace '[^a-zA-Z0-9_-]', '').ToLower()
if (-not $user) { $user = 'ansible' }

$bootstrap = @"
set -e
if ! id -u $user >/dev/null 2>&1; then
    useradd -m -s /bin/bash -G sudo $user
    passwd -d $user
    echo '$user ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/90-$user
    chmod 0440 /etc/sudoers.d/90-$user
fi
printf '[user]\ndefault=$user\n[boot]\nsystemd=true\n' > /etc/wsl.conf
"@

$tmp = Join-Path $env:TEMP 'wsl-bootstrap.sh'
Set-Content -Path $tmp -Value ($bootstrap -replace "`r`n", "`n") -Encoding UTF8 -NoNewline
# Путь Windows -> путь WSL без вызова wslpath: так не страдают обратные слеши при квотировании.
$wslTmp = '/mnt/' + $tmp.Substring(0, 1).ToLower() + ($tmp.Substring(2) -replace '\\', '/')
Invoke-Wsl @('-d', $Name, '-u', 'root', '--', 'bash', $wslTmp) | Out-Null
Remove-Item $tmp -Force -ErrorAction SilentlyContinue

Write-Host ''
Write-Host 'Готово. Перезапуск экземпляра для применения /etc/wsl.conf...' -ForegroundColor Cyan
& wsl.exe --terminate $Name | Out-Null

Write-Host ''
Write-Host "Дистрибутив '$Name' готов, пользователь по умолчанию: $user" -ForegroundColor Green
Write-Host 'Следующий шаг:' -ForegroundColor Green
Write-Host "  wsl -d $Name" -ForegroundColor Gray
Write-Host '  затем внутри WSL запустите scripts/10-ansible-setup.sh' -ForegroundColor Gray
