<#
.SYNOPSIS
    Проверка рабочей станции Windows 11 на пригодность для роли control node Ansible.

.DESCRIPTION
    Скрипт НЕ требует прав администратора и ничего не меняет в системе.
    Собирает факты, от которых зависит выбор варианта установки (A / B / C),
    и печатает итоговую рекомендацию.

.PARAMETER TargetHosts
    Список Linux-хостов для проверки доступности SSH (по умолчанию порт 22).

.PARAMETER Port
    Порт SSH, по умолчанию 22.

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File .\00-check-workstation.ps1
.EXAMPLE
    powershell -ExecutionPolicy Bypass -File .\00-check-workstation.ps1 -TargetHosts 10.0.0.11,10.0.0.12
#>

[CmdletBinding()]
param(
    [string[]] $TargetHosts = @(),
    [int]      $Port = 22
)

$ErrorActionPreference = 'Continue'
$report = [ordered]@{}

function Write-Head($text) {
    Write-Host ''
    Write-Host "=== $text " -ForegroundColor Cyan -NoNewline
    Write-Host ('=' * [Math]::Max(0, 60 - $text.Length)) -ForegroundColor Cyan
}

function Write-Item($name, $value, $ok) {
    $color = if ($null -eq $ok) { 'Gray' } elseif ($ok) { 'Green' } else { 'Yellow' }
    Write-Host ('{0,-34}' -f $name) -NoNewline
    Write-Host $value -ForegroundColor $color
}

# ---------------------------------------------------------------- ОС и права
Write-Head 'Операционная система и права'

$os = Get-CimInstance Win32_OperatingSystem
$build = [int](Get-ItemProperty 'HKLM:\SOFTWARE\Microsoft\Windows NT\CurrentVersion').CurrentBuildNumber
$report.OsCaption = $os.Caption
$report.Build     = $build
Write-Item 'ОС'    "$($os.Caption) (build $build)" $null
Write-Item 'Архитектура' $env:PROCESSOR_ARCHITECTURE $null

$principal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
$isAdmin = $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
$report.IsAdmin = $isAdmin
Write-Item 'Права администратора' $(if ($isAdmin) { 'ЕСТЬ' } else { 'нет (ожидаемо)' }) $null
Write-Item 'Пользователь' "$env:USERDOMAIN\$env:USERNAME" $null

# ---------------------------------------------------------------- Виртуализация
Write-Head 'Виртуализация'

$cs = Get-CimInstance Win32_ComputerSystem
$hypervisorPresent = [bool]$cs.HypervisorPresent
$report.HypervisorPresent = $hypervisorPresent
Write-Item 'Гипервизор активен' $(if ($hypervisorPresent) { 'да (WSL2 / Hyper-V доступны)' } else { 'нет' }) $hypervisorPresent

$vmFirmware = $null
try { $vmFirmware = (Get-CimInstance Win32_Processor | Select-Object -First 1).VirtualizationFirmwareEnabled } catch {}
if ($null -ne $vmFirmware) {
    Write-Item 'VT-x/AMD-V в BIOS' $(if ($vmFirmware) { 'включено' } else { 'выключено' }) $vmFirmware
}

# ---------------------------------------------------------------- WSL
Write-Head 'WSL'

$wsl = Get-Command wsl.exe -ErrorAction SilentlyContinue
$report.WslBinary = [bool]$wsl
Write-Item 'wsl.exe в PATH' $(if ($wsl) { $wsl.Source } else { 'НЕ НАЙДЕН' }) ([bool]$wsl)

$lxssService = Get-Service -Name LxssManager -ErrorAction SilentlyContinue
Write-Item 'Служба LxssManager' $(if ($lxssService) { $lxssService.Status } else { 'не зарегистрирована' }) ([bool]$lxssService)

$wslStatus = $null
$wslDistros = @()
if ($wsl) {
    $raw = & wsl.exe --status 2>&1
    # wsl.exe отдаёт UTF-16LE, при перенаправлении получаются \x00 между символами
    $wslStatus = ($raw | Out-String) -replace "`0", ''
    if ($LASTEXITCODE -eq 0 -and $wslStatus.Trim()) {
        Write-Host $wslStatus.Trim() -ForegroundColor DarkGray
    }

    $rawList = & wsl.exe --list --quiet 2>&1
    $listTxt = ($rawList | Out-String) -replace "`0", ''
    $wslDistros = $listTxt -split "`r?`n" | Where-Object { $_.Trim() } | ForEach-Object { $_.Trim() }
}
$report.WslDistros = $wslDistros
Write-Item 'Установленные дистрибутивы' $(if ($wslDistros.Count) { $wslDistros -join ', ' } else { 'нет' }) ([bool]$wslDistros.Count)

$wslFeatureLikely = $wsl -and $lxssService

# ---------------------------------------------------------------- SSH-клиент
Write-Head 'SSH-клиент Windows'

$ssh = Get-Command ssh.exe -ErrorAction SilentlyContinue
$report.SshClient = [bool]$ssh
if ($ssh) {
    $sshVer = (& ssh.exe -V 2>&1 | Out-String).Trim()
    Write-Item 'ssh.exe' "$($ssh.Source)" $true
    Write-Item 'Версия'  $sshVer $null
} else {
    Write-Item 'ssh.exe' 'НЕ НАЙДЕН (нужен OpenSSH Client)' $false
}

$sshDir = Join-Path $env:USERPROFILE '.ssh'
$keys = @()
if (Test-Path $sshDir) {
    $keys = Get-ChildItem $sshDir -Filter 'id_*' -File -ErrorAction SilentlyContinue |
            Where-Object { $_.Extension -ne '.pub' } | Select-Object -ExpandProperty Name
}
Write-Item 'Ключи в %USERPROFILE%\.ssh' $(if ($keys.Count) { $keys -join ', ' } else { 'нет' }) $null

# ---------------------------------------------------------------- Python
Write-Head 'Python на Windows (нужен только для вспомогательных задач)'

foreach ($exe in 'python.exe', 'py.exe') {
    $cmd = Get-Command $exe -ErrorAction SilentlyContinue
    if ($cmd) {
        $v = (& $cmd.Source -V 2>&1 | Out-String).Trim()
        Write-Item $exe "$v  ($($cmd.Source))" $null
    } else {
        Write-Item $exe 'не найден' $null
    }
}
Write-Host 'Напоминание: Ansible как control node на нативном Windows не работает.' -ForegroundColor DarkGray

# ---------------------------------------------------------------- Прокси
Write-Head 'Сеть и прокси'

$ie = Get-ItemProperty 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Internet Settings' -ErrorAction SilentlyContinue
if ($ie -and $ie.ProxyEnable -eq 1) {
    Write-Item 'HTTP-прокси (пользователь)' $ie.ProxyServer $null
    if ($ie.ProxyOverride) { Write-Item 'Исключения' $ie.ProxyOverride $null }
} else {
    Write-Item 'HTTP-прокси (пользователь)' 'не задан' $null
}
foreach ($v in 'HTTP_PROXY', 'HTTPS_PROXY', 'NO_PROXY') {
    $val = [Environment]::GetEnvironmentVariable($v)
    if ($val) { Write-Item "env:$v" $val $null }
}

$pypiOk = $null
try {
    $r = Invoke-WebRequest -Uri 'https://pypi.org/simple/ansible-core/' -Method Head -TimeoutSec 8 -UseBasicParsing
    $pypiOk = ($r.StatusCode -eq 200)
} catch { $pypiOk = $false }
$report.PyPiReachable = $pypiOk
Write-Item 'pypi.org доступен' $(if ($pypiOk) { 'да' } else { 'нет — потребуется offline-сборка' }) $pypiOk

# ---------------------------------------------------------------- Целевые хосты
if ($TargetHosts.Count) {
    Write-Head "Доступность целевых хостов (TCP/$Port)"
    foreach ($h in $TargetHosts) {
        $sw = [Diagnostics.Stopwatch]::StartNew()
        $client = New-Object Net.Sockets.TcpClient
        $ok = $false
        try {
            $ok = $client.ConnectAsync($h, $Port).Wait(5000)
        } catch { $ok = $false } finally { $client.Close(); $sw.Stop() }
        Write-Item "  $h" $(if ($ok) { "открыт, $($sw.ElapsedMilliseconds) мс" } else { 'недоступен' }) $ok
    }
}

# ---------------------------------------------------------------- Вывод
Write-Head 'РЕКОМЕНДАЦИЯ'

if ($wslDistros.Count) {
    Write-Host 'Вариант A: WSL уже установлен и дистрибутив есть.' -ForegroundColor Green
    Write-Host '  Переходите сразу к scripts/10-ansible-setup.sh внутри WSL:'
    Write-Host "     wsl -d $($wslDistros[0]) -- bash -lc 'cd /mnt/c/... && ./scripts/10-ansible-setup.sh'" -ForegroundColor Gray
}
elseif ($wslFeatureLikely -and $hypervisorPresent) {
    Write-Host 'Вариант A: подсистема WSL включена, дистрибутива нет.' -ForegroundColor Green
    Write-Host '  Ставим дистрибутив без прав администратора: scripts/01-wsl-bootstrap.ps1'
}
elseif ($wslFeatureLikely -and -not $hypervisorPresent) {
    Write-Host 'Вариант A (WSL1): подсистема WSL есть, платформа виртуальной машины выключена.' -ForegroundColor Yellow
    Write-Host '  Ansible работает и на WSL1. Пробуйте scripts/01-wsl-bootstrap.ps1 -WslVersion 1'
    Write-Host '  Для WSL2 нужен разовый запрос к администраторам (см. README, раздел "Что просить у админов").'
}
else {
    Write-Host 'Вариант B: WSL недоступен без прав администратора.' -ForegroundColor Yellow
    Write-Host '  Control node выносим на Linux-хост (jump host), рабочая станция остаётся SSH-клиентом.'
    Write-Host '  См. README, раздел "Вариант B".'
}

Write-Host ''
Write-Host 'Полный отчёт сохранён в check-workstation-report.json' -ForegroundColor DarkGray
$report | ConvertTo-Json -Depth 4 | Set-Content -Path (Join-Path $PWD 'check-workstation-report.json') -Encoding UTF8
