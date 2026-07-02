#### HostCheck

скрипт для проверки SSH-соединения со списком хостов. Проверяет доступность порта 22 (TCP) и опционально пытается выполнить реальное SSH-подключение, если установлен модуль Posh-SSH.

1.Быстрая проверка — по умолчанию скрипт просто проверяет, открыт ли TCP-порт 22 (или указанный вами), без необходимости в дополнительных модулях:
```powershell
powershell   .\Test-SshConnection.ps1 -HostsFile .\hosts.txt
```

2.Проверка через массив хостов прямо в команде:
```
powershell   .\Test-SshConnection.ps1 -HostList "10.0.0.1","server2.local" -Port 2222
```

3.Полноценная проверка SSH-логина (требует модуль Posh-SSH):
```
powershell   Install-Module -Name Posh-SSH -Scope CurrentUser
   $cred = Get-Credential
   .\Test-SshConnection.ps1 -HostsFile .\hosts.txt -Credential $cred
```
Функционал:
* Читает хосты из файла (hosts.txt) или из переданного массива
* Для каждого хоста проверяет TCP-подключение с таймаутом (по умолчанию 5 сек)
* Если передан Credential и установлен Posh-SSH — проверка логина по SSH
* Выводит таблицу результатов в консоль (хост, порт, статус, задержка)
* Сохраняет всё в CSV-файл (ssh_check_results.csv)
