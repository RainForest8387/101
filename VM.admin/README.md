# Ansible на рабочей станции Windows 11 без прав администратора

Управление парком Astra Linux 1.7 и 1.8: сначала ad-hoc команды, затем плейбуки.
Здесь — план действий, варианты установки и готовые скрипты.

## Главное ограничение

Ansible **не работает как control node на нативном Windows**. Это не вопрос
установки пакета: ядро Ansible использует POSIX-механизмы (`fork`, семейство `os.*`,
UNIX-сокеты), которых в Windows нет. Официально control node — только Linux, BSD
или macOS. Windows поддерживается **исключительно как управляемый узел**.

Значит, на станции нужен слой Linux. Вопрос лишь в том, какой доступен без админских прав.

## Варианты

| | Вариант | Условие применимости | Права админа | Сложность |
|---|---|---|---|---|
| **A** | WSL2 (или WSL1) + Debian/Ubuntu | подсистема WSL уже включена в образе станции | не нужны, если включена | низкая |
| **B** | Control node на Linux jump host, станция — только SSH-клиент | есть любой Linux-хост, куда пускают по SSH | не нужны вовсе | низкая |
| **C** | Cygwin/MSYS2 portable | последнее средство | не нужны | высокая, конфигурация не поддерживается |

**Рекомендация:** сначала проверить A. Если WSL в образе не включён — идти в B,
он полностью снимает вопрос прав на станции и заодно даёт единую точку запуска
для всей команды. C рассматривать только как временный обход: на Cygwin
регулярно ломаются `become`, ControlPersist и асинхронные задачи, а времени на
диагностику уйдёт больше, чем на согласование варианта A.

Что именно применимо в вашем случае, покажет скрипт проверки — шаг 1 плана.

## Совместимость версий

Ansible ставим не «последний», а тот, что переварит **самый старый Python в парке**.
Управляемый узел исполняет модули своим интерпретатором, поэтому Astra 1.7 задаёт потолок.

| ansible-core | Python на control node | Python на управляемых узлах |
|---|---|---|
| 2.16 | 3.10 – 3.12 | 2.7, 3.6 – 3.12 |
| 2.17 | 3.10 – 3.12 | 3.7 – 3.12 |
| 2.18 | 3.11 – 3.13 | 3.8 – 3.13 |
| 2.19 | 3.11 – 3.13 | 3.8 – 3.13 |

В базовой поставке Astra Linux 1.7 идёт Python 3.7, в 1.8 — Python 3.11.
Отсюда рабочий выбор: **ansible-core ветки 2.17**, она покрывает обе версии Astra.
Фактические версии обязательно сверьте на своих хостах, они зависят от установленных
обновлений:

```bash
ansible all -m raw -a 'python3 -V; cat /etc/astra_version'
```

Модуль `raw` выбран намеренно: он не требует Python на целевой стороне и потому
работает даже там, где ansible-модули ещё не запускаются.

Если в парке найдётся Astra с Python 3.6 и ниже — понижайтесь до ansible-core 2.16
(`--core-version 2.16` у скрипта установки).

## План действий

| Этап | Что делаем | Чем |
|---|---|---|
| 0 | Согласовать доступы: учётка на целевых хостах, разрешённый SSH со станции | заявка |
| 1 | Снять факты со станции, выбрать вариант A/B/C | `scripts\00-check-workstation.ps1` |
| 2 | Поднять слой Linux (только для A) | `scripts\01-wsl-bootstrap.ps1` |
| 3 | Поставить Ansible в venv, создать проект и ключ | `scripts/10-ansible-setup.sh` |
| 4 | Раздать ключ на целевые хосты, проверить связность | `ssh-copy-id`, `playbooks/smoke.yml` |
| 5 | Ad-hoc эксплуатация: инвентарь, диагностика, отчёты | шпаргалка ниже |
| 6 | Перейти к плейбукам: check → один хост → группа | `playbooks/example-baseline.yml` |

Этапы 0 и 1 идут параллельно. Этап 4 — граница: пока `smoke.yml` не проходит зелёным
по всему парку, к изменяющим плейбукам не переходим.

### Этап 0. Что согласовать заранее

Это самое долгое звено, поэтому запускается первым.

- **Сервисная учётная запись** `ansible` на целевых хостах, вход по SSH-ключу, пароль отключён.
- **Правило sudo**: на старте — `NOPASSWD` только на нужный набор команд, а не `ALL`.
  Полный `ALL` запрашивайте, когда состав задач устоится.
- **Сетевой доступ**: TCP/22 со станции (или с jump host) до всех подсетей парка.
- **Владелец изменений**: кто согласует боевые прогоны и в каком окне.

Для закрытого контура добавьте пункт: канал доставки офлайн-комплекта Python-пакетов
(раздел «Закрытый контур»).

### Этап 1. Проверка станции

```powershell
cd VM.admin\scripts
powershell -ExecutionPolicy Bypass -File .\00-check-workstation.ps1 -TargetHosts 10.0.0.11,10.0.0.21
```

Скрипт ничего не меняет и не требует прав. Он проверяет наличие `wsl.exe` и службы
`LxssManager`, состояние гипервизора, встроенный OpenSSH-клиент, прокси, доступность
pypi.org и TCP/22 до указанных хостов, после чего печатает рекомендацию A/B/C и
складывает отчёт в `check-workstation-report.json`.

Ключевая развилка в его выводе:

- дистрибутивы WSL уже есть → сразу этап 3;
- `wsl.exe` и `LxssManager` есть, дистрибутива нет → этап 2;
- гипервизор выключен, но подсистема есть → WSL1, `-WslVersion 1`, Ansible на нём работает;
- `wsl.exe` нет → вариант B.

### Этап 2. WSL без прав администратора (вариант A)

```powershell
powershell -ExecutionPolicy Bypass -File .\01-wsl-bootstrap.ps1
```

Скрипт ставит Debian через `wsl --install -d Debian --no-launch`, заводит внутри
пользователя под вашим доменным именем с беспарольным sudo и пишет `/etc/wsl.conf`
с включённым systemd. Регистрация экземпляра идёт в профиль пользователя, повышение
прав не требуется — при условии, что сама подсистема уже включена.

Если каталог дистрибутивов недоступен (закрытый контур, заблокированный Store),
используйте офлайн-импорт готового rootfs:

```powershell
powershell -ExecutionPolicy Bypass -File .\01-wsl-bootstrap.ps1 -Name astra18 -RootfsTar D:\images\astra-1.8-rootfs.tar
```

Отдельно стоит рассмотреть **rootfs самой Astra Linux**: тогда control node и целевые
серверы будут одной ОС с одинаковыми версиями Python и OpenSSH, и класс расхождений
«у меня работает, на сервере нет» исчезает. Образ получают на машине с доступом:

```bash
# из контейнера
docker create --name tmp registry.example.ru/astra/base:1.8
docker export tmp -o astra-1.8-rootfs.tar
docker rm tmp

# либо из установленной системы
sudo tar --numeric-owner --exclude=/proc --exclude=/sys --exclude=/dev \
         --exclude=/run --exclude=/tmp -czf astra-1.8-rootfs.tar.gz -C / .
```

### Этап 3. Установка Ansible

Одинаково для варианта A (внутри WSL) и варианта B (на jump host):

```bash
cd /mnt/c/Users/<вы>/VM.admin/scripts     # в WSL; на jump host — свой путь
./10-ansible-setup.sh
```

Что делает скрипт: определяет версию Python и подбирает ветку ansible-core, создаёт
venv в `~/.venvs/ansible`, ставит `ansible-core`, `passlib` и `jmespath`, доустанавливает
коллекции `ansible.posix`, `community.general`, `ansible.utils`, разворачивает каркас
проекта в `~/ansible`, генерирует ключ ed25519 и прописывает `PATH` с `ANSIBLE_CONFIG`
в `.bashrc`. Повторный запуск безопасен: существующие файлы не перезаписываются.

Полезные ключи:

```bash
./10-ansible-setup.sh --core-version 2.16          # в парке есть Python 3.6
./10-ansible-setup.sh --offline-dir ~/bundle       # закрытый контур
./10-ansible-setup.sh --project ~/work/astra       # другой каталог проекта
./10-ansible-setup.sh --no-ssh-key                 # ключ уже есть
```

**Про рабочие файлы в WSL.** Держите проект и ключи внутри файловой системы Linux
(`~/ansible`), а не на `/mnt/c`. На смонтированном диске Windows не работают права
POSIX: SSH откажется брать ключ с правами `0777`, а Ansible будет ругаться на
`world-writable` каталог конфигурации. Репозиторий с `/mnt/c` при этом читать можно,
скрипт установки именно так и запускается.

### Этап 4. Целевые хосты и первая связь

На каждом сервере Astra один раз:

```bash
sudo useradd -m -s /bin/bash ansible
sudo mkdir -p /home/ansible/.ssh && sudo chmod 700 /home/ansible/.ssh
sudo tee /home/ansible/.ssh/authorized_keys < id_ed25519.pub
sudo chmod 600 /home/ansible/.ssh/authorized_keys
sudo chown -R ansible:ansible /home/ansible/.ssh
echo 'ansible ALL=(ALL) NOPASSWD:ALL' | sudo tee /etc/sudoers.d/90-ansible
sudo chmod 0440 /etc/sudoers.d/90-ansible
```

Со станции то же самое делается быстрее, пока пароль ещё действует:

```bash
ssh-copy-id -i ~/.ssh/id_ed25519.pub ansible@10.0.0.11
```

Дальше правим инвентарь и проверяем парк:

```bash
cd ~/ansible
vi inventory/astra.ini
ansible-inventory --graph
ansible all -m ping
ansible-playbook playbooks/smoke.yml
```

`smoke.yml` собирает по каждому хосту версию Astra, ядро, версию Python на целевой
стороне и проверяет беспарольный sudo. Это тот самый зелёный прогон, после которого
разрешено идти дальше.

Ключ хоста при первом подключении: `host_key_checking` в конфиге включён намеренно.
Соберите отпечатки один раз и положите в `known_hosts`, а не отключайте проверку:

```bash
ssh-keyscan -H -f <(awk '/ansible_host=/{sub(/.*ansible_host=/,""); print $1}' inventory/astra.ini) >> ~/.ssh/known_hosts
```

## Ad-hoc команды: шпаргалка

Формат: `ansible <кому> -m <модуль> -a "<аргументы>"`.

```bash
# связность и аптайм
ansible all -m ping
ansible prod -a 'uptime'

# место на дисках и память, только по группе 1.7
ansible astra17 -a 'df -h /' -o
ansible astra17 -m shell -a 'free -m'

# состояние и перезапуск сервиса (перезапуск — с --check сначала!)
ansible db -m service -a 'name=postgresql state=started' --become --check
ansible db -m service -a 'name=postgresql state=restarted' --become --limit astra-db-01

# пакеты
ansible all -m apt -a 'update_cache=yes' --become
ansible all -m shell -a 'apt list --upgradable 2>/dev/null | wc -l'

# файлы туда и обратно
ansible app -m copy -a 'src=./app.conf dest=/etc/app/app.conf backup=yes' --become
ansible app -m fetch -a 'src=/var/log/app/app.log dest=./logs/ flat=no'

# факты одного хоста и выборка из них
ansible astra-app-01 -m setup
ansible all -m setup -a 'filter=ansible_distribution*'

# что-то совсем сырое, без Python на целевой стороне
ansible all -m raw -a 'cat /etc/astra_version'
```

Полезные ключи: `--limit` (сузить парк), `--check` (сухой прогон), `--diff`
(показать изменения в файлах), `-o` (одна строка на хост), `-f 30` (параллелизм),
`--become` (через sudo), `-K` (спросить пароль sudo), `--list-hosts` (проверить,
кого зацепит, ничего не выполняя).

Начинайте любую незнакомую команду с `--limit одного_хоста --check --list-hosts`.

## Переход к плейбукам

Порядок, который стоит закрепить как правило:

```bash
ansible-playbook playbooks/example-baseline.yml --syntax-check
ansible-playbook playbooks/example-baseline.yml --list-tasks
ansible-playbook playbooks/example-baseline.yml --check --diff
ansible-playbook playbooks/example-baseline.yml -l astra-app-01
ansible-playbook playbooks/example-baseline.yml -l astra17
ansible-playbook playbooks/example-baseline.yml
```

`example-baseline.yml` — учебный каркас, а не готовая политика: он показывает
`assert` на входе для отсечения не-Astra хостов, теги, `backup: true` у файловых
задач и хендлеры. Перед боевым применением вычитайте задачи и приведите к своим
стандартам. Задача с шаблоном chrony намеренно ссылается на несуществующий
`templates/chrony.conf.j2`: создайте свой или пропускайте её через `--skip-tags time`.

Второй плейбук, `astra-inventory.yml`, только читает и выгружает паспорт парка
в CSV — с версиями, ядром, объёмом памяти и числом доступных обновлений. Удобно
для первой инвентаризации и для регулярной отчётности.

## Секреты

Пароли, токены и ключи не хранятся в инвентаре и `group_vars` открытым текстом.

```bash
ansible-vault create group_vars/prod/vault.yml
ansible-vault edit   group_vars/prod/vault.yml
ansible-playbook site.yml --ask-vault-pass
```

Для неинтерактивных запусков держите пароль в файле вне репозитория и
пропишите `vault_password_file` в `ansible.cfg`. В `.gitignore` уже стоит
исключение для файлов паролей и логов.

## Закрытый контур

Если pypi.org со станции недоступен, комплект собирается один раз на машине с
интернетом и переносится файлом:

```bash
# на машине с доступом (версии — под ваш control node)
./20-offline-bundle.sh --core-version 2.17 --python 3.11 --archive

# на рабочей станции
tar xzf ansible-offline-bundle.tar.gz
cd ansible-offline-bundle && sha256sum -c SHA256SUMS
cd ../scripts && ./10-ansible-setup.sh --offline-dir ../ansible-offline-bundle --core-version 2.17
```

Важная деталь: `--python` в сборщике должен совпадать с версией Python **на control
node**, иначе бинарные колёса (`PyYAML`, `cryptography`, `MarkupSafe`) не подойдут.
Узнать её: `python3 -V` внутри WSL или на jump host.

Если в контуре есть внутренний индекс пакетов, офлайн-комплект не нужен — пропишите
его в `~/.pip/pip.conf` и ставьте обычным способом.

## Что просить у админов

Если вариант A недоступен, единственный запрос звучит так: включить подсистему WSL.
Это одноразовое действие, дальше вы работаете сами.

```powershell
# от администратора, один раз на станции
dism.exe /online /enable-feature /featurename:Microsoft-Windows-Subsystem-Linux /all /norestart
dism.exe /online /enable-feature /featurename:VirtualMachinePlatform /all /norestart
# перезагрузка, затем:
wsl --set-default-version 2
```

Второй пункт (`VirtualMachinePlatform`) нужен только для WSL2. Если виртуализация
запрещена политикой, попросите включить лишь первый: Ansible прекрасно работает
и на WSL1.

Обоснование для заявки: инструмент нужен для управления серверами Astra Linux,
на станции ничего не публикуется наружу, сетевые обращения — только SSH на порт 22
к серверам парка.

## Вариант B: control node на Linux-хосте

Когда WSL согласовать не удаётся, схема меняется на классическую: Ansible живёт на
Linux-хосте, станция остаётся терминалом.

1. Выделите хост под control node — подойдёт любая Astra 1.8 или Debian с сетевым
   доступом до парка.
2. Выполните на нём `10-ansible-setup.sh` — скрипт не привязан к WSL.
3. Со станции работайте через встроенный OpenSSH-клиент Windows 11, он есть без
   прав администратора:

```powershell
ssh-keygen -t ed25519 -C "%USERNAME%@workstation"
type $env:USERPROFILE\.ssh\id_ed25519.pub | ssh user@jumphost "cat >> ~/.ssh/authorized_keys"
ssh user@jumphost
```

Удобная надстройка: VS Code с расширением Remote-SSH ставится в профиль пользователя
без админских прав и даёт редактирование плейбуков и терминал прямо на jump host.

Плюс варианта: единая точка запуска, общая история и логи, работает с любой станции.
Минус: нужен сам хост и порядок доступа к нему.

## Вариант C: почему не Cygwin

Формально Ansible на Cygwin/MSYS2 запускается: там есть `fork` и POSIX-слой. На
практике вы получите нерабочий `become` в части сценариев, конфликты ControlPersist,
проблемы с временными каталогами и полное отсутствие поддержки со стороны сообщества
при разборе любой ошибки. Как временный обход на день-два — допустимо, как рабочая
схема для парка серверов — нет.

## Аутентификация через домен

Если хосты Astra заведены в домен (ALD Pro, FreeIPA или Active Directory), можно
не раздавать ключи, а ходить по Kerberos. В `ansible.cfg` метод уже разрешён строкой
`PreferredAuthentications=publickey,gssapi-with-mic,password`. Со стороны control node:

```bash
sudo apt-get install -y krb5-user
kinit user@REALM.EXAMPLE.RU
klist
ansible all -m ping -e ansible_user=user
```

Учтите срок жизни билета: для регулярных запусков по расписанию keytab надёжнее
интерактивного `kinit`. Ключевая пара при этом остаётся резервным способом входа —
не отключайте её, пока Kerberos не отработает стабильно.

## Состав каталога

```
VM.admin/
├── README.md
├── scripts/
│   ├── 00-check-workstation.ps1   проверка станции, отчёт и рекомендация A/B/C
│   ├── 01-wsl-bootstrap.ps1       установка WSL-дистрибутива без прав админа
│   ├── 10-ansible-setup.sh        venv, ansible-core, коллекции, ключ, каркас проекта
│   └── 20-offline-bundle.sh       сборка комплекта пакетов для закрытого контура
└── ansible/
    ├── ansible.cfg                конфигурация под парк Astra
    ├── inventory/astra.ini        инвентарь: группы astra17 / astra18 / app / db
    ├── group_vars/all.yml         общие переменные
    └── playbooks/
        ├── smoke.yml              проверка связности и совместимости, ничего не меняет
        ├── astra-inventory.yml    паспорт парка в CSV
        └── example-baseline.yml   пример изменяющего плейбука
```

## Типовые проблемы

| Симптом | Причина и что делать |
|---|---|
| `/usr/bin/python3: not found` на целевом хосте | Python не установлен. Поставьте через `-m raw`, либо укажите путь в `ansible_python_interpreter` |
| `Permissions 0777 for id_ed25519 are too open` | ключ лежит на `/mnt/c`. Перенесите в `~/.ssh` внутри WSL и сделайте `chmod 600` |
| `Ansible is being run in a world writable directory` | проект на `/mnt/c`. Перенесите в домашний каталог Linux |
| `MODULE FAILURE ... SyntaxError` | ansible-core новее, чем понимает Python целевого хоста. Понизьте ветку core |
| `sudo: a password is required` | нет правила `NOPASSWD` — либо добавьте, либо запускайте с `-K` |
| зависает на первом подключении | ждёт подтверждения ключа хоста. Наполните `known_hosts` через `ssh-keyscan` |
| в WSL нет сети или не резолвятся имена | корпоративный VPN подменил маршруты. Проверьте `/etc/resolv.conf` и `/etc/wsl.conf` |
| `wsl --install` требует повышения прав | подсистема не включена. Раздел «Что просить у админов» или вариант B |
