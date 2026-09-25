На хосте Astra Linux docker отказывается запускать контейнер из образа `sha256:99307ab28a49...`:

```
Error response from daemon: directory '/var/lib/docker/overlay2/edaeedd5.../merged' contains vulnerabilities!
[{oval:astra:def:997895486994487483561684888802883 error  Astra Linux - уязвимость в cgal }
 {oval:astra:def:4375253889142617077468908312131 error  Astra Linux - уязвимость в libpgjava }
 {oval:astra:def:4057407953075111658647466628675 error  Astra Linux - уязвимость в linux, linux-5.10, linux-5.15 }
 {oval:astra:def:4056793809313942403693387535939 error  Astra Linux - уязвимость в redis }
 {oval:astra:def:1139791883291237153376263834261059 error  Astra Linux - уязвимость в apache-log4j2 }
 ... (всего 55 записей: cgal ~45, firefox/thunderbird, sox, ruby-rack, libpgjava, redis, linux, apache-log4j2)
]. Image: sha256:99307ab28a49d7bfa4102e3f4961eccb607c7ad9a3aa83c604b14705e6ff0896
```

### Что происходит

Docker из репозитория Astra Linux (сертифицированная сборка) перед стартом контейнера проверяет корневую ФС образа (`.../merged`) по OVAL-базе уязвимостей Astra. Если проверка что-то находит, контейнер не запускается.

**Важная деталь:** второе поле в каждой записи это результат OVAL-проверки, и там везде `error`, а не `true`. В OVAL результат может быть `true` (уязвимость есть), `false` (уязвимости нет), `unknown`, `error`, `not evaluated` или `not applicable`. `error` значит, что проверку **не удалось выполнить**. Демон считает это уязвимостью (fail-closed).

Отсюда главная гипотеза: **образ собран не на Astra Linux** (Debian/Ubuntu/UBI/Alpine и т.п.). OVAL-определения Astra проверяют версии deb-пакетов относительно релиза Astra. В чужом образе нет нужных файлов (`/etc/astra_version`, `/var/lib/dpkg/status` с пакетами Astra) или версии пакетов не сравниваются, поэтому каждая проверка заканчивается `error`.

Косвенно это подтверждает и сам список. Вряд ли в одном образе (например, с Kafka) есть cgal, firefox, thunderbird, sox, ruby-rack, redis и ядро linux одновременно. Скорее всего, это не найденные пакеты, а определения, которые не смогли выполниться.

> Гипотезу нужно подтвердить диагностикой ниже. Названия опций демона и путей к OVAL-базе зависят от версии Astra (1.7.x / 1.8.x) и пакета docker. Сверяйтесь с `man docker` на своём хосте и с документацией Astra (wiki.astralinux.ru).

### Диагностика

**1. Версия ОС, docker и режим безопасности**

```bash
cat /etc/astra_version
cat /etc/os-release
astra-modeswitch get 2>/dev/null        # 0 - Орёл, 1 - Воронеж, 2 - Смоленск
docker version
dpkg -l | grep -iE 'docker|containerd|oval'
```

**2. Какой образ и на чём он собран**

```bash
docker image ls --digests | grep 99307ab28a49
docker image inspect sha256:99307ab28a49d7bfa4102e3f4961eccb607c7ad9a3aa83c604b14705e6ff0896 \
  --format '{{.RepoTags}} {{.Created}} {{.Os}}/{{.Architecture}}'
docker history --no-trunc sha256:99307ab28a49 | head -20
```

Посмотреть базовый дистрибутив и пакетный менеджер внутри образа, не запуская контейнер (запуск всё равно заблокирован):

```bash
cid=$(docker create sha256:99307ab28a49)
docker cp $cid:/etc/os-release - | tar -xO
docker cp $cid:/etc/astra_version - 2>/dev/null | tar -xO || echo "нет /etc/astra_version -> образ не Astra"
docker cp $cid:/var/lib/dpkg/status - 2>/dev/null | tar -xO | grep -c '^Package:' || echo "нет dpkg"
docker cp $cid:/var/lib/rpm - >/dev/null 2>&1 && echo "есть rpm-база (RHEL/UBI-based)"
docker rm $cid
```

Если образ не Astra, гипотеза про `error` почти наверняка верна.

**3. Проверить, есть ли в образе перечисленные пакеты**

```bash
cid=$(docker create sha256:99307ab28a49)
docker cp $cid:/var/lib/dpkg/status - 2>/dev/null | tar -xO \
  | grep -E '^Package: (libcgal|cgal|libpostgresql-jdbc-java|libpgjava|redis|firefox|thunderbird|sox|ruby-rack|liblog4j2-java|linux-image)'
docker rm $cid
```

Пусто (при этом dpkg в образе есть) - значит, срабатывания ложные и дело в самой проверке, а не в пакетах.

Отдельно стоит поискать jar-файлы log4j (приложения на Java вроде Kafka часто несут их с собой, а не ставят пакетом):

```bash
cid=$(docker create sha256:99307ab28a49)
docker export $cid | tar -t | grep -iE 'log4j|postgresql.*\.jar'
docker rm $cid
```

**4. Логи демона: где и чем проверяет**

```bash
journalctl -u docker --since "1 hour ago" | grep -iE 'oval|vulnerab|scan'
```

**5. Найти настройки проверки в своей сборке docker**

Опции у разных сборок называются по-разному, поэтому ищем в конфиге, документации и бинарнике. Сообщение `Error response from daemon` приходит от демона, а не от клиента `docker`, поэтому бинарник демона находим через systemd:

```bash
cat /etc/docker/daemon.json
systemctl cat docker | grep -iE 'ExecStart|Environment'
docker info 2>&1 | grep -iE 'oval|vuln|scan|security|astra'
man docker 2>/dev/null | grep -iE -A3 'oval|vuln'

# бинарник, который реально запущен как демон
DAEMON_BIN=$(readlink -f /proc/$(systemctl show -p MainPID --value docker)/exe)
echo $DAEMON_BIN
strings "$DAEMON_BIN" | grep -iE 'oval|vulnerab' | sort -u | head -50
```

Из вывода `strings` обычно видно имя ключа `daemon.json` и путь к OVAL-файлу.

**6. Актуальность OVAL-базы на хосте**

Путь берётся из шага 5. Обычно это xml-файл из пакета бюллетеней Astra.

```bash
dpkg -S oval 2>/dev/null | head
find / -xdev -iname '*oval*.xml*' 2>/dev/null
ls -la <путь_к_oval_базе>
```

Устаревшая или повреждённая база тоже может давать массовые `error`.

**7. Сверить с образом, который точно работает**

```bash
docker run --rm registry.astralinux.ru/library/astra/ubi18:latest cat /etc/astra_version
```

Если Astra-образ запускается, проверка в целом работает и проблема в конкретном образе. Если тоже падает с `error`, проблема в базе или конфигурации демона на хосте.

### Сравнение dev (ошибка есть) и test (ошибки нет)

Оба хоста на Astra Linux.

| | dev | test |
|---|---|---|
| Ошибка OVAL при запуске | да | нет |
| `/etc/docker/daemon.json` | `insecure-registries: [proxy-nexus.ru, nexus.ru]`, `log-opts` (max-file 3, max-size 100m) | `astra-sec-level: 6`, `live-restore: true` |
| `/usr/share/openscap/cpe/openscap-cpe-oval.xml` | 102K, 26 янв 2023 | 102K, 26 янв 2023 |

Наблюдения:

- **`openscap-cpe-oval.xml` не причина**, если файлы действительно одинаковые (проверить `sha256sum`, а не только размер и дату). Это словарь CPE: по нему OpenSCAP определяет платформу. Самих определений уязвимостей Astra (`oval:astra:def:...`) в нём нет, они лежат в другом файле. Его нужно найти (ниже) и сравнить.
- **`astra-sec-level` есть только на test.** Этот параметр Astra-сборки docker задаёт уровень целостности (МКЦ) для контейнеров. Гипотеза: от него или от связанного режима зависит, как демон проверяет образы. Не подтверждено, проверить опытом (ниже).
- **`insecure-registries` есть только на dev.** Образ на dev мог прийти через `proxy-nexus.ru` и отличаться от образа на test. Проверить, что digest совпадает.
- `live-restore` и `log-opts` на проверку образов не влияют.

**Собрать одинаковый срез с обоих хостов и сравнить**

Запустить на dev и на test:

```bash
H=$(hostname -s); OUT=~/docker-compare-$H.txt
{
  echo "== astra";      cat /etc/astra_version; astra-modeswitch get 2>/dev/null
  echo "== mic";        cat /sys/module/parsec/parameters/max_ilev 2>/dev/null; astra-mic-control status 2>/dev/null
  echo "== docker";     docker version --format '{{.Client.Version}} / {{.Server.Version}}'
  echo "== packages";   dpkg -l | awk '/^ii/ && $2 ~ /docker|containerd|runc|openscap|oval|astra-sec|parsec/ {print $2, $3}'
  echo "== daemon.json"; cat /etc/docker/daemon.json
  echo "== unit";       systemctl cat docker | grep -iE 'ExecStart|Environment'
  echo "== drop-ins";   ls -la /etc/systemd/system/docker.service.d/ 2>/dev/null
  echo "== info";       docker info 2>/dev/null | grep -iE 'security|astra|storage driver|cgroup'
  echo "== oval files"
  find / -xdev \( -iname '*oval*' -o -iname '*astra*bulletin*' \) -type f 2>/dev/null \
    | xargs -r ls -la --time-style=long-iso
  find / -xdev -iname '*oval*' -type f 2>/dev/null | xargs -r sha256sum
  echo "== image";      docker image ls --digests --no-trunc | grep -E '99307ab28a49|IMAGE'
} > "$OUT" 2>&1
echo "$OUT"
```

Затем свести файлы на одну машину и сравнить:

```bash
diff -u docker-compare-dev.txt docker-compare-test.txt
```

На что смотреть в diff:

1. **Версия пакета docker** (`docker.io` или аналог). Если на dev сборка новее, проверку по OVAL могли добавить или включить по умолчанию именно в ней.
2. **Файлы OVAL-базы уязвимостей.** Разные даты или хеши, либо базы нет на test.
3. **Digest образа.** Если отличается, на хостах разные образы и сравнение некорректно.
4. **Режим ОС и МКЦ** (`astra-modeswitch`, `max_ilev`).
5. **Drop-in'ы systemd** с дополнительными флагами демона.

**Результат сравнения (начало diff)**

`crm-tst-brkr01` это test (ошибки нет), `kfk-dev-al-qm01` это dev (ошибка есть):

| Пакет | test | dev |
|---|---|---|
| `oval-db` | **1.2.2+ci2** | **0.0.2.astra1+ci3** |
| `docker.io` | 25.0.5.astra2+**ci6** | 25.0.5.astra2+**ci5** |
| compose | `docker-compose-v2` 29.1.2.astra1+ci5 | `docker-compose` 1.29.2-1astra.se1+ci1 |

Кроме того, строка `Version: 5.0.2.astra1` (строка 40 среза) есть только на test. Нужно уточнить, к какому разделу среза она относится. Строки 49-211 отличаются целиком: это список OVAL-файлов и их хеши, что ожидаемо при разных версиях `oval-db`.

Вывод:

- **Основной кандидат — сильно устаревшая OVAL-база на dev** (`oval-db` 0.0.2 против 1.2.2). Демон проверяет образ по определениям из этого пакета. Старые определения, скорее всего, не выполняются на современных образах или устарели по формату, отсюда сплошные `error`.
- **Второй кандидат — сборка `docker.io` на dev на одну ci-ревизию старше** (ci5 против ci6). В ci6 могли исправить обработку результата `error`. Сборки ci6 в репозиториях dev нет, поэтому сначала обновляется только `oval-db`.
- Compose на запуск контейнеров демоном не влияет, но его лучше привести к одной версии, чтобы окружения не расходились.
- `astra-sec-level` отходит на второй план. Опыт с ним делать, только если обновление пакетов не поможет.

**Обновление `oval-db` на dev**

`apt policy` на dev:

| Пакет | Установлен | Доступно в репозиториях dev |
|---|---|---|
| `oval-db` | 0.0.2.astra1+ci3 | **1.2.2+ci2** (repository-update, как на test), 1.1.0.astra1+ci8 (repository-base) |
| `docker.io` | 25.0.5.astra2+ci5 | 29.5.1.astra1+ci1b1 (update), 29.1.2.astra1+ci5 (base), 20.10.2 (main). **Версии 25.0.5.astra2+ci6, как на test, нет** |

Первая попытка `sudo apt install oval-db=1.2.2+ci2 docker.io=25.0.5.astra2+ci6` закончилась ошибкой `E: Версия «25.0.5.astra2+ci6» для «docker.io» не найдена`. apt отменяет всю команду, если не нашёл хотя бы одну версию, поэтому `oval-db` тоже не обновился.

Поэтому обновляем **только `oval-db`**, а `docker.io` не трогаем.

**Внимание:** кандидат для `docker.io` в репозиториях dev — 29.5.1, то есть смена мажорной версии. Команды `apt upgrade`, `apt full-upgrade` и `apt install --only-upgrade docker.io` без явной версии обновят docker до 29.x. Так делать нельзя без отдельной проверки.

1. Посмотреть, что потянет за собой новая версия `oval-db` (пробный прогон, ничего не ставит):

```bash
apt-cache show oval-db=1.2.2+ci2 | grep -E '^(Version|Depends|Breaks|Conflicts)'
apt-cache rdepends --installed oval-db
sudo apt install -s oval-db=1.2.2+ci2 | grep -E '^(Inst|Remv)'
```

Если в выводе `-s` только `Inst oval-db`, можно ставить. Если там же `docker.io` (обновление до 29.x) или удаление пакетов, остановиться и разобраться с зависимостями.

2. Обновить `oval-db` (в окно работ, перезапуск docker останавливает контейнеры):

```bash
sudo apt install oval-db=1.2.2+ci2
dpkg -l oval-db docker.io | awk '/^ii/ {print $2, $3}'
sudo systemctl restart docker
cid=$(docker create sha256:99307ab28a49) && docker start $cid && echo "OK: контейнер стартовал"
docker rm -f $cid
```

3. Если контейнер стартовал, причина была в устаревшей OVAL-базе. Закрепить `docker.io`, чтобы общий `apt upgrade` не поднял его до 29.x:

```bash
sudo apt-mark hold docker.io
```

4. Если ошибка осталась, выяснить на **test**, откуда там `docker.io 25.0.5.astra2+ci6`:

```bash
apt policy docker.io
grep -rhE '^deb ' /etc/apt/sources.list /etc/apt/sources.list.d/
ls -la /var/cache/apt/archives/docker.io_*.deb 2>/dev/null
```

Если ci6 пришёл из другого репозитория, подключить его на dev. Если пакет ставили вручную из .deb, перенести этот .deb на dev и поставить через `sudo apt install ./docker.io_25.0.5.astra2+ci6_amd64.deb`. Обновление на 29.x рассматривать отдельно: это смена мажорной версии, и её нужно сначала проверить на test.

### Новая ошибка на dev после обновления `oval-db`: permission denied в overlay2

Ошибки OVAL больше нет. Теперь контейнер не создаётся: демон, работающий от root, не может писать в собственный каталог `/var/lib/docker/overlay2`.

Сразу после старта демона (ScanService проверяет уже скачанные образы):

```
dockerd_audit: ScanService: ... ScanService.DirectoryProvider: failed to create container with image 'sha256:99307ab28a49...':
  open /var/lib/docker/overlay2/0f7216ee...-init/merged/etc/resolv.conf: permission denied
dockerd_audit: ScanService: ... DirectoryProvider: empty container handler
dockerd_audit: ScanService: ... failed to create container with image 'sha256:ffb6864bc6f8...':
  open /var/lib/docker/overlay2/6eb349d9...-init/merged/etc/hosts: permission denied
```

При `docker create` от пользователя:

```
dockerd.audit|1021|<user>|...|container.create|failed|id=N/A|mkdir /var/lib/docker/overlay2/33c8b42c...-init/merged/dev/shm: permission denied
dockerd: level=error msg="Handler for POST /v1.44/containers/create returned error: mkdir .../-init/merged/dev/pts: permission denied"
```

Что из этого следует:

- **Сама проверка по OVAL теперь проходит дальше.** Ошибка возникает раньше, при подготовке init-слоя контейнера (`-init/merged`: `/etc/hosts`, `/etc/resolv.conf`, `/dev/shm`, `/dev/pts`). Этот слой создаётся заново для каждого контейнера.
- **Отказ получает сам демон, а не только пользователь.** ScanService работает внутри dockerd сразу после старта, без участия пользователя, и тоже получает `permission denied`. Значит, дело не в правах пользователя на сокет и не в группе `docker`.
- **root получает `permission denied`.** Обычные права Unix root'у так не отказывают. На Astra Linux 1.7 такой отказ обычно даёт мандатная защита: МКЦ (контроль целостности) или МРД (PARSEC). Уровень целостности процесса dockerd ниже метки каталогов в `/var/lib/docker`, или метки каталогов не совпадают с тем, что ожидает демон.
- Раньше контейнер на dev создавался (доходил до проверки OVAL и падал на ней). Значит, между двумя запусками что-то поменялось. Кандидаты:
  1. `daemon.json` на dev (например, уже добавлен `astra-sec-level: 6` для опыта ниже);
  2. пакеты, которые поставились вместе с `oval-db` (зависимости);
  3. уровень целостности, с которым systemd запускает dockerd после перезапуска;
  4. метки на `/var/lib/docker`.

**Диагностика**

1. Что изменилось: конфиг и установленные пакеты.

```bash
sudo cat /etc/docker/daemon.json
grep -E ' (install|upgrade|remove) ' /var/log/dpkg.log | tail -30
dpkg -l oval-db docker.io containerd runc | awk '/^ii/ {print $2, $3}'
```

2. Режим ОС и МКЦ (сравнить с test):

```bash
astra-modeswitch get
astra-mic-control status 2>/dev/null
cat /sys/module/parsec/parameters/max_ilev 2>/dev/null
```

3. Мандатные метки процесса dockerd и каталогов docker (сравнить с test):

```bash
PID=$(systemctl show -p MainPID --value docker)
sudo pdp-ps -p $PID 2>/dev/null || sudo cat /proc/$PID/attr/current 2>/dev/null
sudo pdp-ls -Md /var/lib/docker /var/lib/docker/overlay2
sudo pdp-ls -M /var/lib/docker/overlay2 | head -20
sudo getfattr -d -m - /var/lib/docker/overlay2 2>/dev/null
```

4. Метка текущей сессии пользователя (пользователя, от которого запускается `docker create`):

```bash
pdp-id
```

5. Отказы в журнале ядра и аудите PARSEC в момент `docker create`:

```bash
sudo dmesg -T | grep -iE 'parsec|denied|ilev|mic' | tail -30
sudo journalctl -k --since "10 min ago" | grep -iE 'parsec|denied' | tail -30
```

6. Обычные права и ФС (для исключения причины):

```bash
sudo ls -ld /var/lib/docker /var/lib/docker/overlay2
findmnt -T /var/lib/docker
df -h /var/lib/docker; df -i /var/lib/docker
```

**Варианты решения (выбрать по результатам диагностики)**

- **В `daemon.json` на dev появился `astra-sec-level`**, а режим МКЦ на dev отличается от test: убрать параметр (вернуть конфиг из `.bak`) и перезапустить docker. Или привести режим ОС и МКЦ на dev к режиму test.
- **Метки на `/var/lib/docker` не совпадают с test:** выровнять их по образцу test через `pdpl-file`, только по согласованию с ИБ. Конкретные значения меток брать с test (шаг 3), не угадывать.
- **dockerd запускается с более низким уровнем целостности, чем на test:** сравнить `systemctl cat docker` и drop-in'ы в `/etc/systemd/system/docker.service.d/` на обоих хостах.
- **Проблема появилась именно из-за нового `oval-db` или его зависимостей:** откатить. Версии 0.0.2.astra1+ci3 в репозиториях нет, поэтому смотреть старый .deb в кэше apt:

```bash
ls /var/cache/apt/archives/oval-db_*
sudo apt install /var/cache/apt/archives/oval-db_0.0.2.astra1+ci3_*.deb
```

### Ошибка на dev при `docker load`: `database not exists in /usr/share/oval/db.xml`

На dev удалены все контейнеры и образы, образ загружается заново:

```
$ docker load < ~/08.containers/dockge-latest.tar.gz
1287fbecdfcc: Loading layer [=================================================> ]  76.32MB/77.84MB
database not exists in /usr/share/oval/db.xml
```

Что из этого следует:

- Проверка образа запускается уже на `docker load`, и демон ищет OVAL-базу по жёсткому пути `/usr/share/oval/db.xml`. **Файла там нет.**
- Путь к базе задан в сборке `docker.io` 25.0.5.astra2+**ci5**. Скорее всего, новый `oval-db` 1.2.2+ci2 кладёт базу в другое место или под другим именем (сжатую, в `/var/lib/...` и т.п.). Её могут также ставить отдельным шагом (postinst, служба обновления). На test та же версия `oval-db` работает в паре с `docker.io` **ci6**. Значит, пакеты на dev сейчас **несовместимы**: новый `oval-db` при старом `docker.io`.
- Эта же несовместимость может объяснять и `permission denied` из предыдущего раздела: сканер не может нормально подготовить проверку.

**Диагностика: куда `oval-db` кладёт базу и где её ищет docker (на dev и test)**

```bash
# что ставит пакет
dpkg -L oval-db
ls -la /usr/share/oval/ 2>/dev/null
dpkg -L oval-db | grep -iE '\.(xml|gz|bz2|xz|zst)$' | xargs -r ls -la

# скрипты пакета: не генерируется ли db.xml при установке
ls /var/lib/dpkg/info/oval-db.*
cat /var/lib/dpkg/info/oval-db.postinst 2>/dev/null

# службы и таймеры обновления базы
systemctl list-unit-files | grep -iE 'oval'
dpkg -L oval-db | grep -E 'systemd|cron'

# какой путь к базе зашит в демоне
DAEMON_BIN=$(readlink -f /proc/$(systemctl show -p MainPID --value docker)/exe)
strings "$DAEMON_BIN" | grep -iE '/oval|db\.xml' | sort -u
```

Сравнить с test: где там лежит база, есть ли `/usr/share/oval/db.xml` и какой путь зашит в `docker.io` ci6.

**Результат на dev: `/usr/share/oval/` после установки `oval-db` 1.2.2+ci2**

```
drwxrwxr-x 7 root root 4,0K сен 25 22:19 .
drwxr-xr-x 2 root root 4,0K сен 25 22:19 conf
drwxrwxr-x 3 root root 4,0K сен 25 22:19 db
drwxr-xr-x 2 root root 4,0K сен 25 22:19 history
drwxr-xr-x 3 root root 4,0K сен 25 22:19 localization
drwxr-xr-x 2 root root 4,0K сен 25 22:19 scan-whitelist
```

- Файла `db.xml` в корне `/usr/share/oval/` нет. Вместо него **новая структура каталогов**: база в `db/`, отдельно `conf/`, `history/`, `localization/`, `scan-whitelist/`. Все созданы в 22:19, это момент установки пакета.
- Это подтверждает гипотезу: `oval-db` 1.2.x сменил формат и расположение базы, а `docker.io` ci5 ищет старый `/usr/share/oval/db.xml`. Пара ci5 + 1.2.2 несовместима. Сканер ci6 (test), видимо, понимает новую структуру.
- **`scan-whitelist/`** похож на штатный механизм исключений для сканера: его можно было бы использовать вместо отключения проверки. Посмотреть содержимое и формат на test и в документации Astra.

Следующий шаг: посмотреть содержимое каталогов на dev и test:

```bash
sudo ls -laR /usr/share/oval/db /usr/share/oval/conf /usr/share/oval/scan-whitelist
sudo head -50 /usr/share/oval/conf/* 2>/dev/null
ls -la /usr/share/oval/db.xml 2>/dev/null || echo "db.xml нет"
```

На test дополнительно проверить, есть ли там `/usr/share/oval/db.xml` (если есть, возможно, это ссылка) и что в `conf/`.

**Результат на dev: содержимое `/usr/share/oval/`**

Подтверждено, что вывод с dev. Даты внутри каталогов (31 авг, 4 авг, 11 сен) — это даты из самого пакета: dpkg при распаковке сохраняет время изменения файлов из архива. Поэтому по датам нельзя судить, когда пакет ставился и правили ли файлы. Почему `ls -la /usr/share/oval/` ранее показал 25 сен 22:19 у тех же каталогов, пока не ясно, на вывод это не влияет.

```
/usr/share/oval/conf:
-rw-r--r-- 1 root root   353 сен 11 15:57 docker.json
-rw-r--r-- 1 root root   505 авг  4 13:59 podman.json

/usr/share/oval/db/astra/1.7_x86-64:
-rwxrwxr-x 1 root root   579976 авг  4 13:59 CriticalSeverity.xml
-rwxrwxr-x 1 root root 41320543 авг  4 13:59 HighSeverity.xml
-rwxrwxr-x 1 root root  3140639 авг  4 13:59 LowSeverity.xml
-rwxrwxr-x 1 root root     1041 авг  4 13:59 manifest.json
-rwxrwxr-x 1 root root 55403250 авг  4 13:59 MediumSeverity.xml
-rwxrwxr-x 1 root root  8321047 авг  4 13:59 NoneSeverity.xml

/usr/share/oval/db/astra/1.8_x86-64:
  (тот же набор файлов, меньшего размера)

/usr/share/oval/scan-whitelist:
-rw-r--r-- 1 root root 36864 авг 31 11:51 scan-whitelist.db
```

Наблюдения:

- **Формат базы в 1.2.x:** база разбита по версии Astra (`1.7_x86-64`, `1.8_x86-64`) и по критичности (`Critical/High/Medium/Low/NoneSeverity.xml`), плюс `manifest.json`. Единого `db.xml` больше нет, поэтому `docker.io` ci5 со старым путём `/usr/share/oval/db.xml` с этой базой работать не может.
- **`conf/docker.json` (11 сен 15:57)** — настройки сканера для docker. Дата, скорее всего, из пакета, а не след ручной правки (проверить через `dpkg --verify oval-db`). Нужно сравнить содержимое с test: если на test файл отличается (порог критичности, режим «только предупреждать» и т.п.), **это может быть причиной, почему на test ошибки нет**.
- **`scan-whitelist/scan-whitelist.db`** (36864 байт, кратно странице SQLite) — вероятно, SQLite-база исключений сканера. Если на test в ней есть записи, это тоже объясняет отсутствие ошибки.

Следующий шаг (на обоих хостах):

```bash
sudo cat /usr/share/oval/conf/docker.json
sudo cat /usr/share/oval/db/astra/1.7_x86-64/manifest.json
dpkg -S /usr/share/oval/conf/docker.json
sudo dpkg --verify oval-db             # покажет файлы пакета, изменённые вручную (5 = изменён хеш)
file /usr/share/oval/scan-whitelist/scan-whitelist.db
sudo sqlite3 -readonly /usr/share/oval/scan-whitelist/scan-whitelist.db '.tables'
sudo sqlite3 -readonly /usr/share/oval/scan-whitelist/scan-whitelist.db '.schema'
```

Если `docker.json` или `scan-whitelist.db` на test отличаются от dev, записать сюда, что именно отличается и кем согласовано. Это будет готовое решение для dev вместе с согласованной парой пакетов.

**Результат на test: `/usr/share/oval/` совпадает с dev полностью** (те же файлы, размеры и даты, включая `conf/docker.json` и `scan-whitelist.db`). `db.xml` нет и на test.

Вывод:

- **OVAL-база и её настройки на обоих хостах одинаковые.** Остаётся одно отличие — сборка `docker.io`: на test **ci6** работает с новой структурой `/usr/share/oval/db/astra/<версия>/*Severity.xml`, на dev **ci5** ищет старый `/usr/share/oval/db.xml` и не находит его.
- **Решение для dev: поставить `docker.io` 25.0.5.astra2+ci6**, как на test (см. «Обновление `oval-db` на dev», шаг 4: выяснить на test, откуда пакет). Ссылка `db.xml` не поможет: формат базы другой.
- Совпадение по размеру и дате ещё не доказывает одинаковое содержимое. Для контроля сверить хеши:

```bash
sudo find /usr/share/oval -type f -exec sha256sum {} + | sort -k2 > ~/oval-$(hostname -s).sha256
diff ~/oval-<dev>.sha256 ~/oval-<test>.sha256
```

**Варианты решения**

1. **Привести пару пакетов на dev к паре на test** (`oval-db` 1.2.2+ci2 + `docker.io` 25.0.5.astra2+ci6). Это правильный путь. ci6 в репозиториях dev нет, поэтому сначала выяснить на test, откуда он взялся (см. «Обновление `oval-db` на dev», шаг 4), и перенести пакет или репозиторий.
2. **Откатить `oval-db` на dev до 0.0.2.astra1+ci3.** Вернётся исходная ошибка OVAL, но docker снова будет работать в согласованной паре с ci5. Старая версия есть только в кэше apt, если он не чистился:

   ```bash
   ls /var/cache/apt/archives/oval-db_*
   sudo apt install /var/cache/apt/archives/oval-db_0.0.2.astra1+ci3_*.deb
   sudo systemctl restart docker
   ```

   Если .deb в кэше нет, взять его с другого хоста с этой версией (`dpkg-repack oval-db` на нём) или из архива репозитория Astra.
3. **Промежуточный обходной путь, только для проверки гипотезы.** Если новый `oval-db` ставит базу в другое место и формат совместим, можно сделать ссылку:

   ```bash
   sudo mkdir -p /usr/share/oval
   sudo ln -s <путь_к_базе_из_dpkg -L> /usr/share/oval/db.xml
   sudo systemctl restart docker
   ```

   Это не решение: dpkg про эту ссылку не знает, а формат базы 1.2.2 может не подходить сканеру ci5. Для dev допустимо, чтобы подтвердить причину, дальше всё равно вариант 1.
4. **Обновить `docker.io` на dev до 29.x из репозитория** (там `oval-db` 1.2.2 и docker, вероятно, согласованы). Это смена мажорной версии, поэтому сначала проверить на test и согласовать.

### Обновление `docker.io` на dev до ci6 (как на test)

Цель: на dev `docker.io 25.0.5.astra2+ci6`, как на test. Версии ci6 в репозиториях dev нет, поэтому пакет берётся с test.

**Шаг 1. На test: откуда пакет и с чем он работает**

```bash
apt policy docker.io                      # источник ci6: репозиторий или только /var/lib/dpkg/status
grep -rhE '^deb ' /etc/apt/sources.list /etc/apt/sources.list.d/
ls -la /var/cache/apt/archives/docker.io_* 2>/dev/null
grep -E ' (install|upgrade) docker.io' /var/log/dpkg.log* 2>/dev/null
zgrep -E ' (install|upgrade) docker.io' /var/log/dpkg.log.*.gz 2>/dev/null
dpkg -s docker.io | grep -E '^(Version|Depends|Pre-Depends|Breaks|Conflicts)'
dpkg -l containerd runc tini docker-buildx docker-compose-v2 2>/dev/null | awk '/^ii/ {print $2, $3}'
```

По результату выбрать способ получения пакета.

**Результат на test: `apt policy docker.io`**

```
docker.io:
  Установлен: 25.0.5.astra2+ci6
  Кандидат:   29.5.1.astra1+ci1b1
  Таблица версий:
     29.5.1.astra1+ci1b1 900  .../1.7_x86-64/repository-update
     29.1.2.astra1+ci5   900  .../1.7_x86-64/repository-base
 *** 25.0.5.astra2+ci6   100  /var/lib/dpkg/status
     20.10.2+dfsg1-2astra.se6 900  .../1.7_x86-64/repository-main
```

- ci6 есть только в `/var/lib/dpkg/status`: **ни в одном подключённом репозитории test её сейчас нет**. Скорее всего, её ставили из `repository-update`, когда там была эта версия, а потом зеркало обновилось до 29.5.1. Способ A (подключить репозиторий) отпадает.
- Репозитории на test и dev одинаковые (то же зеркало `astra-repo.mcb.ru`, те же версии).
- Остаются способы: взять `.deb` из кэша apt на test (B), найти ci6 в пуле зеркала или во frozen-репозитории Astra, либо сделать `dpkg-repack` (C).

Проверить на test, есть ли `.deb` в кэше:

```bash
ls -la /var/cache/apt/archives/docker.io_*
```

Поискать ci6 в пуле зеркала: в каталоге `pool` иногда остаются старые версии, даже если в индексе их уже нет.

```bash
for r in repository-update repository-base; do
  curl -s https://astra-repo.mcb.ru/dl.astralinux.ru/astra/stable/1.7_x86-64/$r/pool/main/d/docker.io/ \
    | grep -oE 'docker\.io_[^"<]+\.deb' | sort -u
done
```

Если в пуле есть `docker.io_25.0.5.astra2+ci6_amd64.deb`, скачать его на dev (`curl -O`, `wget`). Это оригинальный подписанный пакет Astra, поэтому такой вариант лучше, чем `dpkg-repack`. Если нет, спросить у администраторов зеркала или посмотреть frozen-репозитории Astra для версии 1.7.x, установленной на test (`cat /etc/astra_version`).

Кэш apt на test пуст (проверено 25.09.2026), поэтому способ B отпадает.

**Если в пуле ci6 нет: `dpkg-repack` на test**

`dpkg-repack` собирает `.deb` из установленных файлов пакета. В пакет попадут текущие версии конфигурационных файлов (conffiles). Поэтому сначала проверить, что файлы пакета на test не изменены:

```bash
# на test
sudo dpkg --verify docker.io          # пустой вывод = файлы как в оригинальном пакете
dpkg-query -W -f='${Conffiles}\n' docker.io
apt policy dpkg-repack
sudo apt install dpkg-repack
mkdir -p ~/repack && cd ~/repack && sudo dpkg-repack docker.io
ls -la ~/repack
dpkg -I ~/repack/docker.io_25.0.5.astra2+ci6_amd64.deb | grep -E 'Version|Depends'
```

На dev перед установкой так же собрать пакет текущей ci5 для отката:

```bash
# на dev
sudo apt install dpkg-repack
mkdir -p ~/repack && cd ~/repack && sudo dpkg-repack docker.io   # docker.io_25.0.5.astra2+ci5_amd64.deb
```

Затем перенести `.deb` ci6 с test на dev и выполнить шаги 3–5 ниже. Пакет без подписи Astra: в сертифицированном контуре согласовать с ИБ.

**Шаг 2. Получить пакет ci6**

- **A. В `apt policy` на test ci6 идёт из репозитория** (например, другое обновление Astra или другое зеркало): подключить этот же репозиторий на dev, сделать `apt update` и проверить `apt policy docker.io`, что `25.0.5.astra2+ci6` появился.
- **B. На test в кэше apt есть `.deb`:** скопировать его на dev.

  ```bash
  # на test
  scp /var/cache/apt/archives/docker.io_25.0.5.astra2+ci6_amd64.deb <dev>:/tmp/
  ```

- **C. `.deb` нигде нет:** пересобрать пакет из установленных файлов на test через `dpkg-repack`. Это крайний вариант: у пакета не будет подписи Astra, в сертифицированном контуре это согласовать с ИБ.

  ```bash
  # на test
  sudo apt install dpkg-repack
  cd /tmp && sudo dpkg-repack docker.io
  scp /tmp/docker.io_25.0.5.astra2+ci6_amd64.deb <dev>:/tmp/
  ```

**Шаг 3. На dev: подготовка**

```bash
# зависимости нового пакета и что поменяется (ничего не ставит)
dpkg -I /tmp/docker.io_25.0.5.astra2+ci6_amd64.deb | grep -E 'Version|Depends|Breaks|Conflicts'
sudo apt install -s /tmp/docker.io_25.0.5.astra2+ci6_amd64.deb | grep -E '^(Inst|Remv)'   # для варианта A: sudo apt install -s docker.io=25.0.5.astra2+ci6

# резервная копия конфигурации
sudo cp -a /etc/docker /etc/docker.bak.$(date +%F)
sudo cp -a /usr/share/oval/conf /root/oval-conf.bak.$(date +%F)
```

В выводе `-s` должно быть только `Inst docker.io [25.0.5.astra2+ci5] (25.0.5.astra2+ci6 ...)`. Если apt хочет поставить docker.io 29.x, обновить или удалить `containerd`/`runc` и т.п., остановиться и сверить версии зависимостей с test (шаг 1).

**Шаг 4. На dev: установка (в окно работ, docker перезапустится)**

```bash
sudo apt-mark unhold docker.io 2>/dev/null
sudo apt install /tmp/docker.io_25.0.5.astra2+ci6_amd64.deb     # вариант A: sudo apt install docker.io=25.0.5.astra2+ci6
sudo apt-mark hold docker.io                                     # чтобы apt upgrade не поднял до 29.x
dpkg -l docker.io oval-db | awk '/^ii/ {print $2, $3}'
sudo systemctl restart docker
systemctl is-active docker
```

**Шаг 5. Проверка**

```bash
docker load < ~/08.containers/dockge-latest.tar.gz
docker image ls
cid=$(docker create <образ>) && docker start $cid && echo "OK: контейнер стартовал"
docker rm -f $cid
sudo journalctl -u docker --since "10 min ago" | grep -iE 'ScanService|oval|permission denied|error'
```

- `docker load` проходит и контейнер стартует: проблема решена, записать итог.
- Снова `permission denied` в `overlay2`: вернуться к разделу про permission denied (МКЦ, метки `/var/lib/docker`).
- Ошибка OVAL с реальными уязвимостями (`true`, а не `error`): это уже настоящие находки, см. «Варианты решения» (пересборка образа, `scan-whitelist`).

**Откат**

```bash
sudo apt-mark unhold docker.io
sudo apt install docker.io=25.0.5.astra2+ci5   # если ci5 есть в кэше apt: sudo apt install /var/cache/apt/archives/docker.io_25.0.5.astra2+ci5_amd64.deb
sudo cp -a /etc/docker.bak.<дата>/. /etc/docker/
sudo systemctl restart docker
```

Перед установкой убедиться, что `.deb` ci5 для отката доступен: `ls /var/cache/apt/archives/docker.io_*`. Если его нет, заранее сделать `sudo dpkg-repack docker.io` на dev.

**Опыт с `astra-sec-level` на dev (если обновление пакетов не помогло)**

Проверить, что проверка зависит от этого параметра. Делать в окно работ: перезапуск docker остановит контейнеры на dev.

```bash
sudo cp /etc/docker/daemon.json /etc/docker/daemon.json.bak.$(date +%F)
sudo tee /etc/docker/daemon.json >/dev/null <<'EOF'
{
  "insecure-registries": ["proxy-nexus.ru", "nexus.ru"],
  "log-opts": { "max-file": "3", "max-size": "100m" },
  "astra-sec-level": 6
}
EOF
python3 -m json.tool /etc/docker/daemon.json >/dev/null && sudo systemctl restart docker
docker info >/dev/null
cid=$(docker create sha256:99307ab28a49) && docker start $cid && echo "OK: контейнер стартовал"
docker rm -f $cid
journalctl -u docker --since "5 min ago" | grep -iE 'oval|vulnerab|sec-level|error'
```

- Контейнер запустился: причина в отличии конфигурации. Решение привести dev к конфигурации test и согласовать это с ИБ.
- Ошибка осталась: откатить (`sudo cp /etc/docker/daemon.json.bak.<дата> /etc/docker/daemon.json && sudo systemctl restart docker`) и искать отличие в версиях пакетов и OVAL-базе по diff.

> `astra-sec-level` работает только если на хосте включён соответствующий режим защиты (МКЦ). Если демон не стартует с этим параметром, смотреть `journalctl -u docker` и сравнивать режим ОС с test.

### Варианты решения

**1. Пересобрать образ на базовом образе Astra (рекомендуется)**

Это правильный путь для сертифицированного контура: проверка пройдёт честно, и отключать защиту не придётся.

```dockerfile
FROM registry.astralinux.ru/library/astra/ubi18:latest   # или ubi17 под Astra 1.7
RUN apt-get update && apt-get dist-upgrade -y && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    rm -rf /var/lib/apt/lists/*
# далее дистрибутив приложения (например, Kafka) и entrypoint
```

После сборки запустить контейнер. Если остались настоящие `true`, обновить пакеты, перечисленные в выводе.

**2. Образ уже Astra-based, но проверка находит уязвимости**

- Обновить пакеты в образе (`apt-get update && apt-get dist-upgrade`) и пересобрать.
- Удалить из образа лишние пакеты (cgal, firefox и т.п. в серверном образе не нужны).
- Базовый образ должен соответствовать версии Astra на хосте (1.7 / 1.8).

**3. Обновить OVAL-базу на хосте**

Если база устарела или повреждена (диагностика, шаг 6), обновить пакет `oval-db` из репозитория Astra и перезапустить docker. Для dev это основной вариант: `oval-db` 0.0.2 против 1.2.2 на test (см. раздел «Сравнение dev и test»).

```bash
sudo apt update && sudo apt install --only-upgrade oval-db   # docker.io не трогать: кандидат 29.x
sudo systemctl restart docker
```

**4. Ослабить или отключить проверку в демоне (только по согласованию с ИБ)**

Если образ сторонний, пересобрать его нельзя, а риск принят, можно отключить проверку или перевести её в режим предупреждения. Ключ в `/etc/docker/daemon.json` берётся из шага 5 диагностики, названия у сборок Astra отличаются. Затем:

```bash
sudo cp /etc/docker/daemon.json /etc/docker/daemon.json.bak.$(date +%F)
sudo vi /etc/docker/daemon.json       # ключ из шага 5
python3 -m json.tool /etc/docker/daemon.json >/dev/null && echo "JSON ok"
sudo systemctl restart docker
docker info >/dev/null && echo "docker поднялся"
```

**Риски:** отключение проверки нарушает сертифицированную конфигурацию (ФСТЭК) и действует на все образы на хосте, а не только на этот. Решение фиксировать документально. Перезапуск docker останавливает все контейнеры без `--restart`/`live-restore`, поэтому делать в окно работ.

**5. Использовать другой образ**

Если есть сертифицированная или Astra-based сборка нужного ПО (реестр `registry.astralinux.ru`, внутренний реестр организации), использовать её вместо стороннего образа.

### Журнал диагностики

Сюда записываем фактические результаты команд.

| Дата | Шаг | Команда / действие | Результат |
|------|-----|--------------------|-----------|
|      | 1   | `cat /etc/astra_version`, `docker version` |  |
|      | 2   | базовый дистрибутив образа |  |
|      | 3   | есть ли пакеты из списка |  |
|      | 5   | имя опции проверки в daemon.json |  |
|      | 6   | дата/версия OVAL-базы |  |
|      | 7   | запуск эталонного Astra-образа |  |
|      | dev/test | `daemon.json` | dev: insecure-registries + log-opts; test: `astra-sec-level: 6`, `live-restore` |
|      | dev/test | `openscap-cpe-oval.xml` | одинаковые по размеру и дате (102K, 26.01.2023), sha256 не сверен |
|      | dev/test | diff срезов | `oval-db`: test 1.2.2+ci2, dev 0.0.2.astra1+ci3; `docker.io`: test ci6, dev ci5; compose: v2 29.1.2 / 1.29.2; `Version: 5.0.2.astra1` только на test (уточнить, откуда) |
|      | dev | `apt install oval-db=1.2.2+ci2 docker.io=25.0.5.astra2+ci6` | не выполнено: версии docker.io ci6 нет в репозиториях dev, кандидат 29.5.1 |
| 25.09.2026 | dev | обновление только `oval-db` до 1.2.2+ci2 | ошибка OVAL ушла; новая ошибка: `permission denied` при создании init-слоя в `/var/lib/docker/overlay2` (ScanService при старте и `docker create`) |
|      | dev | что изменилось: `daemon.json`, `dpkg.log` |  |
|      | dev/test | МКЦ: `astra-modeswitch`, `astra-mic-control`, метки dockerd и `/var/lib/docker` |  |
|      | dev | `dmesg` / `journalctl -k` в момент `docker create` |  |
| 25.09.2026 | dev | удалены все контейнеры и образы, `docker load < dockge-latest.tar.gz` | `database not exists in /usr/share/oval/db.xml` |
| 25.09.2026 | dev | `ls -la /usr/share/oval/` | `db.xml` нет; новая структура: `conf/`, `db/`, `history/`, `localization/`, `scan-whitelist/` (создано 22:19 при установке `oval-db` 1.2.2) |
| 25.09.2026 | dev | `ls -laR /usr/share/oval/{db,conf,scan-whitelist}` | база по версиям Astra и критичности (`*Severity.xml` + `manifest.json`), `conf/docker.json`, `conf/podman.json`, `scan-whitelist.db` (SQLite?); `db.xml` нет |
| 25.09.2026 | test | то же `ls -laR /usr/share/oval/...` | полностью совпадает с dev: те же файлы, размеры и даты, `db.xml` нет. Разница между хостами — только `docker.io` ci5 (dev) / ci6 (test) |
|      | dev/test | `sha256sum` по `/usr/share/oval` |  |
| 25.09.2026 | test | `apt policy docker.io` | ci6 только в `/var/lib/dpkg/status`, в репозиториях нет (кандидат 29.5.1, base 29.1.2+ci5) |
| 25.09.2026 | dev | `apt policy docker.io` | установлен ci5 (только `dpkg/status`); репозитории и кандидаты те же, что на test |
| 25.09.2026 | test | `ls /var/cache/apt/archives/docker.io_*` | пусто, `.deb` в кэше нет |
|      | test | пул зеркала на `docker.io_25.0.5.astra2+ci6` |  |
|      | test | `dpkg-repack docker.io` (если в пуле нет) |  |
|      | dev | установка `docker.io` ci6, `apt-mark hold`, `docker load` |  |
|      | dev/test | `conf/docker.json`, `manifest.json`, `dpkg --verify oval-db`, таблицы `scan-whitelist.db` |  |
|      | dev/test | путь к базе в бинарнике демона |  |
|      | test | откуда `docker.io 25.0.5.astra2+ci6` (если нужно) |  |
|      | dev | опыт с `astra-sec-level: 6` |  |

### Итог

_Заполнить после диагностики: причина, выбранный вариант, что изменено._
