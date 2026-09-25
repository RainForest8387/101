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
- **Второй кандидат — сборка `docker.io` на dev на одну ci-ревизию старше** (ci5 против ci6). В ci6 могли исправить обработку результата `error`. Поэтому обновлять лучше оба пакета, до версий как на test.
- Compose на запуск контейнеров демоном не влияет, но его лучше привести к одной версии, чтобы окружения не расходились.
- `astra-sec-level` отходит на второй план. Опыт с ним делать, только если обновление пакетов не поможет.

**Обновление `oval-db` и `docker.io` на dev до версий test**

Проверить, что нужные версии доступны в репозиториях dev:

```bash
apt update
apt policy oval-db docker.io
apt-cache show oval-db | grep -E '^(Version|Depends)'
```

Если `1.2.2+ci2` и `25.0.5.astra2+ci6` есть в списке кандидатов, обновить до них (в окно работ, так как перезапуск docker останавливает контейнеры):

```bash
sudo apt install oval-db=1.2.2+ci2 docker.io=25.0.5.astra2+ci6
sudo systemctl restart docker
dpkg -l oval-db docker.io | awk '/^ii/ {print $2, $3}'
cid=$(docker create sha256:99307ab28a49) && docker start $cid && echo "OK: контейнер стартовал"
docker rm -f $cid
```

Если в репозиториях dev этих версий нет, сравнить подключённые репозитории на обоих хостах. Скорее всего, test смотрит на более свежий репозиторий или на другое обновление Astra:

```bash
grep -rhE '^deb ' /etc/apt/sources.list /etc/apt/sources.list.d/
```

Порядок проверки: сначала обновить только `oval-db`, перезапустить docker и попробовать запуск. Так станет понятно, какой пакет виноват. Если не помогло, обновить `docker.io`.

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

Если база устарела или повреждена (диагностика, шаг 6), обновить пакет `oval-db` (и при необходимости `docker.io`) из репозитория Astra и перезапустить docker. Для dev это основной вариант: `oval-db` 0.0.2 против 1.2.2 на test (см. раздел «Сравнение dev и test»).

```bash
sudo apt update && sudo apt install --only-upgrade oval-db docker.io
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
|      | dev | обновление `oval-db` |  |
|      | dev | обновление `docker.io` (если нужно) |  |
|      | dev | опыт с `astra-sec-level: 6` |  |

### Итог

_Заполнить после диагностики: причина, выбранный вариант, что изменено._
