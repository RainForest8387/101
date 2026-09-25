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

> Гипотезу нужно подтвердить диагностикой ниже. Названия опций демона и путей к OVAL-базе зависят от версии Astra (1.7.x / 1.8.x) и пакета docker. Сверяйтесь с `man dockerd` на своём хосте и с документацией Astra (wiki.astralinux.ru).

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

Опции у разных сборок называются по-разному, поэтому ищем прямо в бинарнике и документации:

```bash
dockerd --help 2>&1 | grep -iE 'oval|vuln|scan|astra'
man dockerd | grep -iE -A3 'oval|vuln'
strings $(command -v dockerd) | grep -iE 'oval|vulnerab' | sort -u | head -50
cat /etc/docker/daemon.json
systemctl cat docker | grep -i ExecStart
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

Если база устарела или повреждена (диагностика, шаг 6), обновить соответствующий пакет из репозитория Astra (`apt update && apt install --only-upgrade <пакет_с_oval>`) и перезапустить docker.

**4. Ослабить или отключить проверку в демоне (только по согласованию с ИБ)**

Если образ сторонний, пересобрать его нельзя, а риск принят, можно отключить проверку или перевести её в режим предупреждения. Ключ в `/etc/docker/daemon.json` берётся из шага 5 диагностики, названия у сборок Astra отличаются. Затем:

```bash
sudo cp /etc/docker/daemon.json /etc/docker/daemon.json.bak.$(date +%F)
sudo vi /etc/docker/daemon.json       # ключ из шага 5
sudo dockerd --validate --config-file /etc/docker/daemon.json   # если поддерживается
sudo systemctl restart docker
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
|      | 5   | имя опции проверки в dockerd |  |
|      | 6   | дата/версия OVAL-базы |  |
|      | 7   | запуск эталонного Astra-образа |  |

### Итог

_Заполнить после диагностики: причина, выбранный вариант, что изменено._
