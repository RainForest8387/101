# systemd-units-watcher

Watchdog для Kafka-стека: проверяет, запущены ли systemd-юниты
`zookeeper.service`, `kafka.service`, `schema-registry.service`, и поднимает те,
что лежат. Запускается таймером systemd **по вторникам и пятницам, каждые
15 минут в период с 04:00 до 09:00**.

## Состав

| Файл | Назначение |
|---|---|
| `systemd-units-watcher.sh` | сам скрипт проверки/запуска |
| `systemd/systemd-units-watcher.service` | oneshot-юнит, вызывает скрипт с `--apply` |
| `systemd/systemd-units-watcher.timer` | расписание вт/пт, 04:00–09:00, каждые 15 минут |
| `install.sh` | раскладывает всё по местам и включает таймер |

## Установка

```bash
sudo ./install.sh
```

Скрипт будет размещен по пути `/opt/systemd-units-watcher/`, юниты — в `/etc/systemd/system/`,
таймер будет включён (`enable --now`). Откатить всё: `sudo ./install.sh --uninstall`.

Вручную то же самое:

```bash
sudo install -d /opt/systemd-units-watcher
sudo install -m 0755 systemd-units-watcher.sh /opt/systemd-units-watcher/
sudo install -m 0644 systemd/systemd-units-watcher.{service,timer} /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now systemd-units-watcher.timer
```

## Проверка

```bash
# ближайшие срабатывания
systemctl list-timers systemd-units-watcher.timer

# посчитать расписание, не дожидаясь пятницы
systemd-analyze calendar 'Tue,Fri *-*-* 04..08:00/15:00' --iterations=8

# прогнать проверку прямо сейчас
sudo systemctl start systemd-units-watcher.service
journalctl -u systemd-units-watcher.service -n 50 --no-pager

# что делал watchdog за последнюю неделю
journalctl -t units-watcher --since '1 week ago' --no-pager
```

## Ручной запуск скрипта

По умолчанию скрипт работает в **dry-run**: показывает, что сделал бы, и ничего
не трогает. Реальные действия — только с `--apply` (в `.service` он уже прописан).

```bash
./systemd-units-watcher.sh                    # только проверка
sudo ./systemd-units-watcher.sh --apply       # проверить и поднять упавшее
sudo ./systemd-units-watcher.sh --apply -u zookeeper.service -u kafka.service
./systemd-units-watcher.sh --help             # полная справка по параметрам
```

Полезные параметры: `--timeout` (сколько ждать `active` после старта, 120 с),
`--settle` (пауза между юнитами, 15 с), `--retries` (доп. попытки старта, 2),
`--log FILE` (дублировать вывод в файл).

## Поведение

* Порядок обхода = порядок зависимостей: ZooKeeper → Kafka → Schema Registry.
  После успешного старта юнита выдерживается пауза `--settle`, иначе Kafka
  стартует раньше, чем ZooKeeper откроет порт, и падает.
* `active` — юнит не трогается.
* `activating` — скрипт ждёт перехода в `active` до `--timeout`, но `start` не шлёт.
* `inactive` / `failed` — `systemctl start` + ожидание `active`, до `--retries` + 1 попыток.
  Перед стартом `failed`-юнита делается `systemctl reset-failed`, иначе `start`
  упирается в `StartLimitBurst` и молча ничего не делает.
* `masked` или юнит отсутствует — не трогается, только предупреждение в логе:
  маскировка это осознанное решение админа, watchdog его не отменяет.
* Если что-то поднять не удалось — в журнал попадают первые строки
  `systemctl status` и последние 15 строк лога юнита, чтобы причина была видна
  сразу, без ручного разбора.

Коды возврата: `0` — всё активно, `1` — что-то поднять не удалось, `2` — ошибка
параметров. В `.service` прописан `SuccessExitStatus=0 1`: код 1 нужен для
истории в journal и не должен помечать сам watchdog как упавший.

## Расписание

```
OnCalendar=Tue,Fri *-*-* 04..08:00/15:00   # 04:00, 04:15 ... 08:45
OnCalendar=Tue,Fri *-*-* 09:00:00          # закрывающая точка ровно в 09:00
```

21 срабатывание в день. Вторая строка нужна потому, что диапазон `04..08` сам по
себе обрывается на 08:45 — без неё границы «до 9 утра» не было бы.
Время берётся по локальной таймзоне хоста (`timedatectl`); чтобы жёстко
привязаться к UTC, допишите в секцию `[Timer]` строку `OnCalendar=` в форме
`Tue,Fri *-*-* 04..08:00/15:00 UTC`.

### Пропущенные запуски

`Persistent=true` — если хост был выключен во время окна, после загрузки таймер
срабатывает **один раз** (не 21) и проверяет, что сервисы поднялись. Отметка
последнего запуска лежит в `/var/lib/systemd/timers/stamp-systemd-units-watcher.timer`.

Догоняющий запуск происходит сразу после загрузки, независимо от дня недели и
времени суток: хост, включённый в субботу в 14:00, получит проверку в 14:00.
Для watchdog'а это и нужно — сервисы либо поднялись сами, либо их поднимет он.

При первом `enable` отметки ещё нет, базой становится момент включения таймера,
поэтому немедленного срабатывания сразу после установки не будет.

Чтобы догоняющий запуск при загрузке не гонялся с обычным стартом самих
сервисов, в `.service` прописано `After=zookeeper.service kafka.service
schema-registry.service` — **только упорядочивание**, без `Wants=`/`Requires=`,
так что эти юниты в watchdog не подтягиваются. Плюс сам скрипт в состоянии
`activating` ждёт, а не шлёт повторный `start`.

## Замечания

* Имя юнита Schema Registry в разных дистрибутивах отличается:
  `schema-registry.service` (Confluent-пакеты) или
  `confluent-schema-registry.service`. Проверьте своё:
  `systemctl list-unit-files | grep -i schema`, и поправьте `-u` в `.service`.
* Watchdog — это подстраховка, а не замена `Restart=on-failure` в самих юнитах
  Kafka/ZooKeeper и не замена `Requires=`/`After=` между ними.
