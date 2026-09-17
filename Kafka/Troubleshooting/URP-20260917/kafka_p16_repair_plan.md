# План исправления `__consumer_offsets-16`

**Кластер:** Kafka 3.9.1 (ZooKeeper), 5 брокеров
**Партиция:** `__consumer_offsets-16`, реплики `1,4,2`, лидер 1
**Документ составлен:** 17.09.2026

---

## 1. Резюме инцидента

| Параметр | Значение |
|---|---|
| Симптом | `Isr: 1` при `Replicas: 1,4,2`, URP = 1, фолловеры застряли на `fetchOffset=2063764` |
| Ошибка Kafka | `CorruptRecordException: Found record size 0 smaller than minimum record overhead (14) in file /kafka/data/kafka/__consumer_offsets-16/00000000000001539455.log` |
| Первопричина | Повреждение битмапа блоков ext4 на `/dev/sdc` брокера 1 (`ext4_validate_block_bitmap`) |
| Первая ошибка ФС | 12.09.2026 03:15:50 (`FS Error count: 3`, последняя 12.09 04:04:32) |
| Состояние ФС | `clean with errors`, `Errors behavior: Continue` |
| Проявление | 13.09.2026 15:56:50 — `EXT4-fs (sdc): Delayed block allocation failed for inode 109576911 at logical offset 18432 ... error 117 (EUCLEAN)` + `This should not happen!! Data will be lost` |
| Физически потеряно | Байты 75497472…83945495 файла (≈8,05 МиБ) читаются как нули (разреженная дыра, блоки не выделены) |
| Потерянные offset'ы | 2063764…2122407 (≈58 644 записи), записаны 13.09 с 15:56:47 |
| Есть только на брокере 1 | Всё с offset 2122408 и далее (≈1,7 млн офсетов, 5 сегментов, 311 МБ) |
| Побочный эффект | Партиция в `uncleanable` с 17.09 01:49:40, компакция не идёт, партиция растёт |
| Временная мера | `min.insync.replicas` топика снижен с 2 до 1 (17.09 ~01:30) |

### Хронология

| Время | Событие |
|---|---|
| 12.09 03:15:50 | Первая ошибка ext4 `ext4_validate_block_bitmap` на sdc |
| 12.09 03:17:50 | Брокер 2 застрял на offset 1499375 (первый эпизод) |
| 12.09 03:24 | Смена лидера, эпоха 90 → 91 |
| 12.09 08:07 | Log cleaner переписал старые сегменты, повреждение первого эпизода исчезло |
| 13.09 15:56:47 | Записан последний целый батч (baseOffset 2063742) |
| 13.09 15:56:50 | Ядро выбросило ≈8 МиБ грязных страниц; брокеры 2 и 4 застряли на 2063764 |
| 13.09 ~15:57:20 | ISR сократился до `1`, коммиты офсетов начали отклоняться (`NotEnoughReplicasException`) |
| 17.09 01:30 | `min.insync.replicas=1`, коммиты возобновились |
| 17.09 01:49:40 | Сегмент 1539455 закрыт, cleaner упёрся в дыру, партиция → `uncleanable` |

### Способ починки

`e2fsck` файловой системы + вырезание дыры из файла сегмента + один рестарт брокера 1. Для compacted-топика пропуск в offset'ах допустим (log cleaner создаёт их штатно). Брокер 1 остаётся лидером со всеми уцелевшими данными, фолловеры догоняют его.

**Теряется:** только то, что уже потеряно (≈58,6 тыс. записей от 13.09).
**Простой:** партиция 16 недоступна, пока брокер 1 остановлен (в основном — время `e2fsck`).

### Что уже проверено

- Повреждение ФС только на брокере 1 (`/dev/sdc`); на брокерах 2–5 все ФС `clean`.
- Копии партиции 16 на брокерах 2 и 4 целы (по 3 сегмента, 0 проблем) — путь отката доступен.
- `leader-epoch-checkpoint` идентичен на брокерах 1, 2, 4; последняя эпоха 91 начинается с offset 1499743 — **ни одна эпоха не попадает в пропуск** 2063764…2122407.
- Места достаточно: 87 ГБ из 3,5 ТБ (свободно 3,4 ТБ) — срочности нет, окно можно планировать спокойно.
- Вторая дыра найдена в `MKBO_MOBI_MKBONLINE_FC_PAYMENT_CATEGORY_LINK-0/00000000001939196784.log` — **в рамках этих работ не трогаем**, см. раздел 6.

---

## 2. Этап 0. Подготовка (без простоя, только чтение)

### 0.1. Вспомогательные скрипты

Оба скрипта только читают файлы. `holes.py` работает за секунды по метаданным, `scan_segments.py` читает заголовки батчей и ловит нули в выделенных блоках.

```bash
cat > /tmp/holes.py <<'EOF'
#!/usr/bin/env python3
import os, sys
bad = 0
for p in (l.strip() for l in sys.stdin):
    if not p: continue
    try:
        fd = os.open(p, os.O_RDONLY)
        try:
            size = os.fstat(fd).st_size
            h = os.lseek(fd, 0, os.SEEK_HOLE)
            if h < size:
                try: d = os.lseek(fd, h, os.SEEK_DATA)
                except OSError: d = size
                bad += 1; print(f"HOLE {p}: {h}..{d} ({(d-h)/1048576:.2f} МиБ), size {size}")
        finally: os.close(fd)
    except OSError as e:
        print(f"ERR {p}: {e}")
print(f"файлов с дырами: {bad}", file=sys.stderr)
EOF
```

```bash
cat > /tmp/scan_segments.py <<'EOF'
#!/usr/bin/env python3
import sys, os, mmap, struct, datetime

def ts(ms):
    return datetime.datetime.fromtimestamp(ms/1000).strftime('%Y-%m-%d %H:%M:%S') if ms > 0 else '?'

def scan(path):
    if os.path.getsize(path) == 0:
        return None
    with open(path, 'rb') as f, mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
        size, pos, prev, prev_ts = len(mm), 0, -1, -1
        while pos + 17 <= size:
            off, length = struct.unpack_from('>qi', mm, pos)
            magic = mm[pos + 16]
            last = f"последний целый батч baseOffset={prev}, время={ts(prev_ts)}"
            if length < 14:
                return f"pos={pos}: size={length} (нули/мусор); {last}"
            if magic > 2:
                return f"pos={pos}: magic={magic} (мусор); {last}"
            if off < prev:
                return f"pos={pos}: offset {off} < предыдущего {prev}; {last}"
            end = pos + 12 + length
            if end > size:
                return f"pos={pos}: батч обрезан (нужно до {end}, в файле {size}); {last}"
            if magic == 2 and pos + 43 <= size:
                prev_ts = struct.unpack_from('>q', mm, pos + 35)[0]
            prev, pos = off, end
        if pos != size:
            return f"после последнего батча лишние {size - pos} байт"
    return None

paths = [l.strip() for l in sys.stdin if l.strip()]
bad = 0
for i, p in enumerate(paths, 1):
    try:
        r = scan(p)
    except Exception as e:
        r = f"ошибка чтения: {e}"
    if r:
        bad += 1
        print(f"BAD {p}: {r}", flush=True)
    if i % 200 == 0:
        print(f"[{i}/{len(paths)}]", file=sys.stderr, flush=True)
print(f"проверено {len(paths)}, с проблемами {bad}", file=sys.stderr)
EOF
```

**Самопроверка на брокере 1** — оба скрипта обязаны найти известную дыру:

```bash
echo /kafka/data/kafka/__consumer_offsets-16/00000000000001539455.log | python3 /tmp/holes.py
# HOLE ...: 75497472..83943424 (8.05 МиБ), size 104857464

echo /kafka/data/kafka/__consumer_offsets-16/00000000000001539455.log | python3 /tmp/scan_segments.py
# BAD ...: pos=75497472: size=0 (нули/мусор); последний целый батч baseOffset=2063742, время=2026-09-13 15:56:47
```

### 0.2. Закрыть оставшиеся проверки

```bash
# брокер 1: фоновый скан по заголовкам батчей (87 ГБ)
find /kafka/data/kafka -name '*.log' -size +0 | sort > /tmp/segs_all.txt
nohup sh -c 'nice -n 19 ionice -c2 -n7 python3 /tmp/scan_segments.py \
  < /tmp/segs_all.txt > /tmp/scan_result.txt 2> /tmp/scan_progress.txt' &
tail -f /tmp/scan_progress.txt
cat /tmp/scan_result.txt

# брокеры 2, 3, 4, 5: дыры по всей ФС
find /kafka/data/kafka -name '*.log' -size +0 | python3 /tmp/holes.py

# брокер 1: все ли log.dirs на sdc
grep -E '^log\.dirs?=' /kafka/kafka/config/server.properties
df -hT | grep -vE "tmpfs|devtmpfs"
```

> **Стоп-условие.** Если `scan_result.txt` покажет что-то помимо известного файла или на других брокерах найдутся дыры — остановиться и пересмотреть план до окна.

### 0.3. Список групп партиции 16

```bash
kafka-consumer-groups.sh --bootstrap-server $brokers --command-config $config --list > groups.txt

python3 - > groups16.txt <<'EOF'
def jhash(s):
    b = s.encode('utf-16-be'); h = 0
    for i in range(0, len(b), 2):
        h = (31*h + int.from_bytes(b[i:i+2], 'big')) & 0xFFFFFFFF
    return h
for line in open('groups.txt'):
    g = line.rstrip('\n')
    if g and (jhash(g) & 0x7FFFFFFF) % 50 == 16:
        print(g)
EOF

wc -l groups16.txt
grep -x "UserAuditJournal.Prod" groups16.txt      # проверка расчёта: группа обязана быть в списке
```

Определить владельцев этих групп и согласовать остановку их консьюмеров на время окна.

### 0.4. Функция бэкапа офсетов и предварительный бэкап

```bash
backup_offsets() {
  BK=$1; mkdir -p "$BK"
  while read -r g; do
    kafka-consumer-groups.sh --bootstrap-server $brokers --command-config $config \
      --describe --group "$g" --offsets 2>"$BK/$g.err" \
      | awk 'NF>=4 && $4 ~ /^[0-9]+$/ {print $2","$3","$4}' > "$BK/$g.csv"
  done < groups16.txt
  echo "пустых csv: $(find "$BK" -name '*.csv' -empty | wc -l)"
  cat "$BK"/*.err | sort -u | head
}

backup_offsets offsets_pre_$(date +%F_%H%M)
```

Формат CSV — `topic,partition,offset`, совместим с `kafka-consumer-groups.sh --reset-offsets --from-file`.
Пустой `.csv` допустим, если у группы нет коммитов. Ошибки в `.err` разобрать до окна.

### 0.5. Собрать и проверить исправленный сегмент (брокер 1)

Рабочий каталог — **вне `/kafka/data/kafka`**, желательно на другом томе, чтобы `e2fsck` его не задел.

```bash
S=/kafka/data/kafka/__consumer_offsets-16/00000000000001539455.log
W=/kafka/repair_p16; mkdir -p $W
df /kafka/repair_p16                             # проверить, на каком томе

sha256sum $S | tee $W/orig.sha256                # отпечаток исходника, сверим в окне

head -c 75497472 $S >  $W/00000000000001539455.log
tail -c +83945497 $S >> $W/00000000000001539455.log
stat -c %s $W/00000000000001539455.log           # ожидается 96409440

echo $W/00000000000001539455.log | python3 /tmp/scan_segments.py     # «с проблемами 0»
echo $W/00000000000001539455.log | python3 /tmp/holes.py              # «файлов с дырами: 0»

kafka-dump-log.sh --files $W/00000000000001539455.log > $W/dump.txt 2>&1
grep -ciE "exception|isvalid: false" $W/dump.txt                      # 0
grep -E "baseOffset: (2063742|2122408) " $W/dump.txt                  # стык: 2063742 (lastOffset 2063763) → 2122408
tail -2 $W/dump.txt                                                    # lastOffset 2267629
```

### 0.6. Состояние кластера перед окном

```bash
kafka-topics.sh --bootstrap-server $brokers --command-config $config --describe --under-replicated-partitions
# ожидается только __consumer_offsets-16
kafka-topics.sh --bootstrap-server $brokers --command-config $config --describe --under-min-isr-partitions
# пусто
```

Если есть другие под-реплицированные партиции — рестарт брокера 1 может увести их в offline. Сначала разобраться с ними.

---

## 3. Этап 1. Окно работ

### Шаг 1. Остановить консьюмеров групп из `groups16.txt`

```bash
kafka-consumer-groups.sh --bootstrap-server $brokers --command-config $config \
  --describe --group UserAuditJournal.Prod --state     # STATE = Empty
```

Проверить так же несколько других групп из списка.

### Шаг 2. Финальный бэкап офсетов

```bash
backup_offsets offsets_final
```

### Шаг 3. Штатно остановить брокер 1

Остановка обычным способом (systemd / скрипт). Сообщения о неудачном controlled shutdown для партиции 16 ожидаемы — передавать лидерство некому.

```bash
kafka-topics.sh --bootstrap-server $brokers --command-config $config \
  --describe --topic __consumer_offsets | grep -P "Partition: 16\t"     # Leader: none
pgrep -af kafka.Kafka                                                    # на брокере 1 пусто
```

### Шаг 4. Спасти данные партиции 16 на другой том

Записи после 2122408 есть только здесь, а `e2fsck` их может задеть. Каталог назначения — **не на sdc**.

```bash
BK=/mnt/другой_том/p16_backup; mkdir -p $BK
cp -a --sparse=always /kafka/data/kafka/__consumer_offsets-16 $BK/
sha256sum $BK/__consumer_offsets-16/*.log > $BK/sums.txt
du -sh $BK
```

### Шаг 5. Размонтировать и проверить ФС

```bash
sudo umount /kafka
sudo fuser -vm /kafka          # если umount не прошёл — кто держит
```

Прогон без изменений:

```bash
sudo e2fsck -fn /dev/sdc 2>&1 | tee /tmp/e2fsck_dryrun.txt
tail -20 /tmp/e2fsck_dryrun.txt
```

**Ожидаемо** (чинится штатно): `Block bitmap differences`, `Free blocks count wrong for group #N`, `Padding at end of block bitmap is not set`.

> **Стоп-условие.** `Inode ... is corrupt`, `Unattached inode`, `Entry ... has deleted/unused inode` — остановиться и пересмотреть решение (возможно, пересоздание ФС с полной репликацией данных на брокер 1).

Исправление:

```bash
sudo e2fsck -fy /dev/sdc 2>&1 | tee /tmp/e2fsck_fix.txt
sudo e2fsck -fn /dev/sdc                       # повторно: должно быть чисто
sudo tune2fs -e remount-ro /dev/sdc
sudo dumpe2fs -h /dev/sdc 2>/dev/null | grep -iE "^Filesystem state|errors behavior|error count"
# ожидается: clean, Remount read-only, счётчик обнулён

sudo mount /kafka && df -hT /kafka
ls /kafka/lost+found 2>/dev/null | head
```

Добавить `errors=remount-ro` в `/etc/fstab` для этого раздела.

### Шаг 6. Заменить сегмент

```bash
D=/kafka/data/kafka/__consumer_offsets-16
W=/kafka/repair_p16

ls -l $D                                  # каталог цел после fsck
sha256sum -c $W/orig.sha256               # OK — исходник не менялся с момента подготовки
```

Если проверка не прошла или каталог пострадал — восстановить его из `$BK`:
`cp -a $BK/__consumer_offsets-16 /kafka/data/kafka/` и продолжать.

```bash
cp --sparse=never $W/00000000000001539455.log $D/00000000000001539455.log
chown kafka:kafka $D/00000000000001539455.log
rm -f $D/00000000000001539455.index $D/00000000000001539455.timeindex
sync

echo $D/00000000000001539455.log | python3 /tmp/scan_segments.py       # «с проблемами 0»
echo $D/00000000000001539455.log | python3 /tmp/holes.py                # «файлов с дырами: 0»
stat -c '%s %U' $D/00000000000001539455.log                             # 96409440 kafka
ls -l $D                                                                 # 5 сегментов на месте
```

> Индексы удаляются намеренно: без них Kafka при старте перепроверит сегмент и построит их заново.
> Файл `.snapshot` **не удалять** — он нужен для состояния продюсеров.

### Шаг 7. Запустить брокер 1

```bash
grep "__consumer_offsets-16" /kafka/kafka/logs/server.log \
  | grep -iE "index|recover|truncat|corrupt|error|Finished loading"
```

**Ожидаемо:** сообщение о восстановлении сегмента 1539455 и пересборке индекса, затем
`Finished loading offsets and group metadata from __consumer_offsets-16`.

> **Стоп-условие → откат (раздел 5).** `Truncating`, `CorruptRecordException`, ошибки загрузки групп.
> При `Truncating` Kafka обрежет лог и удалит последующие сегменты (≈216 МБ) — восстанавливать партицию из копии `$BK` (шаг 4), а не из фолловеров.

### Шаг 8. Проверить лидера и репликацию

```bash
kafka-topics.sh --bootstrap-server $brokers --command-config $config \
  --describe --topic __consumer_offsets | grep -P "Partition: 16\t"
# сразу Leader: 1; через минуты Isr содержит 1, 2 и 4

kafka-log-dirs.sh --bootstrap-server $brokers --command-config $config --describe \
  --broker-list 1,2,4 --topic-list __consumer_offsets | grep '^{' \
  | jq -c '.brokers[] | {broker, p:[.logDirs[].partitions[] | select(.partition=="__consumer_offsets-16") | {size, offsetLag}]}'
```

На брокерах 2 и 4 ошибки `Error for partition __consumer_offsets-16` должны прекратиться.

### Шаг 9. Сверить офсеты и запустить консьюмеров

```bash
backup_offsets offsets_after

while read -r g; do
  if ! diff -q <(sort "offsets_final/$g.csv") <(sort "offsets_after/$g.csv") >/dev/null; then
    echo "== РАСХОЖДЕНИЕ: $g"; diff <(sort "offsets_final/$g.csv") <(sort "offsets_after/$g.csv") | head
  fi
done < groups16.txt
```

Расхождений нет → запускать консьюмеров.
Есть → восстановить офсеты только у этих групп (сначала `--dry-run`, после проверки то же с `--execute`):

```bash
g="имя_группы"
kafka-consumer-groups.sh --bootstrap-server $brokers --command-config $config \
  --reset-offsets --group "$g" --from-file "offsets_final/$g.csv" --dry-run
```

После старта проверить, что `CURRENT-OFFSET` растёт без скачков лага:

```bash
kafka-consumer-groups.sh --bootstrap-server $brokers --command-config $config \
  --describe --group UserAuditJournal.Prod --offsets
```

---

## 4. Этап 2. Завершение

### Шаг 10. Вернуть `min.insync.replicas=2`

Только после полного ISR:

```bash
kafka-topics.sh --bootstrap-server $brokers --command-config $config \
  --describe --topic __consumer_offsets --under-replicated-partitions    # пусто

kafka-configs.sh --bootstrap-server $brokers --command-config $config --alter \
  --entity-type topics --entity-name __consumer_offsets \
  --add-config min.insync.replicas=2
```

### Шаг 11. Убрать лидерство партиции 16 с брокера 1

Пока нет ответа инфраструктуры о причине повреждения ФС:

```bash
echo '{"version":1,"partitions":[{"topic":"__consumer_offsets","partition":16,"replicas":[4,2,1]}]}' > p16.json
kafka-reassign-partitions.sh --bootstrap-server $brokers --command-config $config \
  --reassignment-json-file p16.json --execute
kafka-reassign-partitions.sh --bootstrap-server $brokers --command-config $config \
  --reassignment-json-file p16.json --verify
kafka-leader-election.sh --bootstrap-server $brokers --admin.config $config \
  --election-type preferred --topic __consumer_offsets --partition 16
```

Координатор групп переедет на брокер 4, клиенты сделают один короткий ребаланс.

### Шаг 12. Контроль в течение суток

Флаг `uncleanable` живёт только в памяти брокера и сбрасывается при рестарте — отдельных действий не требуется. Cleaner переработает накопившиеся сегменты и сожмёт 311 МБ до единиц мегабайт.

```bash
grep -iE "uncleanable|corrupt|exception" /kafka/kafka/logs/log-cleaner.log | tail
grep "__consumer_offsets 16 " /kafka/data/kafka/cleaner-offset-checkpoint    # значение выросло
du -sh /kafka/data/kafka/__consumer_offsets-16                                # заметно меньше 311M
find /kafka/data/kafka -name '*.log' -size +0 | python3 /tmp/holes.py         # 0
sudo dumpe2fs -h /dev/sdc 2>/dev/null | grep -iE "^Filesystem state|error count"
```

Метрика `kafka.log:type=LogCleanerManager,name=uncleanable-partitions-count` должна стать 0.

### Шаг 13. Уборка и профилактика

- Через неделю стабильной работы удалить `/kafka/repair_p16`, `$BK`, резервные каталоги.
- `tune2fs -e remount-ro` на всех пяти брокерах + `errors=remount-ro` в `/etc/fstab` (сейчас везде `Continue` — именно поэтому ФС брокера 1 принимала запись пять суток после обнаружения ошибки).
- Алерты: `EXT4-fs error` в syslog, рост `FS Error count` в `dumpe2fs -h`, `uncleanable-partitions-count`, URP.
- `holes.py` регулярно — хотя бы раз в сутки первую неделю.
- Порядок реплик `[1,4,2]` вернуть, когда причина будет закрыта инфраструктурой.
- Отдельно разобраться с `UserAuditJournal.Prod`: generation 38 379 200 говорит о постоянном шторме ребалансов (`session.timeout.ms`, `max.poll.interval.ms`, частые рестарты консьюмеров).
- Уточнить у инфраструктуры события 12.09 около 03:15: снапшот, миграция ВМ, работы на СХД.

---

## 5. Откат

**Триггер:** на шаге 7 в логе `Truncating`, `CorruptRecordException` или ошибки загрузки групп; либо ISR не расширяется.

1. Остановить брокер 1.
2. Убрать каталог партиции за пределы `log.dirs`:
   ```bash
   mv /kafka/data/kafka/__consumer_offsets-16 /kafka/corrupt_backup/p16_after_repair
   ```
3. **Пока брокер 1 остановлен**, выполнить unclean election. Лидером станет брокер 4 — первый живой в списке `1,4,2`:
   ```bash
   kafka-leader-election.sh --bootstrap-server $brokers --admin.config $config \
     --election-type unclean --topic __consumer_offsets --partition 16
   kafka-topics.sh --bootstrap-server $brokers --command-config $config \
     --describe --topic __consumer_offsets | grep -P "Partition: 16\t"      # Leader: 4
   ```
   > Если запустить брокер 1 с пустым каталогом раньше — он станет лидером с пустым логом, и фолловеры обрежут свои копии до нуля.
4. Запустить брокер 1 — он скопирует партицию с брокера 4.
5. Восстановить офсеты **всех** групп из `offsets_final` (`--dry-run`, затем `--execute`).
6. Запустить консьюмеров, выполнить шаги 10–13.

**Цена отката:** теряется всё после offset 2063763 — ≈1,7 млн офсетов, накопленных с 13.09. Именно поэтому основной путь — починка на месте.

---

## 6. Вне рамок этих работ

### `MKBO_MOBI_MKBONLINE_FC_PAYMENT_CATEGORY_LINK-0`

Вторая дыра того же происхождения: `00000000001939196784.log`, байты 855638016…859295744 (≈3,49 МиБ). Решение — дождаться устаревания сегмента по retention.

**Обязательная проверка перед тем, как просто ждать:**

```bash
T=MKBO_MOBI_MKBONLINE_FC_PAYMENT_CATEGORY_LINK
kafka-configs.sh --bootstrap-server $brokers --command-config $config --describe --all \
  --entity-type topics --entity-name $T | grep -E "cleanup.policy|retention"
kafka-run-class.sh kafka.tools.GetOffsetShell --bootstrap-server $brokers \
  --command-config $config --topic $T --partitions 0 --time -2    # logStartOffset
```

- `cleanup.policy=delete` → ждать, пока `logStartOffset` превысит 1939196784, затем повторить `holes.py`.
- `cleanup.policy` содержит `compact` → **ждать нельзя**: сегмент не удалится по retention, cleaner упрётся в дыру и партиция станет `uncleanable`, как партиция 16. Потребуются отдельные работы.

**Остаточный риск на период ожидания:** консьюмер, отставший до этого участка, получит `CorruptRecordException`; фолловер после truncation застрянет. Партиция не в URP, потому что фолловеры вычитали данные из page cache до того, как ядро выбросило страницы, — то есть у реплик копия полная.

---

## 7. Чего не делать

- Не перезапускать брокер 1 до окна; отключить автоперезагрузки и обновления.
- Не удалять каталоги партиции 16 на брокерах 2 и 4 — это копии для отката.
- Не включать `unclean.leader.election.enable` глобально; точечный `--election-type unclean` нужен только в откате.
- Не запускать `e2fsck` на смонтированной ФС.
- Не возвращать `min.insync.replicas=2` до полного ISR.
- Не удалять `.snapshot` при замене сегмента.

**При любом расхождении с ожидаемым выводом — остановиться и разобраться, не переходя к следующему шагу.**
