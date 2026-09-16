В продакшен кластере kafka 3.9.1 (Zookeeper + Kafka брокеры) в одной из партиций топика __consumer_offsets  под номером 16 две из трех реплик в рассинхроне

![](2-from-3.png)


### Восстановление ISR для __consumer_offsets-16 (Kafka 3.9.1, ZooKeeper)
Что видно на скриншоте:
У партиции 16 реплики 1, 4, 2. Лидер брокер 1 (зелёный), а брокеры 4 и 2 выпали из ISR (красные). Отсюда URP = 1 и 148 of 150.


**Риски:** у партиции сейчас одна рабочая копия. Если брокер 1 упадёт, партиция станет offline, потому что unclean leader election по умолчанию выключен (включать его не нужно!). 
Если по каким-либо причинам брокер 1 станет оффлайн - все consumer group, у которых abs(group.id.hashCode()) % 50 == 16, потеряют координатор и не смогут коммитить и читать офсеты. Брокер 1 до окончания работ по восстановление реплик трогать нельзя.

### Диагностика

Подтвердить состояние и проверить насколько отстают реплики:

```bash
kafka-topics.sh --bootstrap-server b1:9092 --describe \
  --topic __consumer_offsets --under-replicated-partitions

kafka-log-dirs.sh --bootstrap-server b1:9092 --describe \
  --broker-list 1,2,4 --topic-list __consumer_offsets \
  | grep '^{' | jq '.brokers[] | {broker, dirs: [.logDirs[] | {logDir, error,
      p: [.partitions[] | select(.partition=="__consumer_offsets-16")]}]}'
```

Сравнить size и offsetLag на брокерах 2 и 4. Если лаг стоит на месте, репликация остановилась. Если медленно уменьшается, реплики догоняют лидера. Если в error что-то есть, у каталога лога проблема (скорее всего диск).

Посмотеть логи на брокерах 2 и 4:
```bash
grep "__consumer_offsets-16" /path/to/logs/server.log* \
  | grep -iE "error|warn|fail|corrupt|truncat|OutOfRange"
```

Ищем: ReplicaFetcherThread ... Error for partition, marked as failed, CorruptRecordException, InvalidRecordException, OffsetOutOfRange. На брокере 1 ищем Shrinking ISR, по времени станет понятно, когда всё началось.

Проверить JMX-метрику на брокерах 2 и 4: kafka.server:type=ReplicaFetcherManager,name=FailedPartitionsCount,clientId=Replica. Если она больше 0, fetcher исключил партицию из репликации. Сам он её обратно не возьмёт, пока не сменится лидер или брокер не перезапустится.

Проверить, не остались ли throttle после предидущего процесса переназначения реплик (частая и неочевидная причина):

```bash
kafka-configs.sh --bootstrap-server b1:9092 --describe \
  --entity-type topics --entity-name __consumer_offsets

for b in 1 2 4; do
  kafka-configs.sh --bootstrap-server b1:9092 --describe --entity-type brokers --entity-name $b
done
```

### Исправление в зависимости от причины
**1.** Если найдены throttle
Необходимо их удалить:
```bash
kafka-configs.sh --bootstrap-server b1:9092 --alter --entity-type topics \
  --entity-name __consumer_offsets \
  --delete-config leader.replication.throttled.replicas,follower.replication.throttled.replicas

kafka-configs.sh --bootstrap-server b1:9092 --alter --entity-type brokers \
  --entity-name 1 --delete-config leader.replication.throttled.rate,follower.replication.throttled.rate
# повторить для брокеров 2 и 4
```
Реплики должны догнать лидера за минуты: в партиции около 2 млн записей, это немного

**2.** fetcher отвалился (во время диагностики были найдены marked as failed, FailedPartitionsCount > 0), данные не повреждены

Перезапустить по очереди брокер 4, затем брокер 2 штатным controlled shutdown. Перед каждой остановкой необходимо убедиться, что вывод `kafka-topics.sh --describe --under-min-isr-partitions` пуст. Сама партиция 16 от остановки 2 или 4 не пострадает, они и так не в ISR. После старта брокера дождаться, пока он вернётся в ISR, и только потом переходить к следующему.

**3:** повреждение сегмента на фолловерах (Corrupt..., ошибки чтения или truncation)

Есть два способа:
**3.1** Онлайн через переназначение, без рестартов. Этот способ предпочтительнее: Kafka сама создаст чистые копии на других брокерах и удалит старые, возможно битые.
```bash
cat > move16.json <<'EOF'
{"version":1,"partitions":[{"topic":"__consumer_offsets","partition":16,"replicas":[1,3,5]}]}
EOF

kafka-reassign-partitions.sh --bootstrap-server b1:9092 \
  --reassignment-json-file move16.json --execute

kafka-reassign-partitions.sh --bootstrap-server b1:9092 \
  --reassignment-json-file move16.json --verify
```

**Важно!** Брокер 1 нужно оставить первым в списке, чтобы лидер не менялся. Когда --verify покажет, что переназначение завершено, при желании можно вернуть исходную раскладку [1,4,2] тем же способом. Делать после того, как будет понимание, почему копии на 2 и 4 сломались. --verify заодно снимает throttle, если они были заданы ранее через --throttle (или в рамках других работ).

**3.2** Пересоздание реплики вручную, с остановками брокеров. Делается по одному брокеру: сначала 4, потом 2.
Штатно остановить брокер.
Перенести каталог <log.dirs>/__consumer_offsets-16 за пределы log.dirs, не удаляя его сразу, на случай если понадобится разбор.
Запустите брокер. Он скопирует партицию у лидера с нуля.
Дождаться, пока брокер появится в ISR, и  только тогда переходить ко второму.

**4:** реплики живы, но отстают (лаг уменьшается, ошибок нет)
Смотреть нужно на сеть и диск брокеров 2 и 4 (iostat, заполненность диска), GC-паузы и replica.lag.time.max.ms (по умолчанию 30 с). Для одной партиции такое бывает редко, поэтому этот вариант маловероятен, сначала нужно попробовать 1-3

### Проверка после исправления
```bash
kafka-topics.sh --bootstrap-server b1:9092 --describe \
  --topic __consumer_offsets --under-replicated-partitions   # вывод должен быть пустым
```
Лидер уже стоит на предпочтительной реплике (брокер 1), так что перевыборы лидера запускать не нужно.

строка лога с брокера 4
```bash
/kafka/kafka/logs/server.log.6:[2026-09-13 20:57:52,636] ERROR [ReplicaFetcher replicaId=4, leaderId=1, fetcherId=0] Error for partition __consumer_offsets-16 at offset 2063764 (kafka.server.ReplicaFetcherThread) в логах на брокере 4
```

Разбор ошибки на брокере 4
Что означает эта строка

Сообщение Error for partition ... at offset ... в ReplicaFetcherThread появляется, когда лидер (брокер 1) вернул фолловеру код ошибки на fetch-запрос по этой партиции. Если бы фолловер сам нашёл проблему в полученных данных, текст был бы другим: Found invalid messages during fetch. Партиция при этом не помечается как failed. Фолловер ждёт replica.fetch.backoff.ms, запрашивает тот же offset 2063764 и снова получает ошибку. Реплика застревает на месте, а лидер уже ушёл до ~2131447.

Главное из этого: проблема, скорее всего, на стороне лидера, при чтении его лога в районе offset 2063764. Если брокер 2 стоит на том же offset, это почти наверняка так. Тогда варианты B и C из моего прошлого ответа не помогут и навредят:

Переназначение на другие брокеры не сработает, потому что новые реплики упрутся в тот же offset.
Удаление каталога партиции на 2 или 4 уничтожит единственные копии первых ~2 млн записей вне брокера 1.
Рестарт брокера 1 переведёт партицию в offline, а при старте координатор может не загрузить группы из повреждённого лога.

Тип ошибки записан в следующей строке после ERROR (стектрейс). От него зависит, как чинить.

Шаг 1. Собрать недостающее

Стектрейс и повторяемость на брокере 4:

```bash
grep -h -A6 "Error for partition __consumer_offsets-16" /kafka/kafka/logs/server.log* | head -20
# последнее вхождение: повторяется ли до сих пор и на том же ли offset
grep -h "Error for partition __consumer_offsets-16" /kafka/kafka/logs/server.log* | tail -3
```
То же на брокере 2. Если там тот же offset 2063764, причина точно на лидере.

Причина на лидере (брокер 1). Лидер пишет у себя реальное исключение:

bash
grep -h -A10 "Error processing fetch.*__consumer_offsets-16" /kafka/kafka/logs/server.log* | head -30
grep -h -A5 "__consumer_offsets-16" /kafka/kafka/logs/log-cleaner.log* | grep -iE "error|exception|uncleanable" | tail

Проверка сегмента на брокере 1. Это только чтение, лидер продолжает работать:

```bash
DIR=/путь/из/log.dirs/__consumer_offsets-16
SEG=$(ls $DIR/*.log | awk -F/ '{f=$NF; sub(/\.log$/,"",f); if (f+0 <= 2063764) s=$0} END{print s}')
echo $SEG

# батч, содержащий 2063764, и любые исключения
kafka-dump-log.sh --files $SEG 2>&1 | awk '
  /baseOffset:/ {for(i=1;i<=NF;i++){if($i=="baseOffset:")b=$(i+1); if($i=="lastOffset:")l=$(i+1)}
                 if (b+0<=2063764 && l+0>=2063764) print}
  /Exception|invalid bytes|isvalid: false/ {print}'
kafka-dump-log.sh --files $SEG 2>&1 | tail -3
```
# проверка индекса
`kafka-dump-log.sh --files ${SEG%.log}.index --index-sanity-check`

Параллельно посмотрите dmesg -T | grep -iE "i/o error|nvme|sd[a-z]" на брокере 1.

Шаг 2. Сделать прямо сейчас: бэкап офсетов групп партиции 16

Координатор на брокере 1 держит офсеты групп в памяти, и сейчас они корректны, даже если лог повреждён. Если починка потребует обрезать лог на лидере, коммиты после 2063764 пропадут, и группы откатятся назад. Сохраните офсеты заранее:

bash
kafka-consumer-groups.sh --bootstrap-server b1:9092 --list > groups.txt

# группы, чей координатор — партиция 16: Utils.abs(groupId.hashCode) % 50
```bash
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
```
BK=offsets_backup_$(date +%F_%H%M); mkdir -p $BK
while read -r g; do
  kafka-consumer-groups.sh --bootstrap-server b1:9092 --describe --group "$g" --offsets 2>/dev/null \
    | awk 'NF>=4 && $4 ~ /^[0-9]+$/ {print $2","$3","$4}' > "$BK/$g.csv"
done < groups16.txt

CSV получается в формате topic,partition,offset, его понимает kafka-consumer-groups.sh --reset-offsets --from-file. Повторите бэкап непосредственно перед любыми работами на брокере 1.

Шаг 3. Дальнейшие действия по результату

Повреждены данные на лидере. Например, CorruptRecordException, Record size 0 is less than the minimum record overhead, либо dump-log падает или показывает isvalid: false рядом с 2063764. В этом случае лог брокера 1 нужно обрезать до последнего валидного батча, после чего фолловеры догонят. Это делается в окно работ с кратким простоем партиции 16. После этого офсеты затронутых групп восстанавливаются из бэкапа при остановленных консьюмерах.

Данные целы, битый индекс (--index-sanity-check падает). Индекс на брокере 1 пересобирается: остановка, удаление .index/.timeindex этого сегмента, запуск. Данные при этом не теряются, простой партиции короткий.

KafkaStorageException или I/O ошибки в dmesg. Проблема в диске брокера 1. Нужно заранее решить, куда переносить лидерство, и начинать с железа.

Пришлите стектрейс после строки ERROR (с брокера 4 и, если есть, с брокера 1) и результат kafka-dump-log. По ним распишу точную процедуру для вашего случая.


Это не причина, а последствие, и оно уже влияет на работу

Эта строка меняет картину. В прошлый раз я назвал ситуацию риском на случай падения брокера 1. На деле сбой уже идёт:

Для __consumer_offsets действует min.insync.replicas=2, а в ISR партиции 16 остался только брокер 1.
Координатор пишет в эту партицию с acks=all, поэтому каждый коммит офсетов и каждый SyncGroup для групп партиции 16 отклоняется с NotEnoughReplicasException.
Клиент получает COORDINATOR_NOT_AVAILABLE, группа уходит в новый ребаланс, и это повторяется бесконечно. Сейчас в таком цикле UserAuditJournal.Prod: консьюмеры не получают назначение партиций и не коммитят. Остальные группы с координатором на партиции 16 в том же положении.
Шаг 1. Восстановить работу групп прямо сейчас

Посмотрите, откуда взялось значение 2: из конфига топика или из дефолта брокера.

```bash
kafka-configs.sh --bootstrap-server b1:9092 --describe --all \
  --entity-type topics --entity-name __consumer_offsets | grep min.insync
```
Временно снизьте min.insync.replicas до 1 на уровне топика. Это динамическая настройка, рестарт не нужен:

```bash
kafka-configs.sh --bootstrap-server b1:9092 --alter \
  --entity-type topics --entity-name __consumer_offsets \
  --add-config min.insync.replicas=1
```

Цена решения. Пока настройка действует, новые коммиты партиции 16 хранятся в одной копии. Если диск брокера 1 будет потерян, они пропадут. Но выбор сейчас не между надёжностью и риском, а между риском и уже идущим простоем: единственная копия и так одна, а без этой меры группы стоят. Изменение применится ко всем 50 партициям, однако остальные 49 с полным ISR это никак не затронет.

Проверьте, что группы стабилизировались. На брокере 1 должны пойти Stabilized group UserAuditJournal.Prod, а ошибки NotEnoughReplicasException должны прекратиться:

```bash
tail -f /kafka/kafka/logs/server.log | grep -E "__consumer_offsets-16|UserAuditJournal.Prod"
kafka-consumer-groups.sh --bootstrap-server b1:9092 --describe --group UserAuditJournal.Prod --state
```
После этого сделайте бэкап офсетов из прошлого ответа (шаг 2). Он по-прежнему нужен до любых работ с логом брокера 1.

Когда ISR восстановится, верните настройку:

```bash
kafka-configs.sh --bootstrap-server b1:9092 --alter \
  --entity-type topics --entity-name __consumer_offsets \
  --delete-config min.insync.replicas
```

Если значение 2 было задано именно на топике, а не на брокере, вместо удаления снова выставьте --add-config min.insync.replicas=2.

Шаг 2. Найти настоящую причину

В присланной строке с брокера 4 нет самого исключения. Оно печатается на следующей строке после ERROR.

Брокер 4: тип ошибки и повторяется ли она до сих пор

```bash
grep -h -A3 "Error for partition __consumer_offsets-16" /kafka/kafka/logs/server.log* | head -8
grep -h "Error for partition __consumer_offsets-16" /kafka/kafka/logs/server.log* | tail -2
```

Брокер 1: хронология выпадения реплик. В этих строках видны endOffset каждого фолловера и время выпадения:

```bash
grep -h "Shrinking ISR" /kafka/kafka/logs/server.log* | grep "__consumer_offsets-16"
```

Судя по данным, реплики выпали в разное время: брокер 4 застрял на 2063764 ещё 13-го, а лог лидера после этого вырос до ~2131447. Значит, брокер 2 оставался в ISR дольше. Если он стоит заметно дальше брокера 4, это важно для восстановления: у него самая полная копия после лидера.

Брокер 1: ошибки при отдаче данных фолловерам

```bash
grep -h -A10 "Error processing fetch.*__consumer_offsets-16" /kafka/kafka/logs/server.log* | head -30
grep -h -iE "error|uncleanable|exception" /kafka/kafka/logs/log-cleaner.log* | grep -A5 "__consumer_offsets-16" | tail
```

Если по первой команде ничего не найдётся, лидер вернул «тихую» ошибку, которую сам не логирует (OffsetOutOfRange, FencedLeaderEpoch, KafkaStorageException и подобные). Тогда тип ошибки будет виден только в стектрейсе на брокере 4.

Брокер 2: та же проверка

```bash
grep -h -A3 "Error for partition __consumer_offsets-16" /kafka/kafka/logs/server.log* | tail -8
```
Замечание по группе

Generation 38379200 — огромное значение. Даже при ребалансе раз в секунду до него пришлось бы идти больше года. Скорее всего, UserAuditJournal.Prod постоянно ребалансировалась задолго до этого инцидента: частые рестарты, превышение max.poll.interval.ms, короткий session.timeout.ms на rdkafka-клиентах. Каждый ребаланс пишет метаданные группы как раз в партицию 16. К выпадению реплик это напрямую, скорее всего, не относится, но проверить стоит отдельно, когда закроете инцидент.

Пришлите стектрейс с брокера 4 и строки Shrinking ISR с брокера 1. По ним будет понятно, чинить ли лог лидера или достаточно пересоздать реплики.