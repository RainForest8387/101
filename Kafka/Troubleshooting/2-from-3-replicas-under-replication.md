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
