# План распределения данных на расширенный кластер Kafka (3 → 5 брокеров)

**Кластер:** Kafka 3.9.1, режим ZooKeeper
**Было:** брокеры 1, 2, 3
**Стало:** брокеры 1, 2, 3, 4, 5 (4 и 5 зарегистрированы в ZooKeeper, сетевая связность есть)
**Задача:** перераспределить существующие партиции так, чтобы данные и нагрузка легли на все 5 брокеров
**Документ составлен:** 21.09.2026

---

## 0. Ключевая мысль

Kafka **не перемещает существующие партиции автоматически**. Новый брокер получает реплики только:

* новых топиков, созданных после его добавления;
* новых партиций, добавленных в существующие топики;
* партиций, явно перенесённых через `kafka-reassign-partitions.sh`.

Поэтому сейчас брокеры 4 и 5 стоят пустыми и будут стоять пустыми сколь угодно долго. Всё содержание
работ — это подготовка корректного плана переназначения реплик и его аккуратное, порционное
выполнение под троттлингом.

### Что именно будет происходить физически

При переносе реплики партиции с брокера A на брокер B Kafka:

1. добавляет B в набор реплик (набор временно = старые ∪ новые, RF временно больше целевого);
2. B как фолловер вычитывает всю партицию с лидера с нуля — это и есть основной сетевой и дисковый трафик;
3. когда B догоняет и входит в ISR, A удаляется из набора реплик, его данные удаляются с диска;
4. если менялся первый элемент списка реплик — нужен preferred leader election, чтобы лидерство переехало.

Из пункта 2 следует главный риск: **неконтролируемый перенос забивает сеть и диски и роняет latency
продюсеров**. Отсюда обязательные троттлинг и батчи.

### Принципы, на которых построен план

| Принцип | Почему |
|---|---|
| Минимум перемещений | `--generate` строит размещение «с нуля» и легко переносит 80–100 % реплик там, где достаточно 40 %. Мы считаем целевое размещение сами, меняя только то, что нужно для выравнивания. |
| Батчами по 20–50 партиций | Ограничивает объём одновременного копирования, даёт контрольные точки и возможность остановиться. |
| Всегда с троттлингом | Без `--throttle` репликация съест всю полосу. |
| Служебные топики — отдельно и последними | `__consumer_offsets` и `__transaction_state` затрагивают все консьюмер-группы и транзакции. |
| Каждый батч проверяется до начала следующего | URP должен вернуться к 0, прежде чем грузить кластер дальше. |
| Есть готовый откат | Исходное размещение сохраняется до начала работ. |

---

## 1. Этап 0. Предпроверки (без изменений, только чтение)

Ничего из этого этапа не меняет состояние кластера. Выполнять целиком — ошибка на любом пункте
означает «не начинать перенос».

### 1.1. Все 5 брокеров живы и видят друг друга

```bash
# Через ZooKeeper — кто зарегистрирован
zookeeper-shell.sh zk1:2181 <<< "ls /brokers/ids"
# ожидаем: [1, 2, 3, 4, 5]

# Детали регистрации каждого брокера (endpoints, rack, версия)
for id in 1 2 3 4 5; do
  echo "=== broker $id ==="
  zookeeper-shell.sh zk1:2181 <<< "get /brokers/ids/$id" | tail -2
done
```

Проверить в выводе: `endpoints` указывают на **реальные разрешимые** hostname/IP (не `localhost`),
порт совпадает. Поле `rack` в выводе появляться не должно — см. п. 1.4.

Через AdminClient (авторитетнее, чем ZK):

```bash
kafka-broker-api-versions.sh --bootstrap-server broker1:9092 | grep -E '^[a-z0-9._-]+:[0-9]+'
# должно перечислить 5 брокеров с их id
```

### 1.2. Сетевая связность в обе стороны

Недостаточно «пинга». Нужен TCP до listener-порта именно с тех адресов, которыми брокеры
представляются в `advertised.listeners`.

```bash
# с каждого старого брокера к каждому новому и наоборот
for h in broker1 broker2 broker3 broker4 broker5; do
  for p in 9092 9093; do
    timeout 3 bash -c "</dev/tcp/$h/$p" && echo "OK   $h:$p" || echo "FAIL $h:$p"
  done
done
```

Отдельно проверить, что DNS-имя, которым брокер 4/5 себя анонсирует, резолвится **со старых брокеров**
и с хостов клиентов:

```bash
getent hosts broker4 broker5
```

### 1.3. Конфигурации новых брокеров совпадают со старыми

Расхождение здесь — самая частая причина проблем после переноса. Сравнить построчно:

```bash
for id in 1 2 3 4 5; do
  echo "=== broker $id ==="
  kafka-configs.sh --bootstrap-server broker1:9092 \
    --entity-type brokers --entity-name $id --describe --all \
  | grep -E 'log.dirs|num.replica.fetchers|replica.fetch|min.insync|default.replication|log.retention|log.segment|num.network.threads|num.io.threads|socket.|compression.type|unclean.leader|auto.create.topics|message.max.bytes|inter.broker.protocol.version|log.message.format'
done
```

Критично, чтобы совпадали:

| Параметр | Последствие расхождения |
|---|---|
| `inter.broker.protocol.version` | Брокер с более низкой версией не сможет корректно участвовать в репликации/реассайне |
| `message.max.bytes` / `replica.fetch.max.bytes` | Фолловер не сможет вычитать большой батч → реплика навсегда вне ISR |
| `min.insync.replicas` (брокерский дефолт) | Разное поведение acks=all |
| `log.retention.*`, `log.segment.bytes` | Разный объём хранения у копий одной партиции |
| `unclean.leader.election.enable` | Риск потери данных |
| `log.dirs` | Число и размер дисков — влияет на ёмкость |

### 1.4. Rack awareness — подтвердить, что не используется

В этом кластере `broker.rack` не задан, и план построен на этом допущении: целевое размещение
считается только по числу реплик, без ограничений на распределение по стойкам.

Проверка нужна ровно одна — убедиться, что `broker.rack` случайно не попал в конфиг **новых**
брокеров 4 и 5 (например, из шаблона Ansible). Частично заполненный rack хуже, чем его отсутствие:
Kafka начнёт считать брокеров без rack отдельной «стойкой» и размещение новых топиков поедет.

```bash
for id in 1 2 3 4 5; do
  echo -n "broker $id rack: "
  zookeeper-shell.sh zk1:2181 <<< "get /brokers/ids/$id" 2>/dev/null \
    | grep -o '"rack":"[^"]*"' || echo "не задан"
done
```

```bash
# и то же самое в конфигах на дисках
for h in broker1 broker2 broker3 broker4 broker5; do
  echo -n "$h: "; ssh $h "grep -E '^\s*broker.rack' /opt/kafka/config/server.properties || echo 'нет broker.rack'"
done
```

**Ожидаемый результат:** ни у одного из 5 брокеров rack не задан.

Если у 4 и 5 rack всё-таки прописан — убрать его из `server.properties` и перезапустить эти брокеры
(они пустые, рестарт безболезненный) **до** начала переноса. `broker.rack` не является динамическим
параметром, через `kafka-configs.sh` его не снять.

### 1.5. Кластер здоров ДО начала работ

Перенос на нездоровом кластере гарантированно делает хуже.

```bash
# Под-реплицированных партиций быть не должно
kafka-topics.sh --bootstrap-server broker1:9092 --describe --under-replicated-partitions
# ожидаем пустой вывод

# Партиций ниже min.insync.replicas быть не должно
kafka-topics.sh --bootstrap-server broker1:9092 --describe --under-min-isr-partitions
# ожидаем пустой вывод

# Партиций без лидера быть не должно
kafka-topics.sh --bootstrap-server broker1:9092 --describe --unavailable-partitions
# ожидаем пустой вывод

# Не идёт ли уже какой-то реассайн
kafka-reassign-partitions.sh --bootstrap-server broker1:9092 --list
# ожидаем "No partition reassignments found."
```

Если URP ≠ 0 — разобраться с причиной и устранить до начала. Перенос не лечит URP, он его усугубляет.

### 1.6. Остатки троттлинга от прошлых работ

Незамеченный троттл 1 МБ/с — классическая причина «реассайн висит третьи сутки».

```bash
# Брокерские троттлы
for id in 1 2 3 4 5; do
  echo -n "broker $id: "
  kafka-configs.sh --bootstrap-server broker1:9092 \
    --entity-type brokers --entity-name $id --describe \
  | grep -o 'replication.throttled.rate=[0-9]*' | tr '\n' ' '; echo
done

# Топиковые троттлы
kafka-configs.sh --bootstrap-server broker1:9092 --entity-type topics --describe \
  | grep -i 'throttled.replicas'
```

Всё найденное — снять (см. п. 6.2), если это не текущие осознанные ограничения.

### 1.7. Место на дисках

Во время переноса данные существуют **и на источнике, и на приёмнике** одновременно.

```bash
# Реальное распределение по брокерам и лог-дирам
kafka-log-dirs.sh --bootstrap-server broker1:9092 --describe --broker-list 1,2,3,4,5 \
  | tail -1 | python3 -c '
import json,sys,collections
d=json.load(sys.stdin)
for b in d["brokers"]:
    tot=0; parts=0
    for ld in b["logDirs"]:
        for p in ld["partitions"]:
            tot+=p["size"]; parts+=1
    print(f"broker {b[\"broker\"]}: {tot/1024**3:10.2f} GiB, {parts:6d} реплик")
'
```

```bash
# И просто df на каждом брокере
for h in broker1 broker2 broker3 broker4 broker5; do
  echo -n "$h: "; ssh $h "df -h /kafka/data | tail -1"
done
```

**Требование:** после переноса на каждом брокере должно остаться ≥ 30 % свободного места, а на
брокерах 4 и 5 должно хватить места под ~2/5 суммарного объёма кластера.

Грубая оценка: `целевой_объём_на_брокер ≈ суммарный_объём_кластера / 5`.

### 1.8. Снять baseline метрик

Зафиксировать до начала, чтобы было с чем сравнивать:

| Метрика (JMX) | Зачем |
|---|---|
| `kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions` | Должно быть 0 до и после |
| `kafka.server:type=ReplicaManager,name=PartitionCount` | Сколько реплик на брокере |
| `kafka.server:type=ReplicaManager,name=LeaderCount` | Баланс лидеров |
| `kafka.controller:type=KafkaController,name=ActiveControllerCount` | Ровно 1 на весь кластер |
| `kafka.network:type=RequestMetrics,name=TotalTimeMs,request=Produce` (p99) | Влияние на продюсеров |
| `kafka.network:type=RequestMetrics,name=TotalTimeMs,request=FetchConsumer` (p99) | Влияние на консьюмеров |
| `kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec` | Штатная нагрузка |
| `kafka.server:type=ReplicaFetcherManager,name=MaxLag,clientId=Replica` | Отставание фолловеров |
| Consumer group lag (по критичным группам) | Влияние на бизнес |

### 1.9. Организационное

- [ ] Согласовано окно работ (перенос можно вести и под нагрузкой, но начинать лучше вне пика).
- [ ] Известен объём кластера и оценено время переноса (п. 4.3).
- [ ] У исполнителя есть SSH/JMX/Admin-доступ ко всем 5 брокерам.
- [ ] Определён «стоп-сигнал»: при каких значениях latency/lag работы приостанавливаются.
- [ ] Владельцы критичных приложений предупреждены.
- [ ] Обновлён `bootstrap.servers` у клиентов — **не обязательно** (достаточно одного живого брокера),
      но желательно добавить broker4/broker5 в списки при ближайшем деплое.

---

## 2. Этап 1. Инвентаризация и бэкап текущего размещения

### 2.1. Рабочий каталог

```bash
WORKDIR=/var/tmp/kafka-rebalance-$(date +%Y%m%d)
mkdir -p "$WORKDIR" && cd "$WORKDIR"
BS=broker1:9092,broker2:9092,broker3:9092
```

### 2.2. Полный снимок состояния

```bash
kafka-topics.sh --bootstrap-server "$BS" --list > topics.list
wc -l topics.list

kafka-topics.sh --bootstrap-server "$BS" --describe > describe.before.txt

kafka-configs.sh --bootstrap-server "$BS" --entity-type topics --describe > topic-configs.before.txt

kafka-log-dirs.sh --bootstrap-server "$BS" --describe --broker-list 1,2,3,4,5 \
  | tail -1 > log-dirs.before.json
```

### 2.3. Текущее распределение реплик по брокерам

```bash
awk '/^\tTopic:/ {
  for (i=1;i<=NF;i++) if ($i=="Replicas:") { n=split($(i+1),r,","); for (j=1;j<=n;j++) cnt[r[j]]++ }
}
END { for (b in cnt) printf "broker %s: %d реплик\n", b, cnt[b] }' describe.before.txt | sort -V
```

Ожидаемая картина «до»: примерно равные числа на 1, 2, 3 и ноль на 4, 5.

### 2.4. Список топиков для переноса

Пользовательские топики (служебные обрабатываем отдельно на этапе 8):

```bash
grep -vE '^(__consumer_offsets|__transaction_state|_schemas|__.*)$' topics.list > topics.user.list
wc -l topics.user.list

python3 - <<'PY' > topics-to-move.json
import json
topics=[l.strip() for l in open('topics.user.list') if l.strip()]
json.dump({"topics":[{"topic":t} for t in topics],"version":1}, open(1,'w'), indent=1)
PY
```

> ⚠️ Проверьте вывод `grep -v`: если у вас есть значимые топики, начинающиеся с `_`
> (например `_schemas` Confluent Schema Registry), решите осознанно — переносить их
> в общем потоке или отдельным батчем. `_schemas` обычно однопартиционный и безопасен.

### 2.5. 🔴 БЭКАП: сохранить исходное размещение

**Это единственный артефакт, по которому возможен откат. Без него не начинать.**

```bash
kafka-reassign-partitions.sh --bootstrap-server "$BS" \
  --topics-to-move-json-file topics-to-move.json \
  --broker-list "1,2,3,4,5" \
  --generate > generate.out.txt

# Первый JSON в выводе — текущее размещение
sed -n '/Current partition replica assignment/,/^$/p' generate.out.txt \
  | grep '^{' > current-assignment.json

python3 -m json.tool current-assignment.json > /dev/null && echo "JSON валиден"
grep -c '"topic"' current-assignment.json   # число партиций

cp current-assignment.json rollback-assignment.json
chmod 444 rollback-assignment.json
```

Скопировать `rollback-assignment.json` **за пределы кластера** (в git-репозиторий работ, в тикет).

Второй JSON из `generate.out.txt` (`Proposed partition reassignment configuration`) — это
предложение Kafka. Мы его **не используем** как есть (см. п. 3.1), но сохраним для сравнения:

```bash
sed -n '/Proposed partition reassignment/,/^$/p' generate.out.txt | grep '^{' > proposed-by-kafka.json
```

---

## 3. Этап 2. Расчёт целевого размещения

### 3.1. Почему не берём вывод `--generate` напрямую

`--generate` раскладывает партиции round-robin «с чистого листа», не зная, где они лежат сейчас.
Результат корректен, но на кластере 3→5 он обычно перемещает **большинство реплик**, включая те,
которые уже лежат правильно. Это в 2–2,5 раза больше трафика и времени, чем нужно.

Целевая математика: суммарно `N` реплик, брокеров 5 → на каждом должно быть `N/5`.
Сейчас на трёх брокерах по `N/3`. Минимально необходимо перенести
`N - 5·(N/5)`… точнее: `3 · (N/3 − N/5) = 2N/5` реплик, то есть **40 %**. Всё, что сверх — лишняя работа.

Скрипт ниже двигает ровно эти 40 %: берёт текущее размещение и по одной заменяет реплику
на перегруженном брокере на реплику на недогруженном, не нарушая правило «все реплики
партиции на разных брокерах».

### 3.2. Скрипт расчёта

```bash
cat > rebalance.py <<'PY'
#!/usr/bin/env python3
"""
Минимальное по объёму перемещений выравнивание реплик Kafka по брокерам.

Вход  (stdin): current-assignment.json — вывод `kafka-reassign-partitions.sh --generate`
Выход (файлы): batch-NN.json — планы переназначения порциями

Usage:
  python3 rebalance.py --brokers 1,2,3,4,5 [--batch 30] [--prefix batch] < current-assignment.json
"""
import argparse, collections, json, math, sys


def load(parts):
    rep = collections.Counter()
    lead = collections.Counter()
    for p in parts:
        for r in p['replicas']:
            rep[r] += 1
        if p['replicas']:
            lead[p['replicas'][0]] += 1
    return rep, lead


def topic_load(parts):
    tl = collections.Counter()
    for p in parts:
        for r in p['replicas']:
            tl[(p['topic'], r)] += 1
    return tl


def balance_replicas(parts, brokers, log):
    total = sum(len(p['replicas']) for p in parts)
    lo, hi = total // len(brokers), math.ceil(total / len(brokers))
    log(f"реплик всего: {total}; целевой диапазон на брокер: {lo}..{hi}")
    blocked = set()
    moves = 0
    while True:
        rep, _ = load(parts)
        donors = sorted((b for b in brokers if rep[b] > hi), key=lambda b: -rep[b])
        recvs = sorted((b for b in brokers if rep[b] < lo), key=lambda b: rep[b])
        if not donors or not recvs:
            break
        pair = None
        for d in donors:
            for r in recvs:
                if (d, r) in blocked:
                    continue
                cand = [p for p in parts
                        if d in p['replicas'] and r not in p['replicas']]
                if cand:
                    pair = (d, r, cand)
                    break
            if pair:
                break
        if not pair:
            log("больше нет допустимых перемещений — остановка")
            break
        d, r, cand = pair
        tl = topic_load(parts)
        # забираем у топика, который сильнее всего представлен на доноре
        # и слабее всего — на получателе (ровный размаз топика по брокерам)
        p = max(cand, key=lambda p: (tl[(p['topic'], d)] - tl[(p['topic'], r)],
                                     p['topic'], p['partition']))
        p['replicas'][p['replicas'].index(d)] = r
        moves += 1
    log(f"перемещений реплик: {moves}")
    return moves


def balance_leaders(parts, brokers, log):
    n = len(parts)
    lo, hi = n // len(brokers), math.ceil(n / len(brokers))
    swaps = 0
    guard = 0
    while guard < n * len(brokers):
        guard += 1
        _, lead = load(parts)
        over = sorted((b for b in brokers if lead[b] > hi), key=lambda b: -lead[b])
        under = sorted((b for b in brokers if lead[b] < lo), key=lambda b: lead[b])
        if not over or not under:
            break
        done = False
        for o in over:
            for u in under:
                for p in parts:
                    rs = p['replicas']
                    if rs and rs[0] == o and u in rs[1:]:
                        i = rs.index(u)
                        rs[0], rs[i] = rs[i], rs[0]
                        swaps += 1
                        done = True
                        break
                if done:
                    break
            if done:
                break
        if not done:
            break
    log(f"ротаций preferred-лидера: {swaps}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--brokers', required=True)
    ap.add_argument('--batch', type=int, default=30)
    ap.add_argument('--prefix', default='batch')
    ap.add_argument('--no-leader-balance', action='store_true')
    a = ap.parse_args()

    def log(m):
        print(m, file=sys.stderr)

    brokers = [int(x) for x in a.brokers.split(',') if x.strip()]
    src = json.load(sys.stdin)
    parts = src['partitions']
    before = {(p['topic'], p['partition']): list(p['replicas']) for p in parts}

    rep0, lead0 = load(parts)
    log("=== ДО ===")
    for b in brokers:
        log(f"  broker {b}: реплик {rep0[b]:6d}, лидеров {lead0[b]:6d}")

    balance_replicas(parts, brokers, log)
    if not a.no_leader_balance:
        balance_leaders(parts, brokers, log)

    rep1, lead1 = load(parts)
    log("=== ПОСЛЕ ===")
    for b in brokers:
        log(f"  broker {b}: реплик {rep1[b]:6d}, лидеров {lead1[b]:6d}")

    changed = [p for p in parts
               if before[(p['topic'], p['partition'])] != p['replicas']]
    new_copies = sum(len(set(p['replicas']) - set(before[(p['topic'], p['partition'])]))
                     for p in changed)
    log(f"партиций затронуто: {len(changed)} из {len(parts)}")
    log(f"реплик будет скопировано с нуля: {new_copies}")

    changed.sort(key=lambda p: (p['topic'], p['partition']))
    files = []
    for i in range(0, len(changed), a.batch):
        chunk = changed[i:i + a.batch]
        fn = f"{a.prefix}-{i // a.batch + 1:02d}.json"
        with open(fn, 'w') as f:
            json.dump({"version": 1,
                       "partitions": [{"topic": p['topic'],
                                       "partition": p['partition'],
                                       "replicas": p['replicas']} for p in chunk]},
                      f, indent=1)
        files.append((fn, len(chunk)))
    with open(f"{a.prefix}-full.json", 'w') as f:
        json.dump({"version": 1,
                   "partitions": [{"topic": p['topic'],
                                   "partition": p['partition'],
                                   "replicas": p['replicas']} for p in changed]},
                  f, indent=1)
    log("файлы:")
    for fn, c in files:
        log(f"  {fn}: {c} партиций")


if __name__ == '__main__':
    main()
PY
```

### 3.3. Запуск расчёта

```bash
python3 rebalance.py --brokers 1,2,3,4,5 --batch 30 --prefix batch \
  < current-assignment.json 2> plan.report.txt

cat plan.report.txt
ls -la batch-*.json
```

Пример ожидаемого отчёта:

```
реплик всего: 900; целевой диапазон на брокер: 180..180
=== ДО ===
  broker 1: реплик    300, лидеров    100
  broker 2: реплик    300, лидеров    100
  broker 3: реплик    300, лидеров    100
  broker 4: реплик      0, лидеров      0
  broker 5: реплик      0, лидеров      0
перемещений реплик: 360
ротаций preferred-лидера: 120
=== ПОСЛЕ ===
  broker 1: реплик    180, лидеров     60
  ...
партиций затронуто: 276 из 300
реплик будет скопировано с нуля: 360
```

### 3.4. Валидация плана перед выполнением

Обязательная проверка — скрипт не должен был породить некорректного размещения:

```bash
python3 - <<'PY'
import json, collections, glob, sys
cur = {(p['topic'], p['partition']): p['replicas']
       for p in json.load(open('current-assignment.json'))['partitions']}
new = {}
for f in sorted(glob.glob('batch-*.json')):
    if f.endswith('full.json'): continue
    for p in json.load(open(f))['partitions']:
        k = (p['topic'], p['partition'])
        assert k not in new, f"дубликат {k} в {f}"
        new[k] = p['replicas']
err = 0
for k, r in new.items():
    if len(set(r)) != len(r):
        print("ДУБЛЬ БРОКЕРА:", k, r); err += 1
    if len(r) != len(cur[k]):
        print("ИЗМЕНИЛСЯ RF:", k, cur[k], "->", r); err += 1
    if not set(r) <= {1,2,3,4,5}:
        print("ЧУЖОЙ БРОКЕР:", k, r); err += 1
cnt = collections.Counter()
for k, r in {**cur, **new}.items():
    for b in r: cnt[b] += 1
print("итоговое число реплик по брокерам:", dict(sorted(cnt.items())))
print("партиций в плане:", len(new))
sys.exit(1 if err else 0)
PY
echo "код возврата: $?"   # должен быть 0
```

Скрипт не учитывает распределение по стойкам, и это корректно для данного кластера: `broker.rack`
не используется (проверено в п. 1.4). Если rack когда-либо будет введён, этот скрипт придётся
дорабатывать — балансировка по числу реплик и балансировка по стойкам решаются по-разному.

### 3.5. Оценка объёма и порядок батчей

Посчитать, сколько байт реально поедет:

```bash
python3 - <<'PY'
import json, glob
sizes = {}
d = json.load(open('log-dirs.before.json'))
for b in d['brokers']:
    for ld in b['logDirs']:
        for p in ld['partitions']:
            t, _, part = p['partition'].rpartition('-')
            sizes[(t, int(part))] = max(sizes.get((t, int(part)), 0), p['size'])
cur = {(p['topic'], p['partition']): set(p['replicas'])
       for p in json.load(open('current-assignment.json'))['partitions']}
tot = 0
for f in sorted(glob.glob('batch-*.json')):
    if f.endswith('full.json'): continue
    s = 0
    for p in json.load(open(f))['partitions']:
        k = (p['topic'], p['partition'])
        s += len(set(p['replicas']) - cur[k]) * sizes.get(k, 0)
    tot += s
    print(f"{f}: {s/1024**3:8.2f} GiB")
print(f"ИТОГО: {tot/1024**3:.2f} GiB")
PY
```

**Рекомендуемый порядок выполнения батчей:**

1. Сначала — батч с самыми **мелкими** топиками (пилотный прогон, проверка механики и троттлинга).
2. Затем — основная масса в порядке возрастания объёма.
3. Самые крупные топики — отдельными батчами по 5–10 партиций, возможно в разные окна.
4. `__consumer_offsets` и `__transaction_state` — в самом конце (этап 8).

При необходимости пересобрать батчи в нужном порядке можно вручную, перегруппировав содержимое
`batch-full.json`.

---

## 4. Этап 3. Троттлинг

### 4.1. Как считать значение

Троттл задаётся в **байтах в секунду на брокер**, отдельно для лидера (отдача) и фолловера (приём).

```
throttle = min(
    0.3 × пропускная_способность_сети_брокера,
    0.3 × последовательная_скорость_записи_диска
)
```

Ориентиры:

| Сеть / диск | Стартовый троттл | Комментарий |
|---|---|---|
| 1 Gbit/s (≈125 МБ/с) | **30 МБ/с** = `31457280` | Консервативно, не мешает продюсерам |
| 10 Gbit/s (≈1250 МБ/с) | **100–150 МБ/с** = `104857600` | Можно поднимать до 300 МБ/с при запасе |
| NVMe + 25 Gbit/s | **300 МБ/с** = `314572800` | Упирается в диск, не в сеть |

**Начинать всегда с консервативного значения.** Поднять троттл на лету — одна команда без
перезапуска реассайна; опустить, когда latency уже поехала — дороже.

### 4.2. Расчёт времени

```
время_батча ≈ объём_батча / (throttle × число_брокеров_приёмников)
```

Пример: батч 300 GiB, троттл 30 МБ/с, приёмники 4 и 5 →
`300 × 1024 / (30 × 2) ≈ 5120 с ≈ 1 ч 25 мин`.

На практике закладывайте ×1,5 — троттл не выбирается полностью, плюс идёт штатная репликация.

### 4.3. Установка троттла

Инструмент сам расставит `leader.replication.throttled.replicas` / `follower.replication.throttled.replicas`
на топиках при `--execute --throttle`, но брокерские лимиты лучше задать заранее и явно — тогда
они не сбрасываются между батчами:

```bash
THROTTLE=31457280   # 30 МБ/с — подставьте своё значение

for id in 1 2 3 4 5; do
  kafka-configs.sh --bootstrap-server "$BS" \
    --entity-type brokers --entity-name $id --alter \
    --add-config "leader.replication.throttled.rate=$THROTTLE,follower.replication.throttled.rate=$THROTTLE"
done

# проверить
for id in 1 2 3 4 5; do
  echo -n "broker $id: "
  kafka-configs.sh --bootstrap-server "$BS" --entity-type brokers --entity-name $id --describe
done
```

Изменение применяется **динамически, без рестарта**. Поднять/опустить троттл во время выполнения
батча — той же командой.

### 4.4. Тюнинг скорости фолловеров (опционально)

Если троттл выставлен высоко, а реальная скорость ниже — узкое место в фетчерах:

```bash
# число потоков репликации на брокере (дефолт 1, разумно 2-4 на новых брокерах)
for id in 4 5; do
  kafka-configs.sh --bootstrap-server "$BS" \
    --entity-type brokers --entity-name $id --alter \
    --add-config "num.replica.fetchers=4"
done
```

`num.replica.fetchers` — динамический (cluster-wide/per-broker), рестарт не нужен. После
завершения работ вернуть к исходному значению, если оно отличалось.

---

## 5. Этап 4. Выполнение переноса

### 5.1. Цикл для одного батча

Для каждого файла `batch-NN.json` выполняется одна и та же последовательность.

**Шаг 1 — запуск:**

```bash
B=batch-01.json

kafka-reassign-partitions.sh --bootstrap-server "$BS" \
  --reassignment-json-file "$B" \
  --throttle $THROTTLE \
  --execute | tee "exec.$B.log"
```

В выводе будет строка `Successfully started partition reassignments for ...` и сохранённый
«rollback JSON» — **сохраните и его**, он относится именно к этому батчу:

```bash
sed -n '/Current partition replica assignment/,/^$/p' "exec.$B.log" \
  | grep '^{' > "rollback.$B"
```

**Шаг 2 — наблюдение (каждые 30–60 с):**

```bash
watch -n 30 "kafka-reassign-partitions.sh --bootstrap-server $BS \
  --reassignment-json-file $B --verify --preserve-throttles 2>&1 | tail -20"
```

> 🔴 Во время ожидания всегда используйте `--verify --preserve-throttles`.
> `--verify` **без** этого флага снимает троттлы, как только сочтёт реассайн завершённым —
> и оставшиеся движения пойдут на полной скорости.

Параллельно смотреть URP:

```bash
watch -n 30 "kafka-topics.sh --bootstrap-server $BS --describe --under-replicated-partitions | wc -l"
```

**Шаг 3 — завершение:**

`--verify` печатает по каждой партиции одно из:

| Статус | Значение |
|---|---|
| `is still in progress` | Копирование идёт |
| `is completed successfully` | Реплики переехали, старые удалены |
| `failed` | Нужно разбираться (п. 10) |

Когда **все** строки батча — `completed successfully`, и `kafka-reassign-partitions.sh --list` не
показывает активных реассайнов по этим партициям — батч закрыт.

**Шаг 4 — проверка перед следующим батчем:**

```bash
# URP должен вернуться к нулю
kafka-topics.sh --bootstrap-server "$BS" --describe --under-replicated-partitions
# under-min-isr тоже пусто
kafka-topics.sh --bootstrap-server "$BS" --describe --under-min-isr-partitions
# активных реассайнов нет
kafka-reassign-partitions.sh --bootstrap-server "$BS" --list
# место на дисках
kafka-log-dirs.sh --bootstrap-server "$BS" --describe --broker-list 1,2,3,4,5 | tail -1 \
  | python3 -c 'import json,sys;d=json.load(sys.stdin);[print(b["broker"], round(sum(p["size"] for ld in b["logDirs"] for p in ld["partitions"])/1024**3,1),"GiB") for b in d["brokers"]]'
```

Только после этого — следующий батч. **Не запускать два батча одновременно.**

### 5.2. Скрипт-обёртка для последовательного прогона

Для удобства (но с ручным подтверждением между батчами):

```bash
cat > run-batch.sh <<'SH'
#!/usr/bin/env bash
set -euo pipefail
BS="${BS:?set BS}"
THROTTLE="${THROTTLE:?set THROTTLE}"
B="$1"

echo "=== $B: предпроверка ==="
urp=$(kafka-topics.sh --bootstrap-server "$BS" --describe --under-replicated-partitions | wc -l)
[ "$urp" -eq 0 ] || { echo "URP=$urp, отказ"; exit 1; }
kafka-reassign-partitions.sh --bootstrap-server "$BS" --list | grep -q "No partition reassignments" \
  || { echo "есть активный реассайн, отказ"; exit 1; }

echo "=== $B: запуск ==="
kafka-reassign-partitions.sh --bootstrap-server "$BS" \
  --reassignment-json-file "$B" --throttle "$THROTTLE" --execute | tee "exec.$B.log"
sed -n '/Current partition replica assignment/,/^$/p' "exec.$B.log" | grep '^{' > "rollback.$B" || true

echo "=== $B: ожидание ==="
while true; do
  out=$(kafka-reassign-partitions.sh --bootstrap-server "$BS" \
        --reassignment-json-file "$B" --verify --preserve-throttles 2>&1)
  prog=$(grep -c "is still in progress" <<<"$out" || true)
  fail=$(grep -c "failed" <<<"$out" || true)
  urp=$(kafka-topics.sh --bootstrap-server "$BS" --describe --under-replicated-partitions | wc -l)
  echo "$(date +%T) в процессе: $prog, ошибок: $fail, URP: $urp"
  [ "$fail" -gt 0 ] && { echo "ЕСТЬ ОШИБКИ"; echo "$out" | grep failed; exit 2; }
  [ "$prog" -eq 0 ] && break
  sleep 30
done
echo "=== $B: завершён ==="
SH
chmod +x run-batch.sh

# запуск по одному, с контролем оператора
BS="$BS" THROTTLE=$THROTTLE ./run-batch.sh batch-01.json
```

### 5.3. Что делать, если батч идёт слишком медленно

1. Проверить фактическую скорость: `kafka.server:type=BrokerTopicMetrics,name=ReplicationBytesInPerSec`
   на брокерах 4 и 5.
2. Если скорость ≈ троттлу и метрики хоста (сеть/диск) не в потолке — поднять троттл (п. 4.3).
3. Если скорость **ниже** троттла — упирается не в троттл: смотреть `num.replica.fetchers`,
   утилизацию диска (`iostat -x 5`), сеть (`sar -n DEV 5`), GC-паузы на брокерах.
4. Проверить, что нет «забытого» топикового троттла с меньшим значением.

### 5.4. Если нужно срочно остановиться

```bash
# Отменить текущий (незавершённый) реассайн — партиции вернутся к исходному набору реплик
kafka-reassign-partitions.sh --bootstrap-server "$BS" \
  --reassignment-json-file "$B" --cancel
```

`--cancel` откатывает только **незавершённые** партиции батча; уже завершённые остаются на новых
местах — это нормально и безопасно.

---

## 6. Этап 5. Снятие троттлинга

Троттл, оставшийся после работ, тихо замедляет штатную репликацию и восстановление после сбоев.
Снимать обязательно.

### 6.1. Штатный путь

Финальный `--verify` **без** `--preserve-throttles` снимает топиковые троттлы автоматически:

```bash
for f in batch-*.json; do
  [[ "$f" == *full.json ]] && continue
  kafka-reassign-partitions.sh --bootstrap-server "$BS" \
    --reassignment-json-file "$f" --verify
done
```

### 6.2. Ручная зачистка (делать всегда, как контроль)

```bash
# Брокерские лимиты
for id in 1 2 3 4 5; do
  kafka-configs.sh --bootstrap-server "$BS" \
    --entity-type brokers --entity-name $id --alter \
    --delete-config "leader.replication.throttled.rate,follower.replication.throttled.rate" 2>/dev/null
done

# Топиковые списки throttled.replicas
kafka-configs.sh --bootstrap-server "$BS" --entity-type topics --describe \
  | grep -i 'throttled.replicas' \
  | sed -E 's/.*Configs for topic .([^"]+). are.*/\1/' | sort -u > throttled-topics.list

while read -r t; do
  [ -z "$t" ] && continue
  kafka-configs.sh --bootstrap-server "$BS" \
    --entity-type topics --entity-name "$t" --alter \
    --delete-config "leader.replication.throttled.replicas,follower.replication.throttled.replicas"
done < throttled-topics.list
```

### 6.3. Контроль

```bash
kafka-configs.sh --bootstrap-server "$BS" --entity-type topics --describe | grep -i throttled
# ожидаем пустой вывод
for id in 1 2 3 4 5; do
  kafka-configs.sh --bootstrap-server "$BS" --entity-type brokers --entity-name $id --describe
done
# ожидаем отсутствие *.throttled.rate
```

---

## 7. Этап 6. Выравнивание лидеров

Перенос реплики не переносит лидерство. После реассайна preferred-лидер (первый элемент списка
реплик) может не быть фактическим лидером — нагрузка на чтение/запись останется перекошенной.

### 7.1. Посмотреть текущий перекос

```bash
kafka-topics.sh --bootstrap-server "$BS" --describe \
  | awk '/^\tTopic:/ { for (i=1;i<=NF;i++) if ($i=="Leader:") c[$(i+1)]++ }
         END { for (b in c) printf "лидеров на брокере %s: %d\n", b, c[b] }' | sort -V
```

### 7.2. Запустить выборы

```bash
kafka-leader-election.sh --bootstrap-server "$BS" \
  --election-type PREFERRED --all-topic-partitions
```

Операция почти мгновенная, но вызывает кратковременные (доли секунды) `NOT_LEADER_FOR_PARTITION`
у клиентов — они это штатно переживают через retry. На больших кластерах лучше делать
по частям, через `--path-to-json-file`:

```bash
cat > elect.json <<'EOF'
{"partitions":[{"topic":"my-topic","partition":0},{"topic":"my-topic","partition":1}]}
EOF
kafka-leader-election.sh --bootstrap-server "$BS" \
  --election-type PREFERRED --path-to-json-file elect.json
```

### 7.3. Автоматическое выравнивание

Проверить, включено ли штатное периодическое выравнивание:

```bash
kafka-configs.sh --bootstrap-server "$BS" --entity-type brokers --entity-name 1 --describe --all \
  | grep -E 'auto.leader.rebalance.enable|leader.imbalance'
```

* `auto.leader.rebalance.enable=true` (дефолт) — контроллер сам выровняет в течение
  `leader.imbalance.check.interval.seconds` (дефолт 300 с) при перекосе больше
  `leader.imbalance.per.broker.percentage` (дефолт 10 %).
* Если `false` — выборы из п. 7.2 обязательны вручную.

---

## 8. Этап 7. Служебные топики

Выполняется **после** всех пользовательских топиков, когда кластер устоялся.

### 8.1. `__consumer_offsets`

50 партиций (по умолчанию), RF обычно 3, compacted. Хранит офсеты всех консьюмер-групп, каждая
партиция — координатор для своего набора групп.

Риск: во время переезда партиции координатора группы её ребаланс/коммиты могут ненадолго
отклоняться. Поэтому — **батчами по 5–10 партиций**.

```bash
cat > topics-to-move-offsets.json <<'EOF'
{"topics":[{"topic":"__consumer_offsets"}],"version":1}
EOF

kafka-reassign-partitions.sh --bootstrap-server "$BS" \
  --topics-to-move-json-file topics-to-move-offsets.json \
  --broker-list "1,2,3,4,5" --generate > generate.offsets.txt

sed -n '/Current partition replica assignment/,/^$/p' generate.offsets.txt \
  | grep '^{' > current-offsets.json
cp current-offsets.json rollback-offsets.json

python3 rebalance.py --brokers 1,2,3,4,5 --batch 5 --prefix offsets \
  < current-offsets.json 2> plan.offsets.txt
cat plan.offsets.txt
```

Далее — тот же цикл, что в п. 5.1, но с дополнительным контролем после каждого батча:

```bash
# Лаг критичных групп не растёт
kafka-consumer-groups.sh --bootstrap-server "$BS" --describe --group <critical-group>

# Нет групп в состоянии, отличном от Stable/Empty
kafka-consumer-groups.sh --bootstrap-server "$BS" --list \
  | while read -r g; do
      st=$(kafka-consumer-groups.sh --bootstrap-server "$BS" --describe --group "$g" --state \
           2>/dev/null | awk 'NR==2{print $(NF-1)}')
      [[ "$st" == "Stable" || "$st" == "Empty" ]] || echo "$g -> $st"
    done
```

### 8.2. `__transaction_state`

Если используются транзакции / EOS (Kafka Streams, transactional producers) — топик есть и его
тоже надо разложить. Процедура идентична п. 8.1, батчи по 5 партиций.

```bash
kafka-topics.sh --bootstrap-server "$BS" --describe --topic __transaction_state | head -3
```

Если топик отсутствует — транзакции не используются, шаг пропускается.

### 8.3. Прочие `_`-топики

`_schemas` (Schema Registry), `_confluent-*`, топики Kafka Streams (`*-changelog`, `*-repartition`)
переносятся как обычные пользовательские, но:

* `_schemas` — однопартиционный, переносить в одиночном батче, Schema Registry переживёт;
* changelog-топики Streams — во время переезда приложение может уйти в ребаланс; лучше делать
  в окно низкой нагрузки.

---

## 9. Этап 8. Финальная проверка и приёмка

### 9.1. Распределение реплик и лидеров

```bash
kafka-topics.sh --bootstrap-server "$BS" --describe > describe.after.txt

awk '/^\tTopic:/ {
  for (i=1;i<=NF;i++) {
    if ($i=="Replicas:") { n=split($(i+1),r,","); for (j=1;j<=n;j++) rep[r[j]]++ }
    if ($i=="Leader:")   { lead[$(i+1)]++ }
  }
}
END { for (b in rep) printf "broker %s: реплик %5d, лидеров %5d\n", b, rep[b], lead[b] }' \
  describe.after.txt | sort -V
```

**Критерий приёмки:** разброс числа реплик между брокерами ≤ 10 %, разброс лидеров ≤ 10 %.

### 9.2. Объём данных по брокерам

```bash
kafka-log-dirs.sh --bootstrap-server "$BS" --describe --broker-list 1,2,3,4,5 | tail -1 \
  > log-dirs.after.json

python3 - <<'PY'
import json
for tag in ('before','after'):
    d=json.load(open(f'log-dirs.{tag}.json'))
    print(f"--- {tag} ---")
    for b in sorted(d['brokers'], key=lambda x:x['broker']):
        s=sum(p['size'] for ld in b['logDirs'] for p in ld['partitions'])
        n=sum(len(ld['partitions']) for ld in b['logDirs'])
        print(f"  broker {b['broker']}: {s/1024**3:9.2f} GiB, {n:6d} реплик")
PY
```

Ожидание: объёмы на 5 брокерах примерно равны; на 1, 2, 3 объём упал примерно на 40 %.

### 9.3. Здоровье кластера

```bash
kafka-topics.sh --bootstrap-server "$BS" --describe --under-replicated-partitions   # пусто
kafka-topics.sh --bootstrap-server "$BS" --describe --under-min-isr-partitions      # пусто
kafka-topics.sh --bootstrap-server "$BS" --describe --unavailable-partitions        # пусто
kafka-reassign-partitions.sh --bootstrap-server "$BS" --list                        # нет активных
kafka-configs.sh --bootstrap-server "$BS" --entity-type topics --describe | grep -i throttled  # пусто
```

### 9.4. Ничего не потеряно

```bash
# Число топиков и партиций не изменилось
diff <(grep -c 'Topic:' describe.before.txt) <(grep -c 'Topic:' describe.after.txt)

# RF каждой партиции сохранился
python3 - <<'PY'
import re, collections
def rf(fn):
    d={}
    for l in open(fn):
        m=re.search(r'Topic: (\S+)\s+Partition: (\d+).*Replicas: (\S+)', l)
        if m: d[(m.group(1),int(m.group(2)))]=len(m.group(3).split(','))
    return d
a,b=rf('describe.before.txt'), rf('describe.after.txt')
miss=set(a)-set(b); extra=set(b)-set(a)
bad=[k for k in set(a)&set(b) if a[k]!=b[k]]
print("пропало партиций:", len(miss), "появилось:", len(extra), "изменился RF:", len(bad))
for k in list(bad)[:10]: print("  ", k, a[k], "->", b[k])
PY

# Конфиги топиков не изменились
kafka-configs.sh --bootstrap-server "$BS" --entity-type topics --describe > topic-configs.after.txt
diff topic-configs.before.txt topic-configs.after.txt
```

### 9.5. Функциональная проверка

```bash
T=rebalance-smoke-$(date +%s)
kafka-topics.sh --bootstrap-server "$BS" --create --topic "$T" \
  --partitions 10 --replication-factor 3

# новый топик должен автоматически лечь на все 5 брокеров
kafka-topics.sh --bootstrap-server "$BS" --describe --topic "$T"

seq 1 1000 | kafka-console-producer.sh --bootstrap-server "$BS" --topic "$T" \
  --producer-property acks=all
kafka-console-consumer.sh --bootstrap-server "$BS" --topic "$T" \
  --from-beginning --timeout-ms 15000 | wc -l   # ожидаем 1000

kafka-topics.sh --bootstrap-server "$BS" --delete --topic "$T"
```

### 9.6. Метрики и приложения

Сравнить с baseline из п. 1.8:

- [ ] p99 Produce latency вернулся к baseline.
- [ ] p99 FetchConsumer latency вернулся к baseline.
- [ ] `UnderReplicatedPartitions` = 0 на всех брокерах.
- [ ] `ActiveControllerCount` = 1 по кластеру.
- [ ] Лаг всех консьюмер-групп в норме, нет растущих.
- [ ] `BytesInPerSec` / `BytesOutPerSec` распределились по 5 брокерам.
- [ ] Нет ошибок в логах брокеров: `grep -iE 'ERROR|FATAL' /var/log/kafka/server.log | tail -50`.
- [ ] Приложения-владельцы подтвердили отсутствие деградации.

### 9.7. Прибраться

```bash
# Вернуть num.replica.fetchers, если меняли
for id in 4 5; do
  kafka-configs.sh --bootstrap-server "$BS" \
    --entity-type brokers --entity-name $id --alter --delete-config "num.replica.fetchers"
done

# Сохранить артефакты работ
tar czf ~/kafka-rebalance-$(date +%Y%m%d).tar.gz "$WORKDIR"
```

Приложить к тикету: `rollback-assignment.json`, `plan.report.txt`, `describe.before/after.txt`,
`log-dirs.before/after.json`, логи выполнения батчей.

---

## 10. Откат

### 10.1. Откат незавершённого батча

```bash
kafka-reassign-partitions.sh --bootstrap-server "$BS" \
  --reassignment-json-file batch-NN.json --cancel
```

Безопасно, быстро: незавершённые партиции возвращаются к исходному набору реплик,
недокачанные копии удаляются.

### 10.2. Откат уже завершённых батчей

Это **обратный перенос** — такой же по объёму, времени и риску, как прямой. Выполнять только
при реальной необходимости.

```bash
# Полный откат к состоянию до работ
kafka-reassign-partitions.sh --bootstrap-server "$BS" \
  --reassignment-json-file rollback-assignment.json \
  --throttle $THROTTLE --execute

kafka-reassign-partitions.sh --bootstrap-server "$BS" \
  --reassignment-json-file rollback-assignment.json --verify --preserve-throttles
```

Либо точечно, по батчам: `rollback.batch-NN.json`, сохранённые на шаге 1 п. 5.1.

### 10.3. Когда откат НЕ нужен

| Ситуация | Действие |
|---|---|
| Батч завершился, но latency подросла | Не откатывать. Снизить троттл, подождать, продолжить позже |
| Часть батчей выполнена, решено прерваться | Не откатывать. Кластер в корректном, просто частично выровненном состоянии |
| Реплика долго не входит в ISR | Не откатывать. Разбираться (п. 11), `--cancel` только если проблема на новом брокере |
| Место кончается на брокере-приёмнике | `--cancel` текущего батча, освободить место, пересобрать план |

### 10.4. Крайний случай: вывести новые брокеры из кластера

Если решено отказаться от расширения — сначала полный откат размещения (п. 10.2), убедиться, что
на брокерах 4 и 5 нет ни одной реплики (`kafka-log-dirs.sh`), и только потом останавливать их
и удалять `/brokers/ids/4`, `/brokers/ids/5` из ZooKeeper (они удаляются сами — это ephemeral-ноды).

---

## 11. Риски и типичные проблемы

| Проблема | Признак | Причина | Что делать |
|---|---|---|---|
| Реассайн «висит» | `is still in progress` часами, трафика нет | Забытый или слишком низкий троттл | Проверить `throttled.rate` на всех брокерах, поднять |
| Реплика не входит в ISR | URP не уходит, `MaxLag` растёт | `replica.fetch.max.bytes` < `message.max.bytes` топика | Привести конфиги к одному значению, рестарт фолловера |
| Всплеск latency продюсеров | p99 Produce ×3–5 | Троттл слишком высокий / батч слишком большой | Снизить троттл на лету, уменьшить размер батчей |
| Диск кончается на 4/5 | `df` > 85 % | Недооценён объём или слишком много партиций в батче | `--cancel` батча, пересобрать план меньшими порциями |
| Перекос лидеров после переноса | Лидеры на 1–3, нулевые на 4–5 | Не запущены preferred-выборы | `kafka-leader-election.sh --election-type PREFERRED` (п. 7) |
| `--verify` снял троттлы досрочно | Скорость репликации взлетела | Забыт `--preserve-throttles` | Вернуть троттл командой из п. 4.3 |
| Ребалансы консьюмеров | Группы уходят в `PreparingRebalance` | Переезд партиций `__consumer_offsets` | Нормально при батче ≤ 5; если группы «застряли» — дождаться завершения батча, не запускать следующий |
| Контроллер перегружен | Растёт `ActiveControllerCount` flapping, медленные метаданные | Слишком много партиций в одном реассайне | Батчи ≤ 30 партиций |
| Топик с RF > 5 | `--execute` падает с ошибкой | Нельзя RF больше числа брокеров | Не должно быть при 5 брокерах; проверить план |
| Партиция осталась `failed` | `--verify` показывает `failed` | Брокер-приёмник был недоступен | Повторить `--execute` для этой партиции отдельным файлом |

### 11.1. Диагностика «зависшего» переноса

```bash
# 1. Что вообще идёт
kafka-reassign-partitions.sh --bootstrap-server "$BS" --list

# 2. Кто отстаёт (на брокерах 4, 5)
ssh broker4 "grep -iE 'ReplicaFetcher|Truncat|OutOfRange|Error' /var/log/kafka/server.log | tail -40"

# 3. Реальная скорость приёма на новом брокере (JMX)
#    kafka.server:type=BrokerTopicMetrics,name=ReplicationBytesInPerSec

# 4. Железо
ssh broker4 "iostat -x 5 3; sar -n DEV 5 3"

# 5. Действующие троттлы
for id in 1 2 3 4 5; do
  echo -n "broker $id: "
  kafka-configs.sh --bootstrap-server "$BS" --entity-type brokers --entity-name $id --describe
done
```

---

## 12. Опциональные улучшения (после основной задачи)

### 12.1. Увеличение числа партиций

Топик с 3 партициями на 5 брокерах не может задействовать все брокеры как лидеров. Если топик
горячий, имеет смысл увеличить число партиций до кратного 5.

> 🔴 **Необратимо.** Увеличение партиций ломает порядок сообщений по ключу: ключ, ранее
> попадавший в партицию 0, после расширения может пойти в партицию 4. Для топиков, где важен
> порядок по ключу (CDC, event sourcing), делать **нельзя** без согласования с владельцем.
> Compacted-топики (`__consumer_offsets` и пользовательские) расширять особенно опасно.

```bash
# Кандидаты: топики с числом партиций < 5 и заметным трафиком
kafka-topics.sh --bootstrap-server "$BS" --describe \
  | awk '/PartitionCount:/ { for(i=1;i<=NF;i++) if($i=="PartitionCount:") p=$(i+1);
                             for(i=1;i<=NF;i++) if($i=="Topic:") t=$(i+1);
                             if (p+0 < 5) print t, p }'

# Расширение (после согласования!)
kafka-topics.sh --bootstrap-server "$BS" --alter --topic my-topic --partitions 10
```

Новые партиции Kafka сама разложит с учётом всех 5 брокеров.

### 12.2. Cruise Control

Для регулярной балансировки на кластере из 5+ брокеров имеет смысл поставить
[LinkedIn Cruise Control](https://github.com/linkedin/cruise-control): он считает целевое
размещение по реальным метрикам (CPU, диск, сеть, а не по числу партиций), умеет непрерывную
самобалансировку, троттлинг и безопасное выведение брокеров.

Совместим с Kafka 3.9 / ZooKeeper. Разворачивается после того, как кластер уже выровнен
описанным выше способом.

### 12.3. Планирование миграции на KRaft

Kafka 3.9 — последняя мажорная ветка с поддержкой ZooKeeper; в Kafka 4.x его нет. Расширение
кластера — удобный момент, чтобы поставить в бэклог миграцию ZK → KRaft.

---

## 13. Шпаргалка команд

```bash
# Состояние
kafka-topics.sh --bootstrap-server $BS --describe --under-replicated-partitions
kafka-topics.sh --bootstrap-server $BS --describe --under-min-isr-partitions
kafka-topics.sh --bootstrap-server $BS --describe --unavailable-partitions
kafka-reassign-partitions.sh --bootstrap-server $BS --list
kafka-log-dirs.sh --bootstrap-server $BS --describe --broker-list 1,2,3,4,5

# Реассайн
kafka-reassign-partitions.sh --bootstrap-server $BS --topics-to-move-json-file t.json \
  --broker-list 1,2,3,4,5 --generate
kafka-reassign-partitions.sh --bootstrap-server $BS --reassignment-json-file p.json \
  --throttle 31457280 --execute
kafka-reassign-partitions.sh --bootstrap-server $BS --reassignment-json-file p.json \
  --verify --preserve-throttles
kafka-reassign-partitions.sh --bootstrap-server $BS --reassignment-json-file p.json --verify
kafka-reassign-partitions.sh --bootstrap-server $BS --reassignment-json-file p.json --cancel
kafka-reassign-partitions.sh --bootstrap-server $BS --reassignment-json-file p2.json --additional

# Троттлинг
kafka-configs.sh --bootstrap-server $BS --entity-type brokers --entity-name 1 --alter \
  --add-config "leader.replication.throttled.rate=31457280,follower.replication.throttled.rate=31457280"
kafka-configs.sh --bootstrap-server $BS --entity-type brokers --entity-name 1 --alter \
  --delete-config "leader.replication.throttled.rate,follower.replication.throttled.rate"

# Лидеры
kafka-leader-election.sh --bootstrap-server $BS --election-type PREFERRED --all-topic-partitions

# ZooKeeper
zookeeper-shell.sh zk1:2181 <<< "ls /brokers/ids"
zookeeper-shell.sh zk1:2181 <<< "get /brokers/ids/4"
```

---

## 14. Итоговый чек-лист

**Подготовка**
- [ ] 5 брокеров в `/brokers/ids`, `kafka-broker-api-versions.sh` видит все 5
- [ ] TCP-связность проверена во все стороны
- [ ] Конфиги новых брокеров сверены со старыми (п. 1.3)
- [ ] Подтверждено, что `broker.rack` не задан ни на одном из 5 брокеров (п. 1.4)
- [ ] URP = 0, under-min-isr = 0, unavailable = 0, активных реассайнов нет
- [ ] Остатков троттлинга нет
- [ ] Места на дисках хватает (≥ 30 % запаса после переноса)
- [ ] Baseline метрик снят
- [ ] Окно согласовано

**Планирование**
- [ ] `current-assignment.json` сохранён и скопирован за пределы кластера
- [ ] Целевой план рассчитан, отчёт проверен
- [ ] Валидация плана прошла с кодом 0
- [ ] Объём переноса и время оценены
- [ ] Батчи упорядочены: мелкие → крупные → служебные

**Выполнение**
- [ ] Троттл установлен на всех 5 брокерах
- [ ] Пилотный батч прошёл, метрики в норме
- [ ] Остальные батчи выполнены по одному, с проверкой URP=0 между ними
- [ ] `__consumer_offsets` перенесён батчами по 5
- [ ] `__transaction_state` перенесён (или отсутствует)

**Завершение**
- [ ] Троттлы сняты (топиковые и брокерские), проверено
- [ ] Preferred leader election выполнен, лидеры выровнены
- [ ] Реплики и объёмы распределены по 5 брокерам с разбросом ≤ 10 %
- [ ] URP = 0, число топиков/партиций и RF не изменились
- [ ] Smoke-тест продюсер/консьюмер пройден
- [ ] Метрики вернулись к baseline, лаги в норме
- [ ] `num.replica.fetchers` возвращён к исходному
- [ ] Артефакты сохранены в тикет
