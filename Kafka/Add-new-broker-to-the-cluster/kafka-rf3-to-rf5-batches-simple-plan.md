# Упрощённый вариант: RF 3 → 5 пачками через штатный `kafka-reassign-partitions.sh`

**Кластер:** Kafka 3.9.1, режим ZooKeeper
**Было:** брокеры 1, 2, 3, у топиков replication factor = 3
**Стало:** брокеры 1, 2, 3, 4, 5, у топиков replication factor = 5
**Подход:** диагностика кластера с сохранением отчёта → пачки по 10 топиков → план из `--generate`,
дополненный до RF=5 → выполнение пачек по одной под троттлингом
**Документ составлен:** 24.09.2026
**Связанный документ:** [kafka-rebalance-3-to-5-brokers-plan.md](kafka-rebalance-3-to-5-brokers-plan.md) —
полный вариант с перераспределением при RF=3 и подробными разделами про троттлинг, риски и диагностику

---

## 0. Суть варианта и чем он отличается от основного плана

| | Основной план | Этот (упрощённый) вариант |
|---|---|---|
| RF после работ | 3 | **5** |
| Что делаем с репликами | переносим 40 % реплик со старых брокеров на новые | **только добавляем** реплики на 4 и 5, ничего не удаляем |
| Кто считает размещение | свой скрипт (минимум перемещений) | **штатный `--generate`**, результат дополняется до RF=5 |
| Разбиение | по партициям | **по топикам, 10 штук за раз** |
| Откат завершённой пачки | обратный перенос (снова копирование данных) | **только удаление добавленных реплик** — быстро, без копирования |

### Почему при RF=5 план получается простым

Брокеров 5 и RF=5, значит **каждая партиция будет лежать на всех пяти брокерах**. Состав реплик
у всех партиций одинаковый — `{1,2,3,4,5}`. Выбирать можно только **порядок** реплик: от первой реплики
в списке зависит preferred leader. Порядок берём из того, что предложит `--generate`: он раскладывает
лидеров по всем пяти брокерам по кругу.

### Что `--generate` делает и чего не делает

* `--generate` **сохраняет текущий RF**. Для топика с RF=3 он предложит 3 реплики на брокерах 1–5.
  **Увеличить RF одной этой утилитой нельзя.** Поэтому предложение надо дополнить до 5 реплик.
  Это делает маленький скрипт `rf5tool.py expand` (п. 3.2): он берёт порядок из предложения Kafka
  и дописывает в конец недостающих брокеров.
* Первым JSON-ом в выводе `--generate` идёт **текущее размещение**. Сохраняем его как файл отката для
  каждой пачки.
* Выполнение, наблюдение, троттлинг, отмена и снятие троттлов — **только штатными утилитами**:
  `kafka-reassign-partitions.sh --execute / --verify / --cancel`, `kafka-leader-election.sh`.

### Последствия RF=5, которые надо принять осознанно до начала работ

| Что меняется | Как именно | Что проверить |
|---|---|---|
| Место на дисках | Сейчас каждый из брокеров 1–3 хранит **все** партиции (3 брокера × RF 3). После работ **все пять** брокеров будут хранить всё. Суммарный объём хранения вырастет в 5/3 раза | На брокерах 4 и 5 должно поместиться столько же, сколько сейчас лежит на брокере 1, плюс ≥ 30 % запаса |
| Нагрузка на диски брокеров 1–3 | **Не уменьшится.** Их данные остаются на месте. Разгружается только лидерство (запись от продюсеров, чтение консьюмерами) | Если цель — освободить диски старых брокеров, этот вариант не подходит, нужен основной план |
| Сетевой трафик репликации | Каждый лидер отдаёт данные **4** фолловерам вместо 2, то есть исходящий трафик репликации удваивается | Запас сети на брокерах с учётом пикового `BytesInPerSec × 4` |
| Задержка `acks=all` | Лидер ждёт подтверждения от всех реплик ISR, теперь их 5. p99 записи может вырасти, особенно если один брокер медленнее остальных | Сравнить p99 `Produce TotalTimeMs` до и после пилотной пачки |
| Отказоустойчивость | Растёт: при полном ISR данные переживают потерю 4 брокеров. Реальные гарантии для подтверждённой записи задаёт `min.insync.replicas` | Решение по `min.insync.replicas` (п. 8.1) |
| Новые топики | Будут создаваться с `default.replication.factor` (обычно 3), а не 5 | Решение по п. 8.2 |

Если что-то из этого неприемлемо, остановиться и выбрать основной план.

### Схема работ

```
Этап 1  diagnose.sh before     → cluster-report-before.md  + REPORT.md (итоговый файл)
            │  стоп, если НЕ ЗДОРОВ
Этап 2  prepare-batches.sh     → batches/topics-NN.json → --generate → rollback-NN.json
            │                     + batch-NN.json (RF=5)  + сводка в REPORT.md
Этап 3  выбрать THROTTLE
Этап 4  run-batch.sh 01        → пилотная пачка (самые мелкие топики), проверка влияния
        run-batch.sh 02 … NN   → по одной, с проверкой между пачками
Этап 5  служебные топики       → __consumer_offsets по 10 партиций
Этап 6  зачистка троттлов, выравнивание лидеров
Этап 7  diagnose.sh after      → cluster-report-after.md + REPORT.md
```

---

## 1. Подготовка

### 1.1. Переменные окружения

Соглашение то же, что в основном плане (п. 1.0 там): `$broker` и `$config`.

```bash
# доступ к кластеру
broker=broker1:9092,broker2:9092,broker3:9092,broker4:9092,broker5:9092
config=/kafka/secrets/sasl_admin_kafka.properties

# параметры этого плана
BROKERS=1,2,3,4,5          # id всех брокеров
TARGET_RF=5                # целевой replication factor
BATCH_TOPICS=10            # топиков в пачке
MAX_PARTS=300              # не больше партиций в пачке (крупные топики не набьют одну пачку)
LARGE_GIB=100              # топик больше этого объёма (GiB) идёт отдельной пачкой
THROTTLE=31457280          # 30 МБ/с на брокер, уточняется в этапе 3

# необязательные
ZK=zk1:2181                             # для проверки /brokers/ids и /controller; пусто, если нет доступа
DF_HOSTS="broker1 broker2 broker3 broker4 broker5"   # для df по ssh; пусто, если нет ssh
KAFKA_DATA_DIR=/kafka/data              # каталог log.dirs для df

export broker config BROKERS TARGET_RF BATCH_TOPICS MAX_PARTS LARGE_GIB THROTTLE ZK DF_HOSTS KAFKA_DATA_DIR
```

> Утилиты Kafka (`kafka-topics.sh` и т. д.) должны быть в `PATH`. Нужен `python3` (3.6+),
> дополнительные модули не требуются.
> `kafka-leader-election.sh` принимает конфиг через **`--admin.config`**, а не `--command-config`.

Быстрая проверка доступа:

```bash
[ -r "$config" ] || echo "ОШИБКА: $config недоступен"
kafka-broker-api-versions.sh --bootstrap-server $broker --command-config $config | grep -E '^\S+ \(id:'
# ожидаем 5 строк: (id: 1 …) … (id: 5 …)
```

### 1.2. Рабочий каталог

Все артефакты хранятся в одном каталоге. Работать внутри `tmux`/`screen`: пачка может идти часами.

```bash
WORKDIR=/var/tmp/kafka-rf5-$(date +%Y%m%d)
mkdir -p "$WORKDIR" && cd "$WORKDIR"
```

Во что превратится каталог к концу работ:

```
kafka-rf5-YYYYMMDD/
├── rf5tool.py                  # расчёты (п. 1.3)
├── diagnose.sh                 # диагностика (п. 2.1)
├── prepare-batches.sh          # подготовка пачек (п. 3.1)
├── run-batch.sh                # выполнение одной пачки (п. 5.1)
├── REPORT.md                   # ИТОГОВЫЙ ФАЙЛ: диагностика до, план пачек, журнал, диагностика после
├── cluster-report-before.md    # отчёт диагностики до работ
├── cluster-report-after.md     # отчёт диагностики после работ
├── diag-before/, diag-after/   # сырые выводы утилит
└── batches/
    ├── plan.tsv                # какие топики в какой пачке
    ├── topics-NN.json          # вход для --generate
    ├── generate-NN.txt         # полный вывод --generate
    ├── rollback-NN.json        # ВАЖНО: текущее размещение = откат пачки
    ├── proposed-NN.json        # предложение Kafka (RF=3)
    ├── batch-NN.json           # то, что выполняем (RF=5)
    ├── exec-NN.log, verify-NN.txt, elect-NN.*, describe-after-NN.txt
    ├── summary.md              # объёмы и оценка времени по пачкам
    └── progress.log            # журнал выполнения
```

### 1.3. Вспомогательный скрипт `rf5tool.py`

Скрипт **только читает файлы** в рабочем каталоге и в кластер не ходит. Все обращения к Kafka
делаются штатными утилитами в bash-скриптах ниже. Подкоманды:

| Подкоманда | Что делает |
|---|---|
| `diag` | строит отчёт о здоровье и составе кластера из сырых выводов утилит |
| `plan` | делит топики на пачки по 10, от мелких к крупным |
| `expand` | дополняет предложение `--generate` до RF=5, проверяет результат |
| `summary` | считает объём копирования и время по каждой пачке |
| `check` | сверяет фактическое размещение после пачки с планом |
| `elect` | готовит JSON для `kafka-leader-election.sh` по партициям пачки |

```bash
cat > rf5tool.py <<'PY'
#!/usr/bin/env python3
"""rf5tool — расчёты для плана «RF 3 -> 5 пачками через kafka-reassign-partitions.sh».

Только читает файлы, в кластер не ходит. Подкоманды:
  diag     отчёт о состоянии кластера по сырым выводам diagnose.sh
  plan     разбить топики на пачки (topics-NN.json для --generate)
  expand   предложение --generate (RF=3) -> batch-NN.json с целевым RF
  summary  объём копирования и оценка времени по пачкам
  check    сверить результат пачки с фактическим kafka-topics --describe
  elect    JSON для kafka-leader-election.sh по партициям пачки
"""
import argparse, glob, json, os, re, sys
from collections import Counter, defaultdict

GiB = 1024 ** 3


# ---------------------------------------------------------------- парсеры
def parse_describe(path):
    """kafka-topics.sh --describe -> {topic: {rf, pc, configs, parts{p: {...}}}}"""
    topics = {}
    for line in open(path, encoding='utf-8', errors='replace'):
        f = {}
        for chunk in line.strip().split('\t'):
            k, sep, v = chunk.partition(':')
            if sep:
                f[k.strip()] = v.strip()
        t = f.get('Topic')
        if not t:
            continue
        rec = topics.setdefault(t, {'rf': 0, 'pc': 0, 'configs': {}, 'parts': {}})
        if 'Partition' in f:
            ids = lambda s: [int(x) for x in s.split(',') if x.strip().lstrip('-').isdigit()]
            rec['parts'][int(f['Partition'])] = {
                'leader': int(f['Leader']) if f.get('Leader', '').lstrip('-').isdigit() else None,
                'replicas': ids(f.get('Replicas', '')),
                'isr': ids(f.get('Isr', '')),
            }
        elif 'PartitionCount' in f:
            rec['pc'] = int(f['PartitionCount'])
            rec['rf'] = int(f.get('ReplicationFactor', 0))
            for kv in f.get('Configs', '').split(','):
                k, sep, v = kv.partition('=')
                if sep:
                    rec['configs'][k.strip()] = v.strip()
    for rec in topics.values():   # RF по факту: максимум по партициям
        if rec['parts']:
            rec['rf'] = max(len(p['replicas']) for p in rec['parts'].values())
    return topics


def parse_logdirs(path):
    """kafka-log-dirs.sh (последняя строка) -> sizes{(t,p)}, per_broker{b}, errors[]"""
    sizes, per_broker, errors, space = {}, defaultdict(lambda: [0, 0]), [], {}
    try:
        d = json.load(open(path))
    except Exception as e:
        return sizes, per_broker, [f'logdirs.json не разобран: {e}'], space
    for b in d.get('brokers', []):
        bid = b['broker']
        for ld in b.get('logDirs', []):
            if ld.get('error') not in (None, 'null', ''):
                errors.append(f"broker {bid} {ld.get('logDir')}: {ld['error']}")
            if 'totalBytes' in ld and 'usableBytes' in ld:
                space[(bid, ld['logDir'])] = (ld['totalBytes'], ld['usableBytes'])
            for p in ld.get('partitions', []):
                if p.get('isFuture'):
                    continue
                t, _, n = p['partition'].rpartition('-')
                k = (t, int(n))
                sizes[k] = max(sizes.get(k, 0), p['size'])
                per_broker[bid][0] += p['size']
                per_broker[bid][1] += 1
    return sizes, per_broker, errors, space


def count_partition_lines(path):
    if not os.path.exists(path):
        return None
    return sum(1 for l in open(path, errors='replace') if 'Partition:' in l)


def read(path):
    return open(path, errors='replace').read() if os.path.exists(path) else ''


def broker_setting(text, name):
    m = re.search(rf'^\s*{re.escape(name)}=(\S*)', text, re.M)
    return m.group(1) if m else None


def is_internal(t):
    return t.startswith('__')


def ints(s):
    return [int(x) for x in s.split(',') if x.strip()]


def gib(x):
    return f'{x / GiB:,.2f}'.replace(',', ' ')


def hms(sec):
    sec = int(sec)
    return f'{sec // 3600} ч {sec % 3600 // 60:02d} мин'


# ---------------------------------------------------------------- diag
def cmd_diag(a):
    D = a.dir
    brokers = ints(a.brokers)
    topics = parse_describe(f'{D}/describe.txt')
    sizes, per_broker, ld_errors, space = parse_logdirs(f'{D}/logdirs.json')

    api = read(f'{D}/brokers-api.txt')
    seen = {int(m.group(1)): m.group(2) for m in
            re.finditer(r'^\S+ \(id: (\d+) rack: ([^)]*)\)', api, re.M)}
    ball = read(f'{D}/broker-all.txt')
    default_min_isr = int(broker_setting(ball, 'min.insync.replicas') or 1)

    n_parts = n_repl = 0
    rf_topics, rf_parts = Counter(), Counter()
    repl_on, lead_on, pref_on = Counter(), Counter(), Counter()
    no_leader, not_preferred, isr_short = [], 0, 0
    min_isr_topics = Counter()
    risky_min_isr = []
    add_repl = add_bytes = 0
    for t, rec in sorted(topics.items()):
        rf_topics[rec['rf']] += 1
        mi = int(rec['configs'].get('min.insync.replicas', default_min_isr))
        min_isr_topics[mi] += 1
        if mi >= rec['rf'] and not is_internal(t):
            risky_min_isr.append(f'{t} (RF={rec["rf"]}, min.isr={mi})')
        for p, pr in rec['parts'].items():
            n_parts += 1
            n_repl += len(pr['replicas'])
            rf_parts[len(pr['replicas'])] += 1
            for b in pr['replicas']:
                repl_on[b] += 1
            if pr['replicas']:
                pref_on[pr['replicas'][0]] += 1
            if pr['leader'] is None:
                no_leader.append(f'{t}-{p}')
            else:
                lead_on[pr['leader']] += 1
                if pr['replicas'] and pr['leader'] != pr['replicas'][0]:
                    not_preferred += 1
            if len(pr['isr']) < len(pr['replicas']):
                isr_short += 1
            miss = max(0, a.target_rf - len(pr['replicas']))
            add_repl += miss
            add_bytes += miss * sizes.get((t, p), 0)

    unique_bytes = sum(sizes.values())
    urp = count_partition_lines(f'{D}/urp.txt')
    umi = count_partition_lines(f'{D}/under-min-isr.txt')
    unav = count_partition_lines(f'{D}/unavailable.txt')
    rl = read(f'{D}/reassign-list.txt')
    active = len(re.findall(r'^\S+-\d+: replicas:', rl, re.M))
    rl_ok = 'No partition reassignments found' in rl

    b_thr = {}
    for b in brokers:
        txt = read(f'{D}/broker-dyn-{b}.txt')
        b_thr[b] = re.findall(r'((?:leader|follower)\.replication\.throttled\.rate=\d+)', txt)
    t_thr, cur = set(), None
    for line in read(f'{D}/topic-configs.txt').splitlines():
        m = re.search(r'configs for topic (\S+) are', line)
        if m:
            cur = m.group(1)
        elif cur and 'throttled.replicas=' in line:
            t_thr.add(cur)

    # ---- вердикт
    fail, warn = [], []
    missing = [b for b in brokers if b not in seen]
    if missing:
        fail.append(f'брокеры не отвечают через AdminClient: {missing}')
    if urp is None or urp:
        fail.append(f'under-replicated партиций: {urp}')
    if umi is None or umi:
        fail.append(f'партиций ниже min.insync.replicas: {umi}')
    if unav is None or unav:
        fail.append(f'недоступных партиций: {unav}')
    if no_leader:
        fail.append(f'партиций без лидера: {len(no_leader)}')
    if not rl_ok:
        fail.append(f'идёт реассайн или --list не отработал (активных партиций: {active})')
    if ld_errors:
        fail.append(f'ошибки log dirs: {len(ld_errors)}')
    if any(b_thr.values()) or t_thr:
        warn.append('остались троттлы репликации (брокеры/топики) — снять или учесть')
    if not topics:
        fail.append('describe.txt пуст — kafka-topics.sh не отработал')
    if not_preferred:
        warn.append(f'партиций, где лидер не preferred: {not_preferred}')
    odd_rf = sorted(t for t, r in topics.items() if r['rf'] != 3 and not is_internal(t))
    if odd_rf:
        warn.append(f'пользовательских топиков с RF != 3: {len(odd_rf)}')
    if risky_min_isr:
        warn.append(f'топиков с min.insync.replicas >= RF: {len(risky_min_isr)}')

    # ---- отчёт
    o = []
    w = o.append
    w(f'# Диагностика кластера Kafka — этап `{a.stage}`\n')
    w(f'* Дата: {a.date or "-"}')
    w(f'* Версия утилит: {read(f"{D}/version.txt").strip() or "-"}')
    zk = read(f'{D}/zk-brokers.txt').strip().splitlines()
    if zk:
        w(f'* ZooKeeper /brokers/ids: `{zk[-1]}`')
    ctl = re.search(r'"brokerid":(\d+)', read(f'{D}/zk-controller.txt'))
    if ctl:
        w(f'* Активный контроллер: broker {ctl.group(1)}')
    w(f'* Сырые данные: `{D}/`\n')

    w('## 1. Итог\n')
    w('**КЛАСТЕР ЗДОРОВ**\n' if not fail else '**КЛАСТЕР НЕ ЗДОРОВ — перенос не начинать**\n')
    for x in fail:
        w(f'* [FAIL] {x}')
    for x in warn:
        w(f'* [WARN] {x}')
    w('')

    w('## 2. Проверки\n')
    w('| Проверка | Значение | Ожидается |')
    w('|---|---|---|')
    w(f'| Брокеры, ответившие AdminClient | {sorted(seen)} | {brokers} |')
    w(f'| Under-replicated партиции | {urp} | 0 |')
    w(f'| Партиции ниже min.insync.replicas | {umi} | 0 |')
    w(f'| Недоступные партиции / без лидера | {unav} / {len(no_leader)} | 0 / 0 |')
    w(f'| Партиции, где ISR < Replicas (по describe) | {isr_short} | 0 |')
    w(f'| Активные реассайны | {"нет" if rl_ok else active} | нет |')
    w(f'| Ошибки log dirs | {len(ld_errors)} | 0 |')
    w(f'| Брокеры с throttled.rate | {[b for b, v in b_thr.items() if v] or "нет"} | нет |')
    w(f'| Топики с throttled.replicas | {len(t_thr)} | 0 |')
    w(f'| Лидер не на preferred-реплике | {not_preferred} | 0 (или мало) |\n')
    for x in ld_errors:
        w(f'* {x}')

    user = [t for t in topics if not is_internal(t)]
    w('## 3. Топики, партиции, реплики\n')
    w('| Показатель | Значение |')
    w('|---|---|')
    w(f'| Топиков всего | {len(topics)} |')
    w(f'| — пользовательских | {len(user)} |')
    w(f'| — служебных (`__*`) | {len(topics) - len(user)} ({", ".join(sorted(t for t in topics if is_internal(t))) or "-"}) |')
    w(f'| Партиций | {n_parts} |')
    w(f'| Реплик | {n_repl} |')
    w(f'| Объём данных без учёта реплик (уникальный), GiB | {gib(unique_bytes)} |')
    w(f'| Объём данных с репликами, GiB | {gib(sum(v[0] for v in per_broker.values()))} |\n')

    w('### Распределение по replication factor\n')
    w('| RF | Топиков | Партиций |')
    w('|---|---|---|')
    for rf in sorted(set(rf_topics) | set(rf_parts)):
        w(f'| {rf} | {rf_topics[rf]} | {rf_parts[rf]} |')
    w('')
    w(f'### min.insync.replicas (брокерский дефолт: {default_min_isr})\n')
    w('| min.insync.replicas | Топиков |')
    w('|---|---|')
    for k in sorted(min_isr_topics):
        w(f'| {k} | {min_isr_topics[k]} |')
    w('')

    w('## 4. Брокеры\n')
    w('| Брокер | rack | Реплик | Лидеров | Preferred-лидеров | Данные, GiB |')
    w('|---|---|---|---|---|---|')
    for b in brokers:
        w(f'| {b} | {seen.get(b, "НЕ ОТВЕЧАЕТ")} | {repl_on[b]} | {lead_on[b]} | {pref_on[b]} | '
          f'{gib(per_broker[b][0]) if b in per_broker else "-"} |')
    w('')
    if space:
        w('| Брокер | log dir | Всего, GiB | Свободно, GiB |')
        w('|---|---|---|---|')
        for (b, ld), (tot, us) in sorted(space.items()):
            w(f'| {b} | `{ld}` | {gib(tot)} | {gib(us)} |')
        w('')
    df = read(f'{D}/df.txt').strip()
    if df:
        w('Свободное место (df):\n\n```\n' + df + '\n```\n')

    w('### Ключевые параметры брокера (broker-all.txt)\n')
    w('| Параметр | Значение |')
    w('|---|---|')
    for k in ('default.replication.factor', 'min.insync.replicas', 'offsets.topic.replication.factor',
              'transaction.state.log.replication.factor', 'auto.leader.rebalance.enable',
              'unclean.leader.election.enable', 'num.replica.fetchers', 'replica.fetch.max.bytes',
              'message.max.bytes', 'inter.broker.protocol.version'):
        w(f'| `{k}` | {broker_setting(ball, k) or "-"} |')
    w('')

    w(f'## 5. Оценка перехода на RF={a.target_rf}\n')
    w('| Показатель | Значение |')
    w('|---|---|')
    w(f'| Партиций с RF < {a.target_rf} | {sum(v for k, v in rf_parts.items() if k < a.target_rf)} |')
    w(f'| Реплик к добавлению | {add_repl} |')
    w(f'| Объём копирования (все топики, вкл. служебные), GiB | {gib(add_bytes)} |')
    if a.target_rf == len(brokers):
        w(f'| Данных на КАЖДОМ брокере после (≈ уникальный объём), GiB | {gib(unique_bytes)} |')
    if a.throttle:
        # каждый новый брокер должен принять весь уникальный объём
        w(f'| Оценка времени при троттле {a.throttle / 1024 ** 2:.0f} МБ/с (×1,5 запас) | '
          f'{hms(unique_bytes / a.throttle * 1.5)} |')
    w('')

    if odd_rf:
        w('## 6. Пользовательские топики с RF != 3\n')
        for t in odd_rf[:200]:
            w(f'* `{t}` — RF={topics[t]["rf"]}')
        w('')
    if risky_min_isr:
        w('## 7. Топики с min.insync.replicas >= RF (acks=all встанет при потере одного брокера)\n')
        for x in risky_min_isr[:200]:
            w(f'* `{x}`')
        w('')
    if no_leader:
        w('## Партиции без лидера\n')
        for x in no_leader[:200]:
            w(f'* `{x}`')
        w('')

    open(a.out, 'w').write('\n'.join(o) + '\n')
    print(f'отчёт: {a.out}')
    print('ИТОГ:', 'ЗДОРОВ' if not fail else 'НЕ ЗДОРОВ')
    for x in fail:
        print('  FAIL', x)
    for x in warn:
        print('  WARN', x)
    sys.exit(0 if not fail else 1)


# ---------------------------------------------------------------- plan
def cmd_plan(a):
    topics = parse_describe(f'{a.dir}/describe.txt')
    sizes, *_ = parse_logdirs(f'{a.dir}/logdirs.json')
    excl = re.compile(a.exclude) if a.exclude else None
    cand = []
    for t, rec in topics.items():
        if is_internal(t) or (excl and excl.search(t)):
            continue
        if rec['rf'] >= a.target_rf:
            continue
        tb = sum(sizes.get((t, p), 0) for p in rec['parts'])
        cand.append((tb, t, len(rec['parts'])))
    cand.sort()   # от мелких к крупным: первая пачка — пилотная

    batches, cur = [], []
    for tb, t, pc in cand:
        if tb >= a.large_gib * GiB or pc >= a.max_parts:
            batches.append([(tb, t, pc)])      # крупный топик — отдельной пачкой
            continue
        if cur and (len(cur) >= a.batch_topics or sum(x[2] for x in cur) + pc > a.max_parts):
            batches.append(cur)
            cur = []
        cur.append((tb, t, pc))
    if cur:
        batches.append(cur)
    batches.sort(key=lambda b: sum(x[0] for x in b))

    os.makedirs(a.out_dir, exist_ok=True)
    with open(f'{a.out_dir}/plan.tsv', 'w') as tsv:
        tsv.write('batch\ttopics\tpartitions\tGiB\ttopic_list\n')
        for i, b in enumerate(batches, 1):
            n = f'{i:02d}'
            json.dump({'version': 1, 'topics': [{'topic': t} for _, t, _ in b]},
                      open(f'{a.out_dir}/topics-{n}.json', 'w'), indent=1)
            tsv.write(f'{n}\t{len(b)}\t{sum(x[2] for x in b)}\t{sum(x[0] for x in b) / GiB:.2f}\t'
                      f'{",".join(t for _, t, _ in b)}\n')
    print(f'топиков к переносу: {len(cand)}, пачек: {len(batches)} -> {a.out_dir}/topics-NN.json, plan.tsv')


# ---------------------------------------------------------------- expand
def load_assignment(path):
    d = json.load(open(path))
    return {(p['topic'], p['partition']): list(p['replicas']) for p in d['partitions']}


def cmd_expand(a):
    brokers = ints(a.brokers)
    cur = load_assignment(a.current)
    prop = load_assignment(a.proposed)
    if set(cur) != set(prop):
        sys.exit(f'ОШИБКА: набор партиций в {a.current} и {a.proposed} различается')

    load = Counter(b for r in cur.values() for b in r)
    out, errs, lead = [], [], Counter()
    for k in sorted(cur):
        c, p = cur[k], prop[k]
        if len(c) > a.target_rf:
            errs.append(f'{k}: RF {len(c)} > {a.target_rf} — уменьшение RF планом не поддерживается')
            continue
        chosen = list(c)                               # текущие реплики не удаляем
        for b in p + sorted(brokers, key=lambda x: (load[x], x)):
            if len(chosen) >= a.target_rf:
                break
            if b not in chosen:
                chosen.append(b)
                load[b] += 1
        # порядок = порядок из предложения Kafka (preferred leader — первый), затем остальные
        order = [b for b in p if b in chosen] + [b for b in chosen if b not in p]
        if len(order) != a.target_rf or len(set(order)) != len(order) \
                or not set(order) <= set(brokers) or not set(c) <= set(order):
            errs.append(f'{k}: некорректный результат {order}')
        lead[order[0]] += 1
        out.append({'topic': k[0], 'partition': k[1], 'replicas': order})
    if errs:
        sys.exit('ОШИБКИ:\n  ' + '\n  '.join(errs))

    chunks = [out] if not a.chunk else [out[i:i + a.chunk] for i in range(0, len(out), a.chunk)]
    for i, ch in enumerate(chunks, 1):
        path = a.out if len(chunks) == 1 else re.sub(r'\.json$', f'-{i:02d}.json', a.out)
        json.dump({'version': 1, 'partitions': ch}, open(path, 'w'), indent=1)
        print(f'{path}: партиций {len(ch)}')
    print(f'  добавляется реплик: {sum(len(x["replicas"]) for x in out) - sum(len(v) for v in cur.values())}, '
          f'preferred-лидеры: {dict(sorted(lead.items()))}')


# ---------------------------------------------------------------- summary
def cmd_summary(a):
    sizes, *_ = parse_logdirs(f'{a.dir}/logdirs.json')
    topics = parse_describe(f'{a.dir}/describe.txt')
    leader = {(t, p): pr['leader'] for t, r in topics.items() for p, pr in r['parts'].items()}
    rows, total, total_t = [], 0, 0
    for f in sorted(glob.glob(f'{a.batches}/batch-*.json')):
        name = os.path.basename(f)[6:-5]
        rb = f'{a.batches}/rollback-{name}.json'
        if not os.path.exists(rb):    # пачка, порезанная --chunk: batch-co-01 -> rollback-co
            rb = f'{a.batches}/rollback-{re.sub(r"-[0-9]+$", "", name)}.json'
        cur = load_assignment(rb) if os.path.exists(rb) else {}
        tgt = load_assignment(f)
        inb, outb, byt, nadd = Counter(), Counter(), 0, 0
        for k, r in tgt.items():
            added = set(r) - set(cur.get(k, r))
            s = sizes.get(k, 0)
            nadd += len(added)
            byt += s * len(added)
            for b in added:
                inb[b] += s
            outb[leader.get(k)] += s * len(added)
        bottleneck = max(list(inb.values()) + list(outb.values()) + [0])
        eta = bottleneck / a.throttle * 1.5 if a.throttle else 0
        total += byt
        total_t += eta
        rows.append((name, len({k[0] for k in tgt}), len(tgt), nadd, byt, eta))
    o = ['## Пачки RF 3 → 5\n',
         '| Пачка | Топиков | Партиций | +Реплик | Копируется, GiB | Оценка времени |',
         '|---|---|---|---|---|---|']
    for n, nt, np_, na, b, e in rows:
        o.append(f'| {n} | {nt} | {np_} | {na} | {gib(b)} | {hms(e) if a.throttle else "-"} |')
    o.append(f'| **Итого** | | {sum(r[2] for r in rows)} | {sum(r[3] for r in rows)} | '
             f'**{gib(total)}** | **{hms(total_t) if a.throttle else "-"}** |')
    if a.throttle:
        o.append(f'\nТроттл {a.throttle / 1024 ** 2:.0f} МБ/с на брокер, в оценке запас ×1,5.')
    print('\n'.join(o) + '\n')


# ---------------------------------------------------------------- check
def cmd_check(a):
    topics = parse_describe(a.describe)
    tgt = load_assignment(a.batch)
    bad = []
    for (t, p), r in sorted(tgt.items()):
        pr = topics.get(t, {}).get('parts', {}).get(p)
        if not pr:
            bad.append(f'{t}-{p}: нет в describe')
        elif set(pr['replicas']) != set(r):
            bad.append(f'{t}-{p}: replicas {pr["replicas"]}, ожидалось {r}')
        elif set(pr['isr']) != set(r):
            bad.append(f'{t}-{p}: isr {pr["isr"]} неполный')
    print(f'{a.batch}: проверено партиций {len(tgt)}, расхождений {len(bad)}')
    for x in bad[:50]:
        print('  ', x)
    sys.exit(1 if bad else 0)


# ---------------------------------------------------------------- elect
def cmd_elect(a):
    tgt = load_assignment(a.batch)
    json.dump({'partitions': [{'topic': t, 'partition': p} for t, p in sorted(tgt)]},
              open(a.out, 'w'), indent=1)
    print(f'{a.out}: партиций {len(tgt)}')


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sp = ap.add_subparsers(dest='cmd', required=True)

    p = sp.add_parser('diag')
    p.add_argument('--dir', required=True)
    p.add_argument('--out', required=True)
    p.add_argument('--brokers', default='1,2,3,4,5')
    p.add_argument('--target-rf', type=int, default=5)
    p.add_argument('--throttle', type=int, default=0)
    p.add_argument('--stage', default='before')
    p.add_argument('--date', default='')
    p.set_defaults(fn=cmd_diag)

    p = sp.add_parser('plan')
    p.add_argument('--dir', required=True)
    p.add_argument('--out-dir', required=True)
    p.add_argument('--target-rf', type=int, default=5)
    p.add_argument('--batch-topics', type=int, default=10)
    p.add_argument('--max-parts', type=int, default=300)
    p.add_argument('--large-gib', type=float, default=100)
    p.add_argument('--exclude', default='')
    p.set_defaults(fn=cmd_plan)

    p = sp.add_parser('expand')
    p.add_argument('--current', required=True)
    p.add_argument('--proposed', required=True)
    p.add_argument('--brokers', default='1,2,3,4,5')
    p.add_argument('--target-rf', type=int, default=5)
    p.add_argument('--out', required=True)
    p.add_argument('--chunk', type=int, default=0)
    p.set_defaults(fn=cmd_expand)

    p = sp.add_parser('summary')
    p.add_argument('--dir', required=True)
    p.add_argument('--batches', required=True)
    p.add_argument('--throttle', type=int, default=0)
    p.set_defaults(fn=cmd_summary)

    p = sp.add_parser('check')
    p.add_argument('--batch', required=True)
    p.add_argument('--describe', required=True)
    p.set_defaults(fn=cmd_check)

    p = sp.add_parser('elect')
    p.add_argument('--batch', required=True)
    p.add_argument('--out', required=True)
    p.set_defaults(fn=cmd_elect)

    a = ap.parse_args()
    a.fn(a)


if __name__ == '__main__':
    main()
PY
chmod +x rf5tool.py
python3 rf5tool.py --help
```

---

## 2. Этап 1. Диагностика кластера и итоговый файл

На этом этапе состояние кластера **не меняется**, выполняются только чтения.

### 2.1. Скрипт диагностики

Скрипт собирает сырые выводы утилит в `diag-<этап>/`, строит отчёт `cluster-report-<этап>.md` и
дописывает его в итоговый файл `REPORT.md`. Тот же скрипт запускается в конце работ с этапом `after`.

```bash
cat > diagnose.sh <<'SH'
#!/usr/bin/env bash
# Использование: ./diagnose.sh before | after | <любое имя этапа>
set -uo pipefail
STAGE=${1:?укажите этап: before | after}
: "${broker:?переменная broker не задана}" "${config:?переменная config не задана}"
BROKERS=${BROKERS:-1,2,3,4,5}
D="diag-$STAGE"; mkdir -p "$D"
K="--bootstrap-server $broker --command-config $config"

run() {   # run <файл> <команда…>: stdout -> файл, stderr -> файл.err
  local out=$1; shift
  echo "  -> $D/$out"
  "$@" > "$D/$out" 2> "$D/$out.err" || echo "     ВНИМАНИЕ: код возврата $?, см. $D/$out.err"
}

echo "== Диагностика: этап $STAGE"
run version.txt        kafka-topics.sh --version
run brokers-api.txt    kafka-broker-api-versions.sh $K
run describe.txt       kafka-topics.sh $K --describe
run urp.txt            kafka-topics.sh $K --describe --under-replicated-partitions
run under-min-isr.txt  kafka-topics.sh $K --describe --under-min-isr-partitions
run unavailable.txt    kafka-topics.sh $K --describe --unavailable-partitions
run reassign-list.txt  kafka-reassign-partitions.sh $K --list
run topic-configs.txt  kafka-configs.sh $K --entity-type topics --describe
run broker-all.txt     kafka-configs.sh $K --entity-type brokers --entity-name "${BROKERS%%,*}" --describe --all
for id in ${BROKERS//,/ }; do
  run "broker-dyn-$id.txt" kafka-configs.sh $K --entity-type brokers --entity-name "$id" --describe
done
run logdirs.raw        kafka-log-dirs.sh $K --describe --broker-list "$BROKERS"
tail -1 "$D/logdirs.raw" > "$D/logdirs.json"

if [ -n "${ZK:-}" ]; then
  run zk-brokers.txt    zookeeper-shell.sh "$ZK" ls /brokers/ids
  run zk-controller.txt zookeeper-shell.sh "$ZK" get /controller
fi
if [ -n "${DF_HOSTS:-}" ]; then
  for h in $DF_HOSTS; do
    echo "== $h"; ssh -o ConnectTimeout=5 -o BatchMode=yes "$h" "df -hP ${KAFKA_DATA_DIR:-/kafka/data}"
  done > "$D/df.txt" 2>&1
fi

python3 rf5tool.py diag --dir "$D" --out "cluster-report-$STAGE.md" --stage "$STAGE" \
  --brokers "$BROKERS" --target-rf "${TARGET_RF:-5}" --throttle "${THROTTLE:-0}" \
  --date "$(date '+%F %T %Z')"
rc=$?
{ echo; echo '---'; echo; cat "cluster-report-$STAGE.md"; } >> REPORT.md
echo "== отчёт дописан в REPORT.md"
exit $rc
SH
chmod +x diagnose.sh
```

### 2.2. Запуск

```bash
cat > REPORT.md <<EOF
# RF 3 → 5: итоговый отчёт о работах

* Кластер: $broker
* Рабочий каталог: $WORKDIR
* Исполнитель: $(whoami)@$(hostname)
* Начало: $(date '+%F %T %Z')
EOF

./diagnose.sh before; echo "код: $?"
less cluster-report-before.md
```

Код возврата `0` означает «ЗДОРОВ», `1` означает «НЕ ЗДОРОВ».

### 2.3. Что содержит отчёт и как его читать

| Раздел отчёта | Содержание | Что делать |
|---|---|---|
| 1. Итог | ЗДОРОВ / НЕ ЗДОРОВ + список причин `[FAIL]` и предупреждений `[WARN]` | При «НЕ ЗДОРОВ» **не начинать**. Разобраться с каждой причиной и перезапустить `./diagnose.sh before` |
| 2. Проверки | брокеры, URP, under-min-isr, недоступные партиции, активные реассайны, ошибки дисков, остатки троттлов, лидеры не на preferred | Все значения должны совпадать с колонкой «Ожидается» |
| 3. Топики, партиции, реплики | число топиков (пользовательских/служебных), партиций, реплик, объём данных без учёта реплик и с репликами | Зафиксировать как базу для сравнения «после» |
| 3. RF и min.insync.replicas | сколько топиков и партиций с каждым RF, распределение min.isr | Топики с RF ≠ 3 см. ниже |
| 4. Брокеры | по каждому: rack, реплик, лидеров, preferred-лидеров, объём данных, свободное место | Ожидаемо: у 4 и 5 ноль реплик |
| 5. Оценка перехода на RF=5 | сколько реплик добавится, сколько GiB скопируется, сколько будет на каждом брокере, время | Сверить с местом на дисках 4 и 5 |
| 6–7. Внимание | топики с RF ≠ 3, топики с `min.insync.replicas ≥ RF` | Решение по каждому (п. 2.4) |

**Критерии «кластер здоров»** (любое нарушение означает «НЕ ЗДОРОВ»):

* все брокеры из `BROKERS` отвечают через AdminClient;
* URP = 0, under-min-isr = 0, недоступных партиций и партиций без лидера = 0;
* нет активных реассайнов;
* нет ошибок log dirs.

**Предупреждения** (`[WARN]`) работы не блокируют, но решение по каждому надо записать в `REPORT.md`:
остатки троттлов (снять по п. 6.2 основного плана), лидеры не на preferred-репликах,
топики с RF ≠ 3, топики с `min.insync.replicas ≥ RF`.

### 2.4. Решения по итогам диагностики

Дописать в `REPORT.md` вручную:

```bash
cat >> REPORT.md <<'EOF'

## Решения по итогам диагностики

* Место на брокерах 4 и 5 под полный объём данных: ДА / НЕТ (свободно ___ GiB, нужно ___ GiB + 30 %)
* Топики с RF ≠ 3: переводим на RF=5 / исключаем (EXCLUDE='^(topic-a|topic-b)$')
* Топики `_schemas`, `*-changelog`, `*-repartition`: в общих пачках / отдельно
* THROTTLE: ___ МБ/с
* Окно работ: ___
EOF
```

> Топики с RF=1 или RF=2 план тоже переведёт на RF=5. Для них данные будут копироваться и на
> старые брокеры 1–3. Если такие топики временные или намеренно без реплик, исключите их
> переменной `EXCLUDE` (п. 3.1).

---

## 3. Этап 2. Подготовка файлов с пачками

### 3.1. Скрипт подготовки

Что делает скрипт:

1. `rf5tool.py plan` отбирает пользовательские топики (без `__*`) с RF < 5, сортирует их **по
   объёму, от мелких к крупным**, и режет на пачки по `BATCH_TOPICS` топиков, но не больше
   `MAX_PARTS` партиций в пачке. Топик больше `LARGE_GIB` получает отдельную пачку.
   Первая пачка содержит самые мелкие топики и служит пилотной.
2. Для каждой пачки вызывает **штатный** `kafka-reassign-partitions.sh --generate` на брокерах 1–5.
3. Из вывода `--generate` сохраняет:
   * `rollback-NN.json` — текущее размещение (**это откат**);
   * `proposed-NN.json` — рекомендуемое Kafka размещение (RF=3).
4. `rf5tool.py expand` превращает `proposed-NN.json` в `batch-NN.json` с RF=5 и проверяет:
   5 разных брокеров в каждой партиции, все текущие реплики сохранены, набор партиций совпадает с текущим.
5. Считает объём копирования и время по пачкам и дописывает сводку в `REPORT.md`.

```bash
cat > prepare-batches.sh <<'SH'
#!/usr/bin/env bash
set -euo pipefail
: "${broker:?}" "${config:?}"
BROKERS=${BROKERS:-1,2,3,4,5}
K="--bootstrap-server $broker --command-config $config"
D=diag-before
OUT=batches

[ -s "$D/describe.txt" ] || { echo "сначала ./diagnose.sh before"; exit 1; }
if ls $OUT/batch-*.json >/dev/null 2>&1; then
  echo "в $OUT уже есть пачки — перенесите каталог, чтобы не перезаписать rollback-файлы"; exit 1
fi

python3 rf5tool.py plan --dir "$D" --out-dir "$OUT" --target-rf "${TARGET_RF:-5}" \
  --batch-topics "${BATCH_TOPICS:-10}" --max-parts "${MAX_PARTS:-300}" \
  --large-gib "${LARGE_GIB:-100}" --exclude "${EXCLUDE:-}"

for t in "$OUT"/topics-*.json; do
  n=$(basename "$t" .json); n=${n#topics-}
  echo "== пачка $n: $(grep -c '"topic"' "$t") топиков"

  kafka-reassign-partitions.sh $K --topics-to-move-json-file "$t" \
    --broker-list "$BROKERS" --generate > "$OUT/generate-$n.txt"

  awk '/^Current partition replica assignment/{getline; print}'         "$OUT/generate-$n.txt" > "$OUT/rollback-$n.json"
  awk '/^Proposed partition reassignment configuration/{getline; print}' "$OUT/generate-$n.txt" > "$OUT/proposed-$n.json"
  python3 -m json.tool "$OUT/rollback-$n.json" > /dev/null
  python3 -m json.tool "$OUT/proposed-$n.json" > /dev/null

  python3 rf5tool.py expand --current "$OUT/rollback-$n.json" --proposed "$OUT/proposed-$n.json" \
    --brokers "$BROKERS" --target-rf "${TARGET_RF:-5}" --out "$OUT/batch-$n.json"
done
chmod 444 "$OUT"/rollback-*.json

python3 rf5tool.py summary --dir "$D" --batches "$OUT" --throttle "${THROTTLE:-0}" > "$OUT/summary.md"
{ echo; echo '---'; echo; cat "$OUT/summary.md"
  echo; echo '### Состав пачек'; echo; echo '```'; column -t -s $'\t' "$OUT/plan.tsv"; echo '```'
  echo; echo '## Журнал выполнения'; echo
  echo '| Пачка | Партиций | Начало | Конец | Длительность | Результат |'
  echo '|---|---|---|---|---|---|'
} >> REPORT.md
cat "$OUT/summary.md"
SH
chmod +x prepare-batches.sh
```

### 3.2. Запуск и проверка результата

```bash
./prepare-batches.sh
column -t -s $'\t' batches/plan.tsv
```

Проверить глазами одну пачку:

```bash
# текущее размещение (откат)
python3 -m json.tool batches/rollback-01.json | head -30
# что предложил Kafka (RF=3)
python3 -m json.tool batches/proposed-01.json | head -30
# что будем выполнять (RF=5): у каждой партиции 5 разных брокеров, текущие три на месте
python3 -m json.tool batches/batch-01.json | head -30
```

Пример одной партиции:

```
rollback-01.json   "replicas": [2, 3, 1]            ← сейчас
proposed-01.json   "replicas": [4, 1, 2]            ← предложение --generate, RF=3
batch-01.json      "replicas": [4, 1, 2, 3, 5]      ← порядок Kafka + недостающие брокеры
                                ↑ preferred leader
```

Реплики на 1, 2, 3 остаются, добавляются 4 и 5. Данные копируются **только на 4 и 5**.
Preferred leader для этой партиции — брокер 4.

### 3.3. Бэкап (обязательно)

`batches/rollback-*.json` — единственный источник для отката. Сразу после подготовки скопировать
весь каталог **за пределы кластера** (git-репозиторий работ, тикет):

```bash
tar czf ~/kafka-rf5-plan-$(date +%Y%m%d-%H%M).tgz -C "$WORKDIR" batches REPORT.md cluster-report-before.md
```

> Не перезапускайте `prepare-batches.sh` после начала выполнения пачек. `--generate` снимет
> «текущее» размещение уже изменённого кластера, и откат будет потерян. Скрипт отказывается
> работать, если `batches/batch-*.json` уже существуют.

---

## 4. Этап 3. Троттлинг

Подробный расчёт приведён в основном плане, раздел 4. Кратко:

| Сеть / диск | Стартовый `THROTTLE` |
|---|---|
| 1 Gbit/s | 30 МБ/с = `31457280` |
| 10 Gbit/s | 100 МБ/с = `104857600` |
| 25 Gbit/s + NVMe | 300 МБ/с = `314572800` |

Особенность RF 3 → 5: **каждый** из брокеров 4 и 5 принимает **весь** объём пачки. Значит,
время пачки ≈ `уникальный объём пачки / THROTTLE` (столбец «Оценка времени» в `summary.md` уже
считает так, с запасом ×1,5).

Троттл выставляется **самим `--execute --throttle`** (на брокерах `*.replication.throttled.rate`,
на топиках пачки `*.replication.throttled.replicas`). Финальный `--verify` без
`--preserve-throttles` снимает его после каждой пачки. Отдельно задавать троттл через
`kafka-configs.sh` не нужно.

Изменить троттл **во время** выполнения пачки:

```bash
kafka-reassign-partitions.sh --bootstrap-server $broker --command-config $config \
  --reassignment-json-file batches/batch-NN.json \
  --execute --additional --throttle 52428800
```

---

## 5. Этап 4. Выполнение пачек

### 5.1. Скрипт выполнения одной пачки

```bash
cat > run-batch.sh <<'SH'
#!/usr/bin/env bash
# Выполнить ОДНУ пачку:   ./run-batch.sh 01        (для __consumer_offsets: ./run-batch.sh co-01)
# Продолжить наблюдение:  RESUME=1 ./run-batch.sh 01   (если скрипт был прерван, а реассайн идёт)
# Без выбора лидеров:     ELECT=0 ./run-batch.sh 01
set -uo pipefail
n=${1:?укажите пачку, например 01}
B=batches/batch-$n.json
[ -f "$B" ] || { echo "нет файла $B"; exit 1; }
: "${broker:?}" "${config:?}" "${THROTTLE:?задайте THROTTLE, байт/с}"
K="--bootstrap-server $broker --command-config $config"
POLL=${POLL:-60}
LOG=batches/progress.log
ts()  { date '+%F %T'; }
say() { echo "$(ts) [$n] $*" | tee -a "$LOG"; }
row() { echo "| $n | $parts | $t0 | $(ts) | $(( ($(date +%s) - start) / 60 )) мин | $1 |" >> REPORT.md; }
parts=$(grep -c '"topic"' "$B")
t0=$(ts); start=$(date +%s)

if [ "${RESUME:-0}" != 1 ]; then
  # --- 1. предпроверки: кластер в исходном состоянии
  urp=$(kafka-topics.sh $K --describe --under-replicated-partitions | grep -c 'Partition:')
  umi=$(kafka-topics.sh $K --describe --under-min-isr-partitions  | grep -c 'Partition:')
  act=$(kafka-reassign-partitions.sh $K --list | grep -c ': replicas:')
  say "предпроверка: URP=$urp under-min-isr=$umi активных_реассайнов=$act"
  if [ "$urp" -ne 0 ] || [ "$umi" -ne 0 ] || [ "$act" -ne 0 ]; then
    say "СТОП: кластер не в исходном состоянии"; exit 1
  fi

  read -rp "Запустить $B ($parts партиций, троттл $((THROTTLE / 1048576)) МБ/с)? [y/N] " ans
  [ "$ans" = y ] || { echo "отменено"; exit 0; }

  # --- 2. запуск
  kafka-reassign-partitions.sh $K --reassignment-json-file "$B" \
    --throttle "$THROTTLE" --execute 2>&1 | tee "batches/exec-$n.log"
  grep -q 'Successfully started partition reassignment' "batches/exec-$n.log" \
    || { say "СТОП: --execute не запустил реассайн, см. batches/exec-$n.log"; row "ошибка запуска"; exit 1; }
  say "запущено"
else
  say "продолжаем наблюдение (RESUME=1)"
fi

# --- 3. ожидание: только --verify --preserve-throttles, иначе троттл снимется раньше времени
while :; do
  kafka-reassign-partitions.sh $K --reassignment-json-file "$B" \
    --verify --preserve-throttles > "batches/verify-$n.txt" 2>&1
  total=$(grep -c '^Reassignment of partition' "batches/verify-$n.txt")
  left=$(grep -c 'still in progress'         "batches/verify-$n.txt")
  bad=$(grep -c 'rather than'                 "batches/verify-$n.txt")
  say "в процессе: $left из $total, прошло $(( ($(date +%s) - start) / 60 )) мин"
  if [ "$bad" -ne 0 ] || [ "$total" -eq 0 ]; then
    say "СТОП: неожиданный вывод --verify, см. batches/verify-$n.txt"; row "ошибка verify"; exit 2
  fi
  [ "$left" -eq 0 ] && break
  sleep "$POLL"
done

# --- 4. завершение: --verify без --preserve-throttles снимает троттлы пачки
kafka-reassign-partitions.sh $K --reassignment-json-file "$B" --verify > "batches/verify-final-$n.txt" 2>&1
grep -i 'clear' "batches/verify-final-$n.txt" | tee -a "$LOG"

# --- 5. проверка результата
kafka-topics.sh $K --describe > "batches/describe-after-$n.txt"
if ! python3 rf5tool.py check --batch "$B" --describe "batches/describe-after-$n.txt" | tee -a "$LOG"; then
  say "СТОП: фактическое размещение не совпадает с планом"; row "расхождение с планом"; exit 3
fi
urp=$(kafka-topics.sh $K --describe --under-replicated-partitions | grep -c 'Partition:')
umi=$(kafka-topics.sh $K --describe --under-min-isr-partitions  | grep -c 'Partition:')
say "после пачки: URP=$urp under-min-isr=$umi"

# --- 6. preferred leader election по партициям пачки
if [ "${ELECT:-1}" = 1 ]; then
  python3 rf5tool.py elect --batch "$B" --out "batches/elect-$n.json"
  kafka-leader-election.sh --bootstrap-server $broker --admin.config $config \
    --election-type PREFERRED --path-to-json-file "batches/elect-$n.json" > "batches/elect-$n.log" 2>&1
  say "preferred election выполнены, см. batches/elect-$n.log"
fi

say "ГОТОВО за $(( ($(date +%s) - start) / 60 )) мин"
row "OK (URP=$urp)"
SH
chmod +x run-batch.sh
```

### 5.2. Что происходит внутри одной пачки

| Шаг | Команда | Что происходит в кластере |
|---|---|---|
| 1. Предпроверка | `--under-replicated-partitions`, `--under-min-isr-partitions`, `--list` | Ничего. Если есть URP или активный реассайн, пачка не стартует |
| 2. Запуск | `--execute --throttle $THROTTLE` | Контроллер добавляет 4 и 5 в список реплик. Они начинают копировать партиции с лидеров, скорость ограничена троттлом |
| 3. Ожидание | `--verify --preserve-throttles` раз в `POLL` секунд | Копирование. Партиции уже в RF=5 показываются как `is completed`, остальные как `is still in progress` |
| 4. Завершение | `--verify` без `--preserve-throttles` | Снимаются брокерские и топиковые троттлы |
| 5. Проверка | `kafka-topics.sh --describe` + `rf5tool.py check` | Replicas и ISR каждой партиции пачки совпадают с планом (5 брокеров) |
| 6. Лидеры | `kafka-leader-election.sh --election-type PREFERRED` | Лидерство переезжает на первую реплику списка (часть партиций уходит на 4 и 5). Клиенты получают кратковременный `NOT_LEADER_OR_FOLLOWER` и штатно повторяют запрос |

> **Важно.** Во время ожидания используется только `--verify --preserve-throttles`. Обычный `--verify`
> снимает троттлы, как только сочтёт реассайн завершённым. Скрипт вызывает его только на шаге 4.

### 5.3. Порядок выполнения

**Пилот — пачка 01** (самые мелкие топики):

```bash
./run-batch.sh 01
```

После пилота, перед следующими пачками:

* [ ] время пачки сопоставимо с оценкой в `summary.md` (если сильно медленнее, см. п. 11.1 основного плана);
* [ ] p99 `Produce TotalTimeMs` и лаг критичных консьюмер-групп вернулись к baseline;
* [ ] исходящий трафик репликации на лидерах в пределах запаса сети;
* [ ] решение: оставить `THROTTLE` или поднять.

**Остальные пачки — по одной**, между ними проверять те же метрики:

```bash
./run-batch.sh 02
./run-batch.sh 03
# …
```

Посмотреть прогресс по всем пачкам:

```bash
tail -20 batches/progress.log
sed -n '/## Журнал выполнения/,$p' REPORT.md
```

> **Никогда не запускать две пачки одновременно.** Скрипт это проверяет (`--list` на шаге 1),
> но проверка не заменяет внимательности.

### 5.4. Если скрипт прервался (Ctrl+C, обрыв SSH)

Реассайн **продолжается в кластере**, скрипт только наблюдает за ним. Вернуться к наблюдению:

```bash
RESUME=1 ./run-batch.sh 05
```

### 5.5. Если нужно срочно остановить пачку

```bash
kafka-reassign-partitions.sh --bootstrap-server $broker --command-config $config \
  --reassignment-json-file batches/batch-NN.json --cancel
```

Незавершённые партиции пачки возвращаются к исходным 3 репликам, недокачанные копии на 4 и 5
удаляются, троттлы снимаются. Уже завершённые партиции пачки остаются с RF=5. Это корректное
состояние, и пачку можно запустить повторно позже. Дописать в журнал:

```bash
echo "| NN | | | $(date '+%F %T') | | ОТМЕНЕНА: <причина> |" >> REPORT.md
```

---

## 6. Этап 5. Служебные топики

Выполняется **после** всех пользовательских пачек.

### 6.1. `__consumer_offsets`

50 партиций, в каждой лежит координатор для части консьюмер-групп. Режем не по топикам, а по
**10 партиций** в пачке:

```bash
K="--bootstrap-server $broker --command-config $config"
echo '{"version":1,"topics":[{"topic":"__consumer_offsets"}]}' > batches/topics-co.json

kafka-reassign-partitions.sh $K --topics-to-move-json-file batches/topics-co.json \
  --broker-list "$BROKERS" --generate > batches/generate-co.txt
awk '/^Current partition replica assignment/{getline; print}'         batches/generate-co.txt > batches/rollback-co.json
awk '/^Proposed partition reassignment configuration/{getline; print}' batches/generate-co.txt > batches/proposed-co.json
chmod 444 batches/rollback-co.json

python3 rf5tool.py expand --current batches/rollback-co.json --proposed batches/proposed-co.json \
  --brokers "$BROKERS" --target-rf "$TARGET_RF" --out batches/batch-co.json --chunk 10
# -> batches/batch-co-01.json … batch-co-05.json

./run-batch.sh co-01
# проверить группы (ниже), затем co-02 … co-05
```

После каждой пачки проверить, что группы в порядке:

```bash
kafka-consumer-groups.sh --bootstrap-server $broker --command-config $config \
  --describe --all-groups --state | awk 'NF && $0 !~ /^GROUP|Stable|Empty/'
# ожидаем пустой вывод
```

Вместе с этим поднять параметр, с которым топик создаётся заново:
`offsets.topic.replication.factor=5` (п. 8.2).

### 6.2. `__transaction_state`

Если топик есть (используются транзакции/EOS), процедура та же, что в п. 6.1, с префиксом `ts`:

```bash
kafka-topics.sh --bootstrap-server $broker --command-config $config --describe --topic __transaction_state | head -1
```

### 6.3. Прочие

`_schemas`, `_confluent-*` и топики Kafka Streams (`*-changelog`, `*-repartition`) уже попали
в обычные пачки (`plan` пропускает только `__*`). Если их надо выполнить отдельно, исключить из
`plan` через `EXCLUDE` до подготовки пачек (п. 2.4).

---

## 7. Этап 6. Зачистка и лидеры

### 7.1. Троттлы

Каждая пачка снимает свои троттлы сама (шаг 4 скрипта). Контроль — ничего не должно остаться:

```bash
K="--bootstrap-server $broker --command-config $config"
kafka-configs.sh $K --entity-type topics --describe | grep -i 'throttled'
for id in ${BROKERS//,/ }; do
  kafka-configs.sh $K --entity-type brokers --entity-name "$id" --describe | grep -i 'throttled'
done
# ожидаем пустой вывод
```

Если что-то осталось, снять вручную по п. 6.2 основного плана.

### 7.2. Лидеры

Лидеры каждой пачки выбраны на шаге 6 скрипта. Проверить итоговое распределение и при
необходимости выполнить выборы для всего кластера:

```bash
kafka-topics.sh --bootstrap-server $broker --command-config $config --describe \
  | awk '/^\tTopic:/ { for (i=1;i<=NF;i++) if ($i=="Leader:") c[$(i+1)]++ }
         END { for (b in c) printf "лидеров на брокере %s: %d\n", b, c[b] }' | sort -V

kafka-leader-election.sh --bootstrap-server $broker --admin.config $config \
  --election-type PREFERRED --all-topic-partitions
```

Ожидаемо: лидеры распределены примерно поровну между 1–5.

---

## 8. Настройки после перехода на RF=5

Каждое решение фиксируется в `REPORT.md`.

### 8.1. `min.insync.replicas`

| RF | min.isr | Запись `acks=all` доступна при отказе до | Подтверждённые записи гарантированно не теряются при отказе до* |
|---|---|---|---|
| 3 | 2 | 1 брокера | 1 брокера |
| 5 | 2 | 3 брокеров | 1 брокера |
| 5 | **3** | 2 брокеров | 2 брокеров |

\* Худший случай: ISR успел сжаться до `min.insync.replicas`. При полном ISR данные при RF=5
переживают потерю 4 брокеров.

Для RF=5 обычно рекомендуют `min.insync.replicas=3`. Менять **после** завершения всех пачек и
согласовать с владельцами приложений, использующих `acks=all`:

```bash
# на уровне топика
kafka-configs.sh --bootstrap-server $broker --command-config $config \
  --entity-type topics --entity-name my-topic --alter --add-config min.insync.replicas=3
# или брокерский дефолт для всех топиков без явного значения
kafka-configs.sh --bootstrap-server $broker --command-config $config \
  --entity-type brokers --entity-default --alter --add-config min.insync.replicas=3
```

### 8.2. RF для новых топиков

`default.replication.factor`, `offsets.topic.replication.factor`,
`transaction.state.log.replication.factor` действуют только при **создании** топика. Чтобы новые
топики тоже создавались с RF=5, прописать в `server.properties` на всех брокерах и применить при
плановом rolling restart:

```properties
default.replication.factor=5
offsets.topic.replication.factor=5
transaction.state.log.replication.factor=5
```

Проверить после рестарта: `kafka-configs.sh … --entity-type brokers --entity-name 1 --describe --all | grep replication.factor`.
Топики, которые приложения создают сами с явным `replication.factor=3`, останутся с RF=3.
Это надо согласовать с командами приложений.

---

## 9. Этап 7. Финальная диагностика

```bash
./diagnose.sh after; echo "код: $?"

# сравнить «до» и «после»
diff <(sed -n '/## 3\./,/## 5\./p' cluster-report-before.md) \
     <(sed -n '/## 3\./,/## 5\./p' cluster-report-after.md)
```

**Критерии приёмки** (по `cluster-report-after.md`):

| Показатель | Ожидается |
|---|---|
| Итог | ЗДОРОВ |
| Число топиков и партиций | как в `before` |
| RF | все переведённые топики (и `__consumer_offsets`) в строке RF=5, в строке RF=3 нет ничего (кроме исключённых намеренно) |
| Реплик | `партиций × 5` (с поправкой на исключённые) |
| Реплик на брокерах 1–5 | одинаково, равно числу партиций |
| Лидеров на брокерах | примерно поровну |
| Данные на брокерах 4 и 5 | ≈ как на брокерах 1–3 |
| Троттлы | нет |

Закрыть итоговый файл:

```bash
cat >> REPORT.md <<EOF

---

## Завершение

* Окончание: $(date '+%F %T %Z')
* Результат: ______
* min.insync.replicas: ______ (п. 8.1)
* default.replication.factor: ______ (п. 8.2)
EOF

tar czf ~/kafka-rf5-done-$(date +%Y%m%d-%H%M).tgz -C "$WORKDIR" .
```

`REPORT.md` — итоговый документ работ: диагностика до, план пачек с объёмами, журнал выполнения
каждой пачки, решения, диагностика после.

---

## 10. Откат

Главное преимущество этого варианта: откат **ничего не копирует**, реплики на 4 и 5 просто удаляются.

| Ситуация | Действие |
|---|---|
| Пачка идёт, нужно остановить | `--cancel` (п. 5.5) |
| Пачка завершена, нужно вернуть RF=3 | выполнить `rollback-NN.json` (ниже) |
| Нужно откатить всё | выполнить все `rollback-*.json`, по одному, в обратном порядке |
| Выросла latency записи после пачки | Сначала **не** откатывать: проверить, нет ли медленного брокера (ISR, `MaxLag`), снизить нагрузку. Откатывать, только если деградация подтверждена и не лечится |

Откат завершённой пачки:

```bash
n=05
K="--bootstrap-server $broker --command-config $config"
kafka-reassign-partitions.sh $K --reassignment-json-file batches/rollback-$n.json --execute
kafka-reassign-partitions.sh $K --reassignment-json-file batches/rollback-$n.json --verify
# вернуть исходных лидеров
python3 rf5tool.py elect --batch batches/rollback-$n.json --out batches/elect-rollback-$n.json
kafka-leader-election.sh --bootstrap-server $broker --admin.config $config \
  --election-type PREFERRED --path-to-json-file batches/elect-rollback-$n.json
echo "| $n | | | $(date '+%F %T') | | ОТКАТ выполнен |" >> REPORT.md
```

> Для `__consumer_offsets` файл отката общий: `rollback-co.json` (все 50 партиций). Выполнять его
> можно в любой момент. Партиции, которые не менялись, останутся как есть.

---

## 11. Частые проблемы

| Проблема | Признак | Что делать |
|---|---|---|
| `--generate` ругается на топик | `UnknownTopicOrPartitionException` | Топик удалили после диагностики: убрать его из `topics-NN.json` и перезапустить подготовку этой пачки |
| `expand` пишет `набор партиций различается` | — | Топику добавили партиции после диагностики: заново сделать `./diagnose.sh before` и `prepare-batches.sh` в новом каталоге `batches` |
| Пачка висит на последних партициях | `still in progress` не уменьшается | Остатки троттла или реплика на 4/5 не входит в ISR. Разбор по п. 11.1 основного плана |
| После пачки URP ≠ 0 | в `progress.log` `URP>0` | Не запускать следующую. Найти партиции: `kafka-topics.sh … --under-replicated-partitions` |
| Место на 4/5 кончается | `df`, `kafka-log-dirs.sh` | `--cancel` текущей пачки; пересмотреть решение по RF=5 или расширить диски |
| Выросла p99 записи | метрики `Produce TotalTimeMs` | Снизить `THROTTLE` (п. 4). После завершения всех пачек задержка должна частично вернуться, но `acks=all` при RF=5 всегда немного медленнее |
| `ClusterAuthorizationException` | на `--execute` | Нет прав `Alter` на `Cluster`: таблица прав в п. 1.0 основного плана |

---

## 12. Итоговый чек-лист

**Подготовка**
- [ ] Переменные заданы, доступ проверен (п. 1.1)
- [ ] Рабочий каталог, `rf5tool.py` создан (п. 1.2–1.3)
- [ ] Последствия RF=5 приняты: место, сеть, `acks=all` (п. 0)

**Этап 1 — диагностика**
- [ ] `./diagnose.sh before` вернул ЗДОРОВ
- [ ] На 4 и 5 хватает места под полный объём + 30 %
- [ ] Решения по предупреждениям записаны в `REPORT.md` (п. 2.4)

**Этап 2 — пачки**
- [ ] `./prepare-batches.sh` отработал без ошибок
- [ ] `plan.tsv` и `summary.md` просмотрены, время приемлемо
- [ ] `batches/` с `rollback-*.json` скопирован за пределы кластера (п. 3.3)

**Этап 3–4 — выполнение**
- [ ] `THROTTLE` выбран
- [ ] Пилот `01` выполнен, метрики в норме
- [ ] Все пачки выполнены, в журнале `REPORT.md` везде OK
- [ ] `__consumer_offsets` (`co-01…05`), при наличии `__transaction_state`

**Этап 5–7 — завершение**
- [ ] Троттлов не осталось (п. 7.1)
- [ ] Лидеры распределены по 1–5 (п. 7.2)
- [ ] Решения по `min.insync.replicas` и `default.replication.factor` (п. 8)
- [ ] `./diagnose.sh after` вернул ЗДОРОВ, критерии приёмки выполнены (п. 9)
- [ ] `REPORT.md` закрыт, архив работ сохранён
