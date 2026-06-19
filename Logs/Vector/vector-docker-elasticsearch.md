# Vector: установка и настройка Docker → Elasticsearch

## Установка из deb-пакета

### Вариант 1 — через репозиторий

```bash
# Добавить GPG ключ
curl -1sLf 'https://repositories.timber.io/public/vector/gpg.3543DB2D0A2BC4B8.key' \
  | sudo gpg --dearmor -o /usr/share/keyrings/timber-vector-archive-keyring.gpg

# Добавить репозиторий
echo "deb [signed-by=/usr/share/keyrings/timber-vector-archive-keyring.gpg] \
  https://repositories.timber.io/public/vector/deb/ubuntu focal main" \
  | sudo tee /etc/apt/sources.list.d/timber-vector.list

# Установить
sudo apt-get update && sudo apt-get install vector
```

### Вариант 2 — скачать deb напрямую

```bash
# Узнать последнюю версию на https://github.com/vectordotdev/vector/releases
VERSION=0.43.0
curl -LO https://github.com/vectordotdev/vector/releases/download/v${VERSION}/vector_${VERSION}-1_amd64.deb
sudo dpkg -i vector_${VERSION}-1_amd64.deb
```

### Запустить сервис

```bash
sudo systemctl enable --now vector
sudo systemctl status vector
```

---

## Конфигурация: Docker → Vector → Elasticsearch

Файл `/etc/vector/vector.yaml`:

```yaml
# ─── Sources ───────────────────────────────────────────────────────────────────
sources:
  docker_logs:
    type: docker_logs
    # собирать логи со всех контейнеров; для фильтрации — указать include_containers
    # include_containers:
    #   - "my-app"
    #   - "nginx"

# ─── Transforms ────────────────────────────────────────────────────────────────
transforms:
  parse_logs:
    type: remap
    inputs:
      - docker_logs
    source: |
      # Попытаться распарсить JSON-лог, если не JSON — оставить как есть
      parsed, err = parse_json(.message)
      if err == null {
        . = merge(., parsed)
      }

      # Добавить поле для индекса в ES по имени контейнера
      .index_name = "docker-" + string!(.container_name)

# ─── Sinks ─────────────────────────────────────────────────────────────────────
sinks:
  elasticsearch:
    type: elasticsearch
    inputs:
      - parse_logs
    endpoints:
      - "http://localhost:9200"   # адрес ES

    # Динамический индекс на основе имени контейнера
    bulk:
      index: "{{ index_name }}-%Y.%m.%d"
      action: index

    # Если ES требует авторизации:
    # auth:
    #   strategy: basic
    #   user: elastic
    #   password: changeme

    # Для HTTPS с самоподписным сертификатом:
    # tls:
    #   verify_certificate: false
```

---

## Доступ к Docker-сокету

```bash
sudo usermod -aG docker vector
sudo systemctl restart vector
```

---

## Проверка

```bash
# Валидация конфига
vector validate /etc/vector/vector.yaml

# Логи самого Vector
sudo journalctl -u vector -f

# Проверить что данные идут в ES
curl -s "http://localhost:9200/_cat/indices?v&s=index" | grep docker
```

---

## Важные моменты

- Для `docker_logs` source Vector использует Docker API — сокет `/var/run/docker.sock` должен быть доступен пользователю `vector`
- Поле `container_name` в событии Vector содержит имя контейнера без `/`
- Если контейнеры пишут структурированный JSON в stdout — `parse_json` распакует поля на верхний уровень, что удобно для поиска в Kibana


## Установка musl-архива на Astra
Astra основана на Debian, так что установка из tar — стандартная:
```bash
# распаковка (структура архива: bin/vector, config/...)
mkdir -p /tmp/vector && tar xzf vector-0.55.0-x86_64-unknown-linux-musl.tar.gz -C /tmp/vector --strip-components=2

# бинарник на место
sudo install -m755 /tmp/vector/bin/vector /usr/local/bin/vector
/usr/local/bin/vector --version   # должно напечатать версию без ошибки GLIBC

# системный пользователь, доступ к docker-сокету, каталоги
sudo useradd --system --no-create-home --shell /usr/sbin/nologin vector 2>/dev/null
sudo usermod -aG docker vector
sudo mkdir -p /etc/vector /var/lib/vector
sudo chown -R vector:vector /var/lib/vector
# положите свой конфиг в /etc/vector/vector.yaml
```

systemd-юнит /etc/systemd/system/vector.service:
```bash
[Unit]
Description=Vector
After=network-online.target docker.service
Wants=network-online.target

[Service]
User=vector
Group=vector
ExecStartPre=/usr/local/bin/vector validate /etc/vector/vector.yaml
ExecStart=/usr/local/bin/vector --config /etc/vector/vector.yaml
Restart=always
Environment=VECTOR_DATA_DIR=/var/lib/vector

[Install]
WantedBy=multi-user.target
```

Добавление конфига
```bash
sudo mkdir -p /etc/vector
sudo tee /etc/vector/vector.yaml > /dev/null <<'EOF'
sources:
  docker_logs:
    type: docker_logs
    docker_host: unix:///var/run/docker.sock
    # include_containers: ["my-app", "nginx"]
    # exclude_containers: ["vector"]

transforms:
  parse:
    type: remap
    inputs: ["docker_logs"]
    source: |
      structured, err = parse_json(.message)
      if err == null {
        . = merge(., structured)
      }

sinks:
  elasticsearch:
    type: elasticsearch
    inputs: ["parse"]
    endpoints: ["http://elasticsearch:9200"]   # замените на адрес вашего ES
    api_version: auto
    mode: bulk
    bulk:
      index: "docker-logs-%Y.%m.%d"
      action: create
    compression: none
    healthcheck:
      enabled: true
    # auth:
    #   strategy: basic
    #   user: "elastic"
    #   password: "${ELASTIC_PASSWORD}"
EOF
```


```
sudo systemctl daemon-reload
sudo systemctl enable --now vector
sudo journalctl -u vector -f
```
