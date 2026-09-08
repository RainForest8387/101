# kfk-ui-role

Ansible-роль для установки **Kafka UI (provectus) v0.7.2** на **Astra Linux**
(и другие Debian-подобные ОС) с запуском в виде systemd-сервиса.

Особенность: JAR **не скачивается ролью**. Файл выкладывается вручную в
каталог `files/` роли — это удобно для закрытых контуров без доступа в интернет.

Подключение к кластеру Kafka по умолчанию настроено на **SASL_SSL +
SCRAM-SHA-256, порт 9094**.

## Что делает роль

1. Проверяет ОС, наличие JAR в `files/`, корректность переменных.
2. Ставит JRE (по умолчанию `openjdk-17-jre-headless`) и проверяет, что версия ≥ 17.
3. Создаёт системного пользователя `kafka-ui` и каталоги.
4. Копирует JAR в `/opt/kafka-ui/lib/` и делает симлинк `/opt/kafka-ui/kafka-ui.jar`.
5. Копирует truststore (и, при необходимости, keystore) в `/etc/kafka-ui/ssl/`.
6. Генерирует `/etc/kafka-ui/application.yml` и `/etc/kafka-ui/kafka-ui.env`,
   подмешивая в каждый кластер параметры SASL_SSL/SCRAM.
7. Ставит и запускает unit `kafka-ui.service` (с опциями изоляции systemd).
8. Проверяет, что порт слушается и `/actuator/health` отвечает.

## Подготовка

```bash
wget -P files/ \
  https://github.com/provectus/kafka-ui/releases/download/v0.7.2/kafka-ui-api-v0.7.2.jar
```

Страница релиза: <https://github.com/provectus/kafka-ui/releases/tag/v0.7.2>

Требования:

* Ansible ≥ 2.12, `become: true` на целевом хосте;
* Astra Linux 1.7 / Debian 10+ с systemd;
* JRE 17+ в репозиториях (kafka-ui 0.7.2 — это Spring Boot 3);
* коллекция `community.general` — только если `kafka_ui_manage_firewall: true`;
* пароль от `ansible-vault` (`--ask-vault-pass` или `--vault-password-file`) —
  все креды хранятся в зашифрованном виде.

## Быстрый старт

```bash
cd examples
ansible-vault create group_vars/kafka_ui/vault.yml     # пароли, см. vault.yml.example
ansible-playbook -i inventory.ini playbook.yml --ask-vault-pass
```

(в `examples/ansible.cfg` уже прописан `roles_path = ../..`, поэтому роль
`kfk-ui-role` находится автоматически)

Минимальный вызов:

```yaml
- hosts: kafka_ui
  become: true
  roles:
    - role: kfk-ui-role
      vars:
        kafka_ui_sasl_username: "kafka-ui"
        kafka_ui_sasl_password: "{{ vault_kafka_sasl_password }}"
        kafka_ui_ssl_truststore_src: "kafka.truststore.jks"
        kafka_ui_ssl_truststore_password: "{{ vault_kafka_truststore_password }}"
        kafka_ui_clusters:
          - name: "prod"
            bootstrapServers: "kfk-01:9094,kfk-02:9094,kfk-03:9094"
```

`kafka_ui_security_protocol` (`SASL_SSL`) и `kafka_ui_sasl_mechanism`
(`SCRAM-SHA-256`) уже заданы в `defaults/main.yml` — переопределять их
не нужно. Переменные `vault_*` берутся из зашифрованного
`group_vars/kafka_ui/vault.yml` — см. раздел
[«Секреты и ansible-vault»](#секреты-и-ansible-vault).

## Основные переменные

| Переменная | По умолчанию | Назначение |
|---|---|---|
| `kafka_ui_version` | `0.7.2` | версия, подставляется в имя JAR |
| `kafka_ui_jar_name` | `kafka-ui-api-v{{ kafka_ui_version }}.jar` | имя файла в `files/` |
| `kafka_ui_jar_checksum` | `""` | необязательная проверка, формат `sha256:<хеш>` |
| `kafka_ui_user` / `kafka_ui_group` | `kafka-ui` | системная учётная запись сервиса |
| `kafka_ui_base_dir` | `/opt/kafka-ui` | каталог установки |
| `kafka_ui_conf_dir` | `/etc/kafka-ui` | `application.yml` + `kafka-ui.env` |
| `kafka_ui_data_dir` | `/var/lib/kafka-ui` | данные (dynamic config) |
| `kafka_ui_java_install` | `true` | ставить ли JRE |
| `kafka_ui_java_packages` | `[openjdk-17-jre-headless]` | пакеты JRE |
| `kafka_ui_java_bin` | `/usr/bin/java` | путь к java |
| `kafka_ui_heap_opts` | `-Xms256m -Xmx1g` | размер кучи |
| `kafka_ui_java_opts` | список | прочие опции JVM |
| `kafka_ui_server_port` | `8080` | порт web-интерфейса |
| `kafka_ui_server_address` | `0.0.0.0` | адрес прослушивания |
| `kafka_ui_context_path` | `""` | префикс пути за reverse proxy |
| `kafka_ui_clusters` | `[{name: local, ...}]` | список кластеров Kafka |
| `kafka_ui_security_protocol` | `SASL_SSL` | `""` / `PLAINTEXT` / `SSL` / `SASL_PLAINTEXT` / `SASL_SSL` |
| `kafka_ui_sasl_mechanism` | `SCRAM-SHA-256` | механизм SASL |
| `kafka_ui_sasl_username` / `kafka_ui_sasl_password` | `""` | учётные данные SCRAM |
| `kafka_ui_sasl_jaas_config` | `""` | готовая строка JAAS (вместо username/password) |
| `kafka_ui_ssl_truststore_src` | `""` | truststore в `files/` роли или абсолютный путь |
| `kafka_ui_ssl_truststore_path` | `""` | truststore, уже лежащий на целевом хосте |
| `kafka_ui_ssl_truststore_password` | `""` | пароль truststore |
| `kafka_ui_ssl_truststore_type` | `JKS` | `JKS` или `PKCS12` |
| `kafka_ui_ssl_endpoint_identification_algorithm` | `https` | `""` отключает проверку hostname |
| `kafka_ui_ssl_keystore_src` / `..._password` | `""` | клиентский сертификат для mTLS |
| `kafka_ui_ssl_dir` | `/etc/kafka-ui/ssl` | куда кладутся хранилища ключей |
| `kafka_ui_auth_type` | `DISABLED` | `DISABLED` / `LOGIN_FORM` / `OAUTH2` / `LDAP` |
| `kafka_ui_admin_user` / `kafka_ui_admin_password` | `admin` / `""` | учётка для `LOGIN_FORM` |
| `kafka_ui_dynamic_config_enabled` | `false` | правка конфигурации из UI |
| `kafka_ui_config_extra` | `{}` | произвольные ключи в `application.yml` |
| `kafka_ui_env_extra` | `{}` | произвольные переменные окружения |
| `kafka_ui_systemd_hardening` | `true` | опции изоляции в unit-файле |
| `kafka_ui_manage_firewall` | `false` | открыть порт в ufw |
| `kafka_ui_no_log` | `false` | скрывать шаблоны с секретами в выводе |

Полный список — в `defaults/main.yml`.

## Описание кластеров

Ключи внутри `kafka_ui_clusters` пишутся **в нотации самого kafka-ui**
(camelCase) и попадают в `application.yml` без преобразований, поэтому
поддерживается любой параметр из документации проекта:

```yaml
kafka_ui_clusters:
  - name: "prod"
    bootstrapServers: "kfk-01:9094,kfk-02:9094,kfk-03:9094"
    readOnly: false
    schemaRegistry: "https://schema-registry:8081"
    schemaRegistryAuth:
      username: "sr-user"
      password: "{{ vault_sr_password }}"
    kafkaConnect:
      - name: "connect"
        address: "https://kfk-connect:8083"
    ksqldbServer: "https://ksqldb:8088"
    metrics:
      type: JMX          # или PROMETHEUS
      port: 9997
```

Секцию `properties` вручную указывать не нужно — роль сама подставляет туда
параметры SASL_SSL/SCRAM (см. ниже). Если у конкретного кластера настройки
другие, задайте `properties` явно: значения кластера перекрывают общие.

## Подключение к Kafka по SASL_SSL + SCRAM-SHA-256 (порт 9094)

Роль формирует для каждого кластера блок `properties` вида:

```yaml
properties:
  security.protocol: SASL_SSL
  sasl.mechanism: SCRAM-SHA-256
  sasl.jaas.config: org.apache.kafka.common.security.scram.ScramLoginModule required username="kafka-ui" password="***";
  ssl.endpoint.identification.algorithm: https
  ssl.truststore.location: /etc/kafka-ui/ssl/kafka.truststore.jks
  ssl.truststore.type: JKS
  ssl.truststore.password: ***
```

Что нужно задать:

```yaml
kafka_ui_security_protocol: "SASL_SSL"        # значение по умолчанию
kafka_ui_sasl_mechanism: "SCRAM-SHA-256"      # значение по умолчанию
kafka_ui_sasl_username: "kafka-ui"
kafka_ui_sasl_password: "{{ vault_kafka_sasl_password }}"
kafka_ui_ssl_truststore_src: "kafka.truststore.jks"      # файл в files/ роли
kafka_ui_ssl_truststore_password: "{{ vault_kafka_truststore_password }}"
```

И порт `9094` в `bootstrapServers` каждого кластера.

### Подготовка truststore

Truststore должен содержать CA, которым подписаны сертификаты брокеров.
Готовый файл кладётся в `files/` роли рядом с JAR:

```bash
# из PEM-сертификата CA
keytool -importcert -noprompt -alias kafka-ca \
        -file ca.crt \
        -keystore files/kafka.truststore.jks \
        -storetype JKS \
        -storepass '<пароль>'

# проверка содержимого
keytool -list -keystore files/kafka.truststore.jks -storepass '<пароль>'
```

Альтернативы:

* truststore уже развёрнут на серверах другим процессом —
  укажите только `kafka_ui_ssl_truststore_path: /path/to/truststore.jks`,
  копирование пропускается;
* CA брокеров уже в системном хранилище JRE — оставьте обе переменные пустыми,
  роль выведет предупреждение и будет использовать `cacerts`.

Файл копируется в `/etc/kafka-ui/ssl/` с правами `root:kafka-ui 0640`,
каталог — `0750`.

### Проверка со стороны хоста

```bash
# TLS-хендшейк и цепочка сертификатов брокера
openssl s_client -connect kfk-01:9094 -showcerts </dev/null | head -20

# сверка CN/SAN сертификата с DNS-именем из bootstrapServers
openssl s_client -connect kfk-01:9094 </dev/null 2>/dev/null \
  | openssl x509 -noout -subject -ext subjectAltName
```

Типовые ошибки в `journalctl -u kafka-ui`:

| Сообщение | Причина / решение |
|---|---|
| `SSLHandshakeException: PKIX path building failed` | CA брокера нет в truststore — пересоберите truststore |
| `Hostname verification failed` / `No subject alternative names` | в сертификате нет DNS-имени из `bootstrapServers`; исправить SAN либо `kafka_ui_ssl_endpoint_identification_algorithm: ""` |
| `Authentication failed: Invalid username or password` | неверные `kafka_ui_sasl_username` / `kafka_ui_sasl_password` либо пользователь не заведён в Kafka (`kafka-configs.sh --alter --add-config 'SCRAM-SHA-256=[password=...]' --entity-type users --entity-name kafka-ui`) |
| `Unexpected Kafka request of type METADATA` / таймаут | брокер на этом порту ждёт другой протокол — проверьте, что 9094 это listener SASL_SSL |
| `Unsupported SASL mechanism` | брокер не включил SCRAM-SHA-256 на этом listener |

> Пароль подставляется в строку JAAS как есть. Если он содержит `"` или `\`,
> задайте `kafka_ui_sasl_jaas_config` целиком, экранировав символы вручную.

## Расширение конфигурации

Всё, что роль не описывает явно, добавляется через `kafka_ui_config_extra` —
словарь рекурсивно сливается с сгенерированным `application.yml`:

```yaml
kafka_ui_config_extra:
  rbac:
    roles:
      - name: "readonly"
        clusters: ["prod"]
        subjects:
          - provider: oauth_google
            type: domain
            value: "example.com"
        permissions:
          - resource: topic
            value: ".*"
            actions: [VIEW, MESSAGES_READ]
```

## Секреты и ansible-vault

Все пароли (`kafka_ui_admin_password`, `kafka_ui_sasl_password`,
`kafka_ui_ssl_truststore_password`, `kafka_ui_ssl_keystore_password`,
`kafka_ui_ssl_key_password`) попадают в `/etc/kafka-ui/application.yml`
в открытом виде, поэтому в репозитории они должны лежать только
в зашифрованном `ansible-vault` файле.

Готовая раскладка лежит в `examples/`:

```
examples/
├── ansible.cfg                        # roles_path + опция vault_password_file
├── inventory.ini
├── playbook.yml                       # тонкий, только вызов роли
└── group_vars/kafka_ui/
    ├── main.yml                       # открытые параметры, ссылки на vault_*
    ├── vault.yml.example              # шаблон секретов (коммитится)
    └── vault.yml                      # зашифрованный vault (создаёте вы)
```

### 1. Создать vault с паролями

```bash
cd examples

# создать сразу зашифрованный файл (откроется $EDITOR)
ansible-vault create group_vars/kafka_ui/vault.yml

# либо взять шаблон и зашифровать уже готовый файл
cp group_vars/kafka_ui/vault.yml.example group_vars/kafka_ui/vault.yml
ansible-vault encrypt group_vars/kafka_ui/vault.yml
```

Содержимое `vault.yml`:

```yaml
---
vault_kafka_ui_admin_password: "пароль администратора web-интерфейса"
vault_kafka_sasl_password: "пароль пользователя SCRAM-SHA-256 в Kafka"
vault_kafka_truststore_password: "пароль kafka.truststore.jks"
```

Переменные роли ссылаются на них (`group_vars/kafka_ui/main.yml`):

```yaml
kafka_ui_admin_password: "{{ vault_kafka_ui_admin_password }}"
kafka_ui_sasl_password: "{{ vault_kafka_sasl_password }}"
kafka_ui_ssl_truststore_password: "{{ vault_kafka_truststore_password }}"
```

Разделение «открытые переменные → `vault_*`» нужно, чтобы `grep` по
`kafka_ui_sasl_password` показывал, откуда берётся значение, и чтобы
незашифрованным оставался только `main.yml`.

### 2. Работа с vault

```bash
ansible-vault edit    group_vars/kafka_ui/vault.yml   # редактировать
ansible-vault view    group_vars/kafka_ui/vault.yml   # посмотреть
ansible-vault rekey   group_vars/kafka_ui/vault.yml   # сменить пароль
ansible-vault decrypt group_vars/kafka_ui/vault.yml   # расшифровать (осторожно)
```

Зашифровать одну переменную, не заводя отдельный файл:

```bash
ansible-vault encrypt_string --stdin-name 'vault_kafka_sasl_password'
# ввести пароль, затем сам секрет, завершить Ctrl-D
# полученный блок !vault |... вставить прямо в group_vars/kafka_ui/main.yml
```

### 3. Запуск плейбука

```bash
# пароль спросят интерактивно
ansible-playbook -i inventory.ini playbook.yml --ask-vault-pass

# либо пароль из файла (chmod 600, в git не коммитить)
umask 077 && printf '%s' 'СуперПароль' > ~/.vault_pass_kafka_ui
ansible-playbook -i inventory.ini playbook.yml \
  --vault-password-file ~/.vault_pass_kafka_ui
```

Путь к файлу с паролем можно не указывать каждый раз:

* в `examples/ansible.cfg` — `vault_password_file = ~/.vault_pass_kafka_ui`;
* либо через окружение — `export ANSIBLE_VAULT_PASSWORD_FILE=~/.vault_pass_kafka_ui`.

Для нескольких сред используйте `vault-id`:

```bash
ansible-vault create --vault-id prod@prompt  group_vars/prod/vault.yml
ansible-vault create --vault-id stage@prompt group_vars/stage/vault.yml

ansible-playbook site.yml --vault-id prod@~/.vault_pass_prod
```

### 4. Гигиена

* `.gitignore` роли исключает `.vault_pass*` — файл с паролем в репозиторий
  не попадёт; сам `vault.yml` коммитить можно, он зашифрован;
* в `group_vars/kafka_ui/main.yml` выставлено `kafka_ui_no_log: true` —
  Ansible не покажет содержимое шаблонов с паролями в выводе;
* `playbook.yml` в `pre_tasks` проверяет, что переменные из vault
  действительно расшифрованы, и падает с понятным сообщением, если пароль
  не передан;
* читаемый `git diff` для зашифрованных файлов:

```bash
git config --local diff.ansible-vault.textconv "ansible-vault view"
echo 'group_vars/*/vault.yml diff=ansible-vault' >> .gitattributes
```

На целевом хосте `/etc/kafka-ui` создаётся с правами `0750`, файлы
`application.yml` и `kafka-ui.env` — `0640`, владелец `root:kafka-ui`,
truststore — `0640` в `/etc/kafka-ui/ssl/`.

## Теги

`kafka-ui-preflight`, `kafka-ui-java`, `kafka-ui-install`, `kafka-ui-ssl`,
`kafka-ui-config`, `kafka-ui-service`, `kafka-ui-firewall`, `kafka-ui-verify`
и общий `kafka-ui`.

```bash
# только перегенерировать конфиг и перезапустить
ansible-playbook -i inventory.ini playbook.yml --tags kafka-ui-config
```

## Обновление версии

1. Положить новый JAR в `files/`.
2. Поменять `kafka_ui_version` (и при необходимости `kafka_ui_jar_checksum`).
3. Прогнать плейбук — роль скопирует новый файл, перекинет симлинк
   `/opt/kafka-ui/kafka-ui.jar` и перезапустит сервис. Старый JAR остаётся
   в `/opt/kafka-ui/lib/` для быстрого отката.

## Эксплуатация

```bash
systemctl status kafka-ui
journalctl -u kafka-ui -f
curl -s localhost:8080/actuator/health
```

Частые проблемы:

* **`UnsupportedClassVersionError`** — на хосте Java < 17. Поставьте
  `openjdk-17-jre-headless` или укажите другой `kafka_ui_java_bin`.
* **Сервис не стартует на Astra Linux с ЗПС/PARSEC** — попробуйте
  `kafka_ui_systemd_hardening: false`.
* **Пакета `openjdk-17-jre-headless` нет в репозитории** — подключите
  расширенный репозиторий Astra либо поставьте JRE вручную и укажите
  `kafka_ui_java_install: false` + `kafka_ui_java_bin`.
