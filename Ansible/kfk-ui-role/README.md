# kfk-ui-role

Ansible-роль для установки **Kafka UI (provectus) v0.7.2** на **Astra Linux**
(и другие Debian-подобные ОС) с запуском в виде systemd-сервиса.

Особенность: JAR **не скачивается ролью**. Файл выкладывается вручную в
каталог `files/` роли — это удобно для закрытых контуров без доступа в интернет.

## Что делает роль

1. Проверяет ОС, наличие JAR в `files/`, корректность переменных.
2. Ставит JRE (по умолчанию `openjdk-17-jre-headless`) и проверяет, что версия ≥ 17.
3. Создаёт системного пользователя `kafka-ui` и каталоги.
4. Копирует JAR в `/opt/kafka-ui/lib/` и делает симлинк `/opt/kafka-ui/kafka-ui.jar`.
5. Генерирует `/etc/kafka-ui/application.yml` и `/etc/kafka-ui/kafka-ui.env`.
6. Ставит и запускает unit `kafka-ui.service` (с опциями изоляции systemd).
7. Проверяет, что порт слушается и `/actuator/health` отвечает.

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
* коллекция `community.general` — только если `kafka_ui_manage_firewall: true`.

## Быстрый старт

```bash
cd examples
ansible-playbook -i inventory.ini playbook.yml
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
        kafka_ui_clusters:
          - name: "prod"
            bootstrapServers: "kfk-01:9092,kfk-02:9092"
```

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
    bootstrapServers: "kfk-01:9092,kfk-02:9092,kfk-03:9092"
    readOnly: false
    schemaRegistry: "http://schema-registry:8081"
    schemaRegistryAuth:
      username: "sr-user"
      password: "{{ vault_sr_password }}"
    kafkaConnect:
      - name: "connect"
        address: "http://kfk-connect:8083"
    ksqldbServer: "http://ksqldb:8088"
    metrics:
      type: JMX          # или PROMETHEUS
      port: 9997
    properties:
      security.protocol: SASL_SSL
      sasl.mechanism: SCRAM-SHA-512
      sasl.jaas.config: "{{ vault_kafka_jaas }}"
```

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

## Секреты

Пароли (`kafka_ui_admin_password`, SASL/JAAS, OAuth) храните в `ansible-vault`.
Каталог `/etc/kafka-ui` создаётся с правами `0750`, файлы — `0640`,
владелец `root:kafka-ui`. Для скрытия содержимого шаблонов в логах Ansible
поставьте `kafka_ui_no_log: true`.

## Теги

`kafka-ui-preflight`, `kafka-ui-java`, `kafka-ui-install`, `kafka-ui-config`,
`kafka-ui-service`, `kafka-ui-firewall`, `kafka-ui-verify` и общий `kafka-ui`.

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
