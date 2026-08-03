# Настройка OAuth 2.0 (SASL/OAUTHBEARER) в Apache Kafka 4.x (KRaft)

Целевая схема: клиенты и Kafka UI получают JWT в «Фактор ЕСБ» по `client_credentials`, предъявляют его брокеру, брокер проверяет подпись по JWKS провайдера и строит принципал для ACL из claim'а токена.

Проверялось на Kafka 4.3.x. Версии 4.1+ важны — там переехали настройки клиента.

---

## 1. Как это работает

```
                    ┌──────────────────────┐
                    │     Фактор ЕСБ       │
                    │  (OAuth 2.0 / OIDC)  │
                    └──────────────────────┘
                       ▲              ▲
        (1) POST /token│              │(3) GET /jwks — публичные ключи,
            client_id +│              │    брокер кэширует
            client_secret             │
                       │              │
             ┌─────────┴──┐      ┌────┴─────────────┐
             │  Клиент    │─────▶│    Kafka broker  │
             │ (producer/ │ (2)  │  OAUTHBEARER     │
             │  consumer) │ JWT в│  + StandardAuth  │
             └────────────┘ SASL └──────────────────┘
                                  (4) sub -> User:<...> -> ACL
```

1. Клиент сам ходит в IdP за токеном — Kafka этим не занимается, только вызывает `JwtRetriever`.
2. Токен передаётся в SASL-хендшейке (поверх TLS — иначе bearer-токен уходит открытым текстом).
3. Брокер валидирует подпись ключом из JWKS, проверяет `iss`, `aud`, `exp`, `nbf`.
4. Из claim'а `sub` строится принципал `User:<sub>`, по нему работают ACL.

Kafka **не участвует** в user-flow (authorization code): это machine-to-machine аутентификация сервисов.

---

## 2. Конфигурация брокера

### 2.1 Валидация токена (server side)

```properties
# --- listener ---
listeners=CLIENT://:9092,CONTROLLER://:9093
advertised.listeners=CLIENT://kafka-1.example.ru:9092
listener.security.protocol.map=CLIENT:SASL_SSL,CONTROLLER:SSL
controller.listener.names=CONTROLLER
inter.broker.listener.name=CLIENT

sasl.enabled.mechanisms=OAUTHBEARER
sasl.mechanism.inter.broker.protocol=OAUTHBEARER

# --- обработчик валидации ---
listener.name.client.oauthbearer.sasl.server.callback.handler.class=org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler
listener.name.client.oauthbearer.sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;

# --- параметры валидации JWT ---
sasl.oauthbearer.jwks.endpoint.url=https://esb.example.ru/oauth2/jwks
sasl.oauthbearer.expected.issuer=https://esb.example.ru
sasl.oauthbearer.expected.audience=kafka
sasl.oauthbearer.sub.claim.name=sub
sasl.oauthbearer.scope.claim.name=scope
sasl.oauthbearer.jwks.endpoint.refresh.ms=3600000
sasl.oauthbearer.jwks.endpoint.retry.backoff.ms=100
sasl.oauthbearer.jwks.endpoint.retry.backoff.max.ms=10000
sasl.oauthbearer.clock.skew.seconds=30
```

`sasl.oauthbearer.jwt.validator.class` явно задавать не нужно — `DefaultJwtValidator` сам делегирует в `BrokerJwtValidator`, когда указан JWKS endpoint.

### 2.2 Обязательный флаг JVM (главная «грабля» Kafka 4.x)

Начиная с 4.0 брокер и клиенты отказываются обращаться к внешним URL, не перечисленным в системном свойстве. Без него — ошибки вида «URL is not allowed» на старте.

```bash
export KAFKA_OPTS="-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=https://esb.example.ru/oauth2/jwks,https://esb.example.ru/oauth2/token"
```

Значение `*` разрешает любые адреса — для ЗОКИИ так делать не стоит, перечисляйте явно.

### 2.3 Авторизация

```properties
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
super.users=User:kafka-broker;User:kafka-admin
allow.everyone.if.no.acl.found=false
```

Принципал — это `User:` + значение claim'а, указанного в `sasl.oauthbearer.sub.claim.name`. Если «Фактор ЕСБ» кладёт идентификатор сервиса не в `sub`, а, например, в `client_id` или `preferred_username`, поменяйте настройку:

```properties
sasl.oauthbearer.sub.claim.name=client_id
```

Выдача прав:

```bash
bin/kafka-acls.sh --bootstrap-server kafka-1.example.ru:9092 \
  --command-config admin.properties \
  --add --allow-principal User:svc-billing \
  --operation Read --operation Write --operation Describe \
  --topic billing.events
```

> **Ограничение, которое стоит зафиксировать сразу:** штатный Kafka не умеет мапить `scope` из токена на права. `sasl.oauthbearer.scope.claim.name` влияет только на то, откуда читается scope для внутреннего представления токена, а `StandardAuthorizer` смотрит исключительно на принципал. Если нужна авторизация именно по scope/ролям из JWT — потребуется свой `KafkaPrincipalBuilder` или кастомный `Authorizer` (несколько десятков строк на Java + свой jar в `libs/`). Реалистичный компромисс: заводить в «Фактор ЕСБ» отдельный service account на каждую роль и раздавать ACL по `sub`.

### 2.4 Брокер как клиент (inter-broker)

```properties
listener.name.client.oauthbearer.sasl.login.callback.handler.class=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler
sasl.oauthbearer.jwt.retriever.class=org.apache.kafka.common.security.oauthbearer.ClientCredentialsJwtRetriever
sasl.oauthbearer.token.endpoint.url=https://esb.example.ru/oauth2/token
sasl.oauthbearer.client.credentials.client.id=kafka-broker
sasl.oauthbearer.client.credentials.client.secret=${env:KAFKA_BROKER_SECRET}
sasl.oauthbearer.scope=kafka:broker
```

Контроллерный listener в KRaft лучше держать на **mTLS**, а не на OAUTHBEARER: контроллеры должны подниматься независимо от доступности IdP, иначе недоступность «Фактор ЕСБ» роняет кворум.

---

## 3. Конфигурация клиента

### Kafka 4.1 и новее (рекомендуемый вид)

```properties
security.protocol=SASL_SSL
sasl.mechanism=OAUTHBEARER
ssl.truststore.location=/opt/kafka/tls/kafka.truststore.jks
ssl.truststore.password=***

sasl.login.callback.handler.class=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler
sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;

sasl.oauthbearer.jwt.retriever.class=org.apache.kafka.common.security.oauthbearer.ClientCredentialsJwtRetriever
sasl.oauthbearer.token.endpoint.url=https://esb.example.ru/oauth2/token
sasl.oauthbearer.client.credentials.client.id=svc-billing
sasl.oauthbearer.client.credentials.client.secret=***
sasl.oauthbearer.scope=kafka:read kafka:write
```

### Kafka 4.0 и старее (legacy: секреты внутри JAAS)

```properties
sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required \
  clientId="svc-billing" \
  clientSecret="***" \
  scope="kafka:read kafka:write";
sasl.oauthbearer.token.endpoint.url=https://esb.example.ru/oauth2/token
```

### Без client_secret — grant type `jwt-bearer` (KIP-1139, Kafka 4.1+)

Секрет заменяется на подписанную приватным ключом assertion. Для ЗОКИИ это предпочтительнее: долгоживущего пароля в конфигах не остаётся.

```properties
sasl.oauthbearer.jwt.retriever.class=org.apache.kafka.common.security.oauthbearer.JwtBearerJwtRetriever
sasl.oauthbearer.token.endpoint.url=https://esb.example.ru/oauth2/token
sasl.oauthbearer.assertion.private.key.file=/etc/kafka/svc-billing.pem
sasl.oauthbearer.assertion.private.key.passphrase=***
sasl.oauthbearer.assertion.algorithm=RS256
sasl.oauthbearer.assertion.claim.iss=svc-billing
sasl.oauthbearer.assertion.claim.sub=svc-billing
sasl.oauthbearer.assertion.claim.aud=https://esb.example.ru/oauth2/token
sasl.oauthbearer.assertion.claim.exp.seconds=300
sasl.oauthbearer.assertion.claim.jti.include=true
```

Поддержку `urn:ietf:params:oauth:grant-type:jwt-bearer` надо подтвердить у «Фактор ЕСБ» — это первый вопрос в чек-листе.

### Java-приложения: не забыть jose4j

В модуле `kafka-clients` зависимость `jose4j` помечена `compileOnly` (KAFKA-20184). Приложение с `sasl.mechanism=OAUTHBEARER` упадёт с `NoClassDefFoundError`, если не добавить:

```xml
<dependency>
  <groupId>org.bitbucket.b_c</groupId>
  <artifactId>jose4j</artifactId>
  <version>0.9.6</version>
</dependency>
```

### Не-Java клиенты

`librdkafka` (и всё, что на нём: confluent-kafka-python, Go, C++) поддерживает `sasl.oauthbearer.method=oidc`:

```
security.protocol=SASL_SSL
sasl.mechanisms=OAUTHBEARER
sasl.oauthbearer.method=oidc
sasl.oauthbearer.client.id=svc-billing
sasl.oauthbearer.client.secret=***
sasl.oauthbearer.token.endpoint.url=https://esb.example.ru/oauth2/token
sasl.oauthbearer.scope=kafka:read
```

`python3-module-kafka` (чистый Python) собственного OIDC-flow не имеет — там придётся отдавать токен через callback вручную. Для сервисов на Python лучше `python3-module-confluent-kafka`.

---

## 4. Kafka UI

Здесь два независимых OAuth-контура, их часто путают:

1. **Вход пользователя в UI** — authorization code flow, обычный веб-SSO через «Фактор ЕСБ».
2. **Подключение UI к брокеру** — тот же `client_credentials`, что у остальных сервисов.

Для kafbat/provectus Kafka UI (переменные окружения):

```bash
# 1. вход пользователей
AUTH_TYPE=OAUTH2
AUTH_OAUTH2_CLIENT_ESB_CLIENTID=kafka-ui
AUTH_OAUTH2_CLIENT_ESB_CLIENTSECRET=***
AUTH_OAUTH2_CLIENT_ESB_SCOPE=openid,profile
AUTH_OAUTH2_CLIENT_ESB_ISSUER_URI=https://esb.example.ru
AUTH_OAUTH2_CLIENT_ESB_USER_NAME_ATTRIBUTE=preferred_username

# 2. подключение к брокеру
KAFKA_CLUSTERS_0_NAME=zokii-prod
KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=kafka-1.example.ru:9092
KAFKA_CLUSTERS_0_PROPERTIES_SECURITY_PROTOCOL=SASL_SSL
KAFKA_CLUSTERS_0_PROPERTIES_SASL_MECHANISM=OAUTHBEARER
KAFKA_CLUSTERS_0_PROPERTIES_SASL_JAAS_CONFIG=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;
KAFKA_CLUSTERS_0_PROPERTIES_SASL_LOGIN_CALLBACK_HANDLER_CLASS=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler
KAFKA_CLUSTERS_0_PROPERTIES_SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL=https://esb.example.ru/oauth2/token
KAFKA_CLUSTERS_0_PROPERTIES_SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID=kafka-ui
KAFKA_CLUSTERS_0_PROPERTIES_SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET=***

JAVA_OPTS=-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=https://esb.example.ru/oauth2/token
```

UI ходит к брокеру под своим сервисным принципалом — то есть UI видит ровно то, на что выдан ACL для `User:kafka-ui`. Разграничение доступа между людьми делается уже средствами RBAC самого UI.

---

## 5. Порядок внедрения

1. Поднять стенд с локальным mock-IdP (`stand/setup-stand.sh`) — убедиться, что схема Kafka + OAUTHBEARER рабочая в принципе.
2. Прогнать `stand/test-oauth.sh` — проходят шаги 1–8.
3. Заменить mock-IdP на «Фактор ЕСБ»: поменять `token.endpoint.url`, `jwks.endpoint.url`, `expected.issuer`, `expected.audience`, allow-list JVM. Прогнать `test-oauth.sh` заново — шаги 1–5 покажут, чего не хватает на стороне провайдера.
4. Настроить ACL, отключить `allow.everyone.if.no.acl.found`.
5. Перевести inter-broker и контроллеры на mTLS, клиентский listener оставить на OAUTHBEARER.
6. Нагрузочный прогон: посмотреть, что происходит при недоступности IdP и при истечении токенов.

---

## 6. Типовые ошибки и что они значат

| Симптом | Причина | Решение |
|---|---|---|
| `The value ... is not allowed` при старте | не задан allow-list JVM | `-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=...` |
| `NoClassDefFoundError: org.jose4j...` в приложении | jose4j `compileOnly` в kafka-clients | добавить `org.bitbucket.b_c:jose4j` в зависимости |
| `Invalid JWT: Invalid audience` | `aud` в токене ≠ `expected.audience` | согласовать audience с ЕСБ или убрать проверку |
| `Invalid JWT: Invalid issuer` | `iss` с/без завершающего `/` | сравнить строки посимвольно |
| `UnresolvableKeyException: Unable to find a suitable verification key` | нет `kid` в заголовке JWT или ключа нет в JWKS | требовать `kid`; проверить ротацию ключей и `jwks.endpoint.refresh.ms` |
| Аутентификация проходит, но `TopicAuthorizationException` | нет ACL на принципал | `kafka-acls.sh --add --allow-principal User:<sub>` |
| Принципал получается `User:null` | claim, указанный в `sub.claim.name`, отсутствует | поменять на реальный claim (`client_id` и т.п.) |
| Токены истекают, соединения рвутся | слишком короткий TTL | TTL ≥ 5–10 мин; клиент обновляет токен на 80% времени жизни |
| `PKIX path building failed` при загрузке JWKS | CA «Фактор ЕСБ» не в truststore JVM | импортировать CA в truststore брокера |
| Всё работает, но токен виден в трафике | listener на `SASL_PLAINTEXT` | только `SASL_SSL` |

---

## 7. Что зафиксировать для ЗОКИИ

- Bearer-токен = предъявитель. `SASL_PLAINTEXT` недопустим ни на одном контуре.
- Секреты (`client.secret`, пароли keystore) — не в git и не в `server.properties` открытым текстом. Либо `jwt-bearer` вместо секрета, либо подстановка из окружения/vault на старте, права `0600`, владелец — сервисный пользователь.
- Доступность IdP становится частью SLA Kafka: недоступен «Фактор ЕСБ» → новые клиенты не подключаются. Отсюда — резервирование ЕСБ и mTLS на контроллерном контуре.
- Аудит: логировать успешные и неуспешные аутентификации на брокере (`kafka.authorizer.logger` на уровне INFO) и сопоставлять с журналами ЕСБ по `jti`/`sub`.
- ГОСТ TLS: OpenJDK его не умеет, см. `01-alt-linux-packages.md`, раздел о криптографии.

## Источники

- [Apache Kafka 4.1 — Authentication using SASL](https://kafka.apache.org/41/security/authentication-using-sasl/)
- [Javadoc: org.apache.kafka.common.security.oauthbearer (4.2)](https://kafka.apache.org/42/javadoc/org/apache/kafka/common/security/oauthbearer/package-summary.html)
- [ClientCredentialsJwtRetriever](https://kafka.apache.org/41/javadoc/org/apache/kafka/common/security/oauthbearer/ClientCredentialsJwtRetriever.html)
- [KIP-768: расширение SASL/OAUTHBEARER поддержкой OIDC](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=186877575)
- [KIP-1139: grant type jwt-bearer](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1139:+Add+support+for+OAuth+jwt-bearer+grant+type)
- [KAFKA-20101: org.apache.kafka.sasl.oauthbearer.allowed.urls](https://issues.apache.org/jira/browse/KAFKA-20101)
- [KAFKA-20184: jose4j compileOnly](https://issues.apache.org/jira/browse/KAFKA-20184)
- [Apache Kafka 4.1.0 Release Announcement](https://kafka.apache.org/blog/2025/09/04/apache-kafka-4.1.0-release-announcement/)
