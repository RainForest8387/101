#!/bin/bash
# ============================================================================
#  setup-stand.sh — разворачивание тестового стенда Kafka 4.x (KRaft)
#                   с аутентификацией SASL/OAUTHBEARER
#
#  Стенд поднимает:
#    * mock-idp.py       — локальный OAuth 2.0 провайдер (заменяется на Фактор ЕСБ)
#    * Kafka 4.x в KRaft combined-режиме (broker + controller в одном процессе)
#    * TLS на клиентском listener'е (SASL_SSL)
#    * StandardAuthorizer + ACL по принципалу из claim'а sub
#
#  ОС: ALT Linux p11 / p10 (проверялось также на Ubuntu 22.04)
#  Запуск:  sudo ./setup-stand.sh install && ./setup-stand.sh start
# ============================================================================
set -euo pipefail

KAFKA_VERSION="${KAFKA_VERSION:-4.3.1}"
SCALA_VERSION="${SCALA_VERSION:-2.13}"
BASE="${BASE:-/opt/kafka-oauth-stand}"
KAFKA_HOME="$BASE/kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
IDP_PORT="${IDP_PORT:-8443}"
IDP_HOST="${IDP_HOST:-$(hostname -f)}"
ISSUER="https://${IDP_HOST}:${IDP_PORT}"
BROKER_HOST="${BROKER_HOST:-$(hostname -f)}"
AUDIENCE="${AUDIENCE:-kafka}"
STORE_PASS="${STORE_PASS:-changeit}"

log() { printf '\033[1;32m[stand]\033[0m %s\n' "$*"; }
die() { printf '\033[1;31m[ERROR]\033[0m %s\n' "$*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# 1. Зависимости из репозиториев ALT Linux
# ---------------------------------------------------------------------------
install_deps() {
  log "Установка зависимостей из репозитория ALT Linux"
  if command -v apt-get >/dev/null && [ -f /etc/altlinux-release ]; then
    apt-get update
    # JDK 17+ обязателен для брокера Kafka 4.x.
    # Для ЗОКИИ вместо java-17-openjdk ставится Axiom JDK Certified (ФСТЭК).
    apt-get install -y java-17-openjdk-devel \
                       python3-module-cryptography \
                       curl tar openssl
  else
    log "Не ALT Linux — пропускаю установку пакетов, проверьте зависимости вручную"
  fi
  java -version 2>&1 | head -1
  local major
  major=$(java -version 2>&1 | head -1 | sed -E 's/.*"([0-9]+).*/\1/')
  [ "$major" -ge 17 ] || die "Kafka ${KAFKA_VERSION} требует JDK 17+, найден JDK ${major}"
}

# ---------------------------------------------------------------------------
# 2. Дистрибутив Kafka (в репозиториях ALT брокера НЕТ — берём tarball/зеркало)
# ---------------------------------------------------------------------------
install_kafka() {
  mkdir -p "$BASE"
  if [ -d "$KAFKA_HOME" ]; then log "Kafka уже распакована: $KAFKA_HOME"; return; fi
  local tgz="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
  if [ -f "$BASE/$tgz" ]; then
    log "Использую локальный архив $BASE/$tgz"
  else
    log "Скачивание $tgz"
    curl -fL --retry 3 -o "$BASE/$tgz" \
      "${KAFKA_MIRROR:-https://downloads.apache.org/kafka}/${KAFKA_VERSION}/${tgz}" \
      || die "Не удалось скачать. Положите $tgz в $BASE вручную (в закрытом контуре — из внутреннего зеркала)"
  fi
  tar -xzf "$BASE/$tgz" -C "$BASE"
  log "Kafka распакована: $KAFKA_HOME"
  log "jose4j/nimbus в составе дистрибутива:"
  ls "$KAFKA_HOME"/libs | grep -Ei 'jose4j|nimbus|jackson-databind' || true
}

# ---------------------------------------------------------------------------
# 3. TLS: свой CA, сертификат IdP, сертификат брокера, truststore для Java
# ---------------------------------------------------------------------------
make_tls() {
  local d="$BASE/tls"; mkdir -p "$d"; cd "$d"
  [ -f ca.crt ] && { log "TLS уже сгенерирован"; return; }
  log "Генерация тестового CA и сертификатов"

  openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
    -keyout ca.key -out ca.crt -subj "/CN=kafka-oauth-stand-CA"

  gen() { # gen <name> <CN> <SAN>
    openssl req -newkey rsa:2048 -nodes -keyout "$1.key" -out "$1.csr" -subj "/CN=$2"
    printf "subjectAltName=%s\nextendedKeyUsage=serverAuth\n" "$3" > "$1.ext"
    openssl x509 -req -in "$1.csr" -CA ca.crt -CAkey ca.key -CAcreateserial \
      -out "$1.crt" -days 825 -extfile "$1.ext"
  }
  gen idp    "$IDP_HOST"    "DNS:${IDP_HOST},DNS:localhost,IP:127.0.0.1"
  gen broker "$BROKER_HOST" "DNS:${BROKER_HOST},DNS:localhost,IP:127.0.0.1"

  # keystore брокера
  openssl pkcs12 -export -in broker.crt -inkey broker.key -certfile ca.crt \
    -name broker -out broker.p12 -passout "pass:$STORE_PASS"
  keytool -importkeystore -noprompt \
    -srckeystore broker.p12 -srcstoretype PKCS12 -srcstorepass "$STORE_PASS" \
    -destkeystore kafka.broker.keystore.jks -deststorepass "$STORE_PASS"

  # truststore: CA должен быть и у брокера (для JWKS по HTTPS), и у клиентов
  keytool -importcert -noprompt -alias stand-ca -file ca.crt \
    -keystore kafka.truststore.jks -storepass "$STORE_PASS"

  log "TLS готов: $d"
}

# ---------------------------------------------------------------------------
# 4. Конфигурация брокера
# ---------------------------------------------------------------------------
make_config() {
  local cfg="$BASE/config"; mkdir -p "$cfg" "$BASE/data"
  log "Генерация $cfg/server.properties"
  cat > "$cfg/server.properties" <<EOF
############################# KRaft #############################
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@${BROKER_HOST}:9093
log.dirs=${BASE}/data

############################# Listeners #############################
listeners=CLIENT://:9092,CONTROLLER://:9093
advertised.listeners=CLIENT://${BROKER_HOST}:9092
controller.listener.names=CONTROLLER
inter.broker.listener.name=CLIENT
listener.security.protocol.map=CLIENT:SASL_SSL,CONTROLLER:SSL

############################# TLS #############################
ssl.keystore.location=${BASE}/tls/kafka.broker.keystore.jks
ssl.keystore.password=${STORE_PASS}
ssl.key.password=${STORE_PASS}
ssl.truststore.location=${BASE}/tls/kafka.truststore.jks
ssl.truststore.password=${STORE_PASS}
ssl.endpoint.identification.algorithm=HTTPS
# контроллерный listener — взаимная TLS-аутентификация между узлами
listener.name.controller.ssl.client.auth=required

############################# SASL/OAUTHBEARER #############################
sasl.enabled.mechanisms=OAUTHBEARER
sasl.mechanism.inter.broker.protocol=OAUTHBEARER

listener.name.client.oauthbearer.sasl.server.callback.handler.class=org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler
listener.name.client.oauthbearer.sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;

# --- валидация входящего JWT (broker side) ---
sasl.oauthbearer.jwks.endpoint.url=${ISSUER}/jwks
sasl.oauthbearer.expected.issuer=${ISSUER}
sasl.oauthbearer.expected.audience=${AUDIENCE}
sasl.oauthbearer.sub.claim.name=sub
sasl.oauthbearer.scope.claim.name=scope
sasl.oauthbearer.jwks.endpoint.refresh.ms=3600000
sasl.oauthbearer.clock.skew.seconds=30

# --- брокер как клиент самого себя (inter-broker) ---
listener.name.client.oauthbearer.sasl.login.callback.handler.class=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler
sasl.oauthbearer.jwt.retriever.class=org.apache.kafka.common.security.oauthbearer.ClientCredentialsJwtRetriever
sasl.oauthbearer.token.endpoint.url=${ISSUER}/token
sasl.oauthbearer.client.credentials.client.id=kafka-broker
sasl.oauthbearer.client.credentials.client.secret=broker-secret
sasl.oauthbearer.scope=kafka:broker

############################# Авторизация #############################
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
# kafka-broker  — принципал из claim sub токена (клиентский listener)
# CN=<host>     — принципал из сертификата на контроллерном listener'е (mTLS)
super.users=User:kafka-broker;User:CN=${BROKER_HOST}
allow.everyone.if.no.acl.found=false

############################# Прочее #############################
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
EOF

  log "Генерация $cfg/client-oauth.properties"
  cat > "$cfg/client-oauth.properties" <<EOF
security.protocol=SASL_SSL
sasl.mechanism=OAUTHBEARER
ssl.truststore.location=${BASE}/tls/kafka.truststore.jks
ssl.truststore.password=${STORE_PASS}

sasl.login.callback.handler.class=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler
sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;

# Kafka 4.1+ : credentials вынесены из JAAS в отдельные свойства
sasl.oauthbearer.jwt.retriever.class=org.apache.kafka.common.security.oauthbearer.ClientCredentialsJwtRetriever
sasl.oauthbearer.token.endpoint.url=${ISSUER}/token
sasl.oauthbearer.client.credentials.client.id=kafka-client
sasl.oauthbearer.client.credentials.client.secret=client-secret
sasl.oauthbearer.scope=kafka:read kafka:write
EOF
}

# ---------------------------------------------------------------------------
# 5. Запуск
# ---------------------------------------------------------------------------
start_idp() {
  log "Запуск mock IdP на ${ISSUER}"
  nohup python3 "$(dirname "$(readlink -f "$0")")/mock-idp.py" \
      --port "$IDP_PORT" --issuer "$ISSUER" --audience "$AUDIENCE" \
      --tls-cert "$BASE/tls/idp.crt" --tls-key "$BASE/tls/idp.key" \
      > "$BASE/idp.log" 2>&1 &
  sleep 2
  curl -sf --cacert "$BASE/tls/ca.crt" "$ISSUER/.well-known/openid-configuration" >/dev/null \
    && log "IdP отвечает" || die "IdP не поднялся, см. $BASE/idp.log"
}

start_kafka() {
  local cfg="$BASE/config/server.properties"
  if [ ! -f "$BASE/data/meta.properties" ]; then
    log "Форматирование хранилища KRaft"
    "$KAFKA_HOME/bin/kafka-storage.sh" format \
      -t "$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)" -c "$cfg" --standalone
  fi
  log "Запуск брокера"
  # КРИТИЧНО для Kafka 4.x: URL'ы IdP должны быть в allow-list JVM,
  # иначе брокер откажется обращаться к JWKS/token endpoint.
  export KAFKA_OPTS="-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=${ISSUER}/jwks,${ISSUER}/token \
                     -Djavax.net.ssl.trustStore=${BASE}/tls/kafka.truststore.jks \
                     -Djavax.net.ssl.trustStorePassword=${STORE_PASS}"
  nohup "$KAFKA_HOME/bin/kafka-server-start.sh" "$cfg" > "$BASE/kafka.log" 2>&1 &
  sleep 15
  grep -q "started (kafka.server.KafkaRaftServer)" "$BASE/kafka.log" \
    && log "Брокер запущен" || log "Проверьте $BASE/kafka.log"
}

stop_all() {
  pkill -f kafka-server-start || true
  pkill -f mock-idp.py || true
  log "Остановлено"
}

case "${1:-help}" in
  install) install_deps; install_kafka; make_tls; make_config ;;
  start)   start_idp; start_kafka ;;
  stop)    stop_all ;;
  status)  pgrep -af 'kafka-server-start|mock-idp.py' || echo "не запущено" ;;
  *) cat <<EOF
Использование: $0 {install|start|stop|status}

  install  установить зависимости, скачать Kafka, сгенерировать TLS и конфиги
  start    поднять mock IdP и брокер
  stop     остановить всё
  status   что запущено

Переменные окружения: KAFKA_VERSION, BASE, IDP_PORT, IDP_HOST, BROKER_HOST,
                      AUDIENCE, STORE_PASS, KAFKA_MIRROR
EOF
  ;;
esac
