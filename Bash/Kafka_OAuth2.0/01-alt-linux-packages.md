# Наличие пакетов для Kafka + OAuth 2.0 в репозиториях ALT Linux

Проверка выполнена по каталогу `packages.altlinux.org` (ветки Sisyphus, p11, p10) на 31.07.2026.

## Главный вывод

**Брокера Apache Kafka в репозиториях ALT Linux нет** — ни в Sisyphus, ни в p10/p11, ни под именем `kafka`, ни `apache-kafka`, ни `kafka-server`. Отсутствует и `zookeeper` (для Kafka 4.x он и не нужен — KRaft).

Есть только клиентские библиотеки. То есть штатного пути «поставить брокер из репозитория ОС» не существует, и это надо закладывать в решение по ЗОКИИ отдельным пунктом.

## Что есть в репозиториях

| Пакет | Ветка / версия | Роль |
|---|---|---|
| `java-17-openjdk`, `java-17-openjdk-devel` | p10, p11, Sisyphus (17.0.14) | **JRE/JDK для брокера — обязателен** |
| `java-21-openjdk` | Sisyphus (21.0.1) | альтернативная LTS-версия |
| `librdkafka` | Sisyphus 2.6.1-alt1, p9 0.11.6 | C/C++ клиент (в т.ч. для сервисов на C/Go/Python) |
| `libcppkafka` | Sisyphus, p9 | C++ обёртка над librdkafka |
| `python3-module-kafka` | Sisyphus 2.0.2-alt3 | чистый Python-клиент |
| `python3-module-confluent-kafka` | p11 2.4.0-alt1, Sisyphus | Python-клиент поверх librdkafka |
| `python3-module-cryptography` | p10, p11, Sisyphus | нужен для тестового IdP и работы с JWT в скриптах |
| `apache-curator` | p9 | инфраструктура ZooKeeper-стека, для 4.x не требуется |
| `openssl`, `curl`, `tar` | все ветки | генерация TLS, диагностика |

## Чего нет и что с этим делать

### 1. Сам брокер Kafka

Варианты поставки, по возрастанию пригодности для ЗОКИИ:

**A. Upstream-tarball + собственная RPM-сборка.** Забрать `kafka_2.13-4.3.1.tgz`, положить во внутреннее зеркало, собрать RPM через `gear`/`hasher` и завести во внутренний репозиторий. Технически чисто, воспроизводимо, но ПО не в реестре отечественного и не сертифицировано — на аттестации ЗОКИИ это придётся закрывать организационными мерами.

**B. Arenadata Streaming (ADS).** Сборка Kafka + NiFi, включена в единый реестр российского ПО; вендор объявил о процессе сертификации ADS по требованиям ФСТЭК. Для ЗОКИИ это самый защищённый с бумажной стороны вариант — **уточните у вендора текущий статус сертификата, на момент проверки он был заявлен как «в процессе»**.

**C. Готовый контейнерный образ.** Быстро для стенда, но для ЗОКИИ потребует своей регистрации образа и контроля состава.

### 2. JVM

Штатный `java-17-openjdk` из ALT подойдёт для стенда. Для продуктива на ЗОКИИ рассмотрите **Axiom JDK Certified** — единственная российская среда исполнения Java с сертификатом соответствия ФСТЭК (№ 4531, 4 уровень доверия), реестровая запись № 17783, вендор прямо позиционирует её для ГИС, КИИ, ЗОКИИ и ИСПДн. Сертифицированные сборки есть для Java 8/11/17/21, то есть под Kafka 4.x берётся 17 или 21.

### 3. Библиотека валидации JWT (jose4j)

Отдельного пакета в ALT нет — и он не нужен: `jose4j` входит в состав tarball'а Kafka (`libs/jose4j-*.jar`), брокер использует его для проверки подписи JWT.

**Важная особенность (KAFKA-20184):** в модуле `:clients` зависимость `jose4j` помечена как `compileOnly`. Это значит, что **ваши Java-приложения-клиенты**, использующие `sasl.mechanism=OAUTHBEARER`, получат `ClassNotFoundException`/`NoClassDefFoundError`, если не добавят `org.bitbucket.b_c:jose4j` в свои зависимости явно. Для CLI-утилит из дистрибутива Kafka проблемы нет — там jar лежит в `libs/`.

Проверить состав дистрибутива:

```bash
ls /opt/kafka/libs | grep -Ei 'jose4j|jackson-databind'
```

### 4. Kafka UI

Веб-интерфейсов (kafbat/provectus Kafka UI, AKHQ) в ALT нет — ставятся как jar или контейнер. OAUTHBEARER они поддерживают, конфигурация — в документе `02-kafka-oauthbearer-setup.md`.

## Отдельно: ГОСТ-криптография

Kafka работает поверх JSSE, штатной поддержки ГОСТ-шифрсьютов в OpenJDK нет. Если требования к ЗОКИИ предполагают ГОСТ TLS на канале, вариантов два:

- КриптоПро JCP / JCSP как JCE-провайдер в JVM брокера (проверять совместимость с конкретной сборкой Kafka);
- терминирование ГОСТ TLS на отдельном рубеже (КриптоПро NGate / stunnel-GOST), а Kafka оставить на обычном TLS во внутреннем сегменте.

То же касается канала «брокер → Фактор ЕСБ» при загрузке JWKS: HTTPS-соединение устанавливает JVM.

## Итоговый минимальный набор для стенда на ALT p11

```bash
apt-get update
apt-get install -y java-17-openjdk-devel python3-module-cryptography curl tar openssl
# брокер — из внутреннего зеркала/собственного RPM, в репозитории ALT его нет
```

## Источники

- [ALT Linux — librdkafka (Sisyphus)](https://packages.altlinux.org/ru/sisyphus/srpms/librdkafka/3144047643129646225)
- [ALT Linux — python3-module-kafka](https://packages.altlinux.org/en/sisyphus/srpms/python3-module-kafka/2970317622489850802)
- [ALT Linux — python3-module-confluent-kafka (p11)](https://packages.altlinux.org/ru/p11/srpms/python3-module-confluent-kafka/3073036858966147581)
- [ALT Linux — libcppkafka](http://sisyphus.ru/en/srpm/libcppkafka)
- [ALT Linux — java-17-openjdk (Sisyphus)](https://packages.altlinux.org/en/sisyphus/srpms/java-17-openjdk/)
- [ALT Linux — java-21-openjdk (Sisyphus)](https://packages.altlinux.org/en/sisyphus/srpms/java-21-openjdk/3027716438055444854)
- [Arenadata Streaming в едином реестре российского ПО](https://arenadata.tech/about/news/ads-v-edinom-reestre-po/)
- [Arenadata: сертификация ADS по требованиям ФСТЭК](https://arenadata.tech/about/news/arenadata-rabotaet-nad-sertifikacziej-arenadata-streaming-po-trebovaniyam-fstek/)
- [Axiom JDK Certified](https://axiomjdk.ru/products/axiom-jdk-certified)
- [KAFKA-20184: jose4j compileOnly в модуле clients](https://issues.apache.org/jira/browse/KAFKA-20184)
