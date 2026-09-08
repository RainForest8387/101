# files/

Сюда кладётся **вручную скачанный** JAR kafka-ui.

```bash
wget -P files/ \
  https://github.com/provectus/kafka-ui/releases/download/v0.7.2/kafka-ui-api-v0.7.2.jar
sha256sum files/kafka-ui-api-v0.7.2.jar   # значение можно указать в kafka_ui_jar_checksum
```

Страница релиза: https://github.com/provectus/kafka-ui/releases/tag/v0.7.2

Имя файла должно совпадать с переменной `kafka_ui_jar_name`
(по умолчанию `kafka-ui-api-v{{ kafka_ui_version }}.jar`).

JAR весит ~100 МБ и по умолчанию исключён из git (см. `.gitignore` рядом).
Если артефакт нужно хранить в репозитории — уберите правило из `.gitignore`
или используйте git-lfs.
