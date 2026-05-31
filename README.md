# JMS/Oracle-DB Integration Tests

Dieses Projekt enthält einen JUnit-5-Integrationstest für Java 21. Der Test sendet eine XML-Nachricht an eine JMS Queue und prüft anschließend mehrere Oracle-DB-Tabellen anhand derselben `correlation_id`. Für jede konfigurierte Tabelle wird eine Zeile gelesen und es werden alle nicht ignorierten Spalten vollständig gegen konfigurierte Erwartungswerte verglichen.

## Voraussetzungen

* JDK 21
* Maven 3.9+
* Erreichbarer ActiveMQ-Artemis-kompatibler JMS Broker
* Erreichbare Oracle-Datenbank
* Eine Anwendung oder ein Consumer, der die JMS-Nachricht verarbeitet und die erwarteten Datensätze in die Oracle-Datenbank schreibt

## Test ausführen

Die Integrationstests werden standardmäßig übersprungen, damit `mvn test` ohne externe JMS-/DB-Infrastruktur laufen kann. Zum Starten des JMS/Oracle-DB-Tests muss `skipITs` deaktiviert und die Umgebung konfiguriert werden:

```bash
mvn verify -DskipITs=false \
  -Djms.broker.url=tcp://localhost:61616 \
  -Djms.username=admin \
  -Djms.password=admin \
  -Djms.queue.name=example.queue \
  -Ddb.url=jdbc:oracle:thin:@//localhost:1521/FREEPDB1 \
  -Ddb.username=app \
  -Ddb.password=secret \
  -Ddb.assertions.file=db-assertions.properties
```

Alle Werte können alternativ per Umgebungsvariable gesetzt werden.

| System Property | Environment Variable | Standardwert | Beschreibung |
| --- | --- | --- | --- |
| `jms.broker.url` | `JMS_BROKER_URL` | `tcp://localhost:61616` | URL des JMS Brokers |
| `jms.username` | `JMS_USERNAME` | `admin` | JMS Benutzername |
| `jms.password` | `JMS_PASSWORD` | `admin` | JMS Passwort |
| `jms.queue.name` | `JMS_QUEUE_NAME` | `example.queue` | Name der JMS Queue |
| `db.url` | `DB_URL` | _Pflichtwert_ | Oracle JDBC URL |
| `db.username` | `DB_USERNAME` | _Pflichtwert_ | DB Benutzername |
| `db.password` | `DB_PASSWORD` | _Pflichtwert_ | DB Passwort |
| `db.assertions.file` | `DB_ASSERTIONS_FILE` | `db-assertions.properties` | Properties-Datei mit Tabellen- und Spalten-Erwartungen; kann im Classpath oder als Dateipfad liegen |
| `xml.payload` | `XML_PAYLOAD` | siehe Testklasse | XML Payload; `${correlationId}` wird zur Laufzeit ersetzt |
| `test.timeout.seconds` | `TEST_TIMEOUT_SECONDS` | `60` | Maximale Wartezeit auf die DB-Zeilen |
| `test.poll.interval.millis` | `TEST_POLL_INTERVAL_MILLIS` | `1000` | Polling-Intervall für die DB-Abfragen |

## Tabellen- und Spalten-Assertions konfigurieren

Die Datei `src/test/resources/db-assertions.properties` dient als Vorlage. Der Test liest für jede Tabelle per `SELECT * FROM <table> WHERE <correlation-id-column> = ?` genau eine Zeile. Danach vergleicht er die komplette Spalten-Map mit den erwarteten Spaltenwerten. Dadurch schlagen die Assertions fehl, wenn eine nicht ignorierte DB-Spalte fehlt, zusätzlich vorhanden ist oder einen anderen Wert enthält.

```properties
tables=PROCESSED_MESSAGES,AUDIT_EVENTS
correlation-id-column=CORRELATION_ID
ignored-columns=CREATED_AT,UPDATED_AT

PROCESSED_MESSAGES.CORRELATION_ID=${correlationId}
PROCESSED_MESSAGES.STATUS=PROCESSED
PROCESSED_MESSAGES.MESSAGE_TYPE=XML

AUDIT_EVENTS.CORRELATION_ID=${correlationId}
AUDIT_EVENTS.EVENT_TYPE=MESSAGE_PROCESSED
AUDIT_EVENTS.STATUS=SUCCESS
```

Hinweise:

* `${correlationId}` wird sowohl in der XML-Nachricht als auch in den erwarteten DB-Werten durch die pro Testlauf generierte Correlation ID ersetzt.
* `ignored-columns` ist optional und sollte nur für nicht deterministische Spalten wie technische IDs oder Zeitstempel verwendet werden.
* Falls eine Tabelle eine andere Correlation-ID-Spalte verwendet, kann sie tabellenspezifisch überschrieben werden, z. B. `AUDIT_EVENTS.correlation-id-column=JMS_CORRELATION_ID`.
* Tabellennamen und Spaltennamen werden bewusst als Oracle-Identifier validiert, weil sie nicht als JDBC-Parameter gebunden werden können.

## Aufbau

* `pom.xml` definiert Java 21, JUnit 5, ActiveMQ Artemis JMS Client, Oracle JDBC und Awaitility.
* `src/test/java/com/example/integration/JmsQueueDatabaseIT.java` enthält den JMS-Sende- und Oracle-DB-Assertion-Test.
* `src/test/resources/db-assertions.properties` enthält beispielhafte Tabellen- und Spalten-Erwartungen.
