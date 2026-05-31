package com.example.integration;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSException;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JmsQueueDatabaseIT {

    @Test
    void sendsXmlToJmsQueueAndVerifiesOracleTables() throws Exception {
        IntegrationTestConfig config = IntegrationTestConfig.fromEnvironment();
        String correlationId = UUID.randomUUID().toString();
        String xmlPayload = replaceCorrelationId(config.xmlPayload(), correlationId);

        sendXmlMessage(config, correlationId, xmlPayload);

        Awaitility.await()
                .alias("database rows for JMS correlation id " + correlationId)
                .atMost(config.timeout())
                .pollInterval(config.pollInterval())
                .untilAsserted(() -> assertDatabaseRows(config, correlationId));
    }

    private void sendXmlMessage(IntegrationTestConfig config, String correlationId, String xmlPayload) throws JMSException {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                config.jmsBrokerUrl(),
                config.jmsUsername(),
                config.jmsPassword());

        try (Connection connection = connectionFactory.createConnection();
             Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            Queue queue = session.createQueue(config.jmsQueueName());
            TextMessage message = session.createTextMessage(xmlPayload);
            message.setJMSCorrelationID(correlationId);
            message.setStringProperty("correlationId", correlationId);
            message.setStringProperty("contentType", "application/xml");

            try (MessageProducer producer = session.createProducer(queue)) {
                producer.send(message);
            }
        }
    }

    private void assertDatabaseRows(IntegrationTestConfig config, String correlationId) throws SQLException {
        try (java.sql.Connection connection = DriverManager.getConnection(
                config.databaseUrl(),
                config.databaseUsername(),
                config.databasePassword())) {
            for (TableAssertion tableAssertion : config.tableAssertions()) {
                Map<String, String> actualColumns = readSingleRow(connection, tableAssertion, correlationId);
                Map<String, String> comparableActualColumns = withoutIgnoredColumns(
                        actualColumns,
                        tableAssertion.ignoredColumns());
                Map<String, String> expectedColumns = tableAssertion.expectedColumns(correlationId);

                assertEquals(
                        expectedColumns,
                        comparableActualColumns,
                        "Alle nicht ignorierten Spalten der Tabelle " + tableAssertion.tableName()
                                + " muessen fuer correlation_id " + correlationId + " uebereinstimmen.");
            }
        }
    }

    private Map<String, String> readSingleRow(
            java.sql.Connection connection,
            TableAssertion tableAssertion,
            String correlationId) throws SQLException {
        String sql = "select * from " + tableAssertion.tableName()
                + " where " + tableAssertion.correlationIdColumn() + " = ?";

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, correlationId);

            try (ResultSet resultSet = statement.executeQuery()) {
                assertTrue(
                        resultSet.next(),
                        "Keine Zeile in Tabelle " + tableAssertion.tableName()
                                + " fuer correlation_id " + correlationId + " gefunden.");

                Map<String, String> row = currentRow(resultSet);
                assertFalse(
                        resultSet.next(),
                        "Mehr als eine Zeile in Tabelle " + tableAssertion.tableName()
                                + " fuer correlation_id " + correlationId + " gefunden.");
                return row;
            }
        }
    }

    private Map<String, String> currentRow(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        Map<String, String> row = new LinkedHashMap<>();
        for (int columnIndex = 1; columnIndex <= metaData.getColumnCount(); columnIndex++) {
            String columnName = normalizeName(metaData.getColumnLabel(columnIndex));
            row.put(columnName, Optional.ofNullable(resultSet.getObject(columnIndex))
                    .map(Object::toString)
                    .orElse(null));
        }
        return row;
    }

    private Map<String, String> withoutIgnoredColumns(Map<String, String> columns, Set<String> ignoredColumns) {
        return columns.entrySet().stream()
                .filter(entry -> !ignoredColumns.contains(entry.getKey()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (left, right) -> right,
                        LinkedHashMap::new));
    }

    private static String replaceCorrelationId(String value, String correlationId) {
        return value.replace("${correlationId}", correlationId);
    }

    private static String normalizeName(String name) {
        return name.trim().toUpperCase(Locale.ROOT);
    }

    private record TableAssertion(
            String tableName,
            String correlationIdColumn,
            Set<String> ignoredColumns,
            Map<String, String> expectedColumns) {

        private static final Pattern ORACLE_IDENTIFIER = Pattern.compile(
                "[A-Za-z][A-Za-z0-9_$#]*(\\.[A-Za-z][A-Za-z0-9_$#]*)?");

        private TableAssertion {
            validateOracleIdentifier(tableName, "table name");
            validateOracleIdentifier(correlationIdColumn, "correlation id column");
            expectedColumns = expectedColumns.entrySet().stream()
                    .collect(Collectors.toMap(
                            entry -> normalizeName(entry.getKey()),
                            Map.Entry::getValue,
                            (left, right) -> right,
                            LinkedHashMap::new));
            ignoredColumns = ignoredColumns.stream()
                    .map(JmsQueueDatabaseIT::normalizeName)
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        }

        private Map<String, String> expectedColumns(String correlationId) {
            return expectedColumns.entrySet().stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            entry -> replaceCorrelationId(entry.getValue(), correlationId),
                            (left, right) -> right,
                            LinkedHashMap::new));
        }

        private static void validateOracleIdentifier(String identifier, String label) {
            if (!ORACLE_IDENTIFIER.matcher(identifier).matches()) {
                throw new IllegalArgumentException("Invalid Oracle " + label + ": " + identifier);
            }
        }
    }

    private record IntegrationTestConfig(
            String jmsBrokerUrl,
            String jmsUsername,
            String jmsPassword,
            String jmsQueueName,
            String databaseUrl,
            String databaseUsername,
            String databasePassword,
            String xmlPayload,
            Duration timeout,
            Duration pollInterval,
            Set<TableAssertion> tableAssertions) {

        private static IntegrationTestConfig fromEnvironment() throws IOException {
            Properties assertionProperties = loadAssertionProperties(setting(
                    "db.assertions.file",
                    "DB_ASSERTIONS_FILE",
                    "db-assertions.properties"));

            return new IntegrationTestConfig(
                    setting("jms.broker.url", "JMS_BROKER_URL", "tcp://localhost:61616"),
                    setting("jms.username", "JMS_USERNAME", "admin"),
                    setting("jms.password", "JMS_PASSWORD", "admin"),
                    setting("jms.queue.name", "JMS_QUEUE_NAME", "example.queue"),
                    requiredSetting("db.url", "DB_URL"),
                    requiredSetting("db.username", "DB_USERNAME"),
                    requiredSetting("db.password", "DB_PASSWORD"),
                    setting("xml.payload", "XML_PAYLOAD",
                            "<message><correlationId>${correlationId}</correlationId><value>Hello JMS</value></message>"),
                    Duration.ofSeconds(longSetting("test.timeout.seconds", "TEST_TIMEOUT_SECONDS", 60)),
                    Duration.ofMillis(longSetting("test.poll.interval.millis", "TEST_POLL_INTERVAL_MILLIS", 1_000)),
                    tableAssertions(assertionProperties));
        }

        private static Properties loadAssertionProperties(String location) throws IOException {
            Properties properties = new Properties();
            Path filePath = Path.of(location);

            if (Files.isRegularFile(filePath)) {
                try (InputStream inputStream = Files.newInputStream(filePath)) {
                    properties.load(inputStream);
                    return properties;
                }
            }

            try (InputStream inputStream = JmsQueueDatabaseIT.class.getClassLoader().getResourceAsStream(location)) {
                if (inputStream == null) {
                    throw new IllegalArgumentException("DB assertion file not found: " + location);
                }
                properties.load(inputStream);
                return properties;
            }
        }

        private static Set<TableAssertion> tableAssertions(Properties properties) {
            Set<String> tableNames = splitRequiredList(properties, "tables");
            String defaultCorrelationIdColumn = requiredProperty(properties, "correlation-id-column");
            Set<String> defaultIgnoredColumns = splitOptionalList(properties.getProperty("ignored-columns"));

            return tableNames.stream()
                    .map(tableName -> {
                        String tablePrefix = normalizeName(tableName);
                        String correlationIdColumn = property(
                                properties,
                                tablePrefix + ".correlation-id-column",
                                defaultCorrelationIdColumn);
                        Set<String> ignoredColumns = new LinkedHashSet<>(defaultIgnoredColumns);
                        ignoredColumns.addAll(splitOptionalList(property(
                                properties,
                                tablePrefix + ".ignored-columns",
                                null)));

                        Map<String, String> expectedColumns = expectedColumns(properties, tablePrefix);
                        if (expectedColumns.isEmpty()) {
                            throw new IllegalArgumentException(
                                    "No expected columns configured for table " + tablePrefix + ".");
                        }
                        return new TableAssertion(tablePrefix, correlationIdColumn, ignoredColumns, expectedColumns);
                    })
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        }

        private static Map<String, String> expectedColumns(Properties properties, String tablePrefix) {
            String keyPrefix = tablePrefix + ".";
            Map<String, String> columns = new LinkedHashMap<>();

            properties.stringPropertyNames().stream()
                    .filter(key -> normalizeName(key).startsWith(keyPrefix))
                    .filter(key -> !normalizeName(key).endsWith(".CORRELATION-ID-COLUMN"))
                    .filter(key -> !normalizeName(key).endsWith(".IGNORED-COLUMNS"))
                    .sorted()
                    .forEach(key -> columns.put(
                            normalizeName(key.substring(keyPrefix.length())),
                            properties.getProperty(key)));

            return columns;
        }

        private static String property(Properties properties, String key, String defaultValue) {
            return properties.stringPropertyNames().stream()
                    .filter(propertyName -> propertyName.equalsIgnoreCase(key))
                    .findFirst()
                    .map(properties::getProperty)
                    .orElse(defaultValue);
        }

        private static String requiredSetting(String systemProperty, String environmentVariable) {
            return Optional.ofNullable(setting(systemProperty, environmentVariable, null))
                    .filter(value -> !value.isBlank())
                    .orElseThrow(() -> new IllegalStateException(
                            "Required configuration missing: set system property '" + systemProperty
                                    + "' or environment variable '" + environmentVariable + "'."));
        }

        private static String setting(String systemProperty, String environmentVariable, String defaultValue) {
            return Optional.ofNullable(System.getProperty(systemProperty))
                    .or(() -> Optional.ofNullable(System.getenv(environmentVariable)))
                    .filter(value -> !value.isBlank())
                    .orElse(defaultValue);
        }

        private static long longSetting(String systemProperty, String environmentVariable, long defaultValue) {
            String value = setting(systemProperty, environmentVariable, Long.toString(defaultValue));
            try {
                return Long.parseLong(Objects.requireNonNull(value));
            } catch (NumberFormatException exception) {
                throw new IllegalArgumentException("Configuration value must be a number: " + systemProperty
                        + " / " + environmentVariable + " = " + value, exception);
            }
        }

        private static Set<String> splitRequiredList(Properties properties, String key) {
            Set<String> values = splitOptionalList(requiredProperty(properties, key));
            if (values.isEmpty()) {
                throw new IllegalArgumentException("Configuration property must not be empty: " + key);
            }
            return values;
        }

        private static Set<String> splitOptionalList(String value) {
            if (value == null || value.isBlank()) {
                return new LinkedHashSet<>();
            }
            return Arrays.stream(value.split(","))
                    .map(String::trim)
                    .filter(item -> !item.isBlank())
                    .map(JmsQueueDatabaseIT::normalizeName)
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        }

        private static String requiredProperty(Properties properties, String key) {
            return Optional.ofNullable(properties.getProperty(key))
                    .filter(value -> !value.isBlank())
                    .orElseThrow(() -> new IllegalArgumentException("Required property missing: " + key));
        }
    }
}
