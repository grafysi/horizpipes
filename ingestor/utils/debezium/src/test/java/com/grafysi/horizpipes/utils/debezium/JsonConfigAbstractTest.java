package com.grafysi.horizpipes.utils.debezium;

import com.grafysi.horizpipes.utils.debezium.config.Configs;
import com.grafysi.horizpipes.utils.debezium.config.DbzConfigurer;
import io.debezium.engine.ChangeEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public abstract class JsonConfigAbstractTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonConfigAbstractTest.class);
    private static final long DEFAULT_RUNNING_TIME_MS = 15_000;

    private ExecutorService executor;

    private Properties defaultConnectorProperties() {
        var cfg = new DbzConfigurer();
        cfg.set(Configs.CONNECTOR_NAME, "test-connector");
        cfg.set(Configs.CONNECTOR_CLASS, "io.debezium.connector.postgresql.PostgresConnector");
        cfg.set(Configs.TOPIC_PREFIX, "test_dbz");

        cfg.set("plugin.name", "pgoutput");
        cfg.set("slot.name", "debezium_test");

        cfg.set("record.processing.threads", "1");
        cfg.set("max.batch.size", "1");

        cfg.set(Configs.DATABASE_HOSTNAME, "localhost");
        cfg.set(Configs.DATABASE_PORT, "5433");
        cfg.set(Configs.DATABASE_USER, "postgres");
        cfg.set(Configs.DATABASE_PASSWORD, "abcd1234");

        cfg.set(Configs.DATABASE_DBNAME, "mimic4demo");
        cfg.set(Configs.SCHEMA_INCLUDE_LIST, "mimiciv_hosp");

        cfg.set(Configs.KEY_CONVERTER, "org.apache.kafka.connect.json.JsonConverter");
        cfg.set(Configs.VALUE_CONVERTER, "org.apache.kafka.connect.json.JsonConverter");

        return cfg.buildProperties();
    }

    protected JsonConnector createConnector(
            Consumer<ChangeEvent<String, String>> consumer) {
        var props = defaultConnectorProperties();

        props.putAll(overrideProperties());

        LOGGER.info("Connector properties:");
        props.forEach((k, v) -> LOGGER.info("{}: {}", k, v));

        return new JsonConnector(props, consumer);
    }


    protected void runConnector(Consumer<ChangeEvent<String, String>> consumer) {
        var connector = createConnector(consumer);
        executor.submit(connector);
        sleepForMs(DEFAULT_RUNNING_TIME_MS);
        connector.stop();
    }


    protected void runConnector(Consumer<ChangeEvent<String, String>> consumer, long runningTimeMs) {
        var connector = createConnector(consumer);
        executor.submit(connector);
        sleepForMs(runningTimeMs);
        connector.stop();
    }


    @BeforeEach
    protected void setup() {
        executor = Executors.newCachedThreadPool();
    }

    @AfterEach
    protected void cleanup() throws Exception{
        executor.shutdown();
        if (executor.awaitTermination(5, TimeUnit.SECONDS)) {
            LOGGER.info("Executor stopped gracefully.");
        } else {
            LOGGER.info("There are trailing workers when executor stopped.");
        }
    }


    protected void sleepForMs(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    protected Properties overrideProperties() {
        return new Properties();
    }
}
























