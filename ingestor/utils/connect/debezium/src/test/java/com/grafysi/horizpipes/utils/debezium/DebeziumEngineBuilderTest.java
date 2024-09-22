package com.grafysi.horizpipes.utils.debezium;

import com.grafysi.horizpipes.utils.debezium.config.Configs;
import io.debezium.engine.ChangeEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DebeziumEngineBuilderTest extends JsonConfigAbstractTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumEngineBuilderTest.class);

    @Test
    void runJsonConnector() {
        LOGGER.info("Test runJsonConnector");

        var recordCount = new AtomicInteger(0);

        Consumer<ChangeEvent<String, String>> consumer = (record) -> {
            recordCount.incrementAndGet();
        };

        var overrideProps = new Properties();

        overrideProps.put(Configs.TABLE_INCLUDE_LIST, "mimiciv_hosp.patients");

        runConnector(consumer, overrideProps, 10_000);

        final var expectedPatientCount = 100;

        assertEquals(expectedPatientCount, recordCount.get());
    }

}




















