package com.grafysi.horizpipes.utils.debezium;

import com.grafysi.horizpipes.utils.connect.transforms.ExtractTopicName;
import com.grafysi.horizpipes.utils.debezium.config.Configs;
import com.grafysi.horizpipes.utils.debezium.utils.LogHeaderJsonConverter;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.Header;
import org.apache.kafka.common.header.Headers;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PrintHeadersBeforeSerializationTest extends JsonConfigAbstractTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(PrintHeadersBeforeSerializationTest.class);

    @Test
    void runConnectorAndPrintHeaderBeforeSerialization() {
        LOGGER.info("Test runConnectorAndPrintHeaderBeforeSerialization");

        var recordCount = new AtomicInteger(0);

        Consumer<ChangeEvent<String, String>> consumer = (record) -> {
            //logHeaders("Print record headers after serialization for record of " + record.destination(), record.headers());
            recordCount.incrementAndGet();
        };

        runConnector(consumer, 4_000);
        final var expectedPatientCount = 100;
        assertEquals(expectedPatientCount, recordCount.get());
    }

    protected Properties overrideProperties() {
        var props = new Properties();

        props.put(Configs.TABLE_INCLUDE_LIST, "mimiciv_hosp.patients");

        props.put("key.converter.custom.json.converter", LogHeaderJsonConverter.class.getName());
        props.put("value.converter.custom.json.converter", LogHeaderJsonConverter.class.getName());

        props.put("key.converter.class", LogHeaderJsonConverter.class.getName());
        props.put("value.converter.class", LogHeaderJsonConverter.class.getName());

        props.put("transforms", "ExtractTopic");
        props.put("transforms.ExtractTopic.type", ExtractTopicName.class.getName());
        props.put("transforms.ExtractTopic.topic.regex", "test_dbz.mimiciv_hosp.(.*)");
        props.put("transforms.ExtractTopic.header.name", "__from_table");
        props.put("transforms.ExtractTopic.header.value.format", "__from_table__test_dbz.mimiciv_hosp.$1");

        return props;
    }

    private void logHeaders(String msg, List<Header<String>> headers) {
        LOGGER.info(msg);
        headers.forEach(header -> LOGGER.info("{}={}",
                header.getKey(),
                header.getValue()));
    }
}
