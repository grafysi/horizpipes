package io.horizpipes.dbztest;


import io.debezium.engine.Header;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class GetRawByteTest extends BasePgApicurioAvroTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(GetRawByteTest.class);

    @BeforeEach
    public void setup() {
        super.setup();
    }

    @AfterEach
    public void cleanup() throws Exception{
        super.cleanup();
    }

    @Test
    void getRawBytesAndHeaders() {
        LOGGER.info("=============== Test getRawBytesAndHeaders ===============");

        var recordCount = new AtomicInteger(0);
        var nullKeyCount = new AtomicInteger(0);
        var nullValueCount = new AtomicInteger(0);

        runConnector(record -> {
            var keyBytes = record.key();
            if (keyBytes == null) {
                nullKeyCount.incrementAndGet();
            }

            var valueBytes = record.value();
            if (valueBytes == null) {
                nullValueCount.incrementAndGet();
            }

            logRecordHeaders(record.headers());

            recordCount.incrementAndGet();
        });

        assertTrue(recordCount.get() > 0);

        LOGGER.info("Processed {} records", recordCount.get());
        LOGGER.info("Null key count: {}", nullKeyCount.get());
        LOGGER.info("Null value count: {}", nullValueCount.get());
    }

    private void logRecordHeaders(List<Header<Object>> headers) {
        LOGGER.info("Record headers");
        headers.forEach(header -> {
            LOGGER.info("{}: {}", header.getKey(), header.getValue());
        });
    }
}






















