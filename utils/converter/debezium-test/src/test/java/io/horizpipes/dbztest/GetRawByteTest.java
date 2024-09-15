package io.horizpipes.dbztest;


import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class GetRawByteTest extends BasePgApicurioAvroTest {

    private static final Logger LOG = LoggerFactory.getLogger(GetRawByteTest.class);

    @BeforeEach
    public void setup() {
        super.setup();
    }

    @AfterEach
    public void cleanup() throws Exception{
        super.cleanup();
    }

    @Test
    void testGetRawByte() {
        LOG.info("=============== Start testGetRawByte ===============");

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

            recordCount.incrementAndGet();
        });

        assertTrue(recordCount.get() > 0);

        LOG.info("Processed {} records", recordCount.get());
        LOG.info("Null key count: {}", nullKeyCount.get());
        LOG.info("Null value count: {}", nullValueCount.get());
    }
}






















