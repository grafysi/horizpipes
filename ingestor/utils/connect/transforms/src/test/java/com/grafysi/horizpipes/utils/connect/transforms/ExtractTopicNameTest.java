package com.grafysi.horizpipes.utils.connect.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ExtractTopicNameTest {

    private ExtractTopicName<SourceRecord> xform = new ExtractTopicName<>();

    private Map<String, ?> config(String topicRegex, String headerName, String valueFormat) {
        var result = new HashMap<String, String>();
        result.put(ExtractTopicName.TOPIC_REGEX, topicRegex);
        result.put(ExtractTopicName.HEADER_NAME, headerName);
        result.put(ExtractTopicName.HEADER_VALUE_FORMAT, valueFormat);
        return result;
    }

    private SourceRecord sourceRecord(String topic, ConnectHeaders headers) {
        var sourcePartition = singletonMap("partition", "ignored");
        var sourceOffset = singletonMap("offset", "ignored");
        var partition = 0;
        Schema keySchema = null;
        var key = "key";
        Schema valueSchema = null;
        var value = "value";
        var timestamp = 0L;

        return new SourceRecord(
                sourcePartition,
                sourceOffset,
                topic,
                partition,
                keySchema,
                key,
                valueSchema,
                value,
                timestamp,
                headers);
    }

    private SourceRecord sourceRecord(String topic) {
        var headers = new ConnectHeaders();
        return sourceRecord(topic, headers);
    }

    @Test
    void simplyInsertTableNames() {
        xform.configure(
                config("mimic4demo.(.*)", "__dbz_table", "__$1"));

        var topic = "mimic4demo.mimiciv_hosp.patients";

        var expected = new ConnectHeaders();
        expected.addString("__dbz_table", "__mimiciv_hosp.patients");

        var original = sourceRecord(topic);
        var xformed = xform.apply(original);
        assertAllExceptHeaders(original, xformed);
        assertEquals(1, xformed.headers().size());
        assertEquals(expected, xformed.headers());
    }

    @Test
    void multithreadedInsertTableNames() {
        final var TEST_ROUNDS = 100;
        final var tables = List.of("patients", "admissions", "services", "labevents", "prescriptions");

        IntStream.range(0, TEST_ROUNDS)
                .mapToObj(i -> tables)
                .flatMap(List::stream)
                .parallel()
                .forEach(this::testWithTableName);
    }

    private void testWithTableName(String tableName) {
        final var topicPrefix = "mimic4demo.excluded_segment.mimiciv_hosp.";
        final var topic = topicPrefix + tableName;

        xform.configure(config("mimic4demo.(.*segment).(.*)", "__dbz_table", "_\\$1__$2"));

        var expected = new ConnectHeaders();
        expected.addString("__dbz_table", "_$1__mimiciv_hosp." + tableName);

        var original = sourceRecord(topic);
        var xformed = xform.apply(original);
        assertAllExceptHeaders(original, xformed);
        assertEquals(expected, xformed.headers());
    }

    private void assertAllExceptHeaders(SourceRecord original, SourceRecord xformed) {
        assertEquals(original.sourcePartition(), xformed.sourcePartition());
        assertEquals(original.sourceOffset(), xformed.sourceOffset());
        assertEquals(original.topic(), xformed.topic());
        assertEquals(original.kafkaPartition(), xformed.kafkaPartition());
        assertEquals(original.keySchema(), xformed.keySchema());
        assertEquals(original.key(), xformed.key());
        assertEquals(original.valueSchema(), xformed.valueSchema());
        assertEquals(original.value(), xformed.value());
        assertEquals(original.timestamp(), xformed.timestamp());
    }
}





























