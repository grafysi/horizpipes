package com.grafysi.horizpipes.utils.debezium.utils;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class LogHeaderJsonConverter implements Converter {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogHeaderJsonConverter.class);

    private final JsonConverter internal;

    public LogHeaderJsonConverter() {
        this.internal = new JsonConverter();
    }

    @Override
    public ConfigDef config() {
        return internal.config();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        internal.configure(configs, isKey);
    }

    @Override
    public byte[] fromConnectData(String topic, Headers headers, Schema schema, Object value) {
        logHeaders("Print headers before deserialization", headers);
        return internal.fromConnectData(topic, headers, schema, value);
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        return internal.fromConnectData(topic, schema, value);
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        return internal.toConnectData(topic, value);
    }

    @Override
    public SchemaAndValue toConnectData(String topic, Headers headers, byte[] value) {
        return internal.toConnectData(topic, headers, value);
    }

    private void logHeaders(String msg, Headers headers) {
        LOGGER.info(msg);
        headers.forEach(header -> LOGGER.info("{}={}",
                header.key(),
                new String(header.value(), StandardCharsets.UTF_8)));
    }


}















