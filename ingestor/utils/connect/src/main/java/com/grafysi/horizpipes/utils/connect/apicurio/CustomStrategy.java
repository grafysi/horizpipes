package com.grafysi.horizpipes.utils.connect.apicurio;

import io.apicurio.registry.resolver.ParsedSchema;
import io.apicurio.registry.resolver.data.Record;
import io.apicurio.registry.resolver.strategy.ArtifactReference;
import io.apicurio.registry.resolver.strategy.ArtifactReferenceResolverStrategy;
import io.apicurio.registry.serde.data.KafkaSerdeMetadata;
import io.apicurio.registry.serde.data.KafkaSerdeRecord;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.regex.Pattern;

public class CustomStrategy<T> implements ArtifactReferenceResolverStrategy<T, Object> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomStrategy.class);

    private static final String TABLE_ID_HEADER = "__from_table";

    private final Pattern PATTERN = Pattern.compile("\\{\"schema\":.*,\"payload\":\"(.*)\"}");

    private final ConcurrentHashMap<ByteArray, String> cache = new ConcurrentHashMap<>();


    @Override
    public ArtifactReference artifactReference(Record<Object> data, ParsedSchema<T> parsedSchema) {
        var kafkaRecord = (KafkaSerdeRecord<Object>) data;
        var metadata = kafkaRecord.metadata();
        var kafkaHeaders = metadata.getHeaders();

        if (kafkaHeaders == null) {
            LOGGER.warn("Get table id failed for record of topic {}. Headers is null.", metadata.getTopic());
        }

        var tableIdHeader = getFirstHeader(TABLE_ID_HEADER, kafkaHeaders);

        if (tableIdHeader == null) {
            LOGGER.warn("Get table id failed. No header {} found.", TABLE_ID_HEADER);
            return defaultTopicIdReference(metadata);
        }

        return tableIdHeaderReference(tableIdHeader, metadata.isKey());
    }

    /**
     * @see io.apicurio.registry.serde.strategy.ArtifactResolverStrategy#loadSchema()
     */
    @Override
    public boolean loadSchema() {
        return false;
    }

    private String getFirstHeader(String key, Headers headers) {
        var iter = headers.headers(key).iterator();

        return iter.hasNext() ?
                extractTableIdFrom(iter.next().value(), this::parseHeaderValue)
                : null;
    }

    private ArtifactReference defaultTopicIdReference(KafkaSerdeMetadata metadata) {
        return ArtifactReference.builder()
                .groupId(null)
                .artifactId(String.format("%s-%s", metadata.getTopic(), metadata.isKey() ? "key" : "value"))
                .build();
    }

    private ArtifactReference tableIdHeaderReference(String header, boolean isKey) {
        //LOGGER.info("Build artifact from header: {}-{}", header, isKey ? "key" : "value");
        var schemaId = String.format("%s-%s", header, isKey ? "key" : "value");
        return ArtifactReference.builder()
                .groupId(null)
                .artifactId(schemaId)
                .build();
    }

    private String extractTableIdFrom(byte[] headerValue, Function<String, String> extractor) {

        var content = ByteArray.of(headerValue);

        if (cache.contains(content)) {
            return cache.get(content);
        }

        var tableId = extractor.apply(content.asString());
        cache.put(content, tableId);
        return tableId;
    }

    private String parseHeaderValue(String value) {
        var matcher = PATTERN.matcher(value);
        if (!matcher.matches()) {
            return null;
        }
        return matcher.group(1);
    }
}





















