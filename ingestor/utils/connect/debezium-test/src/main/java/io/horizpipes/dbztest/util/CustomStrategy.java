package io.horizpipes.dbztest.util;

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

public class CustomStrategy<T> implements ArtifactReferenceResolverStrategy<T, Object> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomStrategy.class);

    private static final String TABLE_ID_HEADER = "__from_table";

    @Override
    public ArtifactReference artifactReference(Record<Object> data, ParsedSchema<T> parsedSchema) {
        var kafkaRecord = (KafkaSerdeRecord<Object>) data;
        var metadata = kafkaRecord.metadata();
        var kafkaHeader = metadata.getHeaders();

        if (kafkaHeader == null) {
            LOGGER.warn("Get table id failed for record of topic {}. Headers is null.", metadata.getTopic());
        }

        var tableIdHeader = getFirstHeader(TABLE_ID_HEADER, metadata.getHeaders());

        if (tableIdHeader == null) {
            LOGGER.warn("Get table id failed. No header {} found.", TABLE_ID_HEADER);
            return defaultTopicIdReference(metadata);
        }

        return tableIdHeaderReference(tableIdHeader);
    }

    /**
     * @see io.apicurio.registry.serde.strategy.ArtifactResolverStrategy#loadSchema()
     */
    @Override
    public boolean loadSchema() {
        return true;
    }

    private String getFirstHeader(String key, Headers headers) {
        var iter = headers.headers(key).iterator();
        if (iter.hasNext()) {
            var valueBytes = iter.next().value();
            return new String(valueBytes, StandardCharsets.UTF_8);
        }
        return null;
    }

    private ArtifactReference defaultTopicIdReference(KafkaSerdeMetadata metadata) {
        return ArtifactReference.builder()
                .groupId(null)
                .artifactId(String.format("%s-%s", metadata.getTopic(), metadata.isKey() ? "key" : "value"))
                .build();
    }

    private ArtifactReference tableIdHeaderReference(String header) {
        return ArtifactReference.builder()
                .groupId(null)
                .artifactId(header)
                .build();
    }
}





















