package io.horizpipes.dbztest.util;

import io.apicurio.registry.resolver.ParsedSchema;
import io.apicurio.registry.resolver.data.Record;
import io.apicurio.registry.resolver.strategy.ArtifactReference;
import io.apicurio.registry.resolver.strategy.ArtifactReferenceResolverStrategy;
import io.apicurio.registry.serde.data.KafkaSerdeMetadata;
import io.apicurio.registry.serde.data.KafkaSerdeRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomStrategy<T> implements ArtifactReferenceResolverStrategy<T, Object> {

    private static final Logger LOG = LoggerFactory.getLogger(CustomStrategy.class);

    @Override
    public ArtifactReference artifactReference(Record<Object> data, ParsedSchema<T> parsedSchema) {
        if (true) {
            throw new RuntimeException("ParsedSchema reference: " + parsedSchema.referenceName());
        }

        KafkaSerdeRecord<Object> kdata = (KafkaSerdeRecord<Object>) data;

        KafkaSerdeMetadata metadata = kdata.metadata();
        return ArtifactReference.builder().groupId(null)
                .artifactId(String.format("my-custom-%s-%s", metadata.getTopic(), metadata.isKey() ? "key" : "value"))
                .build();
    }

    /**
     * @see io.apicurio.registry.serde.strategy.ArtifactResolverStrategy#loadSchema()
     */
    @Override
    public boolean loadSchema() {
        return true;
    }
}
