package io.horizpipes.dbztest;

import io.apicurio.registry.serde.SerdeConfig;
import io.debezium.engine.ChangeEvent;
import io.horizpipes.dbztest.config.Configs;
import io.horizpipes.dbztest.config.DbzConfigurer;
import io.horizpipes.dbztest.util.CustomStrategy;
import org.apache.kafka.connect.transforms.InsertHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public abstract class BasePgApicurioAvroTest {

    private static final Logger LOG = LoggerFactory.getLogger(BasePgApicurioAvroTest.class);
    private static final long DEFAULT_RUNNING_TIME_MS = 15_000;

    private ExecutorService executor;

    private Properties getDbzProperties() {
        var cfg = new DbzConfigurer();
        cfg.set(Configs.CONNECTOR_NAME, "test-connector");
        cfg.set(Configs.CONNECTOR_CLASS, "io.debezium.connector.postgresql.PostgresConnector");
        cfg.set(Configs.TOPIC_PREFIX, "test_v2-");

        cfg.set("plugin.name", "pgoutput");
        cfg.set("slot.name", "debezium_test_slot_01");

        cfg.set(Configs.DATABASE_HOSTNAME, "localhost");
        cfg.set(Configs.DATABASE_PORT, "5433");
        cfg.set(Configs.DATABASE_USER, "postgres");
        cfg.set(Configs.DATABASE_PASSWORD, "abcd1234");

        cfg.set(Configs.DATABASE_DBNAME, "mimic4demo");
        cfg.set(Configs.SCHEMA_INCLUDE_LIST, "mimiciv_hosp");

        cfg.set(Configs.KEY_CONVERTER, "io.apicurio.registry.utils.converter.AvroConverter");
        cfg.set(Configs.VALUE_CONVERTER, "io.apicurio.registry.utils.converter.AvroConverter");

        cfg.set("key.converter.apicurio.registry.url", "http://apicurio.hzp.local:8000/apis/registry/v2");
        cfg.set("value.converter.apicurio.registry.url", "http://apicurio.hzp.local:8000/apis/registry/v2");

        cfg.set("apicurio.registry.url", "http://apicurio.hzp.local:8000/apis/registry/v2");

        //cfg.set("key.serializer", AvroKafkaSerializer.class.getName());
        //cfg.set("value.serializer", AvroKafkaSerializer.class.getName());
        //cfg.set("key.converter", AvroConverter.class.getName());
        //cfg.set("value.converter", AvroConverter.class.getName());


        cfg.set("key.converter.apicurio.registry.auto-register", "true");
        cfg.set("value.converter.apicurio.registry.auto-register", "true");

        cfg.set("key.converter.apicurio.registry.find-latest", "true");
        cfg.set("value.converter.apicurio.registry.find-latest", "true");

        /*cfg.set("key.converter." + SerdeConfig.ARTIFACT_RESOLVER_STRATEGY,
                CustomStrategy.class.getName());
        cfg.set("value.converter." + SerdeConfig.ARTIFACT_RESOLVER_STRATEGY,
                CustomStrategy.class.getName());*/

        //cfg.set(SerdeConfig.ARTIFACT_RESOLVER_STRATEGY, "io.apicurio.registry.serde.strategy.TopicIdStrategy");

        //cfg.set(SerdeConfig.ARTIFACT_RESOLVER_STRATEGY, "io.apicurio.registry.serde.strategy.SimpleTopicIdStrategy");

        //cfg.set(SerdeConfig.ARTIFACT_RESOLVER_STRATEGY, CustomStrategy.class.getName());

        //cfg.set(SchemaResolverConfig.ARTIFACT_RESOLVER_STRATEGY, RecordIdStrategy.class.getName());

        //cfg.set("key.converter." + SerdeConfig.SCHEMA_RESOLVER, "a.non.existing.Class");

        //cfg.set("schema.name.adjustment.mode", "avro");

        cfg.set("transforms", "InsertHeader");

        // config InsertHeader transform
        cfg.set("transforms.InsertHeader.type", InsertHeader.class.getName());
        cfg.set("transforms.InsertHeader.header", "__dbz_topic");
        cfg.set("transforms.InsertHeader.value.literal", "${topic}");

        // config Reroute transform
        /*cfg.set("transforms.Reroute.type", "io.debezium.transforms.ByLogicalTableRouter");
        cfg.set("transforms.Reroute.topic.regex", ".*");
        cfg.set("transforms.Reroute.topic.replacement", "test.mimic4demo_hosp.all_tables_std2");*/



        return cfg.buildProperties();
    }

    protected AvroConnector createConnector(
            Consumer<ChangeEvent<byte[], byte[]>> consumer) {
        var props = getDbzProperties();

        LOG.info("Connector properties:");
        props.forEach((k, v) -> LOG.info("{}: {}", k, v));

        return new AvroConnector(props, consumer);
    }


    protected void runConnector(Consumer<ChangeEvent<byte[], byte[]>> consumer) {
        var connector = createConnector(consumer);
        executor.submit(connector);
        sleepForMs(DEFAULT_RUNNING_TIME_MS);
        connector.stop();
    }


    protected void runConnector(Consumer<ChangeEvent<byte[], byte[]>> consumer, long runningTimeMs) {
        var connector = createConnector(consumer);
        executor.submit(connector);
        sleepForMs(runningTimeMs);
        connector.stop();
    }


    protected void setup() {
        executor = Executors.newCachedThreadPool();
    }


    protected void cleanup() throws Exception{
        executor.shutdown();
        if (executor.awaitTermination(5, TimeUnit.SECONDS)) {
            LOG.info("Executor stopped gracefully.");
        } else {
            LOG.info("There are trailing workers when executor stopped.");
        }
    }


    protected void sleepForMs(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
























