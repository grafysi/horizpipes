package io.horizpipes.dbztest;

import com.grafysi.horizpipes.utils.connect.apicurio.CustomSchemaResolver;
import com.grafysi.horizpipes.utils.connect.apicurio.CustomStrategy;
import com.grafysi.horizpipes.utils.connect.transforms.ExtractTopicName;
import io.apicurio.registry.serde.SerdeConfig;
import io.debezium.engine.ChangeEvent;
import io.horizpipes.dbztest.config.Configs;
import io.horizpipes.dbztest.config.DbzConfigurer;
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
        cfg.set(Configs.TOPIC_PREFIX, "test_dbz");

        cfg.set("plugin.name", "pgoutput");
        cfg.set("slot.name", "debezium_test_slot_01");

        cfg.set("record.processing.threads", "1");
        cfg.set("max.batch.size", "1");

        cfg.set(Configs.DATABASE_HOSTNAME, "localhost");
        cfg.set(Configs.DATABASE_PORT, "5433");
        cfg.set(Configs.DATABASE_USER, "postgres");
        cfg.set(Configs.DATABASE_PASSWORD, "abcd1234");

        cfg.set(Configs.DATABASE_DBNAME, "mimic4demo");
        cfg.set(Configs.SCHEMA_INCLUDE_LIST, "mimiciv_hosp");
        //cfg.set(Configs.TABLE_INCLUDE_LIST, "mimiciv_hosp.d_icd_procedures");

        cfg.set(Configs.KEY_CONVERTER, "io.apicurio.registry.utils.converter.AvroConverter");
        cfg.set(Configs.VALUE_CONVERTER, "io.apicurio.registry.utils.converter.AvroConverter");

        cfg.set("key.converter.apicurio.registry.url", "http://apicurio.hzp.local:8000/apis/registry/v2");
        cfg.set("value.converter.apicurio.registry.url", "http://apicurio.hzp.local:8000/apis/registry/v2");

        cfg.set("key.converter.apicurio.registry.auto-register", "true");
        cfg.set("value.converter.apicurio.registry.auto-register", "true");

        cfg.set("key.converter.apicurio.registry.find-latest", "true");
        cfg.set("value.converter.apicurio.registry.find-latest", "true");

        // using custom schema resolver
        cfg.set("key.converter.apicurio.registry.schema-resolver", CustomSchemaResolver.class.getName());
        cfg.set("value.converter.apicurio.registry.schema-resolver", CustomSchemaResolver.class.getName());

        // using custom artifact resolver strategy
        cfg.set("key.converter.apicurio.registry.artifact-resolver-strategy", CustomStrategy.class.getName());
        cfg.set("value.converter.apicurio.registry.artifact-resolver-strategy", CustomStrategy.class.getName());

        cfg.set("transforms", "ExtractTopic,Reroute");

        // config ExtractTopic transform
        cfg.set("transforms.ExtractTopic.type", ExtractTopicName.class.getName());
        cfg.set("transforms.ExtractTopic.topic.regex", "test_dbz.mimiciv_hosp.(.*)");
        cfg.set("transforms.ExtractTopic.header.name", "__from_table");
        cfg.set("transforms.ExtractTopic.header.value.format", "__from_table__test_dbz.mimiciv_hosp.$1");

        // config Reroute transform
        cfg.set("transforms.Reroute.type", "io.debezium.transforms.ByLogicalTableRouter");
        cfg.set("transforms.Reroute.topic.regex", ".*");
        cfg.set("transforms.Reroute.topic.replacement", "test_dbz.mimic4demo.hosp.all");

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
























