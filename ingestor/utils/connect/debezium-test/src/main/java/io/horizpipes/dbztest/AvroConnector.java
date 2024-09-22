package io.horizpipes.dbztest;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Avro;
import io.debezium.engine.format.Json;
import io.debezium.engine.format.KeyValueHeaderChangeEventFormat;

import java.util.Properties;
import java.util.function.Consumer;

public class AvroConnector implements Runnable {

    private final DebeziumEngine<?> dbzEngine;

    public AvroConnector(Properties props, Consumer<ChangeEvent<byte[], byte[]>> consumer) {
        this.dbzEngine = DebeziumEngine.create(
                KeyValueHeaderChangeEventFormat.of(Avro.class, Avro.class, Json.class),
                        "io.debezium.embedded.async.ConvertingAsyncEngineBuilderFactory")
                .using(props)
                .notifying(consumer)
                .build();
        /*this.dbzEngine = DebeziumEngine.create(Avro.class)
                .using(props)
                .notifying(consumer)
                .build();*/
    }

    @Override
    public void run() {
        dbzEngine.run();
    }

    public void stop() {
        try {
            dbzEngine.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
