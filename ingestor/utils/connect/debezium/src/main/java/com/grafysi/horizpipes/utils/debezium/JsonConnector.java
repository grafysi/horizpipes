package com.grafysi.horizpipes.utils.debezium;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.engine.format.KeyValueHeaderChangeEventFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.ServiceLoader;
import java.util.function.Consumer;

public class JsonConnector implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonConnector.class);

    private final DebeziumEngine<ChangeEvent<String, String>> engine;

    public JsonConnector(Properties props, Consumer<ChangeEvent<String, String>> consumer) {

        final var serviceLoader = ServiceLoader.load(DebeziumEngine.BuilderFactory.class);

        LOGGER.info("BuilderFactory implementations");
        serviceLoader.stream().forEach(provider -> LOGGER.info("{}", provider.get().getClass().getName()));



        this.engine = DebeziumEngine.create(
                KeyValueHeaderChangeEventFormat.of(Json.class, Json.class, Json.class),
                HzpConvertingAsyncEngineBuilderFactory.class.getName())
                .using(props)
                .notifying(consumer)
                .build();
    }

    @Override
    public void run() {
        engine.run();
    }

    public void stop() {
        try {
            engine.close();
        } catch (Exception e) {
            LOGGER.error("Stop connector failed.");
            throw new RuntimeException(e);
        }
    }
}
