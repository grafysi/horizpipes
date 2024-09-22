package com.grafysi.horizpipes.utils.debezium;

import io.debezium.embedded.ConverterBuilder;
import io.debezium.embedded.async.AsyncEmbeddedEngine;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import io.debezium.engine.format.KeyValueChangeEventFormat;
import io.debezium.engine.format.KeyValueHeaderChangeEventFormat;
import io.debezium.engine.format.SerializationFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HzpConvertingAsyncEngineBuilderFactory implements DebeziumEngine.BuilderFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(HzpConvertingAsyncEngineBuilderFactory.class);

    @Override
    public <T, V extends SerializationFormat<T>> DebeziumEngine.Builder<RecordChangeEvent<T>> builder(ChangeEventFormat<V> format) {
        var builder = (AsyncEmbeddedEngine.AsyncEngineBuilder<RecordChangeEvent<T>>) newAsyncEngineBuilder(ChangeEventFormat.class, format);
        var converterBuilder = new HzpConverterBuilder<>();
        converterBuilder.using(KeyValueHeaderChangeEventFormat.of(null, format.getValueFormat(), null));
        setConverterBuilder(builder, converterBuilder);
        return builder;
    }

    @Override
    public <S, T, K extends SerializationFormat<S>, V extends SerializationFormat<T>> DebeziumEngine.Builder<ChangeEvent<S, T>> builder(
            KeyValueChangeEventFormat<K, V> format) {
        var builder = (AsyncEmbeddedEngine.AsyncEngineBuilder<ChangeEvent<S, T>>) newAsyncEngineBuilder(KeyValueChangeEventFormat.class, format);
        var converterBuilder = new HzpConverterBuilder<>();
        converterBuilder.using(KeyValueHeaderChangeEventFormat.of(format.getKeyFormat(), format.getValueFormat(), null));
        setConverterBuilder(builder, converterBuilder);
        return builder;
    }

    public <S, T, U, K extends SerializationFormat<S>, V extends SerializationFormat<T>, H extends SerializationFormat<U>> DebeziumEngine.Builder<ChangeEvent<S, T>> builder(
            KeyValueHeaderChangeEventFormat<K, V, H> format) {
        var builder = (AsyncEmbeddedEngine.AsyncEngineBuilder<ChangeEvent<S, T>>) newAsyncEngineBuilder(KeyValueHeaderChangeEventFormat.class, format);
        var converterBuilder = new HzpConverterBuilder<>();
        converterBuilder.using(format);
        setConverterBuilder(builder, converterBuilder);
        return builder;
    }

    private AsyncEmbeddedEngine.AsyncEngineBuilder<?> newAsyncEngineBuilder(Class<?> formatClass, Object format) {
        try {
            var clazz = Class.forName(AsyncEmbeddedEngine.AsyncEngineBuilder.class.getName());
            var constructor = clazz.getDeclaredConstructor(formatClass);
            constructor.setAccessible(true);
            return (AsyncEmbeddedEngine.AsyncEngineBuilder<?>) constructor.newInstance(format);
        } catch (Exception e) {
            LOGGER.error("Instantiate {} failed.", AsyncEmbeddedEngine.AsyncEngineBuilder.class.getName());
            throw new RuntimeException(e);
        }
    }

    private void setConverterBuilder(AsyncEmbeddedEngine.AsyncEngineBuilder<?> engineBuilder, ConverterBuilder<?> converterBuilder) {
        try {
            var field = AsyncEmbeddedEngine.AsyncEngineBuilder.class.getDeclaredField("converterBuilder");
            field.setAccessible(true);
            field.set(engineBuilder, converterBuilder);
        } catch (Exception e) {
            LOGGER.error("Set converter builder failed.");
            throw new RuntimeException(e);
        }
    }
}





































