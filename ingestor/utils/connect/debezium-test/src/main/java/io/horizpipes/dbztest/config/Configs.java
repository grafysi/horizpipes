package io.horizpipes.dbztest.config;

public final class Configs {

    public static final String CONNECTOR_CLASS = "connector.class";
    public static final String CONNECTOR_NAME = "name";
    public static final String TOPIC_PREFIX = "topic.prefix";

    public static final String DATABASE_HOSTNAME = "database.hostname";
    public static final String DATABASE_PORT = "database.port";
    public static final String DATABASE_USER = "database.user";
    public static final String DATABASE_PASSWORD = "database.password";

    public static final String DATABASE_DBNAME = "database.dbname";
    public static final String SCHEMA_INCLUDE_LIST = "schema.include.list";
    public static final String TABLE_INCLUDE_LIST = "table.include.list";

    public static final String POLL_INTERVAL_MS = "poll.interval.ms";
    public static final String MAX_BATCH_SIZE = "max.batch.size";
    public static final String RECORD_PROCESSING_THREADS = "record.processing.threads";

    public static final String KEY_CONVERTER = "key.converter";
    public static final String VALUE_CONVERTER = "value.converter";

    public static final String OFFSET_STORAGE = "offset.storage";
}
