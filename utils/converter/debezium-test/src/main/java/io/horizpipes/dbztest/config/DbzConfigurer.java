package io.horizpipes.dbztest.config;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;

public class DbzConfigurer {

    private static final HashSet<Option> namedOptions;

    static {
        namedOptions = new HashSet<>();
        initiateOptions();
    }

    private static void initiateOptions() {
        addOption(new Option(Configs.CONNECTOR_CLASS, true));
        addOption(new Option(Configs.CONNECTOR_NAME, true));
        addOption(new Option(Configs.TOPIC_PREFIX, true));

        addOption(new Option(Configs.DATABASE_HOSTNAME, true));
        addOption(new Option(Configs.DATABASE_PORT, true));
        addOption(new Option(Configs.DATABASE_USER, true));
        addOption(new Option(Configs.DATABASE_PASSWORD, true));

        addOption(new Option(Configs.DATABASE_DBNAME, true));
        addOption(new Option(Configs.SCHEMA_INCLUDE_LIST, false));
        addOption(new Option(Configs.TABLE_INCLUDE_LIST, false));

        addOption(new Option(Configs.KEY_CONVERTER,
                "org.apache.kafka.connect.storage.StringConverter"));
        addOption(new Option(Configs.VALUE_CONVERTER,
                "org.apache.kafka.connect.storage.StringConverter"));

        addOption(new Option(Configs.POLL_INTERVAL_MS, "1000"));
        addOption(new Option(Configs.MAX_BATCH_SIZE, "1000"));
        addOption(new Option(Configs.RECORD_PROCESSING_THREADS, "1"));

        addOption(new Option(Configs.OFFSET_STORAGE,
                "org.apache.kafka.connect.storage.MemoryOffsetBackingStore"));
    }

    private static void addOption(Option option) {
        namedOptions.add(option);
    }

    private final HashMap<String, String> configMap;

    public DbzConfigurer() {
        this.configMap = new HashMap<>();
    }

    public boolean hasConfigured(String name) {
        return configMap.containsKey(name);
    }

    public void set(String name, String value) {
        configMap.put(name, value);
    }

    public Properties buildProperties() {
        var props = new Properties();
        setDefaultValuesIfNotPresent();
        validateRequiredConfigs();
        configMap.forEach(props::setProperty);
        return props;
    }

    private void setDefaultValuesIfNotPresent() {
        namedOptions.stream()
                .filter(option -> option.defaultValue().isPresent()
                        && !configMap.containsKey(option.name()))
                .forEach(option -> configMap.put(option.name(), option.defaultValue().get()));
    }

    private void validateRequiredConfigs() {
        namedOptions.stream()
                .filter(Option::required)
                .forEach(option -> {
                    if (!configMap.containsKey(option.name())) {
                        throw new IllegalArgumentException("Missing required config: " + option.name());
                    }
                });
    }

}



















