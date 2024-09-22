package com.grafysi.horizpipes.utils.debezium.config;

import java.util.Objects;
import java.util.Optional;

public record Option(String name, Optional<String> defaultValue, boolean required) {

    public Option(String name, Optional<String> defaultValue, boolean required) {
        this.name = Objects.requireNonNull(name);
        this.defaultValue = Objects.requireNonNull(defaultValue);
        this.required = required;
    }

    public Option(String name, boolean required) {
        this(name, Optional.empty(), required);
    }

    public Option(String name, String defaultValue) {
        this(name, Optional.ofNullable(defaultValue), false);
    }
}
