package com.grafysi.horizpipes.utils.connect.apicurio;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class ByteArray {

    private final byte[] value;

    public ByteArray(byte[] value) {
        this.value = value;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(value);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof  ByteArray other) {
            return Arrays.equals(this.value, other.value);
        }
        return false;
    }

    public String asString() {
        return new String(value, StandardCharsets.UTF_8);
    }

    public static ByteArray of(byte... value) {
        return new ByteArray(value);
    }
}
