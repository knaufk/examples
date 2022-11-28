package com.immerok.cloud.examples.events;

import java.util.Arrays;
import java.util.Objects;

/** A JSON object with a custom key that Flink recognizes as a valid POJO. */
public class KeyedJson {

    public String key;

    public byte[] rawJson;

    /** A Flink POJO must have a no-args default constructor */
    public KeyedJson() {}

    public KeyedJson(String key, byte[] rawJson) {
        this.key = key;
        this.rawJson = rawJson;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KeyedJson keyedJson = (KeyedJson) o;
        return key.equals(keyedJson.key) && Arrays.equals(rawJson, keyedJson.rawJson);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(key);
        result = 31 * result + Arrays.hashCode(rawJson);
        return result;
    }

    @Override
    public String toString() {
        return "KeyedJson{" + "key='" + key + '\'' + ", rawJson=" + Arrays.toString(rawJson) + '}';
    }
}
