package com.immerok.cloud.examples.events;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

/** A deserializer for a generic JSON object with a custom key from Kafka. */
public class KeyedJsonDeserializationSchema extends AbstractDeserializationSchema<KeyedJson> {

    private static final long serialVersionUID = 1L;

    private final String key;

    private transient ObjectMapper objectMapper;

    public KeyedJsonDeserializationSchema(String key) {
        this.key = key;
    }

    /**
     * For performance reasons it's better to create on ObjectMapper in this open method rather than
     * creating a new ObjectMapper for every record.
     */
    @Override
    public void open(InitializationContext context) {
        objectMapper = new ObjectMapper();
    }

    @Override
    public KeyedJson deserialize(byte[] rawJson) throws IOException {
        JsonNode node = objectMapper.readTree(rawJson);
        String extractedKey = node.get(key).asText();
        return new KeyedJson(extractedKey, rawJson);
    }
}
