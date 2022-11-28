package com.immerok.cloud.examples.events;

import org.apache.flink.api.common.serialization.SerializationSchema;

/** A serializer that just forwards binary data to Kafka. */
public class IdentitySerializationSchema implements SerializationSchema<byte[]> {

    private static final long serialVersionUID = 1L;

    public static final IdentitySerializationSchema INSTANCE = new IdentitySerializationSchema();

    private IdentitySerializationSchema() {
        // enforce singleton usage
    }

    @Override
    public byte[] serialize(byte[] rawJson) {
        return rawJson;
    }
}
