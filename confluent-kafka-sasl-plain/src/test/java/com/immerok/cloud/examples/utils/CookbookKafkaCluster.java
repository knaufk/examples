package com.immerok.cloud.examples.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.stream.Stream;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig;
import net.mguenther.kafka.junit.ReadKeyValues;
import net.mguenther.kafka.junit.SendValues;
import net.mguenther.kafka.junit.TopicConfig;

/** A slim wrapper around <a href="https://mguenther.github.io/kafka-junit/">kafka-junit</a>. */
public class CookbookKafkaCluster extends EmbeddedKafkaCluster {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public CookbookKafkaCluster() {
        super(EmbeddedKafkaClusterConfig.defaultClusterConfig());

        this.start();
    }

    /**
     * Creates a topic with the given name and synchronously writes all data from the given stream
     * to that topic.
     *
     * @param topic topic to create
     * @param topicData data to write
     * @param <EVENT> event type
     */
    public <EVENT> void createTopic(String topic, Stream<EVENT> topicData) {
        createTopic(TopicConfig.withName(topic));
        topicData.forEach(record -> sendEventAsJSON(topic, record));
    }

    public List<String> getTopicRecords(String topic, int numRecords) {
        return getTopicRecords(topic, 0, numRecords);
    }

    public List<String> getTopicRecords(String topic, int offset, int numRecords) {
        final ReadKeyValues.ReadKeyValuesBuilder<String, String> from =
                ReadKeyValues.from(topic).seekTo(0, offset).withLimit(numRecords);

        try {
            return readValues(from);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    /**
     * Sends one JSON-encoded event to the topic and sleeps for 100ms.
     *
     * @param event An event to send to the topic.
     */
    private <EVENT> void sendEventAsJSON(String topic, EVENT event) {
        try {
            final SendValues<String> sendRequest =
                    SendValues.to(topic, OBJECT_MAPPER.writeValueAsString(event)).build();
            this.send(sendRequest);
            Thread.sleep(100);
        } catch (InterruptedException | JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
