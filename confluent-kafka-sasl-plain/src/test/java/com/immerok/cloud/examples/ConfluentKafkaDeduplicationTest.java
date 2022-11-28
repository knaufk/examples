package com.immerok.cloud.examples;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import com.immerok.cloud.examples.events.Message;
import com.immerok.cloud.examples.utils.CookbookKafkaCluster;
import com.immerok.cookbook.extensions.MiniClusterExtensionFactory;
import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class ConfluentKafkaDeduplicationTest {

    static final String TEST_CONFLUENT_CONFIG = "/confluent-java-test.config";
    static final String IN_TOPIC = "MyInTopic";
    static final String OUT_TOPIC = "MyOutTopic";

    @RegisterExtension
    static final MiniClusterExtension FLINK =
            MiniClusterExtensionFactory.withDefaultConfiguration();

    @Test
    void testDeduplicationJob() throws Exception {
        JobClient jobClient = null;
        try (final CookbookKafkaCluster kafka = new CookbookKafkaCluster()) {
            kafka.createTopic(
                    IN_TOPIC,
                    Stream.of(
                            new Message("Bob", "Hello World!"),
                            new Message("Bob", "Hello World!"),
                            new Message("Alice", "¡Hola Mundo!")));
            kafka.createTopic(OUT_TOPIC, Stream.empty());

            final StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment();

            ConfluentKafkaDeduplication.defineWorkflow(
                    env, TEST_CONFLUENT_CONFIG, IN_TOPIC, OUT_TOPIC, "user", 3600000L);
            jobClient = env.executeAsync();

            assertTimeoutPreemptively(
                    Duration.ofSeconds(10),
                    () -> {
                        List<String> output;
                        do {
                            output = kafka.getTopicRecords(OUT_TOPIC, 2);
                        } while (output.size() < 2);

                        assertThat(output)
                                .containsExactly(
                                        "{\"user\":\"Bob\",\"message\":\"Hello World!\"}",
                                        "{\"user\":\"Alice\",\"message\":\"¡Hola Mundo!\"}");
                    });
        } finally {
            if (jobClient != null) {
                jobClient.cancel().get();
            }
        }
    }
}
