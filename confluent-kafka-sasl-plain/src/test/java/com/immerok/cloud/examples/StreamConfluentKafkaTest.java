package com.immerok.cloud.examples;

import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import com.immerok.cloud.examples.utils.CookbookKafkaCluster;
import com.immerok.cookbook.extensions.MiniClusterExtensionFactory;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class StreamConfluentKafkaTest {

    static final String TEST_CONFLUENT_CONFIG = "/confluent-java-test.config";
    static final List<String> EXPECTED_USERS = List.of("Alice", "Bob", "Charly");

    @RegisterExtension
    static final MiniClusterExtension FLINK =
            MiniClusterExtensionFactory.withDefaultConfiguration();

    private PrintStream originalPrintStream;

    @BeforeEach
    public void beforeAll() {
        originalPrintStream = System.out;
    }

    @AfterEach
    public void afterAll() {
        System.setOut(originalPrintStream);
    }

    @Test
    void testStreamJobRoundTrip() throws Exception {
        JobClient jobClient = null;
        try (final CookbookKafkaCluster kafka = new CookbookKafkaCluster()) {
            kafka.createTopic(StreamToConfluentKafka.TOPIC, Stream.empty());

            final ByteArrayOutputStream testOutputStream = new ByteArrayOutputStream();
            System.setOut(new PrintStream(testOutputStream));

            final StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment();

            // fill topic
            StreamToConfluentKafka.defineWorkflow(env, TEST_CONFLUENT_CONFIG);
            env.execute();

            // read topic
            StreamFromConfluentKafka.defineWorkflow(env, TEST_CONFLUENT_CONFIG);
            jobClient = env.executeAsync();

            assertTimeoutPreemptively(
                    Duration.ofSeconds(10),
                    () -> {
                        String output;
                        do {
                            output = testOutputStream.toString();
                        } while (!EXPECTED_USERS.stream().allMatch(output::contains));
                    });
        } finally {
            if (jobClient != null) {
                jobClient.cancel().get();
            }
        }
    }
}
