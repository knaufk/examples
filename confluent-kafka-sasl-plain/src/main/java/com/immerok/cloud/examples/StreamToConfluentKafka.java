package com.immerok.cloud.examples;

import com.immerok.cloud.examples.events.Message;
import com.immerok.cloud.examples.utils.ConfigParser;
import java.util.Properties;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * DataStream API example how to write to Apache Kafka® (in Confluent® Cloud) using SASL/PLAIN
 * authentication.
 *
 * <p>Make sure to replace the configuration in {@code resources/confluent-java.config} with your
 * credentials.
 *
 * <p>The example assumes JSON values that encode messages such as:
 *
 * <pre>
 * {
 * 	"user": "Bob",
 * 	"message": "Hello World!"
 * }
 * </pre>
 *
 * <p>Hint: Check out the source example for checking if the topic has been filled with data.
 *
 * <p>This example is also runnable in an IDE. For IntelliJ, make sure to tick {@code Add
 * dependencies with "provided" scope to the classpath} in the run configuration to avoid a
 * "NoClassDefFoundError".
 */
public class StreamToConfluentKafka {

    static final String CONFLUENT_CONFIG = "/confluent-java.config";
    static final String TOPIC = "MyTopic";

    public static void main(String[] args) throws Exception {
        runJob(CONFLUENT_CONFIG);
    }

    static void runJob(String configPath) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        defineWorkflow(env, configPath);

        env.execute();

        System.out.println(
                ">>> Execution finished. Check on Confluent Cloud whether the data has been written!");
    }

    static void defineWorkflow(StreamExecutionEnvironment env, String configPath) {
        Properties properties = ConfigParser.loadPropertiesResourceFile(configPath);

        KafkaSink<Message> sink =
                KafkaSink.<Message>builder()
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic(TOPIC)
                                        .setValueSerializationSchema(
                                                new JsonSerializationSchema<Message>())
                                        .build())
                        .setKafkaProducerConfig(properties)
                        .build();

        env.fromElements(
                        new Message("Alice", "Hello World!"),
                        new Message("Bob", "Hallo Welt!"),
                        new Message("Charly", "¡Hola Mundo!"))
                .sinkTo(sink);
    }
}
