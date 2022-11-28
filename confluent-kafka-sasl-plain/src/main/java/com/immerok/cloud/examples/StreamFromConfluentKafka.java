package com.immerok.cloud.examples;

import com.immerok.cloud.examples.events.Message;
import com.immerok.cloud.examples.utils.ConfigParser;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * DataStream API example how to read from Apache Kafka® (in Confluent® Cloud) using SASL/PLAIN
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
 * <p>Hint: Check out the sink example for filling the topic in a first step.
 *
 * <p>If you are interested in printing more information to the console during development, change
 * the first line of {@code log4j2.properties} to {@code rootLogger.level = INFO}.
 *
 * <p>This example is also runnable in an IDE. For IntelliJ, make sure to tick {@code Add
 * dependencies with "provided" scope to the classpath} in the run configuration to avoid a
 * "NoClassDefFoundError".
 */
public class StreamFromConfluentKafka {

    static final String CONFLUENT_CONFIG = "/confluent-java.config";
    static final String TOPIC = "MyTopic";

    public static void main(String[] args) throws Exception {
        runJob(CONFLUENT_CONFIG);
    }

    static void runJob(String configPath) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        defineWorkflow(env, configPath);

        env.execute();
    }

    static void defineWorkflow(StreamExecutionEnvironment env, String configPath) {
        Properties properties = ConfigParser.loadPropertiesResourceFile(configPath);

        KafkaSource<Message> source =
                KafkaSource.<Message>builder()
                        .setProperties(properties)
                        .setTopics(TOPIC)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new JsonDeserializationSchema<>(Message.class))
                        .build();

        DataStreamSource<Message> kafka =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Confluent Kafka");

        // additional workflow steps go here

        kafka.print();
    }
}
