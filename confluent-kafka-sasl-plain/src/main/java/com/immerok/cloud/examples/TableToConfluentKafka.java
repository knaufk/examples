package com.immerok.cloud.examples;

import com.immerok.cloud.examples.utils.ConfigParser;
import java.util.Properties;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;

/**
 * Table API example how to write to Apache Kafka® (in Confluent® Cloud) using SASL/PLAIN
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
public class TableToConfluentKafka {

    static final String CONFLUENT_CONFIG = "/confluent-java.config";
    static final String TOPIC = "MyTopic";

    public static void main(String[] args) throws Exception {
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        runJob(tableEnv, CONFLUENT_CONFIG);
    }

    static void runJob(TableEnvironment tableEnv, String configPath) throws Exception {
        TableDescriptor topicTableDescriptor = createConfluentKafkaTableDescriptor(configPath);
        tableEnv.createTemporaryTable("MyTableFromTopic", topicTableDescriptor);

        tableEnv.fromValues(
                        Row.of("Alice", "Hello World!"),
                        Row.of("Bob", "Hallo Welt!"),
                        Row.of("Charly", "¡Hola Mundo!"))
                .insertInto("MyTableFromTopic")
                .execute()
                .await();

        System.out.println(
                ">>> Execution finished. Check out Confluent Cloud whether the data has been written!");
    }

    static TableDescriptor createConfluentKafkaTableDescriptor(String configPath) {
        Properties properties = ConfigParser.loadPropertiesResourceFile(configPath);

        TableDescriptor.Builder descriptorBuilder =
                TableDescriptor.forConnector("kafka")
                        .schema(
                                Schema.newBuilder()
                                        .column("user", DataTypes.STRING())
                                        .column("message", DataTypes.STRING())
                                        .build())
                        .option("value.format", "json")
                        .option("topic", TOPIC)
                        .option("scan.startup.mode", "earliest-offset");

        properties.forEach(
                (key, value) -> descriptorBuilder.option("properties." + key, (String) value));

        TableDescriptor descriptor = descriptorBuilder.build();

        return descriptor;
    }
}
