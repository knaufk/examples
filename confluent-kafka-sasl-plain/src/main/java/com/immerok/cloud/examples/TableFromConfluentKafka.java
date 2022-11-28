package com.immerok.cloud.examples;

import com.immerok.cloud.examples.utils.ConfigParser;
import java.util.Properties;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

/**
 * Table API example how to read from Apache Kafka® (in Confluent® Cloud) using SASL/PLAIN
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
public class TableFromConfluentKafka {

    static final String CONFLUENT_CONFIG = "/confluent-java.config";
    static final String TOPIC = "MyTopic";

    public static void main(String[] args) {
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        runJob(tableEnv, CONFLUENT_CONFIG);
    }

    static TableResult runJob(TableEnvironment tableEnv, String configPath) {
        TableDescriptor topicTableDescriptor = createConfluentKafkaTableDescriptor(configPath);
        tableEnv.createTemporaryTable("MyTableFromTopic", topicTableDescriptor);

        TableDescriptor printTableDescriptor = createPrintTableDescriptor();

        TableResult result =
                tableEnv.sqlQuery("SELECT * FROM MyTableFromTopic")
                        .insertInto(printTableDescriptor)
                        .execute();

        return result;
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

    static TableDescriptor createPrintTableDescriptor() {
        // We don't specify a schema here as Table API can figure out the schema automatically.
        return TableDescriptor.forConnector("print").build();
    }
}
