package com.immerok.cloud.examples;

import com.immerok.cloud.examples.events.IdentitySerializationSchema;
import com.immerok.cloud.examples.events.KeyedJson;
import com.immerok.cloud.examples.events.KeyedJsonDeserializationSchema;
import com.immerok.cloud.examples.utils.ConfigParser;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * DataStream API example how to read from and write to Apache Kafka® (in Confluent® Cloud) using
 * SASL/PLAIN authentication.
 *
 * <p>Make sure to replace the configuration in {@code resources/confluent-java.config} with your
 * credentials.
 *
 * <p>The example is schema agnostic and deduplication happens on JSON level using the provided key.
 *
 * <p>Make sure to pass the following main() args:
 *
 * <pre>
 *   rok create job ... --main-args="--srcTopic MySourceTopic --destTopic MySinkTopic --key user --ttl 3600000"
 * </pre>
 *
 * <ul>
 *   <li>{@code --srcTopic} Source topic to read from.
 *   <li>{@code --destTopic} Destination topic to write to.
 *   <li>{@code --key} The JSON field on which the deduplication will be performed.
 *   <li>{@code --ttl} The defines how long to keep the state used for deduplication. 1 hour by
 *       default.
 * </ul>
 *
 * <p>This example is also runnable in an IDE. For IntelliJ, make sure to tick {@code Add
 * dependencies with "provided" scope to the classpath} in the run configuration to avoid a
 * "NoClassDefFoundError".
 */
public class ConfluentKafkaDeduplication {

    static final String CONFLUENT_CONFIG = "/confluent-java.config";

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new IllegalArgumentException(
                    "This main() method is not runnable out of the box. It requires passing some parameters.");
        }
        ParameterTool param = ParameterTool.fromArgs(args);

        String srcTopic = param.getRequired("srcTopic");
        String destTopic = param.getRequired("destTopic");
        String key = param.getRequired("key");
        String ttl = param.get("ttl", "3600000");

        runJob(CONFLUENT_CONFIG, srcTopic, destTopic, key, Long.parseLong(ttl));
    }

    static void runJob(String configPath, String srcTopic, String destTopic, String key, long ttl)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        defineWorkflow(env, configPath, srcTopic, destTopic, key, ttl);

        env.execute();
    }

    static void defineWorkflow(
            StreamExecutionEnvironment env,
            String configPath,
            String srcTopic,
            String destTopic,
            String key,
            long ttl) {
        Properties properties = ConfigParser.loadPropertiesResourceFile(configPath);

        KafkaSource<KeyedJson> source =
                KafkaSource.<KeyedJson>builder()
                        .setProperties(properties)
                        .setTopics(srcTopic)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new KeyedJsonDeserializationSchema(key))
                        .build();

        KafkaSink<byte[]> sink =
                KafkaSink.<byte[]>builder()
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic(destTopic)
                                        .setValueSerializationSchema(
                                                IdentitySerializationSchema.INSTANCE)
                                        .build())
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .setKafkaProducerConfig(properties)
                        .build();

        DataStreamSource<KeyedJson> sourceStream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Confluent Kafka");

        sourceStream
                .keyBy(json -> json.key)
                .process(new DeduplicationProcessFunction(ttl))
                .sinkTo(sink);
    }

    /**
     * {@link KeyedProcessFunction} that stores a boolean flag for each key, indicating whether or
     * not that key has been seen before.
     */
    public static class DeduplicationProcessFunction
            extends KeyedProcessFunction<String, KeyedJson, byte[]> {

        private final long ttl;

        private transient ValueState<Boolean> seen;

        public DeduplicationProcessFunction(long ttl) {
            this.ttl = ttl;
        }

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Boolean> seenDescriptor =
                    new ValueStateDescriptor<>("seen", Boolean.class);

            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.milliseconds(ttl)).build();
            seenDescriptor.enableTimeToLive(ttlConfig);

            seen = getRuntimeContext().getState(seenDescriptor);
        }

        @Override
        public void processElement(
                KeyedJson keyedJson,
                KeyedProcessFunction<String, KeyedJson, byte[]>.Context context,
                Collector<byte[]> out)
                throws Exception {
            // Check if this key has been emitted before.
            // It only emits if the state is true (not unset and thus null).
            if (!Boolean.TRUE.equals(seen.value())) {
                out.collect(keyedJson.rawJson);
                seen.update(Boolean.TRUE);
            }
        }
    }
}
