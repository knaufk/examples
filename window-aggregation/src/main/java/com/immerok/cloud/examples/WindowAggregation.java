package com.immerok.cloud.examples;

import java.time.Duration;
import java.util.Arrays;
import java.util.Random;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowAggregation {

    public static void main(String[] args) throws Exception {

        ParameterTool parameters = ParameterTool.fromArgs(args);

        int payloadSize = parameters.getInt("payload_size", 10);
        int numKeys = parameters.getInt("num_keys", 10_000);
        int windowLengthSeconds = parameters.getInt("window_length_seconds", 10);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Long> numberStream =
                env.fromSource(
                        new NumberSequenceSource(0, Long.MAX_VALUE),
                        WatermarkStrategy.noWatermarks(),
                        "SequenceSource");

        DataStream<SimpleRecord> recordStream =
                numberStream
                        .flatMap(new SimpleRecordGenerator(payloadSize, numKeys))
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<SimpleRecord>forBoundedOutOfOrderness(
                                                Duration.ofSeconds(windowLengthSeconds))
                                        .withTimestampAssigner(
                                                (event, timestamp) -> event.timestamp));

        recordStream
                .keyBy(record -> record.key)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new CountingAggregateFunction(), new MetadataEnrichingWindowFunction())
                .print();

        env.execute();
    }

    public static class SimpleRecord {

        public final long timestamp;
        public final int key;
        public final byte[] payload;

        public SimpleRecord(long timestamp, int key, byte[] payload) {
            this.timestamp = timestamp;
            this.key = key;
            this.payload = payload;
        }

        @Override
        public String toString() {
            return "SimpleRecord{"
                    + "timestamp="
                    + timestamp
                    + ", key="
                    + key
                    + ", payload="
                    + Arrays.toString(payload)
                    + '}';
        }
    }

    public static class SimpleWindowResults {
        public final long windowTimestamp;
        public final int key;
        public final long value;

        public SimpleWindowResults(long windowTimestamp, int key, long value) {
            this.windowTimestamp = windowTimestamp;
            this.key = key;
            this.value = value;
        }

        @Override
        public String toString() {
            return "SimpleWindowResults{"
                    + "windowTimestamp="
                    + windowTimestamp
                    + ", key="
                    + key
                    + ", value="
                    + value
                    + '}';
        }
    }

    private static class SimpleRecordGenerator extends RichFlatMapFunction<Long, SimpleRecord> {
        private final int payloadSize;
        private final int numKeys;
        private Random rand;

        public SimpleRecordGenerator(int payloadSize, int numKeys) {
            this.payloadSize = payloadSize;
            this.numKeys = numKeys;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            rand = new Random();
        }

        @Override
        public void flatMap(Long in, Collector<SimpleRecord> out) throws Exception {
            byte[] payload = new byte[payloadSize];
            rand.nextBytes(payload);
            out.collect(
                    new SimpleRecord(System.currentTimeMillis(), rand.nextInt(numKeys), payload));
            Thread.sleep(1);
        }
    }

    private static class MetadataEnrichingWindowFunction
            implements WindowFunction<Long, SimpleWindowResults, Integer, TimeWindow> {

        @Override
        public void apply(
                Integer key,
                TimeWindow window,
                Iterable<Long> input,
                Collector<SimpleWindowResults> out)
                throws Exception {
            out.collect(
                    new SimpleWindowResults(window.maxTimestamp(), key, input.iterator().next()));
        }
    }

    private static class CountingAggregateFunction
            implements AggregateFunction<SimpleRecord, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(SimpleRecord in, Long acc) {
            return acc + +1;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long acc2, Long acc1) {
            return acc2 + acc1;
        }
    }
}
