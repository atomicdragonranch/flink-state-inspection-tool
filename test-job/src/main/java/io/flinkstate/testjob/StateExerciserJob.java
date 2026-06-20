package io.flinkstate.testjob;

import io.flinkstate.testjob.generators.ConfigGeneratorFunction;
import io.flinkstate.testjob.generators.TestRecordGeneratorFunction;
import io.flinkstate.testjob.operators.BroadcastConfigProcessor;
import io.flinkstate.testjob.operators.KeyedStateExerciser;
import io.flinkstate.testjob.operators.PartitionedListFunction;
import io.flinkstate.testjob.operators.UnionStateFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class StateExerciserJob {

    private static final Logger LOG = LoggerFactory.getLogger(StateExerciserJob.class);

    public static void main(String[] args) throws Exception {
        Properties config = loadConfig();

        long checkpointInterval = Long.parseLong(
            resolve(config, "flink.checkpoint.interval.ms", "FLINK_CHECKPOINT_INTERVAL_MS"));
        int checkpointRetained = Integer.parseInt(
            resolve(config, "flink.checkpoint.retained", "FLINK_CHECKPOINT_RETAINED"));
        int recordsPerSecond = Integer.parseInt(
            resolve(config, "generator.records.per.second", "GENERATOR_RECORDS_PER_SECOND"));
        int keyCount = Integer.parseInt(
            resolve(config, "generator.key.count", "GENERATOR_KEY_COUNT"));
        String[] categories = resolve(config, "generator.categories", "GENERATOR_CATEGORIES")
            .split(",");
        double emaAlpha = Double.parseDouble(
            resolve(config, "keyed.ema.alpha", "KEYED_EMA_ALPHA"));
        int recentValuesMax = Integer.parseInt(
            resolve(config, "keyed.recent.values.max.size", "KEYED_RECENT_VALUES_MAX_SIZE"));
        int partitionedBufferMax = Integer.parseInt(
            resolve(config, "partitioned.buffer.max.size", "PARTITIONED_BUFFER_MAX_SIZE"));
        double thresholdInitial = Double.parseDouble(
            resolve(config, "broadcast.threshold.initial", "BROADCAST_THRESHOLD_INITIAL"));
        double thresholdIncrement = Double.parseDouble(
            resolve(config, "broadcast.threshold.increment", "BROADCAST_THRESHOLD_INCREMENT"));
        int configIntervalSeconds = Integer.parseInt(
            resolve(config, "broadcast.config.interval.seconds",
                "BROADCAST_CONFIG_INTERVAL_SECONDS"));

        LOG.info("StateExerciserJob: checkpoint={}ms, retained={}, rate={}/s, keys={}",
            checkpointInterval, checkpointRetained, recordsPerSecond, keyCount);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(checkpointInterval);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        DataGeneratorSource<TestRecord> testSource = new DataGeneratorSource<>(
            new TestRecordGeneratorFunction(keyCount, categories),
            Long.MAX_VALUE,
            RateLimiterStrategy.perSecond(recordsPerSecond),
            Types.POJO(TestRecord.class));

        DataStream<TestRecord> records = env
            .fromSource(testSource, WatermarkStrategy.noWatermarks(), "test-source")
            .uid("test-source");

        // Branch 1: keyed state exerciser (VALUE x3, LIST, MAP)
        records
            .keyBy(r -> r.key)
            .process(new KeyedStateExerciser(emaAlpha, recentValuesMax))
            .uid("keyed-exerciser")
            .name("keyed-exerciser")
            .sinkTo(new DiscardingSink<>())
            .uid("keyed-sink")
            .name("keyed-sink");

        // Branch 2: partitioned list operator state (SPLIT_DISTRIBUTE)
        records
            .process(new PartitionedListFunction(partitionedBufferMax))
            .uid("partitioned-list")
            .name("partitioned-list")
            .sinkTo(new DiscardingSink<>())
            .uid("partitioned-sink")
            .name("partitioned-sink");

        // Branch 3: union operator state
        records
            .process(new UnionStateFunction())
            .uid("union-state")
            .name("union-state")
            .sinkTo(new DiscardingSink<>())
            .uid("union-sink")
            .name("union-sink");

        // Branch 4: broadcast state
        int configRate = Math.max(1, 3 / configIntervalSeconds);
        DataGeneratorSource<String> configSource = new DataGeneratorSource<>(
            new ConfigGeneratorFunction(thresholdInitial, thresholdIncrement),
            Long.MAX_VALUE,
            RateLimiterStrategy.perSecond(configRate),
            Types.STRING);

        MapStateDescriptor<String, String> configDescriptor = new MapStateDescriptor<>(
            "config-state", Types.STRING, Types.STRING);

        SingleOutputStreamOperator<String> configStream = env
            .fromSource(configSource, WatermarkStrategy.noWatermarks(), "config-source")
            .uid("config-source");

        BroadcastStream<String> broadcastConfig = configStream.broadcast(configDescriptor);

        records
            .keyBy(r -> r.key)
            .connect(broadcastConfig)
            .process(new BroadcastConfigProcessor(configDescriptor))
            .uid("broadcast-proc")
            .name("broadcast-proc")
            .sinkTo(new DiscardingSink<>())
            .uid("broadcast-sink")
            .name("broadcast-sink");

        env.execute("State Exerciser Job");
    }

    private static Properties loadConfig() {
        Properties props = new Properties();
        try (InputStream in = StateExerciserJob.class
                .getClassLoader().getResourceAsStream("application.properties")) {
            if (in != null) {
                props.load(in);
            }
        } catch (IOException e) {
            LOG.warn("Could not load application.properties, using defaults");
        }
        return props;
    }

    private static String resolve(Properties config, String propKey, String envKey) {
        String envVal = System.getenv(envKey);
        if (envVal != null && !envVal.isEmpty()) return envVal;
        return config.getProperty(propKey, "");
    }
}
