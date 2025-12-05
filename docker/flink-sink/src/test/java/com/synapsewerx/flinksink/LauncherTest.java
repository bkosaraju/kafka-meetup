package com.synapsewerx.flinksink;

import com.synapsewerx.flinksink.utils.Config;
import com.synapsewerx.flinksink.utils.FlinkUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.dynamic.source.DynamicKafkaSource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class LauncherTest {

    MiniClusterResourceConfiguration configuration = new MiniClusterResourceConfiguration.Builder()
            .setConfiguration(getMiniClusterConfig())
            .setNumberSlotsPerTaskManager(20)
            .setNumberTaskManagers(2)
            .build();
    MiniClusterWithClientResource flinkCluster = new MiniClusterWithClientResource(configuration);

    @BeforeEach
    void setUp() throws Exception {
        flinkCluster.before();
    }

    @AfterEach
    void tearDown() {
        flinkCluster.after();
    }

    Configuration getMiniClusterConfig() {
        Configuration config = new Configuration();
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, Config.CHECKPOINT_STORAGE);
        config.set(CheckpointingOptions.MIN_PAUSE_BETWEEN_CHECKPOINTS, Duration.ofMillis(Config.MIN_PAUSE_CHECKPOINT_INTERVAL));
        config.set(CheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS, Config.MAX_CONCURRENT_CHECKPOINTS);
        config.set(CheckpointingOptions.CHECKPOINTING_TIMEOUT, Duration.ofMillis(Config.CHECKPOINT_TIMEOUT));
        config.set(CheckpointingOptions.TOLERABLE_FAILURE_NUMBER, Config.TOLERABLE_CHECKPOINT_FAILURES);
        config.set(CheckpointingOptions.ENABLE_UNALIGNED, Boolean.TRUE);
        config.set(RestOptions.PORT, 9090);
        return config;
    }

    @Test
    @DisplayName("execute end to end behaviour")
    void executeApp() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(getMiniClusterConfig());
        env.setParallelism(4);
        env.getCheckpointConfig().setCheckpointInterval(10000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableUnalignedCheckpoints();

        //SourceReader sourceReader = new SourceReader(env, FlinkUtils.getResourceConfigurations(null));
        DynamicSourceReader sourceReader = new DynamicSourceReader(env, FlinkUtils.getResourceConfigurations(null));
        sourceReader.getRawStream();
//        env.fromElements(1, 2, 3, 4, 5) // DataStream<Integer>
//
//                // 2. ADD A TRANSFORMATION (Optional, but usually present)
//                .map((MapFunction<Integer, Integer>) value -> value * 2) // DataStream<Integer>
//
//                // 3. ADD A SINK (e.g., printing to standard out)
//                .print();
        env.execute();
        assertEquals(true, true);
    }
}