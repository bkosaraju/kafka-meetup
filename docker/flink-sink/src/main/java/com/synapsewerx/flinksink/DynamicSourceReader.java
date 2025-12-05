package com.synapsewerx.flinksink;

import com.synapsewerx.flinksink.utils.FlinkUtils;
import com.synapsewerx.flinksink.utils.IcebegDynamicRecrodGenerator;
import com.synapsewerx.flinksink.utils.RecordDeserializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaMetadataService;
import org.apache.flink.connector.kafka.dynamic.metadata.SingleClusterTopicMetadataService;
import org.apache.flink.connector.kafka.dynamic.source.DynamicKafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.sink.dynamic.DynamicIcebergSink;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.util.*;
import java.util.regex.Pattern;

public class DynamicSourceReader {

    public DynamicSourceReader(StreamExecutionEnvironment env, HashMap<String, String> config) {
        this.env = env;
        this.config = config;
    }

    public DynamicSourceReader() {
    }

    public void setEnv(StreamExecutionEnvironment env) {
        this.env = env;
    }

    public void setConfig(HashMap<String, String> config) {
        this.config = config;
    }

    StreamExecutionEnvironment env;
    HashMap<String, String> config;

    public void getRawStream() throws Exception {

    // --- Configuration Parameters ---
        final String BOOTSTRAP_SERVERS = config.get("bootstrap.servers");
        final String SCHEMA_REGISTRY_URL = config.get("schema.registry.url");
        final String GROUP_ID = config.get("group.id");
        final CatalogLoader icebergCatalog = FlinkUtils.getIcebergCatalogLoader(config);
        Properties props = new Properties();
        config.forEach(props::setProperty);

        KafkaMetadataService kafkaMetadataService = new SingleClusterTopicMetadataService(config.get("kafka.cluster.id"), props);
        DynamicKafkaSource<DynamicRecord> dynamicKafkaSource = DynamicKafkaSource.<DynamicRecord>builder()
                .setKafkaMetadataService(kafkaMetadataService)
                .setGroupId(GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setProperty("commit.offsets.on.checkpoint", "true")
                .setProperty("stream-metadata-discovery-interval-ms", "10000")
                .setDeserializer(new RecordDeserializer(config))
                .setStreamPattern(Pattern.compile(".*"))
                .build();
        DataStream<DynamicRecord> rawStream = env.fromSource(
                dynamicKafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Raw Kafka Record Source"
        ).uid(UUID.randomUUID().toString());

        DynamicIcebergSink.forInput(rawStream)
                .generator(new IcebegDynamicRecrodGenerator())
                .catalogLoader(icebergCatalog)
                .set("write.parquet.compression-codec", "gzip")
                .writeParallelism(4)
                .uidPrefix("dynamic-sink")
                .cacheMaxSize(500)
                .cacheRefreshMs(5000)
                .immediateTableUpdate(true)
                .append();

    }
}