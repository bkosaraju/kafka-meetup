package com.synapsewerx.flinksink;

import com.synapsewerx.flinksink.model.KafkaRecord;
import com.synapsewerx.flinksink.utils.FlinkUtils;
import com.synapsewerx.flinksink.utils.IcebegDynamicRecrodGenerator;
import com.synapsewerx.flinksink.utils.RecordDeserializer;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.dynamic.DynamicIcebergSink;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecord;
import org.apache.iceberg.flink.source.IcebergSource;
import org.apache.iceberg.flink.source.StreamingStartingStrategy;
import org.apache.iceberg.flink.source.assigner.SimpleSplitAssignerFactory;

import java.time.Duration;
import java.util.*;

public class SourceReader {

    public SourceReader(StreamExecutionEnvironment env, HashMap<String, String> config) {
        this.env = env;
        this.config = config;
    }

    public SourceReader() {
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
        final List<String> TOPIC_LIST = Arrays.stream(config.get("topic.list").split(",")).map(String::trim).toList();
        final CatalogLoader icebergCatalog = FlinkUtils.getIcebergCatalogLoader(config);
        KafkaSource<DynamicRecord> source = KafkaSource.<DynamicRecord>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(TOPIC_LIST)
                .setGroupId(GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.committedOffsets())
                .setProperty("commit.offsets.on.checkpoint", "true")
                .setDeserializer(new RecordDeserializer(config))
                .build();
        // 2. Read the DataStream
        DataStream<DynamicRecord> rawStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Raw Kafka Record Source"
        ).uid(UUID.randomUUID().toString());

        // 3. Process the raw data
//        rawStream.map(record -> {
//            String topic = record.topic;
//            long offset = record.offset;
//            byte[] rawValue = record.value;
//            String value = record.rowData;
//            System.out.printf("Topic: %s, Offset: %d, Raw Value Size: %d bytes\n, data: %s\nSchema: %s\n", topic, offset, rawValue.length, value, record.schema);
//            return record;
//        }).print();

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

//        TableIdentifier tableIdentifier = TableIdentifier.of("test_db_4", "transactions");
//        TableLoader tableLoader = TableLoader.fromCatalog(icebergCatalog, tableIdentifier);
//        Schema icebergSchemaFromTable = FlinkUtils.getIcebergSchemaFromTable(config, tableIdentifier);
////        Schema icebergSchema = tableLoader.loadTable().schema();
//        RowType rowType = FlinkSchemaUtil.convert(icebergSchemaFromTable);
//        IcebergSource<RowData> rowDataIcebergSource = IcebergSource.<RowData>builder()
//                .tableLoader(tableLoader)
//                .assignerFactory(new SimpleSplitAssignerFactory())
//                .branch("main")
//                .streaming(true)
//                //https://github.com/apache/iceberg/issues/14526
//                .streamingStartingStrategy(StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
//                .monitorInterval(Duration.ofSeconds(10))
//                .build();
//            env.fromSource(rowDataIcebergSource, WatermarkStrategy.noWatermarks(), "iceberg reader")
//                    .returns(InternalTypeInfo.of(rowType))
//                    .map( rowData -> {
//                    System.out.println(rowData.getString(2));
//                    return rowData.toString();
//                }).print();
    }
}