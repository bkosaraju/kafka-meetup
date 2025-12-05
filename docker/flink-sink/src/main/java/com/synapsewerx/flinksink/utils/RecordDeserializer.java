package com.synapsewerx.flinksink.utils;

import com.synapsewerx.flinksink.model.KafkaRecord;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.sink.AvroGenericRecordToRowDataMapper;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.iceberg.avro.AvroSchemaUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;

public class RecordDeserializer implements KafkaRecordDeserializationSchema<DynamicRecord> {

    private transient Deserializer avroDeserializer;
    SchemaRegUtils  schemaRegUtils;
    private transient AvroGenericRecordToRowDataMapper mapper;
    HashMap<String, String> config;

    public RecordDeserializer(HashMap<String, String> config) {
        this.config = config;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        KafkaAvroDeserializer confluentDeserializer = new KafkaAvroDeserializer();
        confluentDeserializer.configure(config, false);
        this.avroDeserializer =  confluentDeserializer;
        this.schemaRegUtils = new SchemaRegUtils(config);
        KafkaRecordDeserializationSchema.super.open(context);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<DynamicRecord> collector) throws IOException {
        KafkaRecord kafkaRecord = null;
        Schema baseSchema = null;
        boolean isAvroMessage = false;
        Optional<Integer> schemaId = SchemaRegUtils.getSchemaIdFromValue(consumerRecord.value());
        if (schemaId.isPresent()) {
            try {
                isAvroMessage = schemaRegUtils.getSchemaTypeById(schemaId.get()).equals("AVRO");
            } catch (RestClientException e) {
                throw new RuntimeException(e);
            }
        }
        if (isAvroMessage) {
            try {
                baseSchema = schemaRegUtils.getSchemaFromValue(consumerRecord.value());
            } catch (RestClientException e) {
                throw new RuntimeException(e);
            }
            GenericRecord deserializedValue = (GenericRecord) avroDeserializer.deserialize(
                    consumerRecord.topic(),
                    consumerRecord.value()
            );
            //Add Kafka timestamp for partitions
            Schema schema = baseSchema.getField("kafka_partition") == null
                    ? SchemaRegUtils.copySchemaAndAddField(
                    baseSchema,
                    "kafka_partition",
                    Schema.create(Schema.Type.INT),
                    null
            ) : baseSchema;

            GenericRecord injectedGenericRecord = new GenericData.Record(schema);
            for (Schema.Field field : baseSchema.getFields()) {
                injectedGenericRecord.put(field.name(), deserializedValue.get(field.name()));
            }
            injectedGenericRecord.put("kafka_partition", consumerRecord.partition());

            AvroGenericRecordToRowDataMapper mapper = AvroGenericRecordToRowDataMapper.forAvroSchema(schema);
            RowData rowData = null;

            try {
                rowData = mapper.map(injectedGenericRecord);
                System.out.println("schema = " + schema.toString());
                System.out.println("consumerRecord = " + deserializedValue.toString());
                System.out.println("row Data" + rowData);
                System.out.println("offset = " + consumerRecord.offset());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            TableIdentifier tableIdentifier = TableIdentifier.of(config.get("iceberg.database"), consumerRecord.topic());
            org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(schema);
            PartitionSpec partitionSpec = PartitionSpec.unpartitioned();
            FlinkUtils.createTableIfNotExists(tableIdentifier, icebergSchema, config);

            DynamicRecord iceBergRecord = new DynamicRecord(
                    tableIdentifier,
                    "main",
                    icebergSchema,
                    rowData,
                    partitionSpec,
                    DistributionMode.NONE,
                    1);
            collector.collect(iceBergRecord);
        }
    }

    @Override
    public TypeInformation<DynamicRecord> getProducedType() {
        return TypeInformation.of(DynamicRecord.class);    }
}
