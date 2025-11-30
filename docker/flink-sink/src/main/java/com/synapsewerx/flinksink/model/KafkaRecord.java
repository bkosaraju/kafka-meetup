package com.synapsewerx.flinksink.model;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.table.data.RowData;
import org.apache.kafka.common.header.Headers;

public class KafkaRecord {
    public final String topic;
    public final byte[] key;
    public final byte[] value;
    public final Headers headers;
    public final long timestamp;
    public final int partition;
    public final long offset;
    public final String rowData;
    public final String schema;

    public KafkaRecord(String topic, byte[] key, byte[] value, Headers headers, long timestamp, int partition, long offset, String rowData, String schema) {
        this.topic = topic;
        this.key = key;
        this.value = value;
        this.headers = headers;
        this.timestamp = timestamp;
        this.partition = partition;
        this.offset = offset;
        this.rowData = rowData;
        this.schema = schema;
    }

//    public KafkaRecord(String topic, byte[] key, byte[] value, Headers headers, long timestamp, int partition, long offset, RowData rowData, Schema schema) {
//        this.topic = topic;
//        this.key = key;
//        this.value = value;
//        this.headers = headers;
//        this.timestamp = timestamp;
//        this.partition = partition;
//        this.offset = offset;
//        this.rowData = rowData;
//        this.schema = schema;
//    }
}