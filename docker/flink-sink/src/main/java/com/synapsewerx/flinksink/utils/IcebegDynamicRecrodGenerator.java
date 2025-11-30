package com.synapsewerx.flinksink.utils;

import com.synapsewerx.flinksink.model.KafkaRecord;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.util.Collector;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecord;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecordGenerator;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.HashMap;

public class IcebegDynamicRecrodGenerator implements DynamicRecordGenerator<DynamicRecord> {

    /**
     * Takes the user-defined input and yields zero, one, or multiple {@link DynamicRecord}s using the
     * {@link Collector}.
     *
     * @param inputRecord
     * @param out
     */
    @Override
    public void generate(DynamicRecord inputRecord, Collector<DynamicRecord> out) throws Exception {
        out.collect(inputRecord);
    }
}
