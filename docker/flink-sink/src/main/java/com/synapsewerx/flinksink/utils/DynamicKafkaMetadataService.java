package com.synapsewerx.flinksink.utils;

import org.apache.flink.connector.kafka.dynamic.metadata.KafkaMetadataService;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaStream;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class DynamicKafkaMetadataService implements KafkaMetadataService {
    @Override
    public Set<KafkaStream> getAllStreams() {
        return Set.of();
    }

    @Override
    public Map<String, KafkaStream> describeStreams(Collection<String> streamIds) {
        return Map.of();
    }

    @Override
    public boolean isClusterActive(String kafkaClusterId) {
        return false;
    }

    @Override
    public void close() throws Exception {

    }
}
