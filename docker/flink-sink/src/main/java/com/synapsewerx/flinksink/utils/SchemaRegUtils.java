package com.synapsewerx.flinksink.utils;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class SchemaRegUtils {

    public static final Logger logger = LoggerFactory.getLogger(SchemaRegUtils.class);
    private final HashMap<String, String> config;
    private transient SchemaRegistryClient client;
    private static final int IDENTITY_MAP_CAPACITY = 1000;

    private static final byte CONFLUENT_MAGIC_BYTE = 0x0;
    private static final int ID_SIZE = 4;
    private static final int MESSAGE_HEADER_SIZE = 1 + ID_SIZE; // Total 5 bytes

    public SchemaRegUtils(HashMap<String, String> config) {
        this.config = config;
    }

    /**
     * Initializes the SchemaRegistryClient if it hasn't been already.
     */
    private void ensureClientInitialized() {
        if (client == null) {
            client = new CachedSchemaRegistryClient(
                    config.get(SCHEMA_REGISTRY_URL_CONFIG),
                    IDENTITY_MAP_CAPACITY,
                    config
            );
        }
    }

    /**
     * Fetches the Avro Schema object for a specific unique Schema ID.
     * @param id The unique ID of the schema in the registry.
     * @return The Avro Schema object.
     * @throws IOException If there's an I/O error during communication.
     * @throws RestClientException If the Schema Registry returns an error (e.g., ID not found).
     */
    public Schema getSchemaById(int id) throws IOException, RestClientException {
        ensureClientInitialized();
        // This is the direct method call to retrieve the schema by its ID.
        ParsedSchema parsedSchema = client.getSchemaById(id);
        Schema schema =  parsedSchema instanceof AvroSchema ? ((AvroSchema) parsedSchema).rawSchema() : null;
        if (schema == null) {
            throw new RestClientException("Schema with ID " + id + " not found.", 404, 40401);
        }
        return schema;
    }

    public static Optional<Integer> getSchemaIdFromValue(byte[] value) {
        if (value == null || value.length < MESSAGE_HEADER_SIZE) {
            return Optional.empty();
        }
        if (value[0] != CONFLUENT_MAGIC_BYTE) {
            return Optional.empty();
        }

        try {
            ByteBuffer buffer = ByteBuffer.wrap(value);
            // Move the position past the 1-byte Magic Byte (to offset 1)
            buffer.position(1);
            // Read the 4 bytes at the current position as a big-endian integer
            int schemaId = buffer.getInt();
            // Return the extracted ID
            return Optional.of(schemaId);
        } catch (Exception e) {
            logger.warn("Error reading ByteBuffer to extract Schema ID: {}", e.getMessage());
            return Optional.empty();
        }
    }

    public  Schema getSchemaFromValue( byte[] value) throws RestClientException, IOException {
        Optional<Integer> schemaId = getSchemaIdFromValue(value);
        try {
        if (schemaId.isPresent()) {
            return getSchemaById(schemaId.get());
        } else {
            logger.error("Unable to fetch the schema from Schema registry as the schema Id from value could not be extracted");
            return null;
        }
    } catch (Exception e) {
        logger.warn("Error while getting schema from data: {}", e.getMessage());
         throw e;
        }
    }

    public static Schema copySchemaAndAddField(Schema originalSchema, String fieldName, Schema fieldType, Object defaultValue) {

        if (originalSchema.getType() != Schema.Type.RECORD) {
            throw new IllegalArgumentException("Cannot add field to non-RECORD Avro schema type: " + originalSchema.getType());
        }
        List<Schema.Field> newFields = new java.util.ArrayList<>();
        for (Schema.Field originalField : originalSchema.getFields()) {
            newFields.add(new Schema.Field(originalField.name(), originalField.schema(), originalField.doc(), originalField.defaultVal(), originalField.order()));
        }
        Schema.Field newField;
        if (defaultValue != null) {
            newField = new Schema.Field(fieldName, fieldType, null, defaultValue);
        } else {
            newField = new Schema.Field(fieldName, fieldType, null, (Object)null);
        }
        newFields.add(newField);
        Schema newSchema = Schema.createRecord(
                originalSchema.getName(),
                originalSchema.getDoc(),
                originalSchema.getNamespace(),
                originalSchema.isError()
        );
        newSchema.setFields(newFields);
        return newSchema;
    }
}