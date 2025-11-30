package com.synapsewerx.flinksink.utils;

import com.synapsewerx.flinksink.SourceReader;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.flink.TableLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;



import static com.synapsewerx.flinksink.utils.Config.DEFAULT_CHECKPOINT_INTERVAL;

public class FlinkUtils {
private static final Logger logger = LoggerFactory.getLogger(FlinkUtils.class);
    public static StreamExecutionEnvironment createStreamExecutionEnvironment() {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.enableCheckpointing(DEFAULT_CHECKPOINT_INTERVAL, CheckpointingMode.EXACTLY_ONCE);
        streamExecutionEnvironment.enableChangelogStateBackend(true);
        streamExecutionEnvironment.configure(getConfiguration());
        return streamExecutionEnvironment;
    }

    private static Configuration getConfiguration() {
        Configuration config = new Configuration();
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, Config.CHECKPOINT_STORAGE);
        config.set(CheckpointingOptions.MIN_PAUSE_BETWEEN_CHECKPOINTS, Duration.ofMillis(Config.MIN_PAUSE_CHECKPOINT_INTERVAL));
        config.set(CheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS, Config.MAX_CONCURRENT_CHECKPOINTS);
        config.set(CheckpointingOptions.CHECKPOINTING_TIMEOUT, Duration.ofMillis(Config.CHECKPOINT_TIMEOUT));
        config.set(CheckpointingOptions.TOLERABLE_FAILURE_NUMBER, Config.TOLERABLE_CHECKPOINT_FAILURES);
        config.set(CheckpointingOptions.ENABLE_UNALIGNED, Boolean.TRUE);
        return config;
    }

    public static HashMap<String, String> getResourceConfigurations(String inputConfig) throws IOException {
        Properties properties = new Properties();
        if (inputConfig != null) {
            try (InputStream inputStream = new FileInputStream(inputConfig)) {
                properties.load(inputStream);
            }
        } else {
            InputStream resourceAsStream = FlinkUtils.class.getClassLoader().getResourceAsStream(Config.APP_PROPERTIES);
            properties.load(resourceAsStream);
        }
        HashMap<String, String> propsHmap = new HashMap<>((Map) properties);
        return propsHmap;
    }

    public static CatalogLoader getIcebergCatalogLoader(HashMap<String, String> config) {
        Map<String, String> catalogProperties = new HashMap<>();
        //catalogProperties.putAll(config);
        catalogProperties.put("uri", config.get("iceberg.catalog.uri"));
        catalogProperties.put("warehouse", config.get("iceberg.catalog.warehouse"));

        catalogProperties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        catalogProperties.put(S3FileIOProperties.ACCESS_KEY_ID, config.get("iceberg.catalog.access.key"));
        catalogProperties.put(S3FileIOProperties.SECRET_ACCESS_KEY, config.get("iceberg.catalog.secret.key"));
        catalogProperties.put(S3FileIOProperties.ENDPOINT, config.get("iceberg.catalog.s3.endpoint"));
        catalogProperties.put("client.region",  config.get("iceberg.catalog.client.region"));
        catalogProperties.put(S3FileIOProperties.PATH_STYLE_ACCESS, "true");
        catalogProperties.put("ref", "main");
        catalogProperties.put("property-version", "1");
        org.apache.hadoop.conf.Configuration hadoopConfiguration = new org.apache.hadoop.conf.Configuration();
        config.forEach((k,v) -> {
            if(k.startsWith("fs.s3")) {
                hadoopConfiguration.set(k, v);
            }
        });
       // hadoopConfiguration.forEach(System.out::println);

        CatalogLoader restCatalogLoader = CatalogLoader.rest(
                "iceberg_catalog",
                hadoopConfiguration,
                catalogProperties
        );
        return restCatalogLoader;
    }
    //New code

    public static PartitionSpec generateTablePartitions(Schema schema) {
        return PartitionSpec
                .builderFor(schema)
                .build();
    }
    public static Table createTableIfNotExists(TableIdentifier tableId, Schema schema, HashMap<String, String> config) {
        Catalog catalog = getIcebergCatalogLoader(config).loadCatalog();
        String database = tableId.namespace().toString();
        String tableName = tableId.name();
        if (catalog instanceof SupportsNamespaces) {
        SupportsNamespaces nsCatalog = (SupportsNamespaces) getIcebergCatalogLoader(config).loadCatalog();
        ensureNamespaceExists(nsCatalog, database);
        }
        TableIdentifier tableIdentifier = TableIdentifier.of(database, tableName);
        try {
            // Attempt to load the table first. If successful, return it.
            return catalog.loadTable(tableIdentifier);
        } catch (NoSuchTableException e) {
            logger.warn("Table " + tableIdentifier + " does not exist. Creating now...");
            HashMap<String, String> tableProperties = new HashMap<>();
            tableProperties.put("write.format.default", "parquet");
            tableProperties.put("commit.num-retries", "3");
            // Use the provided schema and the default partition spec/properties
            Table newTable = catalog.createTable(
                    tableIdentifier,
                    schema,
                    generateTablePartitions(schema),
                    tableProperties
            );

            System.out.println("Successfully created table: " + tableIdentifier);
            return newTable;
        } catch (Exception ex) {
            System.err.println("Failed to interact with Iceberg Catalog during table creation: " + ex.getMessage());
            throw new RuntimeException("Catalog operation failed.", ex);
        }
    }

    /**
     * Checks if the namespace exists and creates it if it does not.
     */
    private static void ensureNamespaceExists(SupportsNamespaces catalog, String namespace) {

        List<String> namespaceList = catalog.listNamespaces().stream()
                .map(Namespace::toString)
                .toList();

        if (!namespaceList.contains(namespace)) {
            System.out.println("Namespace '" + namespace + "' not found. Creating...");
            try {
                catalog.createNamespace(org.apache.iceberg.catalog.Namespace.of(namespace));
                System.out.println("Successfully created namespace: " + namespace);
            } catch (Exception e) {
                // Check if creation failed due to concurrency (another job created it simultaneously)
                if (!catalog.namespaceExists(org.apache.iceberg.catalog.Namespace.of(namespace))) {
                    throw new RuntimeException("Failed to create namespace: " + namespace, e);
                }
            }
        }
    }

   public static Schema getIcebergSchemaFromTable(HashMap<String, String> config, TableIdentifier tableIdentifier) {
        return getIcebergCatalogLoader(config)
                .loadCatalog()
                .loadTable(tableIdentifier)
                .schema();
    }
}
