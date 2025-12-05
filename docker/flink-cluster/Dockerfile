FROM flink:2.0.1-scala_2.12
RUN wget -P /opt/flink/lib https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.0-2.0/flink-sql-connector-kafka-4.0.0-2.0.jar; \
    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc-core/4.0.0-2.0/flink-connector-jdbc-core-4.0.0-2.0.jar; \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-csv/2.0.1/flink-csv-2.0.1.jar; \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-json/2.0.1/flink-json-2.0.1.jar; \
    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/4.0.0-2.0/flink-connector-kafka-4.0.0-2.0.jar; \
    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/4.0.0/kafka-clients-4.0.0.jar; \
    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-connector-datagen/2.1.1/flink-connector-datagen-2.1.1.jar;\
    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-avro-confluent-registry/2.0.1/flink-avro-confluent-registry-2.0.1.jar; \
    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-avro/2.0.1/flink-avro-2.0.1.jar; \
    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.16.0/jackson-core-2.16.0.jar; \
    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.16.0/jackson-databind-2.16.0.jar; \
    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/com/fasterxml/jackson/datatype/jackson-datatype-jdk8/2.16.0/jackson-datatype-jdk8-2.16.0.jar; \
    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/2.16.0/jackson-annotations-2.16.0.jar; \
    wget -P /opt/flink/lib/ https://packages.confluent.io/maven/io/confluent/kafka-schema-registry-client/8.0.0/kafka-schema-registry-client-8.0.0.jar; \
    wget -P /opt/flink/lib/ https://packages.confluent.io/maven/io/confluent/kafka-avro-serializer/8.0.0/kafka-avro-serializer-8.0.0.jar; \
    wget -P /opt/flink/lib/ https://packages.confluent.io/maven/io/confluent/kafka-schema-serializer/8.0.0/kafka-schema-serializer-8.0.0.jar; \
    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/com/google/guava/guava/33.5.0-jre/guava-33.5.0-jre.jar; \
    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/com/google/guava/failureaccess/1.0.3/failureaccess-1.0.3.jar; \
    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar; \
    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/jline/jline/3.21.0/jline-3.21.0.jar
#    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.10.0/iceberg-aws-bundle-1.10.0.jar;
#    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-core/1.10.0/iceberg-core-1.10.0.jar; \
#    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-api/1.10.0/iceberg-api-1.10.0.jar; \
#    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-2.0/1.10.0/iceberg-flink-runtime-2.0-1.10.0.jar; \
#    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink/1.10.0/iceberg-flink-1.10.0.jar; \
#    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/httpcomponents/core5/httpcore5/5.3.4/httpcore5-5.3.4.jar; \
#    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/httpcomponents/client5/httpclient5/5.5/httpclient5-5.5.jar; \
#    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/httpcomponents/core5/httpcore5-h2/5.3.4/httpcore5-h2-5.3.4.jar; \
#    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/avro/avro/1.12.0/avro-1.12.0.jar; \

COPY deps /deps
RUN  cp /deps/*.jar /opt/flink/lib/
COPY conf /opt/flink/conf
#Remove flink table planner to avoid conflicts
RUN rm /opt/flink/lib/flink-table-planner-*.jar

