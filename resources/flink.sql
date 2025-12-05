CREATE TABLE `default_catalog`.`default_database`.transactions (
                              `accountId` bigint,
                              amount integer,
                              `timestamp` STRING
) WITH (
      'connector' = 'kafka',
      'topic' = 'transactions',
      'properties.bootstrap.servers' = 'kafka:9092',
      'value.format' = 'avro-confluent',
      'value.avro-confluent.url' = 'http://schema-registry:8081',
      'scan.startup.mode' = 'latest-offset',
      'properties.group.id' = 'cflt-meetup-1'
      );

--select * from `default_catalog`.`default_database`.transactions;

CREATE CATALOG iceberg_catalog WITH (
  'type'='iceberg',
  'catalog-type'='rest',
  'uri'='http://iceberg-rest:8181/',
  's3.endpoint' = 'http://minio:9000',
  's3.path-style-access' = 'true',
  's3.access-key-id' = 'admin',
  's3.secret-access-key' = 'password'
);
create database if not exists iceberg_catalog.test_db;

CREATE TABLE `iceberg_catalog`.`test_db`.`kafka_transactions` (
   `accountId` bigint,
   amount integer,
   `timestamp` STRING
) ;

/*+ OPTIONS('upsert-enabled'='true') */

insert into `iceberg_catalog`.`test_db`.`kafka_transactions`
select `accountId`, `amount`, `timestamp` from `default_catalog`.`default_database`.transactions;


select * from iceberg_catalog.test_db.kafka_transactions /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/ ;


CREATE CATALOG iceberg_catalog WITH (
  'type'='iceberg',
  'catalog-type'='rest',
  'uri'='http://iceberg-rest:8181/',
  's3.endpoint' = 'http://minio:9000',
  's3.region' = 'us-east-1',
  's3.path-style-access' = 'true',
  's3.access-key-id' = 'admin',
  's3.secret-access-key' = 'password',
    'version'= 'main',
    'write.parquet.compression-codec'='zstd'
);
ALTER TABLE iceberg_catalog_2.test_db.transactions SET ('property-version' = '1');
ALTER TABLE iceberg_catalog_2.test_db.transactions SET ('write.parquet.compression-codec'= 'none');

ALTER TABLE `iceberg_catalog`.`test_db_4`.`transactions` RESET ('schema.name');

select * from iceberg_catalog.test_db_4.transactions /*+ OPTIONS('streaming'='false', 'monitor-interval'='1s')*/ ;

select * from iceberg_catalog.test_db.transactions;

        select count(*) from `iceberg_catalog`.`test_db_4`.`transactions` /*+ OPTIONS('streaming'='true', 'starting-strategy'='TABLE_SCAN_THEN_INCREMENTAL', 'monitor-interval'='1s')*/ ;


select * from iceberg_catalog.meetup.transactions  /*+ OPTIONS('streaming'='false','starting-strategy'='TABLE_SCAN_THEN_INCREMENTAL', 'monitor-interval'='5s')*/ where `transactionId` is not null ;