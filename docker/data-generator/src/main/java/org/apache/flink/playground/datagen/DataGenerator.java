/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.playground.datagen;

import java.util.Optional;

import org.apache.flink.playground.datagen.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A basic data generator for continuously writing data into a Kafka topic.
 */
public class DataGenerator {


    private static final Logger LOG = LoggerFactory.getLogger(DataGenerator.class);

    private static final String KAFKA =
            Optional.ofNullable(System.getenv("DATAGEN_KAFKA")).orElse("localhost:9094");

    private static final String TOPIC =
            Optional.ofNullable(System.getenv("DATAGEN_TOPIC")).orElse("accounts");

    public static void main(String[] args) {
        final AutoCloseable generatedProducer;

        if (args.length == 0) {
            LOG.info("Using default topic {}", TOPIC);
            generatedProducer = new Producer(KAFKA, TOPIC);
        } else {
            LOG.info("producing to target - {}", args[0]);
            switch (String.valueOf(args[0])) {
                case "accountsv1" -> generatedProducer = new ProducerV2<>(KAFKA, "accounts", AccountV1Supplier.class);
                case "transactionsv1" ->
                        generatedProducer = new ProducerV2<>(KAFKA, "transactions", TransactionV1Supplier.class);
                case "transactionsv2" ->
                        generatedProducer = new ProducerV2<>(KAFKA, "transactions", TransactionV2Supplier.class);
                default -> {
                    throw new RuntimeException("Unknown target " + args[0]);
                }
            }
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutting down");
            try {
                if (generatedProducer != null) generatedProducer.close();
            } catch (Exception e) {
                LOG.error("Error closing producer", e);
            }
        }));

        if (generatedProducer instanceof Runnable runnableProducer) {
            runnableProducer.run();
        }
    }
}
