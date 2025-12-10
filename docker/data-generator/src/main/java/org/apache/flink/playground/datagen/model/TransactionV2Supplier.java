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

package org.apache.flink.playground.datagen.model;

import com.github.javafaker.Faker;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Iterator;
import java.util.Random;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

/** A supplier that generates an arbitrary transaction. */
public class TransactionV2Supplier implements Supplier<TransactionsV2> {

    private final Faker faker = new Faker();
    private final Random generator = new Random();
    private final Status[] types = Status.values();


    private final Iterator<Long> accounts =
      Stream.generate(() -> Stream.of(1L, 2L, 3L, 4L, 5L))
          .flatMap(UnaryOperator.identity())
          .iterator();

  private final Iterator<LocalDateTime> timestamps =
      Stream.iterate(
              LocalDateTime.of(2000, 1, 1, 1, 0),
              time -> time.plusMinutes(5).plusSeconds(generator.nextInt(58) + 1))
          .iterator();

  @Override
  public TransactionsV2 get() {
      TransactionsV2 transaction = new TransactionsV2();
      transaction.setAccountId(generator.nextLong(1000000, 9999999));
      transaction.setAmount(generator.nextInt(1000));
      transaction.setTimestamp(timestamps.next().toInstant(ZoneOffset.UTC));
      transaction.setTransactionId(UUID.randomUUID().toString());
      transaction.setCurrency(faker.currency().code());
      transaction.setDescription(faker.lorem().sentence());
      transaction.setStatus(types[new Random().nextInt(types.length)]);
      transaction.setCoRelationId(UUID.randomUUID().toString());
    return transaction;
  }
}
