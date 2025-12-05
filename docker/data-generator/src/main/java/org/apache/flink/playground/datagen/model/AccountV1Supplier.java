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

import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.Random;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

/** A supplier that generates an arbitrary transaction. */
public class AccountV1Supplier implements Supplier<AccountV1> {

  private final Random generator = new Random();

  private final Iterator<Long> accounts =
      Stream.generate(() -> Stream.of(1L, 2L, 3L, 4L, 5L))
          .flatMap(UnaryOperator.identity())
          .iterator();

  @Override
  public AccountV1 get() {
    AccountV1 accountV1 = new AccountV1();
    accountV1.setAccountId(accounts.next());
    accountV1.setAccountName(UUID.randomUUID().toString());
    return accountV1;
  }
}
