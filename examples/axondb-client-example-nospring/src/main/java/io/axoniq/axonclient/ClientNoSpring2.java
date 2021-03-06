/*
 * Copyright (c) 2017. AxonIQ
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.axoniq.axonclient;

import io.axoniq.axondb.client.AxonDBConfiguration;
import io.axoniq.axondb.client.axon.AxonDBEventStore;
import io.axoniq.axondb.performancetest.TestEvent;
import org.axonframework.eventsourcing.GenericDomainEventMessage;
import org.axonframework.messaging.unitofwork.DefaultUnitOfWork;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.json.JacksonSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 * Author: marc
 */
public class ClientNoSpring2 {


    private static final int NR_AGGREGATES = 1000;
    private static final int NR_EVENTS = 100000;
    private static Map<String, AtomicInteger> sequenceNumbers = new HashMap<>();
    private static String[] aggregateIds = new String[NR_AGGREGATES];

    public static void main(String[] args) {
        AxonDBConfiguration axonDBConfiguration = AxonDBConfiguration.newBuilder("eventstore.axoniq.io:8123")
                                                                                 //.flowControl(10, 1, 0)
                                                                                 .token("4e173955-f887-465a-a7d8-2fdbca7e4e37")
                                                                                 .connectionRetry(3000, 30)
                                                                                 .ssl("resources/axoniq-public.crt")
                                                                                 .build();

        Serializer serializer = new JacksonSerializer();
        AxonDBEventStore eventStore = new AxonDBEventStore(axonDBConfiguration, serializer);

        IntStream.range(0, NR_AGGREGATES).forEach(i -> {
            aggregateIds[i] = UUID.randomUUID().toString();
            sequenceNumbers.put(aggregateIds[i], new AtomicInteger(0));
        });

        long start = System.currentTimeMillis();
        IntStream.range(0, NR_EVENTS).parallel().forEach(i -> generateMessages(eventStore));
        long end = System.currentTimeMillis();
        sequenceNumbers.entrySet().stream().filter(e -> e.getValue().get() > 0).forEach(e -> System.out.println(e.getKey() + '=' + e.getValue().get()));

        System.out.println("Submitted " + NR_EVENTS + " events in " + (end - start) + "ms.");
    }

    private static void generateMessages(AxonDBEventStore eventStore) {
        int i = ThreadLocalRandom.current().nextInt(0, NR_AGGREGATES);
        String aggId = aggregateIds[i];
        AtomicInteger seqHolder = sequenceNumbers.get(aggId);
        seqHolder.getAndAccumulate(1, (old, acc) -> {
            DefaultUnitOfWork uow = DefaultUnitOfWork.startAndGet(null);
            eventStore.publish(new GenericDomainEventMessage<>(TestEvent.class.getName(), aggId, old, new TestEvent("1", "one")));
            uow.commit();
            return old + acc;
        });
    }
}
