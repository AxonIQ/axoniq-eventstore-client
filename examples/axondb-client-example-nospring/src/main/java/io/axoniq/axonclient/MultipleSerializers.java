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
import io.axoniq.axondb.performancetest.TestAggregate;
import io.axoniq.axondb.performancetest.TestEvent;
import org.axonframework.eventsourcing.GenericDomainEventMessage;
import org.axonframework.messaging.unitofwork.DefaultUnitOfWork;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.json.JacksonSerializer;
import org.axonframework.serialization.upcasting.event.NoOpEventUpcaster;
import org.axonframework.serialization.xml.XStreamSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

/**
 * @author Marc Gathier
 */
public class MultipleSerializers {



    public static void main(String[] args) {
        AxonDBConfiguration axonDBConfiguration = AxonDBConfiguration.newBuilder("localhost")
                                                                                 .build();

        Serializer serializer = new XStreamSerializer();
        Serializer eventSerializer = new JacksonSerializer();
        AxonDBEventStore eventStore = new AxonDBEventStore(axonDBConfiguration, serializer, eventSerializer, NoOpEventUpcaster.INSTANCE);
        String aggregateId = UUID.randomUUID().toString();


        generateMessages(eventStore, aggregateId, 5, 0);
        eventStore.readEvents(aggregateId).asStream().forEach(e -> System.out.println(e));
        eventStore.storeSnapshot(new GenericDomainEventMessage<>(TestAggregate.class.getName(), aggregateId, 4, new TestAggregate(aggregateId, "this was the data after 5 events")));

        generateMessages(eventStore, aggregateId, 5, 5);

        eventStore.readEvents(aggregateId).asStream().forEach(e -> System.out.println(e));

        System.out.println(eventStore.lastSequenceNumberFor(aggregateId));


    }

    private static void generateMessages(AxonDBEventStore eventStore, String aggregateId, int count, int offset) {
        IntStream.range(0, count).forEach(i -> {
            DefaultUnitOfWork uow = DefaultUnitOfWork.startAndGet(null);

            Map<String, Object> metadata = new HashMap<>();
            metadata.put("timestamp", System.currentTimeMillis());
            eventStore.publish(new GenericDomainEventMessage<>(TestEvent.class.getName(), aggregateId, i + offset,
                                                               new TestEvent("1", "one"),
                                                               metadata));
            uow.commit();
        });
    }
}
