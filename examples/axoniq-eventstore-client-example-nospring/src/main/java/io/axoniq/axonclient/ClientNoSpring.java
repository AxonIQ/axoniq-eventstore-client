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

import io.axoniq.eventstore.client.EventStoreConfiguration;
import io.axoniq.eventstore.client.axon.AxonIQEventStore;
import org.axonframework.eventhandling.TrackedEventMessage;
import org.axonframework.eventsourcing.GenericTrackedDomainEventMessage;
import org.axonframework.eventsourcing.eventstore.TrackingEventStream;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.json.JacksonSerializer;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

/**
 * Author: marc
 */
public class ClientNoSpring {

    public static void main(String[] args) throws Exception {
        System.out.println(InetAddress.getLocalHost().getCanonicalHostName());
        EventStoreConfiguration eventStoreConfiguration = EventStoreConfiguration
                .newBuilder("eventstore.axoniq.io:8123")
                .flowControl(0, 10000, 1000)
                .token("4e173955-f887-465a-a7d8-2fdbca7e4e37")
                .connectionRetry(1000, 3)
                .ssl("resources/axoniq-public.crt")
                .build();

        Serializer serializer = new JacksonSerializer();
        AxonIQEventStore eventStore = new AxonIQEventStore(eventStoreConfiguration, serializer);
        try {
            TrackingEventStream stream = eventStore.openStream(null);
            int count = 20;
            while (stream.hasNextAvailable(5, TimeUnit.MINUTES) && count > 0) {
                TrackedEventMessage<?> event = stream.nextAvailable();
                GenericTrackedDomainEventMessage e = (GenericTrackedDomainEventMessage) event;
                System.out.println(event.trackingToken() + "=>" + e.getAggregateIdentifier());
                count--;
            }
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            System.exit(1);
        }
        System.exit(0);
    }
}
