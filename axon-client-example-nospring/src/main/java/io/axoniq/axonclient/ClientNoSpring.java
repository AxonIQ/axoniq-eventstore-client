package io.axoniq.axonclient;

import io.axoniq.eventstore.AxoniqEventStore;
import io.axoniq.eventstore.EventStoreConfiguration;
import io.axoniq.eventstore.performancetest.TestEvent;
import org.axonframework.eventhandling.TrackedEventMessage;
import org.axonframework.eventsourcing.GenericTrackedDomainEventMessage;
import org.axonframework.eventsourcing.eventstore.GlobalSequenceTrackingToken;
import org.axonframework.eventsourcing.eventstore.TrackingEventStream;
import org.axonframework.serialization.*;
import org.axonframework.serialization.json.JacksonSerializer;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

/**
 * Author: marc
 */
public class ClientNoSpring {

    public static void main(String[] args) throws Exception {
        System.out.println(InetAddress.getLocalHost().getCanonicalHostName());
        EventStoreConfiguration eventStoreConfiguration = EventStoreConfiguration.newBuilder("eventstore.axoniq.io:8123")
                .flowControl(0, 10000, 1000)
                .token( "4e173955-f887-465a-a7d8-2fdbca7e4e37")
                .connectionRetry(1000, 3)
                .ssl("resources/axoniq-public.crt")
                .build();

        Serializer serializer = new JacksonSerializer();
        AxoniqEventStore eventStore = new AxoniqEventStore( eventStoreConfiguration, serializer);
        try {
            // TrackingEventStream stream = eventStore.openStream(null);
            TrackingEventStream stream = eventStore.openStream(new GlobalSequenceTrackingToken(10000000));
            int count = 20;
            while (stream.hasNextAvailable(5, TimeUnit.MINUTES) && count > 0) {
                TrackedEventMessage<?> event = stream.nextAvailable();
                GenericTrackedDomainEventMessage e = (GenericTrackedDomainEventMessage)event;
                System.out.println(event.trackingToken() + "=>" +  e.getAggregateIdentifier());
                count--;
            }
        } catch( Exception ex) {
            System.out.println(ex.getMessage());
            System.exit(1);
        }
        System.exit(0);
    }
}