package io.axoniq.axonclient;

import com.google.common.collect.Lists;
import io.axoniq.eventstore.axon.AxoniqEventStore;
import io.axoniq.eventstore.EventStoreConfiguration;
import io.axoniq.eventstore.gateway.EventStoreGateway;
import io.axoniq.eventstore.performancetest.TestEvent;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventsourcing.GenericDomainEventMessage;
import org.axonframework.messaging.unitofwork.DefaultUnitOfWork;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.json.JacksonSerializer;

import java.util.HashMap;
import java.util.List;
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

    public static void main(String[] args) throws Exception {
        EventStoreConfiguration eventStoreConfiguration = EventStoreConfiguration.newBuilder("eventstore.axoniq.io:8123")
                //.flowControl(10, 1, 0)
                .token( "4e173955-f887-465a-a7d8-2fdbca7e4e37")
                .connectionRetry(3000, 30)
                .ssl("resources/axoniq-public.crt")
                .build();

        Serializer serializer = new JacksonSerializer();
        EventStoreGateway gateway = new EventStoreGateway(eventStoreConfiguration);

        AxoniqEventStore eventStore = new AxoniqEventStore( eventStoreConfiguration, gateway, serializer);

        IntStream.range(0, NR_AGGREGATES).forEach(i -> {
            aggregateIds[i] = UUID.randomUUID().toString();
            sequenceNumbers.put(aggregateIds[i], new AtomicInteger(0));
        });

        long start = System.currentTimeMillis();
        IntStream.range(0, NR_EVENTS).parallel().forEach( i -> generateMessages(eventStore));
        long end  = System.currentTimeMillis();
        sequenceNumbers.entrySet().stream().filter(e -> e.getValue().get() > 0).forEach(e -> System.out.println( e.getKey() + '=' + e.getValue().get()));

        System.out.println("Submitted " + NR_EVENTS + " events in " + (end-start) + "ms.");
    }

    private static void generateMessages(AxoniqEventStore eventStore) {
        int i = ThreadLocalRandom.current().nextInt(0,NR_AGGREGATES);
        String aggId = aggregateIds[i];
        AtomicInteger seqHolder = sequenceNumbers.get(aggId);
        seqHolder.getAndAccumulate(1, (old, acc) -> {
            EventMessage<TestEvent> eventMessage = new GenericDomainEventMessage(TestEvent.class.getName(), aggId, old, new TestEvent("1", "one"));
            List<? extends EventMessage<?>> eventMessages = Lists.newArrayList(eventMessage);
            DefaultUnitOfWork uow = DefaultUnitOfWork.startAndGet(eventMessages.get(0));
            eventStore.publish(eventMessages);
            uow.commit();
            return old + acc;
        });
    }
}
