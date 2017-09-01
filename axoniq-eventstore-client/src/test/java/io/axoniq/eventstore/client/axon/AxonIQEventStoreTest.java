package io.axoniq.eventstore.client.axon;

import io.axoniq.eventstore.client.EventStoreConfiguration;
import io.axoniq.eventstore.client.StubServer;
import org.axonframework.eventhandling.GenericEventMessage;
import org.axonframework.eventsourcing.eventstore.TrackingEventStream;
import org.axonframework.messaging.Message;
import org.axonframework.messaging.unitofwork.DefaultUnitOfWork;
import org.axonframework.messaging.unitofwork.UnitOfWork;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class AxonIQEventStoreTest {

    private StubServer server;
    private AxonIQEventStore testSubject;

    @Before
    public void setUp() throws Exception {
        server = new StubServer(8123);
        server.start();
        EventStoreConfiguration config = EventStoreConfiguration.newBuilder("localhost:8123")
                                                                .flowControl(2, 1, 1)
                                                                .build();
        testSubject = new AxonIQEventStore(config, new XStreamSerializer());
    }

    @After
    public void tearDown() throws Exception {
        server.shutdown();
    }

    @Test
    public void testPublishAndConsumeEvents() throws Exception {
        UnitOfWork<Message<?>> uow = DefaultUnitOfWork.startAndGet(null);
        testSubject.publish(GenericEventMessage.asEventMessage("Test1"),
                            GenericEventMessage.asEventMessage("Test2"),
                            GenericEventMessage.asEventMessage("Test3"));
        uow.commit();

        TrackingEventStream stream = testSubject.openStream(null);

        List<String> received = new ArrayList<>();
        while(stream.hasNextAvailable(100, TimeUnit.MILLISECONDS)) {
            received.add(stream.nextAvailable().getPayload().toString());
        }
        stream.close();

        assertEquals(Arrays.asList("Test1", "Test2", "Test3"), received);
    }

}
