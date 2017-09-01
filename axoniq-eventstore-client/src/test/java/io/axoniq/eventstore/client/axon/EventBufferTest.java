package io.axoniq.eventstore.client.axon;

import com.google.protobuf.ByteString;
import io.axoniq.eventstore.Event;
import io.axoniq.eventstore.SerializedObject;
import io.axoniq.eventstore.grpc.EventWithToken;
import org.axonframework.eventhandling.TrackedEventMessage;
import org.axonframework.eventsourcing.DomainEventMessage;
import org.axonframework.eventsourcing.eventstore.GlobalSequenceTrackingToken;
import org.axonframework.serialization.upcasting.event.EventUpcaster;
import org.axonframework.serialization.upcasting.event.IntermediateEventRepresentation;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;

public class EventBufferTest {
    private EventUpcaster stubUpcaster;
    private EventBuffer testSubject;

    private XStreamSerializer serializer;
    private org.axonframework.serialization.SerializedObject<byte[]> serializedObject;

    @Before
    public void setUp() throws Exception {
        stubUpcaster = mock(EventUpcaster.class);
        when(stubUpcaster.upcast(any())).thenAnswer((Answer<Stream<IntermediateEventRepresentation>>) invocationOnMock -> (Stream<IntermediateEventRepresentation>) invocationOnMock.getArguments()[0]);
        serializer = new XStreamSerializer();

        serializedObject = serializer.serialize("some object", byte[].class);
    }

    @Test
    public void testDataUpcastAndDeserialized() throws InterruptedException {
        testSubject = new EventBuffer(stubUpcaster, serializer);

        assertFalse(testSubject.hasNextAvailable());
        testSubject.push(createEventData(1L));
        assertTrue(testSubject.hasNextAvailable());

        TrackedEventMessage<?> peeked = testSubject.peek().orElseThrow(() -> new AssertionError("Expected value to be available"));
        assertEquals(new GlobalSequenceTrackingToken(1L), peeked.trackingToken());
        assertTrue(peeked instanceof DomainEventMessage<?>);

        assertTrue(testSubject.hasNextAvailable());
        assertTrue(testSubject.hasNextAvailable(1, TimeUnit.SECONDS));
        testSubject.nextAvailable();
        assertFalse(testSubject.hasNextAvailable());
        assertFalse(testSubject.hasNextAvailable(10, TimeUnit.MILLISECONDS));

        verify(stubUpcaster).upcast(isA(Stream.class));
    }

    @Test
    public void testConsumptionIsRecorded() {
        stubUpcaster = stream -> stream.filter(i -> false);
        testSubject = new EventBuffer(stubUpcaster, serializer);

        testSubject.push(createEventData(1));
        testSubject.push(createEventData(2));
        testSubject.push(createEventData(3));

        AtomicInteger consumed = new AtomicInteger();
        testSubject.registerConsumeListener(consumed::addAndGet);

        testSubject.peek(); // this should consume 3 incoming messages
        assertEquals(3, consumed.get());
    }

    private EventWithToken createEventData(long sequence) {
        return EventWithToken.newBuilder()
                             .setToken(sequence)
                             .setEvent(Event.newBuilder()
                                            .setPayload(SerializedObject.newBuilder()
                                                                        .setData(ByteString.copyFrom(serializedObject.getData()))
                                                                        .setType(serializedObject.getType().getName()))
                                            .setMessageIdentifier(UUID.randomUUID().toString())
                                            .setAggregateType("Test")
                                            .setAggregateSequenceNumber(sequence)
                                            .setTimestamp(System.currentTimeMillis())
                                            .setAggregateIdentifier("1235"))
                             .build();
    }


}
