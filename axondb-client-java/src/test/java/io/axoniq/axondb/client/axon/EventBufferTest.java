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

package io.axoniq.axondb.client.axon;

import com.google.protobuf.ByteString;
import io.axoniq.axondb.Event;
import io.axoniq.axondb.client.ClientConnectionException;
import io.axoniq.axondb.grpc.EventWithToken;
import io.axoniq.platform.SerializedObject;
import org.axonframework.eventhandling.TrackedEventMessage;
import org.axonframework.eventsourcing.DomainEventMessage;
import org.axonframework.eventsourcing.eventstore.GlobalSequenceTrackingToken;
import org.axonframework.eventsourcing.eventstore.TrackedDomainEventData;
import org.axonframework.eventsourcing.eventstore.TrackedEventData;
import org.axonframework.serialization.upcasting.event.EventUpcaster;
import org.axonframework.serialization.upcasting.event.IntermediateEventRepresentation;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
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
        testSubject = new EventBuffer(stubUpcaster, serializer, 0);

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
        testSubject = new EventBuffer(stubUpcaster, serializer, 0);

        testSubject.push(createEventData(1));
        testSubject.push(createEventData(2));
        testSubject.push(createEventData(3));

        AtomicInteger consumed = new AtomicInteger();
        testSubject.registerConsumeListener(consumed::addAndGet);

        testSubject.peek(); // this should consume 3 incoming messages
        assertEquals(3, consumed.get());
    }

    @Test
    public void testPollTimeOnEventQueueLimitedToHeartbeatInterval() throws InterruptedException {

        stubUpcaster = stream -> stream.filter(i -> false);
        LinkedBlockingQueue<TrackedEventData<byte[]>> queue = new LinkedBlockingQueue<>();
        BlockingQueue<TrackedEventData<byte[]>> spiedQueue = spy(queue);
        queue.offer(createTrackedEventData(1));
        testSubject = new EventBuffer(stubUpcaster, serializer, 2, spiedQueue);

        testSubject.hasNextAvailable(10, TimeUnit.MILLISECONDS); // this should consume 3 incoming messages
        verify(spiedQueue).poll(eq(2L), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    public void testPollTimeOnEventQueueNotLimitedwWithZeroHeartbeatInterval() throws InterruptedException {
        stubUpcaster = stream -> stream.filter(i -> false);
        LinkedBlockingQueue<TrackedEventData<byte[]>> queue = new LinkedBlockingQueue<>();
        BlockingQueue<TrackedEventData<byte[]>> spiedQueue = spy(queue);
        queue.offer(createTrackedEventData(1));
        testSubject = new EventBuffer(stubUpcaster, serializer, 0, spiedQueue);

        testSubject.hasNextAvailable(10, TimeUnit.MILLISECONDS);
        verify(spiedQueue).poll(eq(10L), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    public void testAbsenceOfAMessageDuringTwiceTheHeartbeatIntervalRaisesException() throws InterruptedException {
        stubUpcaster = stream -> stream.filter(i -> false);
        BlockingQueue<TrackedEventData<byte[]>> blockingQueue = spy(new LinkedBlockingQueue<>());
        testSubject = new EventBuffer(stubUpcaster, serializer, 4, blockingQueue);
        try {
            testSubject.hasNextAvailable(10, TimeUnit.MILLISECONDS);
            testSubject.peek();
            fail("Expected ClientConnectionException");
        } catch (ClientConnectionException e) {
            //expected
        }

        verify(blockingQueue, atLeastOnce()).poll(eq(4L), eq(TimeUnit.MILLISECONDS));
    }

    private TrackedEventData<byte[]> createTrackedEventData(long sequence) {
        EventWithToken eventWithToken = createEventData(sequence);
        return new TrackedDomainEventData<>(new GlobalSequenceTrackingToken(eventWithToken.getToken()),
                                            new GrpcBackedDomainEventData(eventWithToken.getEvent()));
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
