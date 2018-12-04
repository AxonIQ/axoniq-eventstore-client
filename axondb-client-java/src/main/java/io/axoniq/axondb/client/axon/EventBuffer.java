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

import io.axoniq.axondb.client.ClientConnectionException;
import io.axoniq.axondb.grpc.EventWithToken;
import org.axonframework.eventhandling.TrackedEventMessage;
import org.axonframework.eventsourcing.eventstore.*;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.upcasting.event.EventUpcaster;
import org.axonframework.serialization.upcasting.event.NoOpEventUpcaster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

import static java.lang.Math.min;
import static org.axonframework.common.ObjectUtils.getOrDefault;

/**
 * Client-side buffer of messages received from the server. Once consumed from this buffer, the client is notified
 * of a permit being consumed, potentially triggering a permit refresh, if flow control is enabled.
 * <p>
 * This class is intended for internal use. Be cautious.
 *
 * @author Marc Gathier
 * @author Allard Buijze
 */
public class EventBuffer implements TrackingEventStream {
    final Logger logger = LoggerFactory.getLogger(EventBuffer.class);

    private final BlockingQueue<TrackedEventData<byte[]>> events;

    private final Iterator<TrackedEventMessage<?>> eventStream;
    private final int heartbeatInterval;
    private final AtomicLong lastServerInteraction = new AtomicLong(System.currentTimeMillis());
    private TrackedEventData<byte[]> peekData;
    private TrackedEventMessage<?> peekEvent;
    private Consumer<EventBuffer> closeCallback;
    private volatile RuntimeException exception;
    private volatile boolean closed;
    private Consumer<Integer> consumeListener = i -> {
    };

    /**
     * Initializes an Event Buffer, passing messages through given {@code upcasterChain} and deserializing events using
     * given {@code serializer}.
     *
     * @param upcasterChain     The upcasterChain to translate serialized representations before deserializing
     * @param serializer        The serializer capable of deserializing incoming messages
     * @param heartbeatInterval The interval at which heartbeat messages are excpected to arrive, when no events are
     *                          being streamed.
     */
    public EventBuffer(EventUpcaster upcasterChain, Serializer serializer, int heartbeatInterval) {
        this(upcasterChain, serializer, heartbeatInterval, new LinkedBlockingQueue<>());
    }

    protected EventBuffer(EventUpcaster upcasterChain, Serializer serializer, int heartbeatInterval, BlockingQueue<TrackedEventData<byte[]>> events) {
        this.heartbeatInterval = heartbeatInterval;
        this.events = events;
        eventStream = EventUtils.upcastAndDeserializeTrackedEvents(StreamSupport.stream(new SimpleSpliterator<>(this::poll), false),
                                                                   new GrpcMetaDataAwareSerializer(serializer),
                                                                   getOrDefault(upcasterChain, NoOpEventUpcaster.INSTANCE),
                                                                   true)
                                .iterator();
    }

    private TrackedEventData<byte[]> poll() {
        TrackedEventData<byte[]> nextItem;
        if (peekData != null) {
            nextItem = peekData;
            peekData = null;
            return nextItem;
        }
        nextItem = events.poll();
        if (nextItem != null) {
            consumeListener.accept(1);
        }
        return nextItem;
    }

    private void waitForData(long deadline) throws InterruptedException {
        long now = System.currentTimeMillis();
        long timeLeft = deadline - now;
        if (peekData == null && timeLeft > 0) {
            peekData = events.poll(heartbeatInterval > 0 ? min(heartbeatInterval, timeLeft) : timeLeft, TimeUnit.MILLISECONDS);
            if (peekData != null) {
                consumeListener.accept(1);
            } else if (heartbeatInterval > 0) {
                // no data. Check last time
                if (lastServerInteraction.get() < System.currentTimeMillis() - (2 * heartbeatInterval)) {
                    fail(new ClientConnectionException("Connection timed out"));
                    close();
                }
            }
        }
    }

    /**
     * Registers the callback to invoke when the reader wishes to close the stream.
     *
     * @param closeCallback The callback to invoke when the reader wishes to close the stream
     */
    public void registerCloseListener(Consumer<EventBuffer> closeCallback) {
        this.closeCallback = closeCallback;
    }

    /**
     * Registers the callback to invoke when a raw input message was consumed from the buffer.
     *
     * @param consumeListener the callback to invoke when a raw input message was consumed from the buffer
     */
    public void registerConsumeListener(Consumer<Integer> consumeListener) {
        this.consumeListener = consumeListener;
    }

    @Override
    public Optional<TrackedEventMessage<?>> peek() {
        if (peekEvent == null && eventStream.hasNext()) {
            peekEvent = eventStream.next();
        }
        return Optional.ofNullable(peekEvent);
    }

    @Override
    public boolean hasNextAvailable(int timeout, TimeUnit timeUnit)  {
        long deadline = System.currentTimeMillis() + timeUnit.toMillis(timeout);
        try {
            while (exception == null && peekEvent == null && !eventStream.hasNext() && System.currentTimeMillis() < deadline) {
                waitForData(deadline);
            }
            if (exception != null) {
                RuntimeException runtimeException = exception;
                this.exception = null;
                throw runtimeException;
            }
            return peekEvent != null || eventStream.hasNext();
        } catch (InterruptedException e) {
            logger.warn("Consumer thread was interrupted. Returning thread to event processor.", e);
            Thread.currentThread().interrupt();
            return false;
        }
    }

    @Override
    public TrackedEventMessage<?> nextAvailable() {
        try {
            hasNextAvailable(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
            return peekEvent == null ? eventStream.next() : peekEvent;
        } finally {
            peekEvent = null;
        }
    }

    @Override
    public void close() {
        closed = true;
        if (closeCallback != null) closeCallback.accept(this);
        events.clear();
    }

    public boolean push(EventWithToken event) {
        if (closed) {
            logger.debug("Received event while closed: {}", event.getToken());
            return false;
        }
        try {
            touch();
            TrackingToken trackingToken = new GlobalSequenceTrackingToken(event.getToken());
            events.put(new TrackedDomainEventData<>(trackingToken, new GrpcBackedDomainEventData(event.getEvent())));
        } catch (InterruptedException e) {
            closeCallback.accept(this);
            return false;
        }
        return true;
    }

    public void fail(RuntimeException e) {
        this.exception = e;
    }

    public void touch() {
        this.lastServerInteraction.set(System.currentTimeMillis());
    }

    private static class SimpleSpliterator<T> implements Spliterator<T> {

        private final Supplier<T> supplier;

        protected SimpleSpliterator(Supplier<T> supplier) {
            this.supplier = supplier;
        }

        @Override
        public boolean tryAdvance(Consumer<? super T> action) {
            T nextValue = supplier.get();
            if (nextValue != null) {
                action.accept(nextValue);
            }
            return nextValue != null;
        }

        @Override
        public Spliterator<T> trySplit() {
            return null;
        }

        @Override
        public long estimateSize() {
            return Long.MAX_VALUE;
        }

        @Override
        public int characteristics() {
            return ORDERED | NONNULL | IMMUTABLE;
        }
    }
}
