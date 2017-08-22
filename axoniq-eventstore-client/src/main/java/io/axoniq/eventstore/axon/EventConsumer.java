package io.axoniq.eventstore.axon;

import io.axoniq.eventstore.EventWithToken;
import org.axonframework.eventhandling.TrackedEventMessage;
import org.axonframework.eventsourcing.GenericTrackedDomainEventMessage;
import org.axonframework.eventsourcing.eventstore.GlobalSequenceTrackingToken;
import org.axonframework.eventsourcing.eventstore.TrackingEventStream;
import org.axonframework.eventsourcing.eventstore.TrackingToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Created by marc on 7/18/2017.
 */
class EventConsumer implements TrackingEventStream {
    final Logger logger = LoggerFactory.getLogger(EventConsumer.class);
    final private PayloadMapper payloadMapper;
    private BlockingQueue<TrackedEventMessage> events;
    private TrackedEventMessage<?> peekEvent;
    private Consumer<EventConsumer> closeCallback;
    private RuntimeException exception;

    public EventConsumer(PayloadMapper payloadMapper) {
        this.events = new LinkedBlockingQueue<>();
        this.payloadMapper = payloadMapper;
    }

    public void registerCloseListener(Consumer<EventConsumer> closeCallback) {
        this.closeCallback = closeCallback;
    }
    @Override
    public Optional<TrackedEventMessage<?>> peek() {
        return Optional.ofNullable(peekEvent == null && !hasNextAvailable() ? null : peekEvent);
    }

    @Override
    public boolean hasNextAvailable(int timeout, TimeUnit timeUnit) throws InterruptedException {
        if( exception != null) throw exception;
        try {
            return peekEvent != null || (peekEvent = events.poll(timeout, timeUnit)) != null;
        } catch (InterruptedException e) {
            logger.warn("Consumer thread was interrupted. Returning thread to event processor.", e);
            Thread.currentThread().interrupt();
            return false;
        }
    }

    @Override
    public TrackedEventMessage<?> nextAvailable() throws InterruptedException {
        if( exception != null) throw exception;
        try {
            return peekEvent == null ? events.take() : peekEvent;
        } catch (InterruptedException e) {
            logger.warn("Consumer thread was interrupted. Returning thread to event processor.", e);
            Thread.currentThread().interrupt();
            return null;
        } finally {
            peekEvent = null;
        }
    }

    @Override
    public void close() {
        if( closeCallback != null) closeCallback.accept(this);
    }


    public void push(EventWithToken event) {
        try {
            TrackingToken trackingToken = new GlobalSequenceTrackingToken(event.getToken());
            GenericTrackedDomainEventMessage trackedEventMessage = new GenericTrackedDomainEventMessage(trackingToken, payloadMapper.map(event.getEvent()));
            events.put(trackedEventMessage);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e) {
            logger.info(e.getMessage());
        }
    }

    public void fail(RuntimeException e) {
        this.exception  = e;
    }
}
