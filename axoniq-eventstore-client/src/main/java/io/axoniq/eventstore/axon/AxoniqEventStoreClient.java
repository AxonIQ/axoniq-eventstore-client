package io.axoniq.eventstore.axon;

import io.axoniq.eventstore.EventStoreConfiguration;
import io.axoniq.eventstore.grpc.EventWithToken;
import io.axoniq.eventstore.gateway.AppendEventTransaction;
import io.axoniq.eventstore.gateway.EventStoreGateway;
import io.axoniq.eventstore.grpc.GetAggregateEventsRequest;
import io.axoniq.eventstore.grpc.GetEventsRequest;
import io.axoniq.eventstore.util.RateLimitingStreamObserver;
import org.axonframework.common.Assert;
import org.axonframework.eventhandling.AbstractEventBus;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventsourcing.DomainEventMessage;
import org.axonframework.eventsourcing.eventstore.*;
import org.axonframework.messaging.unitofwork.CurrentUnitOfWork;
import org.axonframework.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @author Zoltan Altfatter
 * @author Marc Gathier
 * @author Allard Buijze
 */
public class AxoniqEventStoreClient extends AbstractEventBus implements EventStore {

    private static final Logger logger = LoggerFactory.getLogger(AxoniqEventStoreClient.class);
    private final String APPEND_EVENT_TRANSACTION = this + "/APPEND_EVENT_TRANSACTION";
    private final EventStoreConfiguration eventStoreConfiguration;
    private final EventStoreGateway eventStoreGateway;
    private final PayloadMapper payloadMapper;

    public AxoniqEventStoreClient(EventStoreConfiguration eventStoreConfiguration, Serializer serializer) {
        this(eventStoreConfiguration, new DefaultPayloadMapper(serializer));
    }

    public AxoniqEventStoreClient(EventStoreConfiguration eventStoreConfiguration, PayloadMapper payloadMapper) {
        this.eventStoreConfiguration = eventStoreConfiguration;
        this.eventStoreGateway = new EventStoreGateway(eventStoreConfiguration);
        this.payloadMapper = payloadMapper;
    }

    @Override
    public DomainEventStream readEvents(String aggregateIdentifier) {
        logger.debug("Reading events for aggregate id {}", aggregateIdentifier);
        GetAggregateEventsRequest request = GetAggregateEventsRequest.newBuilder().setAggregateId(aggregateIdentifier).build();
        try {
            return DomainEventStream.of(eventStoreGateway.listAggregateEvents(request).map(payloadMapper::mapDomainEvent));
        } catch (Throwable e) {
            throw convertExcpetion(e);
        }
    }

    private RuntimeException convertExcpetion(Throwable ex) {
        if (ex instanceof ExecutionException) ex = ex.getCause();
        return (ex instanceof RuntimeException) ? (RuntimeException) ex : new RuntimeException(ex);
    }


    @Override
    public TrackingEventStream openStream(TrackingToken trackingToken) {
        Assert.isTrue(trackingToken == null || trackingToken instanceof GlobalSequenceTrackingToken,
                      () -> "Invalid tracking token type. Must be GlobalSequenceTrackingToken.");
        long nextToken = trackingToken == null ? 0 : ((GlobalSequenceTrackingToken) trackingToken).getGlobalIndex() + 1;
        EventConsumer consumer = new EventConsumer(payloadMapper);

        logger.info("open stream: {}", nextToken);
        RateLimitingStreamObserver<GetEventsRequest, EventWithToken> observer = new RateLimitingStreamObserver<>(
                eventStoreGateway::listEvents,
                (eventWithToken, getEventsRequestStreamObserver) -> {
                    logger.debug("Received event with token: {}", eventWithToken.getToken());
                    consumer.push(eventWithToken);
                },
                throwable -> {
                    logger.error("Failed to receive events", throwable);
                    consumer.fail(convertExcpetion(throwable));
                });
        GetEventsRequest request = GetEventsRequest.newBuilder()
                                                   .setTrackingToken(nextToken)
                                                   .setNumberOfPermits(eventStoreConfiguration.getInitialNrOfPermits())
                                                   .build();

        GetEventsRequest nextRequest = GetEventsRequest.newBuilder().setNumberOfPermits(eventStoreConfiguration.getNrOfNewPermits()).build();
        observer.start(request, nextRequest, eventStoreConfiguration.getInitialNrOfPermits(), eventStoreConfiguration.getNrOfNewPermits(), eventStoreConfiguration.getNewPermitsThreshold());

        consumer.registerCloseListener((eventConsumer) -> observer.stop());
        return consumer;
    }

    protected void prepareCommit(List<? extends EventMessage<?>> events) {
        AppendEventTransaction sender = CurrentUnitOfWork.get().getOrComputeResource(APPEND_EVENT_TRANSACTION, k -> {
            AppendEventTransaction appendEventTransaction = eventStoreGateway.createAppendEventConnection();
            CurrentUnitOfWork.get().onRollback(u -> appendEventTransaction.rollback(u.getExecutionResult().getExceptionResult()));
            return appendEventTransaction;
        });

        for (EventMessage<?> eventMessage : events) {
            sender.append(payloadMapper.map(eventMessage));
        }

        super.prepareCommit(events);
    }

    protected void commit(List<? extends EventMessage<?>> events) {
        super.commit(events);

        AppendEventTransaction sender = CurrentUnitOfWork.get().getResource(APPEND_EVENT_TRANSACTION);
        try {
            sender.commit();
        } catch (Throwable e) {
            throw convertExcpetion(e);
        }
    }

    protected void afterCommit(List<? extends EventMessage<?>> events) {
        super.afterCommit(events);
    }

    @Override
    public void storeSnapshot(DomainEventMessage<?> snapshot) {
        try {
            eventStoreGateway.appendSnapshot(payloadMapper.map(snapshot)).get(10, TimeUnit.SECONDS);
        } catch (Throwable e) {
            throw convertExcpetion(e);
        }
        logger.info("Store snapshot: {}", snapshot);
    }

}
