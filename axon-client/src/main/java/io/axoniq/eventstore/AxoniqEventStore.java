package io.axoniq.eventstore;

import io.axoniq.eventstore.grpc.*;
import io.axoniq.eventstore.util.CheckedStreamObserver;
import io.axoniq.eventstore.util.DuplexStreamObserver;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.axonframework.common.Assert;
import org.axonframework.eventhandling.AbstractEventBus;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventsourcing.DomainEventMessage;
import org.axonframework.eventsourcing.GenericDomainEventMessage;
import org.axonframework.eventsourcing.eventstore.*;
import org.axonframework.messaging.unitofwork.CurrentUnitOfWork;
import org.axonframework.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Zoltan Altfatter
 */
public class AxoniqEventStore extends AbstractEventBus implements EventStore {

    private static final Logger logger = LoggerFactory.getLogger(AxoniqEventStore.class);
    private final String GRPC_SENDER = this + "/GRPC_SENDER";
    private final TokenAddingInterceptor tokenAddingInterceptor;
    private final EventStoreConfiguration eventStoreConfiguration;
    private PayloadMapper payloadMapper;

    public AxoniqEventStore( EventStoreConfiguration eventStoreConfiguration, Serializer serializer) {
        this.eventStoreConfiguration = eventStoreConfiguration;
        payloadMapper = new PayloadMapper(serializer);
        this.tokenAddingInterceptor = new TokenAddingInterceptor(eventStoreConfiguration.getToken());
    }

    @Override
    public DomainEventStream readEvents(String aggregateIdentifier) {
        logger.info("Reading events for aggregate id {}", aggregateIdentifier);
        EventReaderGrpc.EventReaderStub reader = EventReaderGrpc.newStub(eventStoreConfiguration.getChannelToEventStore()).withInterceptors(tokenAddingInterceptor);


        List<GenericDomainEventMessage<?>> aggregateEvents = new ArrayList<>();

        GetAggregateEventsRequest request = GetAggregateEventsRequest.newBuilder().setAggregateId(aggregateIdentifier).build();
        CheckedStreamObserver<Event> eventCustomStreamObserver = new CheckedStreamObserver<>( event -> aggregateEvents.add(payloadMapper.map(event)));
        reader.listAggregateEvents(request, eventCustomStreamObserver);
        eventCustomStreamObserver.waitForCompletion(TimeUnit.SECONDS, 10, this::checkConnection);
        return DomainEventStream.of(aggregateEvents.stream());
    }

    private void checkConnection(RuntimeException ex) {
        if( ex instanceof StatusRuntimeException && ((StatusRuntimeException)ex).getStatus().getCode().equals(Status.UNAVAILABLE.getCode()) ) {
            eventStoreConfiguration.stopChannelToEventStore();
        }
        throw ex;
    }


    @Override
    public TrackingEventStream openStream(TrackingToken trackingToken) {
        Assert.isTrue(trackingToken == null || trackingToken instanceof GlobalSequenceTrackingToken, () -> "Invalid tracking token type. Must be GlobalSequenceTrackingToken.");
        long nextToken = trackingToken == null ? 0 : ((GlobalSequenceTrackingToken) trackingToken).getGlobalIndex() + 1;
        EventConsumer consumer = new EventConsumer(payloadMapper);
        restartStream(consumer, nextToken);
        return consumer;
    }

    private void restartStream(EventConsumer consumer, long nextToken) {
        logger.info("open stream: {}", nextToken);
        AtomicLong receivedToken = new AtomicLong(-1);
        EventReaderGrpc.EventReaderStub reader = EventReaderGrpc.newStub(eventStoreConfiguration.getChannelToEventStore()).withInterceptors(tokenAddingInterceptor);
        DuplexStreamObserver<GetEventsRequest, EventWithToken> observer = new DuplexStreamObserver<>(reader::listEvents,
                (eventWithToken, getEventsRequestStreamObserver) -> {
                    logger.info("Received event with token: {}", eventWithToken.getToken());
                    consumer.push(eventWithToken);
                    receivedToken.set(eventWithToken.getToken());
                },
                throwable -> {
                    logger.error("Failed to receive events", throwable);
                    eventStoreConfiguration.stopChannelToEventStore();
                    try {
                        if(! eventStoreConfiguration.isShutdown() )
                            restartStream(consumer, receivedToken.get() == -1 ? nextToken : receivedToken.get() + 1);
                    } catch( Exception ex) {
                        logger.error("Failed to restart stream: - {}", throwable.getMessage());
                        consumer.close();
                    }
                });

        consumer.registerCloseListener((eventConsumer) -> observer.stop());
        GetEventsRequest request = GetEventsRequest.newBuilder()
                .setTrackingToken(nextToken)
                .setNumberOfPermits(eventStoreConfiguration.getInitialNrOfPermits())
                .build();

        GetEventsRequest nextRequest = GetEventsRequest.newBuilder().setNumberOfPermits(eventStoreConfiguration.getNrOfNewPermits()).build();
        observer.start(request, nextRequest, eventStoreConfiguration.getInitialNrOfPermits(), eventStoreConfiguration.getNrOfNewPermits(), eventStoreConfiguration.getNewPermitsThreshold());
    }

    protected void prepareCommit(List<? extends EventMessage<?>> events) {
        GrpcConnection sender = CurrentUnitOfWork.get().getOrComputeResource(GRPC_SENDER, k -> {
            EventWriterGrpc.EventWriterStub asyncStub = EventWriterGrpc.newStub(eventStoreConfiguration.getChannelToEventStore()).withInterceptors(tokenAddingInterceptor);
            CheckedStreamObserver<Confirmation> observer = new CheckedStreamObserver<>( confirmation -> { });

            CurrentUnitOfWork.get().onRollback(u -> observer.onError(u.getExecutionResult().getExceptionResult()));

            return new GrpcConnection(asyncStub.appendEvent(observer), observer);
        });

        for (EventMessage<?> eventMessage : events) {
            EventWithContext event = payloadMapper.map(eventMessage);
            logger.debug("sending event: {}/{}", event.getEvent().getAggregateIdentifier(),  event.getEvent().getAggregateSequenceNumber());
            sender.send(event);
        }

        super.prepareCommit(events);
    }

    protected void commit(List<? extends EventMessage<?>> events) {
        super.commit(events);

        GrpcConnection sender = CurrentUnitOfWork.get().getResource(GRPC_SENDER);
        sender.commit();
    }

    protected void afterCommit(List<? extends EventMessage<?>> events) {
        super.afterCommit(events);
    }

    @Override
    public void storeSnapshot(DomainEventMessage<?> snapshot) {
        EventWriterGrpc.EventWriterStub asyncStub = EventWriterGrpc.newStub(eventStoreConfiguration.getChannelToEventStore()).withInterceptors(tokenAddingInterceptor);
        CheckedStreamObserver<Confirmation> observer = new CheckedStreamObserver<>( confirmation -> {});
        asyncStub.appendSnapshot(payloadMapper.map(snapshot), observer);
        observer.waitForCompletion(TimeUnit.SECONDS, 10, this::checkConnection);
        logger.info("Store snapshot: {}", snapshot);
    }

    private class GrpcConnection {
        private final StreamObserver<EventWithContext> eventStreamObserver;
        private final CheckedStreamObserver<Confirmation> observer;

        public GrpcConnection(StreamObserver<EventWithContext> eventStreamObserver, CheckedStreamObserver<Confirmation> observer) {
            this.eventStreamObserver = eventStreamObserver;
            this.observer = observer;
        }

        public void send(EventWithContext event) {
            eventStreamObserver.onNext(event);
        }

        public void commit() {
            eventStreamObserver.onCompleted();
            observer.waitForCompletion(TimeUnit.SECONDS, 5, AxoniqEventStore.this::checkConnection);
        }
    }
}
