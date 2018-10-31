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
import io.axoniq.axondb.client.AppendEventTransaction;
import io.axoniq.axondb.client.AxonDBClient;
import io.axoniq.axondb.client.AxonDBConfiguration;
import io.axoniq.axondb.client.util.FlowControllingStreamObserver;
import io.axoniq.axondb.grpc.EventWithToken;
import io.axoniq.axondb.grpc.GetAggregateEventsRequest;
import io.axoniq.axondb.grpc.GetEventsRequest;
import io.axoniq.axondb.grpc.QueryEventsRequest;
import io.axoniq.axondb.grpc.QueryEventsResponse;
import io.axoniq.axondb.grpc.ReadHighestSequenceNrResponse;
import io.grpc.stub.StreamObserver;
import org.axonframework.common.Assert;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.eventhandling.*;
import org.axonframework.eventsourcing.eventstore.AbstractEventStorageEngine;
import org.axonframework.eventsourcing.eventstore.AbstractEventStore;
import org.axonframework.eventsourcing.eventstore.DomainEventStream;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.EventStoreException;
import org.axonframework.messaging.unitofwork.CurrentUnitOfWork;
import org.axonframework.serialization.LazyDeserializingObject;
import org.axonframework.serialization.SerializedMessage;
import org.axonframework.serialization.SerializedObject;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.upcasting.event.EventUpcaster;
import org.axonframework.serialization.upcasting.event.InitialEventRepresentation;
import org.axonframework.serialization.upcasting.event.IntermediateEventRepresentation;
import org.axonframework.serialization.upcasting.event.NoOpEventUpcaster;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.axonframework.common.BuilderUtils.assertNonNull;
import static org.axonframework.common.ObjectUtils.getOrDefault;

/**
 * Axon EventStore implementation that connects to the AxonIQ Event Store Server to store and retrieve Events.
 *
 * @author Zoltan Altfatter
 * @author Marc Gathier
 * @author Allard Buijze
 */
public class AxonDBEventStore extends AbstractEventStore {

    private static final Logger logger = LoggerFactory.getLogger(AxonDBEventStore.class);

    /**
     * Initialize the Event Store using given {@code configuration} and given {@code serializer}.
     * <p>
     * The Event Store will delay creating the connection until the first activity takes place.
     *
     * @param configuration The configuration describing the servers to connect with and how to manage flow control
     * @param serializer    The serializer to serialize Event payloads with
     */
    public AxonDBEventStore(AxonDBConfiguration configuration, Serializer serializer) {
        this(configuration, serializer, NoOpEventUpcaster.INSTANCE);
    }

    /**
     * Initialize the Event Store using given {@code configuration}, {@code serializer} and {@code upcasterChain}
     * <p>
     * The Event Store will delay creating the connection until the first activity takes place.
     *
     * @param configuration The configuration describing the servers to connect with and how to manage flow control
     * @param serializer    The serializer to serialize Event payloads with
     * @param upcasterChain The upcaster to modify received Event representations with
     */
    public AxonDBEventStore(AxonDBConfiguration configuration, Serializer serializer, EventUpcaster upcasterChain) {
        super(builder()
            .eventSerializer(serializer)
            .snapshotSerializer(serializer)
            .upcasterChain(upcasterChain)
            .configuration(configuration)
            .eventStoreClient(new AxonDBClient(configuration))
            .buildStorageEngineIfMissing());
    }

    /**
     * Initialize the Event Store using given {@code configuration}, {@code serializer} and {@code upcasterChain}
     * Allows for different serializers for snapshots and events (requires AxonFramework version 3.3 or higer)
     *
     * @param configuration      The configuration describing the servers to connect with and how to manage flow control
     * @param snapshotSerializer The serializer to serialize Snapshot payloads with
     * @param eventSerializer    The serializer to serialize Event payloads with
     * @param upcasterChain      The upcaster to modify received Event representations with
     */
    public AxonDBEventStore(AxonDBConfiguration configuration, Serializer snapshotSerializer, Serializer eventSerializer, EventUpcaster upcasterChain) {
        super(builder()
            .eventSerializer(eventSerializer)
            .snapshotSerializer(snapshotSerializer)
            .upcasterChain(upcasterChain)
            .configuration(configuration)
            .eventStoreClient(new AxonDBClient(configuration))
            .buildStorageEngineIfMissing());
    }

    @Override
    public TrackingEventStream openStream(TrackingToken trackingToken) {
        return storageEngine().openStream(trackingToken);
    }


    public QueryResultStream query(String query, boolean liveUpdates) {
        return storageEngine().query(query, liveUpdates);
    }

    public static Builder builder() {
        return new Builder();
    }

    private static class Builder extends AbstractEventStore.Builder {

        private AxonDBConfiguration configuration;
        private AxonDBClient eventStoreClient;
        private Serializer snapshotSerializer = XStreamSerializer.builder().build();
        private Serializer eventSerializer = XStreamSerializer.builder().build();
        private EventUpcaster upcasterChain = NoOpEventUpcaster.INSTANCE;

        @Override
        public Builder storageEngine(EventStorageEngine storageEngine) {
            super.storageEngine(storageEngine);
            return this;
        }

        public Builder configuration(AxonDBConfiguration configuration) {
            assertNonNull(configuration, "The configuration may not be null");
            this.configuration = configuration;
            return this;
        }

        public Builder eventStoreClient(AxonDBClient eventStoreClient) {
            assertNonNull(eventStoreClient, "The eventStoreClient may not be null");
            this.eventStoreClient = eventStoreClient;
            return this;
        }

        public Builder snapshotSerializer(Serializer snapshotSerializer) {
            assertNonNull(snapshotSerializer, "The Snapshot Serializer may not be null");
            this.snapshotSerializer = snapshotSerializer;
            return this;
        }

        public Builder eventSerializer(Serializer eventSerializer) {
            assertNonNull(eventSerializer, "The Event Serializer may not be null");
            this.eventSerializer = eventSerializer;
            return this;
        }

        public Builder upcasterChain(EventUpcaster upcasterChain) {
            assertNonNull(upcasterChain, "EventUpcaster may not be null");
            this.upcasterChain = upcasterChain;
            return this;
        }

        public Builder buildStorageEngineIfMissing() {
            if (storageEngine == null) {
                buildStorageEngine();
            }
            return this;
        }

        private void buildStorageEngine() {
            super.storageEngine(new AxonIQEventStorageEngine(snapshotSerializer, eventSerializer, upcasterChain, configuration, eventStoreClient));
        }

        /**
         * Validates whether the fields contained in this Builder are set accordingly.
         *
         * @throws AxonConfigurationException if one field is asserted to be incorrect according to the Builder's
         *                                    specifications
         */
        @Override
        protected void validate() throws AxonConfigurationException {
            super.validate();
        }
    }

    @Override
    protected AxonIQEventStorageEngine storageEngine() {
        return (AxonIQEventStorageEngine) super.storageEngine();
    }

    private static class AxonIQEventStorageEngine extends AbstractEventStorageEngine {

        public static final int ALLOW_SNAPSHOTS_MAGIC_VALUE = -42;
        private final String APPEND_EVENT_TRANSACTION = this + "/APPEND_EVENT_TRANSACTION";

        private final EventUpcaster upcasterChain;
        private final AxonDBConfiguration configuration;
        private final AxonDBClient eventStoreClient;
        private final GrpcMetaDataConverter converter;

        private AxonIQEventStorageEngine(Serializer serializer,
                                         EventUpcaster upcasterChain,
                                         AxonDBConfiguration configuration,
                                         AxonDBClient eventStoreClient) {
            super(builder()
                .eventSerializer(serializer)
                .snapshotSerializer(serializer)
                .upcasterChain(upcasterChain));
            this.upcasterChain = getOrDefault(upcasterChain, NoOpEventUpcaster.INSTANCE);
            this.configuration = configuration;
            this.eventStoreClient = eventStoreClient;
            this.converter = new GrpcMetaDataConverter(serializer);
        }

        private AxonIQEventStorageEngine(Serializer serializer,
                                         Serializer eventSerializer,
                                         EventUpcaster upcasterChain,
                                         AxonDBConfiguration configuration,
                                         AxonDBClient eventStoreClient) {
            super(builder()
                .eventSerializer(eventSerializer)
                .snapshotSerializer(serializer)
                .upcasterChain(upcasterChain));
            this.upcasterChain = getOrDefault(upcasterChain, NoOpEventUpcaster.INSTANCE);
            this.configuration = configuration;
            this.eventStoreClient = eventStoreClient;
            this.converter = new GrpcMetaDataConverter(serializer);
        }

        private static Builder builder() {
            return new Builder();
        }

        @Override
        protected void appendEvents(List<? extends EventMessage<?>> events, Serializer serializer) {
            AppendEventTransaction sender;
            if (CurrentUnitOfWork.isStarted()) {
                sender = CurrentUnitOfWork.get().root().getOrComputeResource(APPEND_EVENT_TRANSACTION, k -> {
                    AppendEventTransaction appendEventTransaction = eventStoreClient.createAppendEventConnection();
                    CurrentUnitOfWork.get().root().onRollback(u -> appendEventTransaction.rollback(u.getExecutionResult().getExceptionResult()));
                    CurrentUnitOfWork.get().root().onCommit(u -> appendEventTransaction.commit());
                    return appendEventTransaction;
                });
            } else {
                sender = eventStoreClient.createAppendEventConnection();
            }
            for (EventMessage<?> eventMessage : events) {
                sender.append(map(eventMessage, serializer));
            }
            if (!CurrentUnitOfWork.isStarted()) {
                sender.commit();
            }
        }

        public Event map(EventMessage eventMessage, Serializer serializer) {
            Event.Builder builder = Event.newBuilder();
            if (eventMessage instanceof GenericDomainEventMessage) {
                builder.setAggregateIdentifier(((GenericDomainEventMessage) eventMessage).getAggregateIdentifier())
                    .setAggregateSequenceNumber(((GenericDomainEventMessage) eventMessage).getSequenceNumber())
                    .setAggregateType(((GenericDomainEventMessage) eventMessage).getType());
            }
            SerializedObject<byte[]> serializedPayload = eventMessage.serializePayload(serializer, byte[].class);
            builder.setMessageIdentifier(eventMessage.getIdentifier())
                .setPayload(io.axoniq.platform.SerializedObject.newBuilder()
                    .setType(serializedPayload.getType().getName())
                    .setRevision(getOrDefault(serializedPayload.getType().getRevision(), ""))
                    .setData(ByteString.copyFrom(serializedPayload.getData())))
                .setTimestamp(eventMessage.getTimestamp().toEpochMilli());
            eventMessage.getMetaData().forEach((k, v) -> builder.putMetaData(k, converter.convertToMetaDataValue(v)));
            return builder.build();
        }


        @Override
        protected void storeSnapshot(DomainEventMessage<?> snapshot, Serializer serializer) {
            try {
                eventStoreClient.appendSnapshot(map(snapshot, serializer)).whenComplete((c, e) -> {
                    if (e != null) {
                        logger.warn("Error occurred while creating a snapshot", e);
                    } else if (c != null) {
                        if (c.getSuccess()) {
                            logger.info("Snapshot created");
                        } else {
                            logger.warn("Snapshot creation failed for unknown reason. Check server logs for details.");
                        }
                    }
                });
            } catch (Throwable e) {
                throw AxonErrorMapping.convert(e);
            }
        }


        @Override
        protected Stream<? extends DomainEventData<?>> readEventData(String aggregateIdentifier, long firstSequenceNumber) {
            logger.debug("Reading events for aggregate id {}", aggregateIdentifier);
            GetAggregateEventsRequest.Builder request = GetAggregateEventsRequest.newBuilder()
                .setAggregateId(aggregateIdentifier);
            if (firstSequenceNumber > 0) {
                request.setInitialSequence(firstSequenceNumber);
            } else if (firstSequenceNumber == ALLOW_SNAPSHOTS_MAGIC_VALUE) {
                request.setAllowSnapshots(true);
            }
            try {
                return eventStoreClient.listAggregateEvents(request.build()).map(GrpcBackedDomainEventData::new);
            } catch (Exception e) {
                throw AxonErrorMapping.convert(e);
            }
        }

        public TrackingEventStream openStream(TrackingToken trackingToken) {
            Assert.isTrue(trackingToken == null || trackingToken instanceof GlobalSequenceTrackingToken,
                () -> "Invalid tracking token type. Must be GlobalSequenceTrackingToken.");
            long nextToken = trackingToken == null ? 0 : ((GlobalSequenceTrackingToken) trackingToken).getGlobalIndex() + 1;
            EventBuffer consumer = new EventBuffer(upcasterChain, getEventSerializer());

            logger.info("open stream: {}", nextToken);
            StreamObserver<GetEventsRequest> requestStream = eventStoreClient.listEvents(new StreamObserver<EventWithToken>() {
                @Override
                public void onNext(EventWithToken eventWithToken) {
                    logger.debug("Received event with token: {}", eventWithToken.getToken());
                    consumer.push(eventWithToken);
                }

                @Override
                public void onError(Throwable throwable) {
                    logger.error("Failed to receive events", throwable);
                    consumer.fail(new EventStoreException("Error while reading events from the server", throwable));
                }

                @Override
                public void onCompleted() {

                }
            });
            FlowControllingStreamObserver<GetEventsRequest> observer = new FlowControllingStreamObserver<>(
                requestStream, configuration, t -> GetEventsRequest.newBuilder().setNumberOfPermits(t).build(), t -> false);

            GetEventsRequest request = GetEventsRequest.newBuilder()
                .setTrackingToken(nextToken)
                .setNumberOfPermits(configuration.getInitialNrOfPermits())
                .build();
            observer.onNext(request);

            consumer.registerCloseListener((eventConsumer) -> observer.onCompleted());
//                                                   observer.onError( new RuntimeException("Closed by client")));
            consumer.registerConsumeListener(observer::markConsumed);
            return consumer;
        }

        public QueryResultStream query(String query, boolean liveUpdates) {
            QueryResultBuffer consumer = new QueryResultBuffer();

            logger.debug("query: {}", query);
            StreamObserver<QueryEventsRequest> requestStream = eventStoreClient.query(new StreamObserver<QueryEventsResponse>() {
                @Override
                public void onNext(QueryEventsResponse eventWithToken) {
                    consumer.push(eventWithToken);
                }

                @Override
                public void onError(Throwable throwable) {
                    logger.info("Failed to receive events - {}", throwable.getMessage());
                    consumer.fail(new EventStoreException("Error while reading query results from the server", throwable));
                }

                @Override
                public void onCompleted() {
                    consumer.close();
                }
            });
            FlowControllingStreamObserver<QueryEventsRequest> observer = new FlowControllingStreamObserver<>(
                requestStream, configuration, t -> QueryEventsRequest.newBuilder().setNumberOfPermits(t).build(), t -> false);

            observer.onNext(QueryEventsRequest.newBuilder()
                .setQuery(query)
                .setNumberOfPermits(configuration.getInitialNrOfPermits())
                .setLiveEvents(liveUpdates)
                .build());

            consumer.registerCloseListener((eventConsumer) -> observer.onCompleted());
            consumer.registerConsumeListener(observer::markConsumed);
            return consumer;
        }


        @Override
        public DomainEventStream readEvents(String aggregateIdentifier) {
            Stream<? extends DomainEventData<?>> input = this.readEventData(aggregateIdentifier, ALLOW_SNAPSHOTS_MAGIC_VALUE);
            return DomainEventStream.of(input.map(this::upcastAndDeserializeDomainEvent).filter(Objects::nonNull));
        }

        private DomainEventMessage<?> upcastAndDeserializeDomainEvent(DomainEventData<?> domainEventData) {
            DomainEventStream upcastedStream = upcastAndDeserializeDomainEvents(
                Stream.of(domainEventData),
                new GrpcMetaDataAwareSerializer(isSnapshot(domainEventData) ? getSnapshotSerializer() : getEventSerializer()),
                upcasterChain);
            return upcastedStream.hasNext() ? upcastedStream.next() : null;
        }

        private DomainEventStream upcastAndDeserializeDomainEvents(
            Stream<? extends DomainEventData<?>> eventEntryStream, Serializer serializer, EventUpcaster upcasterChain) {
            AtomicReference<Long> currentSequenceNumber = new AtomicReference<>();
            Stream<IntermediateEventRepresentation> upcastResult =
                upcastAndDeserialize(eventEntryStream, upcasterChain, entry -> {
                    InitialEventRepresentation result = new InitialEventRepresentation(entry, serializer);
                    currentSequenceNumber.set(result.getSequenceNumber().get());
                    return result;
                });
            Stream<? extends DomainEventMessage<?>> stream = upcastResult.map(ir -> {
                SerializedMessage<?> serializedMessage = new SerializedMessage<>(ir.getMessageIdentifier(),
                    new LazyDeserializingObject<>(
                        ir::getData,
                        ir.getType(), serializer),
                    ir.getMetaData());
                if (ir.getTrackingToken().isPresent()) {
                    return new GenericTrackedDomainEventMessage<>(ir.getTrackingToken().get(), ir.getAggregateType().get(),
                        ir.getAggregateIdentifier().get(),
                        ir.getSequenceNumber().get(), serializedMessage,
                        ir::getTimestamp);
                } else {
                    return new GenericDomainEventMessage<>(ir.getAggregateType().get(), ir.getAggregateIdentifier().get(),
                        ir.getSequenceNumber().get(), serializedMessage,
                        ir::getTimestamp);
                }
            });
            return DomainEventStream.of(stream, currentSequenceNumber::get);
        }

        private Stream<IntermediateEventRepresentation> upcastAndDeserialize(Stream<? extends EventData<?>> eventEntryStream, EventUpcaster upcasterChain,
                                                                             Function<EventData<?>, IntermediateEventRepresentation> entryConverter) {
            return upcasterChain.upcast(eventEntryStream.map(entryConverter));
        }

        private boolean isSnapshot(DomainEventData<?> domainEventData) {
            if (domainEventData instanceof GrpcBackedDomainEventData) {
                GrpcBackedDomainEventData grpcBackedDomainEventData = (GrpcBackedDomainEventData) domainEventData;
                return grpcBackedDomainEventData.isSnapshot();
            }
            return false;
        }

        @Override
        public Optional<Long> lastSequenceNumberFor(String aggregateIdentifier) {
            try {
                ReadHighestSequenceNrResponse lastSequenceNr = eventStoreClient
                    .lastSequenceNumberFor(aggregateIdentifier).get();
                return lastSequenceNr.getToSequenceNr() < 0 ? Optional.empty() : Optional.of(lastSequenceNr.getToSequenceNr());
            } catch (Throwable e) {
                throw AxonErrorMapping.convert(e);
            }
        }

        @Override
        public TrackingToken createTailToken() {
            try {
                io.axoniq.axondb.grpc.TrackingToken token = eventStoreClient.getFirstToken().get();
                if (token.getToken() < 0) {
                    return null;
                }
                return new GlobalSequenceTrackingToken(token.getToken() - 1);
            } catch (Throwable e) {
                throw AxonErrorMapping.convert(e);
            }
        }

        @Override
        public TrackingToken createHeadToken() {
            try {
                io.axoniq.axondb.grpc.TrackingToken token = eventStoreClient.getLastToken().get();
                return new GlobalSequenceTrackingToken(token.getToken());
            } catch (Throwable e) {
                throw AxonErrorMapping.convert(e);
            }
        }

        @Override
        public TrackingToken createTokenAt(Instant instant) {
            try {
                io.axoniq.axondb.grpc.TrackingToken token = eventStoreClient.getTokenAt(instant).get();
                if (token.getToken() < 0) {
                    return null;
                }
                return new GlobalSequenceTrackingToken(token.getToken() - 1);
            } catch (Throwable e) {
                throw AxonErrorMapping.convert(e);
            }
        }

        @Override
        protected Stream<? extends TrackedEventData<?>> readEventData(TrackingToken trackingToken, boolean mayBlock) {
            throw new UnsupportedOperationException("This method is not optimized for the AxonIQ Event Store and should not be used");
        }

        @Override
        protected Stream<? extends DomainEventData<?>> readSnapshotData(String aggregateIdentifier) {
            // snapshots are automatically fetched server-side, which is faster
            return Stream.empty();
        }

        private static class Builder extends AbstractEventStorageEngine.Builder {

        }

    }
}
