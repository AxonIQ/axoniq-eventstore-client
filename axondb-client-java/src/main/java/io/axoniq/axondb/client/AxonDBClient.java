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

package io.axoniq.axondb.client;

import io.axoniq.axondb.Event;
import io.axoniq.axondb.client.util.Broadcaster;
import io.axoniq.axondb.client.util.EventCipher;
import io.axoniq.axondb.client.util.EventStoreClientException;
import io.axoniq.axondb.client.util.GrpcExceptionParser;
import io.axoniq.axondb.grpc.Confirmation;
import io.axoniq.axondb.grpc.EventStoreGrpc;
import io.axoniq.axondb.grpc.EventWithToken;
import io.axoniq.axondb.grpc.GetAggregateEventsRequest;
import io.axoniq.axondb.grpc.GetEventsRequest;
import io.axoniq.axondb.grpc.GetFirstTokenRequest;
import io.axoniq.axondb.grpc.GetLastTokenRequest;
import io.axoniq.axondb.grpc.GetTokenAtRequest;
import io.axoniq.axondb.grpc.QueryEventsRequest;
import io.axoniq.axondb.grpc.QueryEventsResponse;
import io.axoniq.axondb.grpc.ReadHighestSequenceNrRequest;
import io.axoniq.axondb.grpc.ReadHighestSequenceNrResponse;
import io.axoniq.axondb.grpc.TrackingToken;
import io.axoniq.platform.grpc.ClientIdentification;
import io.axoniq.platform.grpc.NodeInfo;
import io.axoniq.platform.grpc.PlatformInboundInstruction;
import io.axoniq.platform.grpc.PlatformInfo;
import io.axoniq.platform.grpc.PlatformOutboundInstruction;
import io.axoniq.platform.grpc.PlatformServiceGrpc;
import io.grpc.Channel;
import io.grpc.ClientInterceptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

/**
 */
public class AxonDBClient {
    private final Logger logger = LoggerFactory.getLogger(AxonDBClient.class);

    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
    private final AxonDBConfiguration eventStoreConfiguration;
    private final ClientInterceptor[] interceptors;
    private final EventCipher eventCipher;

    private final AtomicReference<PlatformInfo> eventStoreServer = new AtomicReference<>();
    private final ChannelManager channelManager;
    private final long commitTimeout;
    private boolean shutdown;
    private final Map<UUID,Runnable> connectionCloseListeners = new ConcurrentHashMap<>();
    private SendingStreamObserver<PlatformInboundInstruction> streamToAxonDB;

    public AxonDBClient(AxonDBConfiguration eventStoreConfiguration) {
        this.eventStoreConfiguration = eventStoreConfiguration;
        this.interceptors = new ClientInterceptor[] {
                new TokenAddingInterceptor(eventStoreConfiguration.getToken()),
                new ContextAddingInterceptor(eventStoreConfiguration.getContext())
        };
        this.channelManager = new ChannelManager(eventStoreConfiguration.isSslEnabled(), eventStoreConfiguration.getCertFile(),
                                                 eventStoreConfiguration.getKeepAliveTime(),
                                                 eventStoreConfiguration.getKeepAliveTimeout());
        this.eventCipher = eventStoreConfiguration.eventCipher();
        this.commitTimeout = eventStoreConfiguration.getCommitTimeout();
    }

    public void shutdown() {
        shutdown = true;
        channelManager.cleanup();
    }

    private EventStoreGrpc.EventStoreStub eventStoreStub() {
        return EventStoreGrpc.newStub(getChannelToEventStore()).withInterceptors(interceptors);
    }


    private PlatformInfo discoverEventStore() {
        eventStoreServer.set(null);
        Broadcaster<PlatformInfo> b = new Broadcaster<>(eventStoreConfiguration.serverNodes(), this::retrieveClusterInfo, this::nodeReceived);
        try {
            b.broadcast(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ClientConnectionException("Thread was interrupted while attempting to connect to the server", e);
        }
        return eventStoreServer.get();
    }

    private void nodeReceived(PlatformInfo node) {
        logger.info("Received: {}:{}", node.getPrimary().getHostName(), node.getPrimary().getGrpcPort());
        eventStoreServer.set(node);
    }

    private void retrieveClusterInfo(NodeInfo nodeInfo, StreamObserver<PlatformInfo> streamObserver) {
        Channel channel = channelManager.getChannel(nodeInfo);
        PlatformServiceGrpc.PlatformServiceStub clusterManagerStub = PlatformServiceGrpc.newStub(channel).withInterceptors(interceptors);
        clusterManagerStub.getPlatformServer(ClientIdentification.newBuilder().build(), streamObserver);
    }

    private Channel getChannelToEventStore() {
        if (shutdown) return null;
        CompletableFuture<PlatformInfo> masterInfoCompletableFuture = new CompletableFuture<>();
        getEventStoreAsync(eventStoreConfiguration.getConnectionRetryCount(), masterInfoCompletableFuture);
        try {
            return channelManager.getChannel(masterInfoCompletableFuture.get().getPrimary());
        } catch( ExecutionException e) {
            throw (RuntimeException) e.getCause();
        } catch (InterruptedException e) {
            throw new EventStoreClientException("AXONIQ-0001", e.getMessage(), e);
        }
    }

    private void getEventStoreAsync(int retries, CompletableFuture<PlatformInfo> result) {
        PlatformInfo currentEventStore = eventStoreServer.get();
        if (currentEventStore != null) {
            result.complete(currentEventStore);
        } else {
            currentEventStore = discoverEventStore();
            if (currentEventStore != null) {
                openStream(currentEventStore);
                result.complete(currentEventStore);
            } else {
                if (retries > 0)
                    executorService.schedule(() -> getEventStoreAsync(retries - 1, result),
                                             eventStoreConfiguration.getConnectionRetry(), TimeUnit.MILLISECONDS);
                else
                    result.completeExceptionally(new EventStoreClientException("AXONIQ-0001", "No available event store server"));
            }
        }
    }

    private void openStream(PlatformInfo currentEventStore) {
        Channel channel = channelManager.getChannel(currentEventStore.getPrimary());
        PlatformServiceGrpc.PlatformServiceStub platformService = PlatformServiceGrpc.newStub(channel).withInterceptors(
                interceptors);
        streamToAxonDB = new SendingStreamObserver<>(platformService.openStream(new StreamObserver<PlatformOutboundInstruction>() {
            @Override
            public void onNext(PlatformOutboundInstruction value) {
            }

            @Override
            public void onError(Throwable t) {
                stopChannelToEventStore();
            }

            @Override
            public void onCompleted() {
                stopChannelToEventStore();
            }
        }));

        streamToAxonDB.onNext(PlatformInboundInstruction.newBuilder()
                                                        .setRegister(ClientIdentification.getDefaultInstance())
                                                        .build());
    }

    private void stopChannelToEventStore() {
        PlatformInfo current = eventStoreServer.getAndSet(null);
        if (current != null) {
            logger.info("Shutting down gRPC channel");
            connectionCloseListeners.forEach((key,callback) -> callback.run());
            connectionCloseListeners.clear();
            channelManager.shutdown(current);
            closeStream();
        }
    }

    private void closeStream() {
        if( streamToAxonDB != null) {
            streamToAxonDB.onCompleted();
            streamToAxonDB = null;
        }
    }

    /**
     * Retrieves the events for an aggregate described in given {@code request}.
     *
     * @param request The request describing the aggregate to retrieve messages for
     * @return a Stream providing access to Events published by the aggregate described in the request
     * @throws ExecutionException   when an error was reported while reading events
     * @throws InterruptedException when the thread was interrupted while reading events from the server
     */
    public Stream<Event> listAggregateEvents(GetAggregateEventsRequest request) throws ExecutionException, InterruptedException {
        CompletableFuture<Stream<Event>> stream = new CompletableFuture<>();
        long before = System.currentTimeMillis();

        eventStoreStub().listAggregateEvents(request, new StreamObserver<Event>() {
            Stream.Builder<Event> eventStream = Stream.builder();
            int count;

            @Override
            public void onNext(Event event) {
                eventStream.accept(eventCipher.decrypt(event));
                count++;
            }

            @Override
            public void onError(Throwable throwable) {
                checkConnectionException(throwable);
                stream.completeExceptionally(GrpcExceptionParser.parse(throwable));
            }

            @Override
            public void onCompleted() {
                stream.complete(eventStream.build());
                if (logger.isDebugEnabled()) {
                    logger.debug("Done request for {}: {}ms, {} events", request.getAggregateId(), System.currentTimeMillis() - before, count);
                }
            }
        });
        return stream.get();
    }

    /**
     *
     * @param responseStreamObserver: observer for messages from server
     * @return stream observer to send request messages to server
     */
    public StreamObserver<GetEventsRequest> listEvents(StreamObserver<EventWithToken> responseStreamObserver) {
        UUID id = UUID.randomUUID();
        StreamObserver<EventWithToken> wrappedStreamObserver = new StreamObserver<EventWithToken>() {
            @Override
            public void onNext(EventWithToken eventWithToken) {
                responseStreamObserver.onNext(eventCipher.decrypt(eventWithToken));
            }

            @Override
            public void onError(Throwable throwable) {
                checkConnectionException(throwable);
                responseStreamObserver.onError(GrpcExceptionParser.parse(throwable));
                connectionCloseListeners.remove(id);
            }

            @Override
            public void onCompleted() {
                responseStreamObserver.onCompleted();
                connectionCloseListeners.remove(id);
            }
        };
        StreamObserver<GetEventsRequest> requestStream = new CancelOnCompleteStreamObserver<>(eventStoreStub().listEvents(wrappedStreamObserver));
        connectionCloseListeners.put(id, () -> {
            try {
                requestStream.onCompleted();
            } catch(Exception ignore) {}
            responseStreamObserver.onError(new RuntimeException("Connection to AxonDB lost"));
        });
        return requestStream;
    }

    public CompletableFuture<Confirmation> appendSnapshot(Event snapshot) {

        CompletableFuture<Confirmation> confirmationFuture = new CompletableFuture<>();
        eventStoreStub().appendSnapshot(eventCipher.encrypt(snapshot), new SingleResultStreamObserver<>(confirmationFuture));
        return confirmationFuture;
    }

    public AppendEventTransaction createAppendEventConnection() {
        CompletableFuture<Confirmation> futureConfirmation = new CompletableFuture<>();
        return new AppendEventTransaction(eventStoreStub().appendEvent(new SingleResultStreamObserver<>(futureConfirmation)), futureConfirmation, commitTimeout, eventCipher);
    }

    private void checkConnectionException(Throwable ex) {
        if (ex instanceof StatusRuntimeException && ((StatusRuntimeException) ex).getStatus().getCode().equals(Status.UNAVAILABLE.getCode())) {
            stopChannelToEventStore();
        }
    }

    public StreamObserver<QueryEventsRequest> query(StreamObserver<QueryEventsResponse> responseStreamObserver) {
        StreamObserver<QueryEventsResponse> wrappedStreamObserver = new StreamObserver<QueryEventsResponse>() {
            @Override
            public void onNext(QueryEventsResponse eventWithToken) {
                responseStreamObserver.onNext(eventWithToken);
            }

            @Override
            public void onError(Throwable throwable) {
                checkConnectionException(throwable);
                responseStreamObserver.onError(GrpcExceptionParser.parse(throwable));
            }

            @Override
            public void onCompleted() {
                responseStreamObserver.onCompleted();

            }
        };
        return eventStoreStub().queryEvents(wrappedStreamObserver);
    }

    public CompletableFuture<ReadHighestSequenceNrResponse> lastSequenceNumberFor(String aggregateIdentifier) {
        CompletableFuture<ReadHighestSequenceNrResponse> completableFuture = new CompletableFuture<>();
        eventStoreStub().readHighestSequenceNr(ReadHighestSequenceNrRequest.newBuilder()
                                                                           .setAggregateId(aggregateIdentifier).build(),
                                               new SingleResultStreamObserver<>(completableFuture));
        return completableFuture;
    }

    public CompletableFuture<TrackingToken> getLastToken() {
        CompletableFuture<TrackingToken> trackingTokenFuture = new CompletableFuture<>();
        eventStoreStub().getLastToken(GetLastTokenRequest.getDefaultInstance(), new SingleResultStreamObserver<>(trackingTokenFuture));
        return trackingTokenFuture;
    }

    public CompletableFuture<TrackingToken> getFirstToken() {
        CompletableFuture<TrackingToken> trackingTokenFuture = new CompletableFuture<>();
        eventStoreStub().getFirstToken(GetFirstTokenRequest.getDefaultInstance(), new SingleResultStreamObserver<>(trackingTokenFuture));
        return trackingTokenFuture;
    }

    public CompletableFuture<TrackingToken> getTokenAt(Instant instant) {
        CompletableFuture<TrackingToken> trackingTokenFuture = new CompletableFuture<>();
        eventStoreStub().getTokenAt(GetTokenAtRequest.newBuilder()
                                                     .setInstant(instant.toEpochMilli())
                                                     .build(), new SingleResultStreamObserver<>(trackingTokenFuture));
        return trackingTokenFuture;
    }

    private class CancelOnCompleteStreamObserver<T> implements StreamObserver<T> {
        private final ClientCallStreamObserver<T> requestStream;

        private CancelOnCompleteStreamObserver(StreamObserver<T> requestStream) {
            this.requestStream = (ClientCallStreamObserver<T>) requestStream;
        }

        @Override
        public void onNext(T t) {
            requestStream.onNext(t);
        }

        @Override
        public void onError(Throwable throwable) {
            requestStream.onError(throwable);
        }

        @Override
        public void onCompleted() {
            requestStream.cancel("Request stream was closed", new EventStoreClientException("AXONIQ-0001", "Request stream was closed"));
        }
    }

    private class SingleResultStreamObserver<T> implements StreamObserver<T> {
        private final CompletableFuture<T> future;

        private SingleResultStreamObserver(CompletableFuture<T> future) {
            this.future = future;
        }

        @Override
        public void onNext(T t) {
            future.complete(t);
        }

        @Override
        public void onError(Throwable throwable) {
            checkConnectionException(throwable);
            future.completeExceptionally(GrpcExceptionParser.parse(throwable));
        }

        @Override
        public void onCompleted() {
            if( ! future.isDone()) future.completeExceptionally(new EventStoreClientException("AXONIQ-0001", "Async call completed before answer"));
        }
    }

}
