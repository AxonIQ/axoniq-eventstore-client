package io.axoniq.eventstore.gateway;

import io.axoniq.eventstore.Event;
import io.axoniq.eventstore.EventStoreConfiguration;
import io.axoniq.eventstore.grpc.EventWithToken;
import io.axoniq.eventstore.grpc.*;
import io.axoniq.eventstore.util.Broadcaster;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

/**
 * Author: marc
 */
public class EventStoreGateway {
    private final Logger logger = LoggerFactory.getLogger(EventStoreGateway.class);

    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
    private final EventStoreConfiguration eventStoreConfiguration;
    private final TokenAddingInterceptor tokenAddingInterceptor;

    private final AtomicReference<ClusterInfo> eventStoreServer = new AtomicReference<>();
    private final ChannelManager channelManager;
    private boolean shutdown;

    public EventStoreGateway(EventStoreConfiguration eventStoreConfiguration ) {
        this.eventStoreConfiguration = eventStoreConfiguration;
        this.tokenAddingInterceptor = new TokenAddingInterceptor(eventStoreConfiguration.getToken());
        this.channelManager = new ChannelManager(eventStoreConfiguration.getCertFile());
    }

    public void shutdown() {
        shutdown = true;
        channelManager.cleanup();
    }

    private EventStoreGrpc.EventStoreStub eventStoreStub() {
        return EventStoreGrpc.newStub(getChannelToEventStore()).withInterceptors(tokenAddingInterceptor);
    }

    private ClusterInfo discoverEventStore() {
        eventStoreServer.set(null);
        Broadcaster<ClusterInfo> b = new Broadcaster<>(eventStoreConfiguration.getServerNodes(), this::retrieveClusterInfo, this::nodeReceived);
        b.broadcast(TimeUnit.SECONDS, 1);
        return eventStoreServer.get();
    }

    private void nodeReceived(ClusterInfo node) {
        logger.info("Received: {}:{}", node.getMaster().getHostName(), node.getMaster().getGrpcPort());
        eventStoreServer.set(node);
    }

    private void retrieveClusterInfo(NodeInfo nodeInfo, StreamObserver<ClusterInfo> streamObserver) {
        Channel channel = channelManager.getChannel(nodeInfo);
        ClusterGrpc.ClusterStub clusterManagerStub = ClusterGrpc.newStub(channel).withInterceptors(new TokenAddingInterceptor(eventStoreConfiguration.getToken()));
        clusterManagerStub.retrieveClusterInfo(RetrieveClusterInfoRequest.newBuilder().build(), streamObserver);
    }

    private Channel getChannelToEventStore() {
        if( shutdown) return null;
        CompletableFuture<ClusterInfo> masterInfoCompletableFuture = new CompletableFuture<>();
        getEventStoreAsync(eventStoreConfiguration.getConnectionRetryCount(), masterInfoCompletableFuture);
        try {
            return channelManager.getChannel(masterInfoCompletableFuture.get().getMaster());
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private void getEventStoreAsync(int retries, CompletableFuture<ClusterInfo> result) {
        ClusterInfo currentEventStore = eventStoreServer.get();
        if( currentEventStore != null) {
            result.complete(currentEventStore);
        } else  {
            currentEventStore = discoverEventStore();
            if( currentEventStore != null) {
                result.complete(currentEventStore);
            } else {
                if( retries > 0)
                    executorService.schedule( () -> getEventStoreAsync( retries-1, result),
                            eventStoreConfiguration.getConnectionRetry(), TimeUnit.MILLISECONDS);
                else
                    result.completeExceptionally(new RuntimeException("No available event store server"));
            }
        }
    }

    private void stopChannelToEventStore() {
        eventStoreServer.getAndUpdate(current -> {
            if( current != null) {
                logger.info("Shutting down gRPC channel");
                channelManager.shutdown(current);
            }
            return null;
        });
    }

    public Stream<Event> listAggregateEvents(GetAggregateEventsRequest request) throws ExecutionException, InterruptedException {
        CompletableFuture<Stream<Event>> stream  = new CompletableFuture<>();
        long before = System.currentTimeMillis();
        eventStoreStub().listAggregateEvents(request, new StreamObserver<Event>() {
            Stream.Builder<Event> eventStream = Stream.builder();
            int count;
            @Override
            public void onNext(Event event) {
                eventStream.accept(event);
                count++;
            }

            @Override
            public void onError(Throwable throwable) {
                checkConnectionException(throwable);
                stream.completeExceptionally(throwable);
            }

            @Override
            public void onCompleted() {
                stream.complete(eventStream.build());
                logger.debug("Done request for {}: {}ms, {} events", request.getAggregateId(), System.currentTimeMillis() - before, count);
            }
        });
        return stream.get();
    }

    public StreamObserver<GetEventsRequest> listEvents(StreamObserver<EventWithToken> responseStreamObserver) {
        StreamObserver<EventWithToken> wrappedStreamObserver = new StreamObserver<EventWithToken>() {
            @Override
            public void onNext(EventWithToken eventWithToken) {
                responseStreamObserver.onNext(eventWithToken);
            }

            @Override
            public void onError(Throwable throwable) {
                checkConnectionException(throwable);
                responseStreamObserver.onError(throwable);
            }

            @Override
            public void onCompleted() {
                responseStreamObserver.onCompleted();

            }
        };
        return eventStoreStub().listEvents(wrappedStreamObserver);
    }

    public CompletableFuture<Confirmation> appendSnapshot(EventWithContext snapshot) {

        CompletableFuture<Confirmation> confirmationFuture = new CompletableFuture<>();
        eventStoreStub().appendSnapshot(snapshot, new StreamObserver<Confirmation>() {
            @Override
            public void onNext(Confirmation confirmation) {
                confirmationFuture.complete(confirmation);
            }

            @Override
            public void onError(Throwable throwable) {
                checkConnectionException(throwable);
                confirmationFuture.completeExceptionally(throwable);
            }

            @Override
            public void onCompleted() {

            }
        });

        return confirmationFuture;
    }

    public AppendEventTransaction createAppendEventConnection() {
        CompletableFuture<Confirmation> futureConfirmation = new CompletableFuture<>();
        return new AppendEventTransaction(eventStoreStub().appendEvent(new StreamObserver<Confirmation>() {
            @Override
            public void onNext(Confirmation confirmation) {
                futureConfirmation.complete(confirmation);
            }

            @Override
            public void onError(Throwable throwable) {
                checkConnectionException(throwable);
                futureConfirmation.completeExceptionally(throwable);
            }

            @Override
            public void onCompleted() {
                // no-op: already
            }
        }), futureConfirmation);
    }

    private void checkConnectionException(Throwable ex) {
        if( ex instanceof StatusRuntimeException && ((StatusRuntimeException)ex).getStatus().getCode().equals(Status.UNAVAILABLE.getCode()) ) {
            stopChannelToEventStore();
        }
    }

}
