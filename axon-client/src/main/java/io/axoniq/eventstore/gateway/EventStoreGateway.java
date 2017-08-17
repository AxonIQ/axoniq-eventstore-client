package io.axoniq.eventstore.gateway;

import io.axoniq.eventstore.Event;
import io.axoniq.eventstore.EventStoreConfiguration;
import io.axoniq.eventstore.EventWithToken;
import io.axoniq.eventstore.axon.AxoniqEventStore;
import io.axoniq.eventstore.grpc.*;
import io.axoniq.eventstore.util.Broadcaster;
import io.axoniq.eventstore.util.GrpcConnection;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

/**
 * Author: marc
 */
@Component
public class EventStoreGateway {
    private final Logger logger = LoggerFactory.getLogger(EventStoreGateway.class);

    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
    private final EventStoreConfiguration eventStoreConfiguration;
    private final TokenAddingInterceptor tokenAddingInterceptor;

    private final AtomicReference<MasterInfo> eventStoreServer = new AtomicReference<>();
    private ChannelManager channelManager;
    private boolean shutdown;

    public EventStoreGateway(EventStoreConfiguration eventStoreConfiguration ) {
        this.eventStoreConfiguration = eventStoreConfiguration;
        this.tokenAddingInterceptor = new TokenAddingInterceptor(eventStoreConfiguration.getToken());
    }

    @PostConstruct
    public void init(){
        channelManager = new ChannelManager(eventStoreConfiguration.getCertFile());
    }

    @PreDestroy
    public void cleanup() {
        shutdown = true;
        channelManager.cleanup();
    }


    private EventWriterGrpc.EventWriterStub eventWriterStub() {
        return EventWriterGrpc.newStub(getChannelToEventStore()).withInterceptors(tokenAddingInterceptor);
    }

    private EventReaderGrpc.EventReaderStub eventReaderStub() {
        return EventReaderGrpc.newStub(getChannelToEventStore()).withInterceptors(tokenAddingInterceptor);
    }

    private MasterInfo discoverEventStore() {
        eventStoreServer.set(null);
        Broadcaster<MasterInfo> b = new Broadcaster<>(eventStoreConfiguration.getServerNodes(), this::join, this::nodeReceived);
        b.broadcast(TimeUnit.SECONDS, 1);
        return eventStoreServer.get();
    }

    private void nodeReceived(MasterInfo node) {
        logger.info("Received: {}:{}", node.getHostName(), node.getGrpcPort());
        eventStoreServer.set(node);
    }

    private void join(MasterInfo nodeInfo, StreamObserver<MasterInfo> streamObserver) {
        Channel channel = channelManager.getChannel(nodeInfo);
        ClusterGrpc.ClusterStub clusterManagerStub = ClusterGrpc.newStub(channel).withInterceptors(new TokenAddingInterceptor(eventStoreConfiguration.getToken()));
        clusterManagerStub.join(JoinRequest.newBuilder().build(), streamObserver);
    }

    private Channel getChannelToEventStore() {
        if( shutdown) return null;
        CompletableFuture<MasterInfo> masterInfoCompletableFuture = new CompletableFuture<>();
        getEventStoreAsync(eventStoreConfiguration.getConnectionRetryCount(), masterInfoCompletableFuture);
        try {
            return channelManager.getChannel(masterInfoCompletableFuture.get());
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private void getEventStoreAsync(int retries, CompletableFuture<MasterInfo> result) {
        MasterInfo currentEventStore = eventStoreServer.get();
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
        eventReaderStub().listAggregateEvents(request, new StreamObserver<Event>() {
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
                logger.info("Done request for {}: {}ms, {} events", request.getAggregateId(), System.currentTimeMillis() - before, count);
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
        return eventReaderStub().listEvents(wrappedStreamObserver);
    }

    public CompletableFuture<Confirmation> appendSnapshot(EventWithContext snapshot) {

        CompletableFuture<Confirmation> confirmationFuture = new CompletableFuture<>();
        eventWriterStub().appendSnapshot(snapshot, new StreamObserver<Confirmation>() {
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

    public GrpcConnection createAppendEventConnection() {
        CompletableFuture<Confirmation> futureConfirmation = new CompletableFuture<>();
        return new GrpcConnection(eventWriterStub().appendEvent(new StreamObserver<Confirmation>() {
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
