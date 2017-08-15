package io.axoniq.eventstore;

import io.axoniq.eventstore.grpc.ClusterGrpc;
import io.axoniq.eventstore.grpc.JoinRequest;
import io.axoniq.eventstore.grpc.MasterInfo;
import io.axoniq.eventstore.util.Broadcaster;
import io.axoniq.eventstore.util.ChannelManager;
import io.grpc.Channel;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Author: marc
 */
@Component
public class EventStoreConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(AxoniqEventStore.class);
    private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
    @Value("${axoniq.eventstore.servers:#{null}}")
    private String servers;
    private List<MasterInfo> serverNodes = new ArrayList<>();
    private final AtomicReference<MasterInfo> eventStoreServer = new AtomicReference<>();
    private ChannelManager channelManager;

    @Value("${axoniq.eventstore.flowControl.initialNrOfPermits:10000}")
    private Integer initialNrOfPermits;
    @Value("${axoniq.eventstore.flowControl.nrOfNewPermits:9000}")
    private Integer nrOfNewPermits;
    @Value("${axoniq.eventstore.flowControl.newPermitsThreshold:1000}")
    private Integer newPermitsThreshold;
    @Value("${axoniq.eventstore.token:#{null}}")
    private String token;
    @Value("${axoniq.eventstore.ssl.certChainFile:#{null}}")
    private String certFile;
    @Value("${axoniq.eventstore.connectionRetry:2500}")
    private long connectionRetry;
    @Value("${axoniq.eventstore.connectionRetryCount:5}")
    private int connectionRetryCount;

    private boolean shutdown;

    public  EventStoreConfiguration() {
    }

    @PostConstruct
    public void init(){
        if( servers != null) {
            String[] serverArr = servers.split(",");
            Arrays.stream(serverArr).forEach(serverString -> {
                String[] hostPort = serverString.trim().split(":", 2);
                MasterInfo nodeInfo = MasterInfo.newBuilder().setHostName(hostPort[0])
                        .setGrpcPort(Integer.valueOf(hostPort[1]))
                        .build();
                serverNodes.add(nodeInfo);
            });
        }

        channelManager = new ChannelManager(certFile);
    }

    @PreDestroy
    public void cleanup() {
        shutdown = true;
        channelManager.cleanup();
    }

    public String getToken() {
        return token;
    }

    public Integer getInitialNrOfPermits() {
        return initialNrOfPermits;
    }

    public Integer getNrOfNewPermits() {
        return nrOfNewPermits;
    }

    public Integer getNewPermitsThreshold() {
        return newPermitsThreshold;
    }

    private MasterInfo discoverEventStore() {
        eventStoreServer.set(null);
        Broadcaster<MasterInfo> b = new Broadcaster<>(serverNodes, this::join, this::nodeReceived);
        b.broadcast(TimeUnit.SECONDS, 1);
        return eventStoreServer.get();
    }

    private void nodeReceived(MasterInfo node) {
        logger.info("Received: {}:{}", node.getHostName(), node.getGrpcPort());
        eventStoreServer.set(node);
    }

    private void join(MasterInfo nodeInfo, StreamObserver<MasterInfo> streamObserver) {
        Channel channel = channelManager.getChannel(nodeInfo);
        ClusterGrpc.ClusterStub clusterManagerStub = ClusterGrpc.newStub(channel).withInterceptors(new TokenAddingInterceptor(token));
        clusterManagerStub.join(JoinRequest.newBuilder().build(), streamObserver);
    }

    public Channel getChannelToEventStore() {
        if( shutdown) return null;
        CompletableFuture<MasterInfo> masterInfoCompletableFuture = new CompletableFuture<>();
        getEventStoreAsync(connectionRetryCount, masterInfoCompletableFuture);
        try {
            return channelManager.getChannel(masterInfoCompletableFuture.get());
        } catch (InterruptedException | ExecutionException  e) {
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
                    executorService.schedule( () -> getEventStoreAsync( retries-1, result), connectionRetry, TimeUnit.MILLISECONDS);
                else
                    result.completeExceptionally(new RuntimeException("No available event store server"));
            }
        }
    }

    public void stopChannelToEventStore() {
        eventStoreServer.getAndUpdate(current -> {
            if( current != null) {
                logger.info("Shutting down gRPC channel");
                channelManager.shutdown(current);
            }
            return null;
        }) ;
    }

    public static Builder newBuilder(String servers) {
        return new Builder(servers);
    }

    public boolean isShutdown() {
        return shutdown;
    }

    public static class Builder {
        private EventStoreConfiguration instance;

        public Builder(String servers) {
            instance = new EventStoreConfiguration();
            instance.servers = servers;
            instance.initialNrOfPermits = 0;
            instance.nrOfNewPermits = 0;
            instance.newPermitsThreshold = 0;
            instance.connectionRetry = 2500;
            instance.connectionRetryCount = 5;
        }

        public Builder ssl( String certificateFilePath) {
            instance.certFile = certificateFilePath;
            return this;
        }

        public Builder token( String token) {
            instance.token = token;
            return this;
        }

        public Builder connectionRetry( long connectionRetryTime, int attempts) {
            instance.connectionRetry = connectionRetryTime;
            instance.connectionRetryCount = attempts;
            return this;
        }

        public EventStoreConfiguration build() {
            instance.init();
            return instance;
        }

        public Builder flowControl(Integer initialNrOfPermits, Integer nrOfNewPermits, Integer newPermitsThreshold) {
            instance.initialNrOfPermits = initialNrOfPermits;
            instance.nrOfNewPermits = nrOfNewPermits;
            instance.newPermitsThreshold = newPermitsThreshold;
            return this;
        }
    }

}
