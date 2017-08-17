package io.axoniq.eventstore;

import io.axoniq.eventstore.axon.AxoniqEventStore;
import io.axoniq.eventstore.gateway.TokenAddingInterceptor;
import io.axoniq.eventstore.grpc.ClusterGrpc;
import io.axoniq.eventstore.grpc.JoinRequest;
import io.axoniq.eventstore.grpc.MasterInfo;
import io.axoniq.eventstore.util.Broadcaster;
import io.axoniq.eventstore.gateway.ChannelManager;
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
    @Value("${axoniq.eventstore.servers:#{null}}")
    private String servers;
    private List<MasterInfo> serverNodes = new ArrayList<>();


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
    }

    public List<MasterInfo> getServerNodes() {
        return serverNodes;
    }

    public long getConnectionRetry() {
        return connectionRetry;
    }

    public int getConnectionRetryCount() {
        return connectionRetryCount;
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

    public String getCertFile() {
        return certFile;
    }

    public static Builder newBuilder(String servers) {
        return new Builder(servers);
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
