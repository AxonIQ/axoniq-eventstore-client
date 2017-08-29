package io.axoniq.eventstore;

import io.axoniq.eventstore.grpc.NodeInfo;
import org.springframework.beans.factory.annotation.Value;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Author: marc
 */
public class EventStoreConfiguration {
    @Value("${axoniq.eventstore.servers:#{null}}")
    private String servers;

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


    public EventStoreConfiguration() {
    }

    public static Builder newBuilder(String servers) {
        return new Builder(servers);
    }

    public List<NodeInfo> getServerNodes() {
        List<NodeInfo> serverNodes = new ArrayList<>();
        if (servers != null) {
            String[] serverArr = servers.split(",");
            Arrays.stream(serverArr).forEach(serverString -> {
                String[] hostPort = serverString.trim().split(":", 2);
                NodeInfo nodeInfo = NodeInfo.newBuilder().setHostName(hostPort[0])
                                            .setGrpcPort(Integer.valueOf(hostPort[1]))
                                            .build();
                serverNodes.add(nodeInfo);
            });
        }
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

        public Builder ssl(String certificateFilePath) {
            instance.certFile = certificateFilePath;
            return this;
        }

        public Builder token(String token) {
            instance.token = token;
            return this;
        }

        public Builder connectionRetry(long connectionRetryTime, int attempts) {
            instance.connectionRetry = connectionRetryTime;
            instance.connectionRetryCount = attempts;
            return this;
        }

        public EventStoreConfiguration build() {
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
