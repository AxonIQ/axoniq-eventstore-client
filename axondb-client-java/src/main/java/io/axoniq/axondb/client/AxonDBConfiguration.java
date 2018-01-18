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

import io.axoniq.axondb.client.util.EventCipher;
import io.axoniq.platform.grpc.NodeInfo;
import org.springframework.beans.factory.annotation.Value;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 */
public class AxonDBConfiguration {
    @Value("${axoniq.axondb.servers:#{null}}")
    private String servers;

    @Value("${axoniq.axondb.flowControl.initialNrOfPermits:100000}")
    private Integer initialNrOfPermits;
    @Value("${axoniq.axondb.flowControl.nrOfNewPermits:90000}")
    private Integer nrOfNewPermits;
    @Value("${axoniq.axondb.flowControl.newPermitsThreshold:10000}")
    private Integer newPermitsThreshold;
    @Value("${axoniq.axondb.token:#{null}}")
    private String token;
    @Value("${axoniq.axondb.ssl.certChainFile:#{null}}")
    private String certFile;
    @Value("${axoniq.axondb.ssl.enabled:false}")
    private boolean sslEnabled;
    @Value("${axoniq.axondb.connectionRetry:2500}")
    private long connectionRetry;
    @Value("${axoniq.axondb.connectionRetryCount:5}")
    private int connectionRetryCount;

    private EventCipher eventCipher = new EventCipher();
    @Value("${axoniq.axondb.context:#{null}}")
    private String context;

    public AxonDBConfiguration() {
    }

    public AxonDBConfiguration(EventCipher eventCipher) {
        this.eventCipher = eventCipher;
    }

    @Value("${axoniq.axondb.eventSecretKey:#{null}}")
    private void setEventSecretKey(String key) {
        if(key != null && key.length() > 0) {
            eventCipher = new EventCipher(key.getBytes(StandardCharsets.US_ASCII));
        }
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

    public EventCipher getEventCipher() {
        return eventCipher;
    }

    public boolean isSslEnabled() {
        return sslEnabled;
    }

    public String getContext() {
        return context;
    }

    public static class Builder {
        private AxonDBConfiguration instance;

        public Builder(String servers) {
            instance = new AxonDBConfiguration();
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

        public Builder context(String context) {
            instance.context = context;
            return this;
        }

        public Builder connectionRetry(long connectionRetryTime, int attempts) {
            instance.connectionRetry = connectionRetryTime;
            instance.connectionRetryCount = attempts;
            return this;
        }

        public Builder flowControl(Integer initialNrOfPermits, Integer nrOfNewPermits, Integer newPermitsThreshold) {
            instance.initialNrOfPermits = initialNrOfPermits;
            instance.nrOfNewPermits = nrOfNewPermits;
            instance.newPermitsThreshold = newPermitsThreshold;
            return this;
        }

        public Builder setEventSecretKey(String key) {
            instance.setEventSecretKey(key);
            return this;
        }

        public Builder eventCipher(EventCipher eventCipher) {
            instance.eventCipher = eventCipher;
            return this;
        }

        public AxonDBConfiguration build() {
            return instance;
        }

    }

}
