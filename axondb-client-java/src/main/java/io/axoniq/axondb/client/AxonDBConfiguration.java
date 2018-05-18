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
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 */
@Configuration
@ConfigurationProperties(prefix = "axoniq.axondb")
public class AxonDBConfiguration {
    private String servers;

    private FlowControl flowControl = new FlowControl();
    private String token;

    private AxonSSL ssl = new AxonSSL();

    private long connectionRetry = 2500;
    private int connectionRetryCount = 5;

    private EventCipher eventCipher = new EventCipher();
    private String context;
    private long heartbeatTimeout = 5000;

    private long checkAliveDelay = 1000;
    private long checkAliveInterval = 1000;

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

    public List<NodeInfo> serverNodes() {
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
        return flowControl.getInitialNrOfPermits();
    }

    public Integer getNrOfNewPermits() {
        return flowControl.getNrOfNewPermits();
    }

    public Integer getNewPermitsThreshold() {
        return flowControl.getNewPermitsThreshold();
    }

    public String getCertFile() {
        return ssl.getCertFile();
    }

    public EventCipher eventCipher() {
        return eventCipher;
    }

    public boolean isSslEnabled() {
        return ssl.isEnabled();
    }

    public String getContext() {
        return context;
    }

    public void setServers(String servers) {
        this.servers = servers;
    }

    public void setFlowControl(FlowControl flowControl) {
        this.flowControl = flowControl;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public void setSsl(AxonSSL ssl) {
        this.ssl = ssl;
    }

    public void setConnectionRetry(long connectionRetry) {
        this.connectionRetry = connectionRetry;
    }

    public void setConnectionRetryCount(int connectionRetryCount) {
        this.connectionRetryCount = connectionRetryCount;
    }

    public void setEventCipher(EventCipher eventCipher) {
        this.eventCipher = eventCipher;
    }

    public void setContext(String context) {
        this.context = context;
    }

    public long getCheckAliveDelay() {
        return checkAliveDelay;
    }

    public void setCheckAliveDelay(long checkAliveDelay) {
        this.checkAliveDelay = checkAliveDelay;
    }

    public long getCheckAliveInterval() {
        return checkAliveInterval;
    }

    public void setCheckAliveInterval(long checkAliveInterval) {
        this.checkAliveInterval = checkAliveInterval;
    }

    public String getServers() {
        return servers;
    }

    public FlowControl getFlowControl() {
        return flowControl;
    }

    public AxonSSL getSsl() {
        return ssl;
    }

    public long getHeartbeatTimeout() {
        return heartbeatTimeout;
    }

    public void setHeartbeatTimeout(long heartbeatTimeout) {
        this.heartbeatTimeout = heartbeatTimeout;
    }

    public static class Builder {
        private AxonDBConfiguration instance;

        public Builder(String servers) {
            instance = new AxonDBConfiguration();
            instance.servers = servers;
            instance.connectionRetry = 2500;
            instance.connectionRetryCount = 5;
        }

        public Builder ssl(String certificateFilePath) {
            instance.ssl.certFile = certificateFilePath;
            instance.ssl.enabled = certificateFilePath != null;
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
            instance.flowControl.setInitialNrOfPermits(initialNrOfPermits);
            instance.flowControl.setNrOfNewPermits(nrOfNewPermits);
            instance.flowControl.setNewPermitsThreshold( newPermitsThreshold);
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

    public static class AxonSSL {
        private String certFile;
        private boolean enabled;

        public String getCertFile() {
            return certFile;
        }

        public void setCertFile(String certFile) {
            this.certFile = certFile;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }

    /**
     * Author: marc
     */
    public static class FlowControl {

        private int initialNrOfPermits = 100000;
        private int nrOfNewPermits = 90000;
        private int newPermitsThreshold = 10000;

        public int getInitialNrOfPermits() {
            return initialNrOfPermits;
        }

        public void setInitialNrOfPermits(int initialNrOfPermits) {
            this.initialNrOfPermits = initialNrOfPermits;
        }

        public int getNrOfNewPermits() {
            return nrOfNewPermits;
        }

        public void setNrOfNewPermits(int nrOfNewPermits) {
            this.nrOfNewPermits = nrOfNewPermits;
        }

        public int getNewPermitsThreshold() {
            return newPermitsThreshold;
        }

        public void setNewPermitsThreshold(int newPermitsThreshold) {
            this.newPermitsThreshold = newPermitsThreshold;
        }
    }
}
