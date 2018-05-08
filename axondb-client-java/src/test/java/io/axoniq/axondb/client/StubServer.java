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

import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import org.axonframework.serialization.SerializedObject;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class StubServer {

    private final Server server;
    private final TokenStoreImpl tokenStore;

    public StubServer(int port) {
        tokenStore = new TokenStoreImpl();
        server = NettyServerBuilder.forPort(port)
                                   .addService(new EventStoreImpl())
                                   .addService(tokenStore)
                                   .addService(new PlatformService(port))
                                   .build();
    }

    public void start() throws IOException {
        server.start();
    }

    public void shutdown() throws InterruptedException {
        server.shutdown().awaitTermination(100, TimeUnit.MILLISECONDS);
    }

    public void storeToken(SerializedObject<byte[]> axonSerializedToken, String processorName, int segment) {
        tokenStore.putToken(axonSerializedToken, processorName, segment);
    }

    public void claim(String processorName, int segment, String owner) {
        tokenStore.claim(processorName, segment, owner);
    }

    public TokenStoreImpl.TokenClaim getToken(String processorName, int segment) {
        return tokenStore.getToken(processorName, segment);
    }
}
