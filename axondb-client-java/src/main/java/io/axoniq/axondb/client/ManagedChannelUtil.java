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

import io.grpc.ManagedChannel;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.SslContext;

import java.io.File;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;

/**
 */
public class ManagedChannelUtil {
    public static ManagedChannel createManagedChannel(String host, int port, boolean sslEnabled, String certChainFile, long keepAliveTime, long keepAliveTimeout) {
        NettyChannelBuilder builder = NettyChannelBuilder.forAddress(host, port);

        if( keepAliveTime > 0) {
            builder.keepAliveTime(keepAliveTime, TimeUnit.MILLISECONDS)
                   .keepAliveTimeout(keepAliveTimeout, TimeUnit.MILLISECONDS)
                   .keepAliveWithoutCalls(true);
        }

        if (sslEnabled) {
            try {
                if( certChainFile == null) throw new RuntimeException("SSL enabled but no certificate file specified");
                File certFile = new File(certChainFile);
                if( ! certFile.exists()) {
                    throw new RuntimeException("Certificate file " + certChainFile + " does not exist");
                }
                SslContext sslContext = GrpcSslContexts.forClient()
                        .trustManager(new File(certChainFile))
                        .build();
                builder.sslContext(sslContext);
            } catch (SSLException e) {
                throw new RuntimeException("Couldn't set up SSL context", e);
            }
        } else {
            builder.usePlaintext(true);
        }
        return builder.build();
    }
}
