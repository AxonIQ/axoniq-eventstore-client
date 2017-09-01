package io.axoniq.eventstore.client;

import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;

import java.io.IOException;

public class StubServer {

    private final Server server;

    public StubServer(int port) {
        server = NettyServerBuilder.forPort(port)
                                   .addService(new EventStoreImpl())
                                   .addService(new ClusterImpl())
                                   .build();
    }

    public void start() throws IOException {
        server.start();
    }

    public void shutdown() throws InterruptedException {
        server.shutdown();
        server.awaitTermination();
    }
}
