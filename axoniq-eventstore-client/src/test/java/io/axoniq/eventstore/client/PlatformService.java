package io.axoniq.eventstore.client;

import io.axoniq.platform.grpc.*;
import io.grpc.stub.StreamObserver;

/**
 * Author: marc
 */
public class PlatformService extends PlatformServiceGrpc.PlatformServiceImplBase {
    private final int port;

    public PlatformService(int port) {

        this.port = port;
    }

    @Override
    public void getPlatformServer(ClientIdentification request, StreamObserver<PlatformInfo> responseObserver) {
        responseObserver.onNext(PlatformInfo.newBuilder()
                .setPrimary(NodeInfo.newBuilder()
                        .setGrpcPort(port)
                        .setHostName("localhost")
                        .setNodeName("test")
                        .setVersion(0)
                        .build())
                .build());
        responseObserver.onCompleted();

    }

    @Override
    public StreamObserver<PlatformInboundInstruction> openStream(StreamObserver<PlatformOutboundInstruction> responseObserver) {
        return new StreamObserver<PlatformInboundInstruction>() {
            @Override
            public void onNext(PlatformInboundInstruction platformInboundInstruction) {

            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {

            }
        };
    }
}
