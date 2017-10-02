package io.axoniq.eventstore.client;

import io.axoniq.eventstore.grpc.ClusterGrpc;
import io.axoniq.eventstore.grpc.ClusterInfo;
import io.axoniq.eventstore.grpc.NodeInfo;
import io.axoniq.eventstore.grpc.RetrieveClusterInfoRequest;
import io.grpc.stub.StreamObserver;

public class ClusterImpl extends ClusterGrpc.ClusterImplBase {
    @Override
    public void retrieveClusterInfo(RetrieveClusterInfoRequest request, StreamObserver<ClusterInfo> responseObserver) {
        responseObserver.onNext(ClusterInfo.newBuilder().setMaster(NodeInfo.newBuilder().setHostName("localhost").setGrpcPort(9123).build()).build());
        responseObserver.onCompleted();
    }
}
