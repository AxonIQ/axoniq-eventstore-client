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

package io.axoniq.eventstore.client;

import io.axoniq.eventstore.grpc.ClusterGrpc;
import io.axoniq.eventstore.grpc.ClusterInfo;
import io.axoniq.eventstore.grpc.NodeInfo;
import io.axoniq.eventstore.grpc.RetrieveClusterInfoRequest;
import io.grpc.stub.StreamObserver;

public class ClusterImpl extends ClusterGrpc.ClusterImplBase {
    @Override
    public void retrieveClusterInfo(RetrieveClusterInfoRequest request, StreamObserver<ClusterInfo> responseObserver) {
        responseObserver.onNext(ClusterInfo.newBuilder().setMaster(NodeInfo.newBuilder().setHostName("localhost").setGrpcPort(8123).build()).build());
        responseObserver.onCompleted();
    }
}
