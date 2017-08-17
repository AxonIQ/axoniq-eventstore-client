package io.axoniq.eventstore.util;

import io.axoniq.eventstore.grpc.Confirmation;
import io.axoniq.eventstore.grpc.EventWithContext;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Author: marc
 */
public class GrpcConnection {
    private final StreamObserver<EventWithContext> eventStreamObserver;
    private final CompletableFuture<Confirmation> observer;

    public GrpcConnection(StreamObserver<EventWithContext> eventStreamObserver, CompletableFuture<Confirmation> observer) {
        this.eventStreamObserver = eventStreamObserver;
        this.observer = observer;
    }

    public void send(EventWithContext event) {
        eventStreamObserver.onNext(event);
    }

    public void commit() throws InterruptedException, ExecutionException, TimeoutException {
        eventStreamObserver.onCompleted();
        observer.get(10, TimeUnit.SECONDS);
    }

    public void rollback(Throwable reason) {
        eventStreamObserver.onError(reason);
    }

}
