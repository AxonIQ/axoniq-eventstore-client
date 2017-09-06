package io.axoniq.eventstore.client;

import io.axoniq.eventstore.Event;
import io.axoniq.eventstore.client.axon.AxonErrorMapping;
import io.axoniq.eventstore.grpc.Confirmation;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Author: marc
 */
public class AppendEventTransaction {
    private final StreamObserver<Event> eventStreamObserver;
    private final CompletableFuture<Confirmation> observer;

    public AppendEventTransaction(StreamObserver<Event> eventStreamObserver, CompletableFuture<Confirmation> observer) {
        this.eventStreamObserver = eventStreamObserver;
        this.observer = observer;
    }

    public void append(Event event) {
        eventStreamObserver.onNext(event);
    }

    public void commit()  {
        eventStreamObserver.onCompleted();
        try {
            observer.get(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw AxonErrorMapping.convert(e);
        } catch (ExecutionException e) {
            throw AxonErrorMapping.convert(e.getCause());
        } catch (TimeoutException e) {
            throw AxonErrorMapping.convert(e);
        }
    }

    public void rollback(Throwable reason) {
        eventStreamObserver.onError(reason);
    }

}
