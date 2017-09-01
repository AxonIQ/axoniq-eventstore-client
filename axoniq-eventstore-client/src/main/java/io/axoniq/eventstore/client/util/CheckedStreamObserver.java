package io.axoniq.eventstore.client.util;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Created by marc on 7/18/2017.
 */
public class CheckedStreamObserver<T> implements StreamObserver<T> {
    final Logger logger = LoggerFactory.getLogger(CheckedStreamObserver.class);
    private Consumer<T> action;
    private T last;
    private CompletableFuture<T> result = new CompletableFuture<>();

    public CheckedStreamObserver( Consumer<T> action) {
        this.action = action;
    }

    @Override
    public void onNext(T object) {
        action.accept(object);
        last = object;
    }

    @Override
    public void onError(Throwable throwable) {
        logger.warn("Error on connection: {}", throwable.getMessage());
        result.completeExceptionally(throwable);
    }

    @Override
    public void onCompleted() {
        result.complete(last);
    }

    public CompletableFuture<T> result() {
        return result;
    }
}
