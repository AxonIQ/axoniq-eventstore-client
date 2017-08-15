package io.axoniq.eventstore.util;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Created by marc on 7/18/2017.
 */
public class CheckedStreamObserver<T> implements StreamObserver<T> {
    final CountDownLatch finishLatch = new CountDownLatch(1);
    final Logger logger = LoggerFactory.getLogger(CheckedStreamObserver.class);
    private Throwable throwable;
    private Consumer<T> action;

    public CheckedStreamObserver( Consumer<T> action) {
        this.action = action;
    }

    @Override
    public void onNext(T object) {
        action.accept(object);
    }

    @Override
    public void onError(Throwable throwable) {
        logger.warn("Error on connection: {}", throwable.getMessage());
        finishLatch.countDown();
        this.throwable = throwable;
    }

    @Override
    public void onCompleted() {
        finishLatch.countDown();
    }

    public void waitForCompletion(TimeUnit unit, int count, Consumer<RuntimeException> errorHandler) {
        long start = System.currentTimeMillis();
        try {
            finishLatch.await(count, unit);
        } catch (InterruptedException e) {
            throwable = e;
        }

        logger.debug("Finished waiting after: {}ms.", System.currentTimeMillis()-start);

        if (throwable != null) {
            if( throwable instanceof  RuntimeException) errorHandler.accept((RuntimeException)throwable);
            errorHandler.accept(new RuntimeException(throwable));
        }
    }
}
