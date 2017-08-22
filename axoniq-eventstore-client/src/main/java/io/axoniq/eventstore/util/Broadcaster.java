package io.axoniq.eventstore.util;

import io.axoniq.eventstore.grpc.MasterInfo;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;


/**
 * Author: marc
 */
public class Broadcaster<T> {
    private static final Logger logger = LoggerFactory.getLogger(Broadcaster.class);
    private final Collection<MasterInfo> destinations;
    private final Action<T> action;
    private final Consumer<T> onNextCallback;

    public interface Action<T> {
        void perform(MasterInfo request, StreamObserver<T> observer);
    }

    public Broadcaster(Collection<MasterInfo> destinations, Action<T> action, Consumer<T> onNextCallback) {

        this.destinations = destinations;
        this.action = action;
        this.onNextCallback = onNextCallback;
    }

    public void broadcast( TimeUnit waitTimeUnit, long waitTime) {
        CountDownLatch countDownLatch = new CountDownLatch(destinations.size());

        destinations.forEach(nodeInfo -> {
            action.perform(nodeInfo, new StreamObserver<T>() {
                @Override
                public void onNext(T t) {
                    onNextCallback.accept(t);
                }

                @Override
                public void onError(Throwable throwable) {
                    logger.warn("Error from: {}:{} - {}", nodeInfo.getHostName(), nodeInfo.getGrpcPort(), throwable.getMessage());
                    countDownLatch.countDown();
                }

                @Override
                public void onCompleted() {
                    countDownLatch.countDown();
                }
            });

        });

        if( waitTimeUnit != null) {
            try {
                countDownLatch.await(waitTime, waitTimeUnit);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
