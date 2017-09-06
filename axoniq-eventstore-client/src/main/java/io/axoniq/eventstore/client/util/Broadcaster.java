package io.axoniq.eventstore.client.util;

import io.axoniq.eventstore.grpc.NodeInfo;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;


/**
 * Utility class for broadcasting messages to a number of nodes, and waiting until all replies have been received.
 *
 * @param <T> The type of return messages expected from the broadcast
 * @author Marc Gathier
 */
public class Broadcaster<T> {
    private static final Logger logger = LoggerFactory.getLogger(Broadcaster.class);
    private final Collection<NodeInfo> destinations;
    private final Action<T> action;
    private final Consumer<T> onNextCallback;

    /**
     * Initialize the broadcaster to send to given {@code destinations} using the given {@code action}, invoking the
     * given {@code onNextCallback} when replies are received.
     *
     * @param destinations   The destinations to broadcast messages to
     * @param action         The action to take for each destination
     * @param onNextCallback The callback to invoke when results are received
     */
    public Broadcaster(Collection<NodeInfo> destinations, Action<T> action, Consumer<T> onNextCallback) {
        this.destinations = destinations;
        this.action = action;
        this.onNextCallback = onNextCallback;
    }

    /**
     * Send a broadcast, waiting at most the given {@code waitTime} for results to come back. This method returns when
     * all responses have been received, or the timeout has been reached.
     *
     * @param timeout The amount of time to wait for all responses
     * @param unit    The unit of time
     * @throws InterruptedException when the thread was interrupted while waiting
     */
    public void broadcast(int timeout, TimeUnit unit) throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(destinations.size());

        destinations.forEach(node -> action.perform(node, new StreamObserver<T>() {
            @Override
            public void onNext(T t) {
                onNextCallback.accept(t);
            }

            @Override
            public void onError(Throwable throwable) {
                logger.warn("Error from: {}:{} - {}", node.getHostName(), node.getGrpcPort(), GrpcExceptionParser.parse(throwable).toString());
                countDownLatch.countDown();
            }

            @Override
            public void onCompleted() {
                countDownLatch.countDown();
            }
        }));

        if (unit != null) {
            countDownLatch.await(timeout, unit);
        }
    }

    /**
     * Interface describing the action to take.
     *
     * @param <T> The type of return message expected from the broadcast
     */
    public interface Action<T> {
        /**
         * Perform the action for the given {@code node}, which is expected to invoke the given {@code resultObserver}
         * when results are received.
         *
         * @param node           The node to perform the action for
         * @param resultObserver The observer to report the results of the action
         */
        void perform(NodeInfo node, StreamObserver<T> resultObserver);
    }

}
