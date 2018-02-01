package io.axoniq.axondb.client.axon;

import io.axoniq.axondb.grpc.QueryEventsResponse;
import io.axoniq.axondb.grpc.RowResponse;
import org.axonframework.eventsourcing.eventstore.EventStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Author: marc
 */
public class QueryResultBuffer implements QueryResultStream {
    final Logger logger = LoggerFactory.getLogger(EventBuffer.class);

    private final BlockingQueue<RowResponse> queryResultQueue;

    private QueryResult peekEvent;
    private RuntimeException exception;

    private Consumer<QueryResultBuffer> closeCallback;
    private Consumer<Integer> consumeListener = i -> {};
    private List<String> columns;

    public QueryResultBuffer() {
        queryResultQueue = new LinkedBlockingQueue<>();
    }

    @Override
    public boolean hasNext(int timeout, TimeUnit timeUnit) {
        checkException();
        long deadline = System.currentTimeMillis() + timeUnit.toMillis(timeout);
        try {
            while (peekEvent == null && System.currentTimeMillis() < deadline) {
                waitForData(deadline);
            }
            return peekEvent != null;
        } catch (InterruptedException e) {
            logger.warn("Consumer thread was interrupted. Returning thread to event processor.", e);
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private void checkException() {
        if (exception != null) {
            RuntimeException runtimeException = exception;
            this.exception = null;
            throw runtimeException;
        }
    }

    private void waitForData(long deadline) throws InterruptedException {
        do {
            RowResponse row = queryResultQueue.poll(Math.min(deadline - System.currentTimeMillis(), 200), TimeUnit.MILLISECONDS);
            if (row != null) {
                peekEvent = new QueryResult(row, columns);
            }
            checkException();
        } while( ! closed && peekEvent == null && System.currentTimeMillis() < deadline);
    }

    @Override
    public QueryResult next() {
        checkException();
        try {
            consumeListener.accept(1);
            return peekEvent;
        } finally {
            peekEvent = null;
        }
    }

    private volatile boolean closed;
    @Override
    public void close() {
        closed = true;
        if (closeCallback != null) closeCallback.accept(this);

    }

    public void registerCloseListener(Consumer<QueryResultBuffer> closeCallback) {
        this.closeCallback = closeCallback;
    }

    public void registerConsumeListener(Consumer<Integer> consumeListener) {
        this.consumeListener = consumeListener;
    }

    public void push(QueryEventsResponse eventWithToken) {
        switch(eventWithToken.getDataCase()) {
            case COLUMNS:
                this.columns = eventWithToken.getColumns().getColumnList();
                break;
            case ROW:
                try {
                    queryResultQueue.put(eventWithToken.getRow());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                break;
            case FILES_COMPLETED:
                break;
            case DATA_NOT_SET:
                break;
        }

    }

    public void fail(EventStoreException exception) {
        this.exception = exception;
    }
}
