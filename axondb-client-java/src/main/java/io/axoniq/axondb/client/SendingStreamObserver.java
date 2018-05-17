package io.axoniq.axondb.client;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Author: marc
 */
public class SendingStreamObserver<T> implements StreamObserver<T> {
    private final static Logger logger = LoggerFactory.getLogger(SendingStreamObserver.class);

    private final StreamObserver<T> delegate;

    public SendingStreamObserver(StreamObserver<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void onNext(T t) {
        synchronized (delegate) {
            if( logger.isTraceEnabled()) logger.trace("Send {}", t);
            delegate.onNext(t);
        }
    }

    @Override
    public void onError(Throwable throwable) {
        try {
            delegate.onError(throwable);
        } catch( Throwable t) {
            logger.debug("Failed send error on connection", t);
        }
    }

    @Override
    public void onCompleted() {
        try {
            delegate.onCompleted();
        } catch( Throwable t) {
            logger.debug("Failed to complete connection", t);
        }
    }
}
