package io.axoniq.eventstore.util;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Author: marc
 */
public class DuplexStreamObserver<Request,Response> {
    private Logger logger = LoggerFactory.getLogger(DuplexStreamObserver.class);
    private StreamObserver<Request> requestStream;

    private final StartCommand<Request, Response> startCommand;
    private Request initialRequest;
    private Request nextRequest;
    private int permitsLeft;
    private int next;
    private int threshold;
    private boolean flowControl;


    private final OnNext<Request, Response> onNext;
    private final OnError onError;
    private boolean stopped;

    public void stop() {
        logger.info( "Observer stopped");
        stopped = true;
        try {
            requestStream.onCompleted();
        } catch( Exception ex) {

        }
    }

    public interface StartCommand<Request,Response> {
        StreamObserver<Request> call(StreamObserver<Response> responseObserver);
    }

    public interface OnNext<Request, Response> {
        void next(Response response, StreamObserver<Request> requestStreamObserver);
    }

    public interface OnError {
        void error(Throwable throwable);
    }

    private class FlowControlledResponseStream implements StreamObserver<Response> {

        @Override
        public void onNext(Response response) {
            onNext.next( response, requestStream);
            if( flowControl ){
                permitsLeft--;
                if( permitsLeft == threshold) {
                    requestStream.onNext(nextRequest);
                    permitsLeft += next;
                }
            }
        }

        @Override
        public void onError(Throwable throwable) {
            logger.error("Received error: {}", throwable.getMessage());
            handleError(throwable);
        }

        @Override
        public void onCompleted() {
            logger.debug( "OnCompleted");
        }
    }

    public DuplexStreamObserver(StartCommand<Request,Response> startCommand, OnNext<Request,Response> onNext, OnError onError) {
        this.startCommand = startCommand;
        this.onNext = onNext;
        this.onError = onError;
    }


    public void start( Request initialRequest, Request nextRequest, int initial, int next, int threshold) {
        this.initialRequest = initialRequest;
        this.nextRequest = nextRequest;
        this.permitsLeft = initial;
        this.next = next;
        this.threshold = threshold;
        this.flowControl = (nextRequest != null && initial > 0);

        requestStream = startCommand.call( new FlowControlledResponseStream());
        requestStream.onNext(initialRequest);
    }

    private void handleError(Throwable throwable) {
        if( ! stopped) {
            onError.error(throwable);
        }
    }

}
