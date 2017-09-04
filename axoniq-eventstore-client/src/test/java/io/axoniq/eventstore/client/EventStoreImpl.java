package io.axoniq.eventstore.client;

import io.axoniq.eventstore.Event;
import io.axoniq.eventstore.grpc.*;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class EventStoreImpl extends EventStoreGrpc.EventStoreImplBase {

    private final List<Event> events = new LinkedList<>();

    @Override
    public StreamObserver<Event> appendEvent(StreamObserver<Confirmation> responseObserver) {
        return new StreamObserver<Event>() {

            private final List<Event> eventsInTx = new LinkedList<>();

            @Override
            public void onNext(Event event) {
                eventsInTx.add(event);
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {
                events.addAll(eventsInTx);
                responseObserver.onNext(Confirmation.newBuilder().setSuccess(true).build());
                responseObserver.onCompleted();
            }
        };
    }

    @Override
    public void appendSnapshot(Event request, StreamObserver<Confirmation> responseObserver) {
        super.appendSnapshot(request, responseObserver);
    }

    @Override
    public void listAggregateEvents(GetAggregateEventsRequest request, StreamObserver<Event> responseObserver) {
        events.stream().filter(e -> e.getAggregateIdentifier().equals(request.getAggregateId()))
              .forEach(responseObserver::onNext);
        responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<GetEventsRequest> listEvents(StreamObserver<EventWithToken> responseObserver) {
        return new StreamObserver<GetEventsRequest>() {
            private AtomicLong permits = new AtomicLong();
            private Iterator<Event> eventsAtRead;
            private long token;

            @Override
            public void onNext(GetEventsRequest getEventsRequest) {
                long oldPermits = permits.getAndAdd(getEventsRequest.getNumberOfPermits());
                if (token == 0) {
                    token = getEventsRequest.getTrackingToken();
                }
                if (oldPermits == 0 && getEventsRequest.getNumberOfPermits() > 0) {
                    if (eventsAtRead == null) {
                        eventsAtRead = new ArrayList<>(events).iterator();
                        for (long i = 0; i < getEventsRequest.getTrackingToken(); i++) {
                            eventsAtRead.next();
                        }
                    }
                    do {
                        responseObserver.onNext(EventWithToken.newBuilder().setEvent(eventsAtRead.next()).setToken(token++).build());
                    } while (eventsAtRead.hasNext() && permits.decrementAndGet() > 0);
                }
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };
    }

    @Override
    public void readHighestSequenceNr(ReadHighestSequenceNrRequest request, StreamObserver<ReadHighestSequenceNrResponse> responseObserver) {
        super.readHighestSequenceNr(request, responseObserver);
    }
}
