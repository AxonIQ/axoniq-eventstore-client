package io.axoniq.akkaclient;

import akka.actor.ActorRef;
import akka.dispatch.Futures;
import akka.persistence.AtomicWrite;
import akka.persistence.PersistentImpl;
import akka.persistence.PersistentRepr;
import akka.persistence.journal.japi.AsyncWriteJournal;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.axoniq.eventstore.axon.AxoniqEventStore;
import io.axoniq.eventstore.EventStoreConfiguration;
import io.axoniq.eventstore.gateway.EventStoreGateway;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventsourcing.DomainEventMessage;
import org.axonframework.eventsourcing.GenericDomainEventMessage;
import org.axonframework.eventsourcing.eventstore.DomainEventStream;
import org.axonframework.messaging.unitofwork.DefaultUnitOfWork;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.json.JacksonSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.concurrent.Future;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

public class EventStoreJournalPlugin extends AsyncWriteJournal {

    private static final Logger logger = LoggerFactory.getLogger(EventStoreJournalPlugin.class);

    private final ObjectMapper mapper = new ObjectMapper();
    private final Serializer serializer;
    private final AxoniqEventStore eventStore;
    private final EventStoreGateway eventStoreGateway;

    public EventStoreJournalPlugin() {
        logger.debug("EventStoreJournalPlugin constructor");
        EventStoreConfiguration eventStoreConfiguration = EventStoreConfiguration.newBuilder("localhost:8123")
                .flowControl(10, 1, 0)
                .token( "please run with security off")
                .connectionRetry(1000, 3)
//                .ssl("resources/axoniq-public.crt")
                .build();
        serializer = new JacksonSerializer();
        eventStoreGateway = new EventStoreGateway(eventStoreConfiguration);
        eventStore = new AxoniqEventStore(eventStoreConfiguration, eventStoreGateway, serializer);
    }

    @Override
    public Future<Void> doAsyncReplayMessages(String persistenceId, long fromSequenceNr, long toSequenceNr,
                                              long max, Consumer<PersistentRepr> recoveryCallback) {
        logger.debug("doAsyncReplayMessages");
        DomainEventStream domainEventStream = eventStore.readEvents(persistenceId);
        if(!domainEventStream.hasNext()) logger.debug("no events");
        long count = 0;
        while(domainEventStream.hasNext()) {
            logger.debug("processing event");
            count++;
            if(count > max) break;
            DomainEventMessage<?> domainEventMessage = domainEventStream.next();
            long sequenceNr = domainEventMessage.getSequenceNumber();
            if(sequenceNr > toSequenceNr) break;
            if(sequenceNr < fromSequenceNr) continue;
            logger.debug("event is within sequence number, sending as PersistentRepr");
            PersistentRepr persistentRepr = new PersistentImpl(
                    domainEventMessage.getPayload(),
                    sequenceNr,
                    domainEventMessage.getAggregateIdentifier(),
                    "",
                    false,
                    ActorRef.noSender(),
                    ""
            );
            recoveryCallback.accept(persistentRepr);
        }
        return Futures.successful(null);
    }

    @Override
    public Future<Long> doAsyncReadHighestSequenceNr(String persistenceId, long fromSequenceNr) {
        logger.debug("doAsyncReadHighestSequenceNr");
        DomainEventStream domainEventStream = eventStore.readEvents(persistenceId);
        if(!domainEventStream.hasNext()) logger.debug("no events");
        long toSequenceNr = 0;
        while(domainEventStream.hasNext()) {
            toSequenceNr = domainEventStream.next().getSequenceNumber();
        }
        return Futures.successful(toSequenceNr);
    }

    @Override
    public Future<Iterable<Optional<Exception>>> doAsyncWriteMessages(Iterable<AtomicWrite> messages) {
        logger.debug("doAsyncWriteMessages");
        try {
            List<EventMessage<?>> eventMessages = new ArrayList<>();
            List<Optional<Exception>> results = new ArrayList<>();
            for (AtomicWrite atomicWrite : messages) {
                List<PersistentRepr> events = JavaConverters.seqAsJavaList(atomicWrite.payload());
                for (PersistentRepr event : events) {
                    logger.debug("writing event for persistence id: " + event.persistenceId());
                    logger.debug("event.payload: " + event.payload());
                    eventMessages.add(new GenericDomainEventMessage(
                            event.payload().getClass().getName(),
                            event.persistenceId(),
                            event.sequenceNr(),
                            event.payload()));
                }
                results.add(Optional.empty());
            }
            DefaultUnitOfWork uow = DefaultUnitOfWork.startAndGet(eventMessages.get(0));
            eventStore.publish(eventMessages);
            uow.commit();
            return Futures.successful(results);
        } catch(Exception ex) {
            return Futures.failed(ex);
        }
    }

    @Override
    public Future<Void> doAsyncDeleteMessagesTo(String persistenceId, long toSequenceNr) {
        throw new UnsupportedOperationException("AxonIQ EventStore is append-only");
    }
}
