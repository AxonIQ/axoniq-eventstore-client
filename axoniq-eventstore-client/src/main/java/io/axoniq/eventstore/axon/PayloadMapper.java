package io.axoniq.eventstore.axon;

import io.axoniq.eventstore.Event;
import io.axoniq.eventstore.grpc.EventWithContext;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventsourcing.DomainEventMessage;

public interface PayloadMapper {

    EventWithContext map(EventMessage eventMessage);

    EventMessage<?> map(Event event);

    default DomainEventMessage<?> mapDomainEvent(Event event) {
        return (DomainEventMessage<?>) map(event);
    }
}
