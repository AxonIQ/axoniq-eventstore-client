package io.axoniq.eventstore.axon;

import com.google.protobuf.ByteString;
import io.axoniq.eventstore.Event;
import io.axoniq.eventstore.Payload;
import io.axoniq.eventstore.grpc.EventWithContext;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventsourcing.GenericDomainEventMessage;
import org.axonframework.eventsourcing.GenericTrackedDomainEventMessage;
import org.axonframework.eventsourcing.eventstore.GlobalSequenceTrackingToken;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.SimpleSerializedObject;

/**
 * @author Zoltan Altfatter
 */
public class PayloadMapper {

    private final Serializer serializer;

    public PayloadMapper(Serializer serializer) {
        this.serializer = serializer;
    }

    public EventWithContext map(EventMessage eventMessage) {
        Event.Builder builder = Event.newBuilder();
        if( eventMessage instanceof GenericDomainEventMessage) {
            builder.setAggregateIdentifier(((GenericDomainEventMessage) eventMessage).getAggregateIdentifier())
                   .setAggregateSequenceNumber(((GenericDomainEventMessage) eventMessage).getSequenceNumber());
        }
        builder.setPayload(Payload.newBuilder()
                        .setType(eventMessage.getPayloadType().getName())
                        .setData(ByteString.copyFrom(getPayloadAsBytes(eventMessage.getPayload()))))
                .setTimestamp(eventMessage.getTimestamp().getEpochSecond());
        return EventWithContext.newBuilder().setEvent(builder.build()).build();
    }

    public GenericDomainEventMessage<?> map(Event event) {
        try {
            Object payload = serializer.deserialize( new SimpleSerializedObject<>(event.getPayload().getData().toByteArray(), byte[].class, event.getPayload().getType(), event.getPayload().getRevision()));
            return new GenericTrackedDomainEventMessage<>(
                    new GlobalSequenceTrackingToken(event.getAggregateSequenceNumber()),
                    new GenericDomainEventMessage<>(event.getPayload().getType(),
                            event.getAggregateIdentifier(),
                            event.getAggregateSequenceNumber(),
                            payload));
        } catch (Exception e) {
            throw new RuntimeException("error parsing Axoniq event");
        }
    }

    private byte[] getPayloadAsBytes(Object payload) {
        return serializer.serialize(payload, byte[].class).getData();
    }
}
