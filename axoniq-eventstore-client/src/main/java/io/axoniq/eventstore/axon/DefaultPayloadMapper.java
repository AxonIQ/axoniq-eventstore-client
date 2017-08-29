package io.axoniq.eventstore.axon;

import com.google.protobuf.ByteString;
import io.axoniq.eventstore.Event;
import io.axoniq.eventstore.MetaDataValue;
import io.axoniq.eventstore.grpc.EventWithContext;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.GenericEventMessage;
import org.axonframework.eventsourcing.GenericDomainEventMessage;
import org.axonframework.messaging.GenericMessage;
import org.axonframework.serialization.SerializedObject;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.SimpleSerializedObject;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Zoltan Altfatter
 */
public class DefaultPayloadMapper implements PayloadMapper {

    private final Serializer serializer;

    public DefaultPayloadMapper(Serializer serializer) {
        this.serializer = serializer;
    }

    @Override
    public EventWithContext map(EventMessage eventMessage) {
        Event.Builder builder = Event.newBuilder();
        if (eventMessage instanceof GenericDomainEventMessage) {
            builder.setAggregateIdentifier(((GenericDomainEventMessage) eventMessage).getAggregateIdentifier())
                   .setAggregateSequenceNumber(((GenericDomainEventMessage) eventMessage).getSequenceNumber())
                   .setAggregateType(((GenericDomainEventMessage) eventMessage).getType());
        }
        builder.setPayload(io.axoniq.eventstore.SerializedObject.newBuilder()
                                                                .setType(eventMessage.getPayloadType().getName())
                                                                .setData(ByteString.copyFrom(getPayloadAsBytes(eventMessage.getPayload()))))

               .setTimestamp(eventMessage.getTimestamp().getEpochSecond());
        eventMessage.getMetaData().forEach((k, v) -> builder.putMetaData(k, convertToMetaDataValue(v)));
        return EventWithContext.newBuilder().setEvent(builder.build()).build();
    }

    @Override
    public EventMessage<?> map(Event event) {
        try {
            Object payload = serializer.deserialize(new SimpleSerializedObject<>(event.getPayload().getData().toByteArray(), byte[].class, event.getPayload().getType(), event.getPayload().getRevision()));
            if ("".equals(event.getAggregateIdentifier()) && "".equals(event.getAggregateType())) {
                return new GenericEventMessage<>(payload, event.getMetaDataMap());
            }

            return new GenericDomainEventMessage<>(event.getAggregateType(),
                                                   event.getAggregateIdentifier(),
                                                   event.getAggregateSequenceNumber(),
                                                   new GenericMessage<>(event.getMessageIdentifier(), payload, convert(event.getMetaDataMap())), () -> Instant.ofEpochMilli(event.getTimestamp()));
        } catch (Exception e) {
            throw new RuntimeException("error parsing Axoniq event", e);
        }
    }

    private Map<String, ?> convert(Map<String, MetaDataValue> metaDataMap) {
        if (metaDataMap.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, Object> metaData = new HashMap<>(metaDataMap.size());
        metaDataMap.forEach((k, v) -> metaData.put(k, convertFromMetaDataValue(v)));
        return metaData;
    }

    private MetaDataValue convertToMetaDataValue(Object v) {
        MetaDataValue.Builder builder = MetaDataValue.newBuilder();
        if (v instanceof CharSequence) {
            builder.setTextValue(v.toString());
        } else if (v instanceof Double || v instanceof Float) {
            builder.setDoubleValue(((Number) v).doubleValue());
        } else if (v instanceof Number) {
            builder.setNumberValue(((Number) v).longValue());
        } else if (v instanceof Boolean) {
            builder.setBooleanValue((Boolean) v);
        } else {
            SerializedObject<byte[]> serializedObject = serializer.serialize(v, byte[].class);
            builder.setBytesValue(io.axoniq.eventstore.SerializedObject.newBuilder()
                                                                       .setType(serializedObject.getType().getName())
                                                                       .setData(ByteString.copyFrom(serializedObject.getData()))
                                                                       .setRevision(serializedObject.getType().getRevision())
                                                                       .build());
        }
        return builder.build();
    }

    private Object convertFromMetaDataValue(MetaDataValue v) {
        switch (v.getDataCase()) {
            case TEXT_VALUE:
                return v.getTextValue();
            case BYTES_VALUE:
                io.axoniq.eventstore.SerializedObject bytesValue = v.getBytesValue();
                return serializer.deserialize(new SimpleSerializedObject<>(bytesValue.getData().toByteArray(),
                                                                           byte[].class,
                                                                           bytesValue.getType(),
                                                                           bytesValue.getRevision()));

            case DATA_NOT_SET:
                return null;
            case DOUBLE_VALUE:
                return v.getDoubleValue();
            case NUMBER_VALUE:
                return v.getNumberValue();
            case BOOLEAN_VALUE:
                return v.getBooleanValue();
        }
        return null;
    }

    private byte[] getPayloadAsBytes(Object payload) {
        return serializer.serialize(payload, byte[].class).getData();
    }
}
