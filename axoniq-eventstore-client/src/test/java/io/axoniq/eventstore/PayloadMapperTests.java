package io.axoniq.eventstore;

import com.google.protobuf.ByteString;
import io.axoniq.eventstore.axon.PayloadMapper;
import io.axoniq.eventstore.grpc.EventWithContext;
import org.axonframework.eventsourcing.GenericDomainEventMessage;
import org.axonframework.serialization.SerializedObject;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.json.JacksonSerializer;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author Zoltan Altfatter
 */
public class PayloadMapperTests {

    // under test
    PayloadMapper payloadMapper;
    Serializer serializer;

    @Before
    public void setUp() {
        serializer = new JacksonSerializer();
        payloadMapper = new PayloadMapper(serializer);
    }

    @Test
    public void mapEventMessage() {
        ExamplePayload examplePayload = new ExamplePayload();
        examplePayload.setValue("foo");
        SerializedObject<String> ser = serializer.serialize(examplePayload, String.class);
        System.out.println(ser.getData());

        GenericDomainEventMessage message =
                new GenericDomainEventMessage<>("BankAccountAggregate",
                        "088e8464-48d9-4a29-9597-6ebe33a38127",
                        0, examplePayload);

        EventWithContext event = payloadMapper.map(message);

        assertThat(event.getEvent().getAggregateIdentifier(), is("088e8464-48d9-4a29-9597-6ebe33a38127"));
        assertThat(event.getEvent().getPayload().getType(), is("io.axoniq.eventstore.PayloadMapperTests$ExamplePayload"));
        assertThat(event.getEvent().getPayload().getData().toStringUtf8(), is("{\"value\":\"foo\"}"));
    }

    @Test
    public void mapEvent() {
        Event event = Event.newBuilder()
                .setAggregateIdentifier("04b5b7f5-ff2b-4a8d-9fe1-103dce4450a3")
                .setAggregateSequenceNumber(0)
                .setPayload(Payload.newBuilder()
                        .setType("io.axoniq.eventstore.PayloadMapperTests$ExamplePayload")
                        .setData(ByteString.copyFromUtf8("{\"value\":\"foo\"}")))
                .build();

        GenericDomainEventMessage<?> message = payloadMapper.map(event);

        assertThat(message.getAggregateIdentifier(), is("04b5b7f5-ff2b-4a8d-9fe1-103dce4450a3"));
        assertThat(message.getSequenceNumber(), is(0L));
        assertThat(message.getType(), is("io.axoniq.eventstore.PayloadMapperTests$ExamplePayload"));

    }

    static class ExamplePayload {
        String value;

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

}
