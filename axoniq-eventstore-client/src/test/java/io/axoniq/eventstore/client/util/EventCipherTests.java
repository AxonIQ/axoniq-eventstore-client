package io.axoniq.eventstore.client.util;

import com.google.protobuf.ByteString;
import io.axoniq.eventstore.Event;
import io.axoniq.eventstore.SerializedObject;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class EventCipherTests {

    /**
     * Most basic test of a simple encryption scenario.
     */
    @Test
    public void smoke() {
        String message = "Hello World! AxonIQ Rulez!";
        String aggregateIdentifier = "1234";
        byte[] clearPayload = message.getBytes(StandardCharsets.UTF_8);
        EventCipher eventCipher = new EventCipher(getRandomKeyBytes(16));
        Event clearEvent = Event
                .newBuilder()
                .setAggregateIdentifier(aggregateIdentifier)
                .setPayload(SerializedObject
                        .newBuilder()
                        .setData(ByteString.copyFrom(clearPayload))
                        .build())
                .build();

        Event cryptoEvent = eventCipher.encrypt(clearEvent);
        byte[] encryptedPayload = cryptoEvent.getPayload().getData().toByteArray();

        assertTrue(!Arrays.equals(encryptedPayload, clearPayload));
        assertTrue(encryptedPayload.length % 16 == 0);
        assertEquals(aggregateIdentifier, cryptoEvent.getAggregateIdentifier());

        Event decipheredEvent = eventCipher.decrypt(cryptoEvent);
        assertEquals(clearEvent, decipheredEvent);
    }

    @Test
    public void defaultEventCipherShouldNotEncrypt() {
        String message = "Hello World! AxonIQ Rulez!";
        String aggregateIdentifier = "1234";
        byte[] clearPayload = message.getBytes(StandardCharsets.UTF_8);
        EventCipher eventCipher = new EventCipher();
        Event clearEvent = Event
                .newBuilder()
                .setAggregateIdentifier(aggregateIdentifier)
                .setPayload(SerializedObject
                        .newBuilder()
                        .setData(ByteString.copyFrom(clearPayload))
                        .build())
                .build();

        Event cryptoEvent = eventCipher.encrypt(clearEvent);
        assertEquals(clearEvent, cryptoEvent);

        Event decipheredEvent = eventCipher.decrypt(cryptoEvent);
        assertEquals(clearEvent, decipheredEvent);
    }

    @Test
    public void encryptionShouldBeRandom() {
        String message = "Hello World! AxonIQ Rulez!";
        String aggregateIdentifier = "1234";
        byte[] clearPayload = message.getBytes(StandardCharsets.UTF_8);
        EventCipher eventCipher = new EventCipher(getRandomKeyBytes(16));
        Event clearEvent = Event
                .newBuilder()
                .setAggregateIdentifier(aggregateIdentifier)
                .setPayload(SerializedObject
                        .newBuilder()
                        .setData(ByteString.copyFrom(clearPayload))
                        .build())
                .build();

        Event cryptoEvent1 = eventCipher.encrypt(clearEvent);
        Event cryptoEvent2 = eventCipher.encrypt(clearEvent);
        assertNotEquals(cryptoEvent1, cryptoEvent2);
    }

    private String getRandomKeyString(int length) {
        byte[] bytes = new byte[length];
        for(int i = 0; i < length; i++) {
            bytes[i] = (byte)(ThreadLocalRandom.current().nextInt(33, 127));
        }
        return new String(bytes, StandardCharsets.US_ASCII);
    };

    private byte[] getRandomKeyBytes(int length) {
        byte[] bytes = new byte[length];
        ThreadLocalRandom.current().nextBytes(bytes);
        return bytes;
    };
}
