package io.axoniq.eventstore.client.util;

import com.google.protobuf.ByteString;
import io.axoniq.eventstore.Event;
import io.axoniq.eventstore.SerializedObject;
import io.axoniq.eventstore.grpc.EventWithToken;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class EventCipher {

    private final static String ALGORITHM = "AES/CBC/PKCS5Padding";
    private final static String MAGIC_NUMBER_STRING = "AxIQ";
    private final static int NONCE_LENGTH = 6;

    private Function<Event, Integer> keySelector;
    private SecretKeySpec[] secretKeys;
    private IvParameterSpec ivParameterSpec;
    private byte[] magicNumber;
    private ThreadLocal<Cipher>[] encryptingCiphers;
    private ThreadLocal<Cipher>[] decryptingCiphers;
    private ThreadLocal<SecureRandom> nonceGenerator;

    public EventCipher() {
        this(event -> -1, Collections.emptyList());
    }

    public EventCipher(byte[] secretKey) {
        this(event -> 0, Collections.singletonList(secretKey));
    }

    public EventCipher(Function<Event, Integer> keySelector, List<byte[]> secretKeys) {
        this.keySelector = keySelector;
        this.secretKeys = new SecretKeySpec[secretKeys.size()];
        for(int i = 0; i < this.secretKeys.length; i++) {
            byte[] key = secretKeys.get(i);
            if(key.length != 16 && key.length != 24 && key.length != 24) {
                throw new EventStoreClientException("AXONIQ-8001",
                        String.format("secret key length should be 128, 196 or 258 bits but is %d bytes for key %d",
                                key.length, i));
            }
            this.secretKeys[i] = new SecretKeySpec(key,"AES");
        }
        this.ivParameterSpec = new IvParameterSpec(new byte[16]); /* All-zero IV */
        this.magicNumber = MAGIC_NUMBER_STRING.getBytes(StandardCharsets.US_ASCII);
        this.encryptingCiphers = new ThreadLocal[this.secretKeys.length];
        for(int i = 0; i < this.secretKeys.length; i++) {
            final int keyIndex = i;
            this.encryptingCiphers[i] = new ThreadLocal<Cipher>() {
                @Override
                protected Cipher initialValue() {
                    return initCipher(Cipher.ENCRYPT_MODE, keyIndex);
                }
            };
            this.encryptingCiphers[i].get(); // If we can't create the cipher, better to know it sooner than later
        }
        this.decryptingCiphers = new ThreadLocal[this.secretKeys.length];
        for(int i = 0; i < this.secretKeys.length; i++) {
            final int keyIndex = i;
            this.decryptingCiphers[i] = new ThreadLocal<Cipher>() {
                @Override
                protected Cipher initialValue() {
                    return initCipher(Cipher.DECRYPT_MODE, keyIndex);
                }
            };
            this.decryptingCiphers[i].get(); // If we can't create the cipher, better to know it sooner than later
        }
        this.nonceGenerator = new ThreadLocal<SecureRandom>() {
            @Override
            protected SecureRandom initialValue() {
                return new SecureRandom();
            }
        };
    }

    private Cipher initCipher(int mode, int keyIndex) {
        Cipher cipher = null;
        try {
            cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(mode, secretKeys[keyIndex], ivParameterSpec);
            return cipher;
        } catch (Exception ex) {
            throw new EventStoreClientException("AXONIQ-8000", "Unexpected exception initializing crypto algorithm", ex);
        }
    }

    public EventWithToken encrypt(EventWithToken clearEventWithToken) {
        return EventWithToken
                .newBuilder(clearEventWithToken)
                .setEvent(encrypt(clearEventWithToken.getEvent()))
                .build();
    }

    public EventWithToken decrypt(EventWithToken cryptoEventWithToken) {
        return EventWithToken
                .newBuilder(cryptoEventWithToken)
                .setEvent(decrypt(cryptoEventWithToken.getEvent()))
                .build();
    }

    public Event encrypt(Event clearEvent) {
        int keyIndex = keySelector.apply(clearEvent);
        if(keyIndex < 0) return clearEvent;
        return Event
                .newBuilder(clearEvent)
                .setPayload(SerializedObject
                        .newBuilder(clearEvent.getPayload())
                        .setData(ByteString.copyFrom(encryptBytes(keyIndex, clearEvent.getPayload().getData().toByteArray())))
                        .build())
                .build();
    }

    public Event decrypt(Event cryptoEvent) {
        int keyIndex = keySelector.apply(cryptoEvent);
        if(keyIndex < 0) return cryptoEvent;
        return Event
                .newBuilder(cryptoEvent)
                .setPayload(SerializedObject
                        .newBuilder(cryptoEvent.getPayload())
                        .setData(ByteString.copyFrom(decryptBytes(keyIndex, cryptoEvent.getPayload().getData().toByteArray())))
                        .build())
                .build();
    }

    protected byte[] encryptBytes(int keyIndex, byte[] clearBytes) {
        Cipher cipher = encryptingCiphers[keyIndex].get();

        byte[] messageBytes = new byte[NONCE_LENGTH + magicNumber.length + clearBytes.length];
        byte[] nonce = new byte[NONCE_LENGTH];
        nonceGenerator.get().nextBytes(nonce);
        System.arraycopy(nonce, 0, messageBytes, 0, NONCE_LENGTH);
        System.arraycopy(magicNumber, 0, messageBytes, NONCE_LENGTH, magicNumber.length);
        System.arraycopy(clearBytes, 0, messageBytes, NONCE_LENGTH + magicNumber.length, clearBytes.length);
        try {
            return cipher.doFinal(messageBytes);
        } catch (IllegalBlockSizeException | BadPaddingException ex) {
            throw new EventStoreClientException("AXONIQ-8000", "Unexpected error encrypting cleartext", ex);
        }
    }

    protected byte[] decryptBytes(int keyIndex, byte[] cryptoBytes) {
        Cipher cipher = decryptingCiphers[keyIndex].get();
        byte[] decryptedBytes;
        try {
            decryptedBytes = cipher.doFinal(cryptoBytes);
        } catch (IllegalBlockSizeException | BadPaddingException ex) {
            throw new EventStoreClientException("AXONIQ-8002", "Crypto error decrypting payload", ex);
        }
        byte[] magicNumber = Arrays.copyOfRange(decryptedBytes, NONCE_LENGTH, NONCE_LENGTH + this.magicNumber.length);
        if(!Arrays.equals(this.magicNumber, magicNumber)) {
            throw new EventStoreClientException("AXONIQ-8002", "Missing magic number after decryption");
        }
        byte[] clearBytes = Arrays.copyOfRange(decryptedBytes, NONCE_LENGTH + magicNumber.length, decryptedBytes.length);
        return clearBytes;
    }

}
