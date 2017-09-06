package io.axoniq.eventstore.client.util;

/**
 * Author: marc
 */
public class EventStoreClientException extends RuntimeException {

    private final String code;

    public EventStoreClientException(String code, String message) {
        this(code, message, null);
    }

    public EventStoreClientException(String code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public String getCode() {
        return code;
    }

}
