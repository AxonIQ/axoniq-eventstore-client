package io.axoniq.eventstore.client;

/**
 * Exception describing an error reported by the AxonIQ Event Store Client.
 */
public class ClientConnectionException extends RuntimeException {

    /**
     * Initialize the exception with given {@code message} and {@code cause}
     *
     * @param message A message describing the error
     * @param cause   The cause of the exception
     */
    public ClientConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
