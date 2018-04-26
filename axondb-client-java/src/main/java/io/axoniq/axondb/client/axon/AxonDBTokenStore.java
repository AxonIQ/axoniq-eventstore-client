package io.axoniq.axondb.client.axon;

import io.axoniq.axondb.client.AxonDBClient;
import io.axoniq.axondb.client.AxonDBConfiguration;
import org.axonframework.eventhandling.tokenstore.TokenStore;
import org.axonframework.eventhandling.tokenstore.UnableToClaimTokenException;
import org.axonframework.eventsourcing.eventstore.TrackingToken;
import org.axonframework.serialization.Serializer;

import java.util.concurrent.ExecutionException;

/**
 * Author: marc
 */
public class AxonDBTokenStore implements TokenStore {

    private final AxonDBClient axonDBClient;
    private final Serializer serializer;

    public AxonDBTokenStore(AxonDBConfiguration configuration, Serializer serializer) {
        axonDBClient = new AxonDBClient(configuration);
        this.serializer = serializer;


    }
    @Override
    public void storeToken(TrackingToken trackingToken, String processorName, int segment) throws UnableToClaimTokenException {
        try {
            axonDBClient.storeToken(serializer.serialize(trackingToken, byte[].class), processorName, segment).get();
        } catch (InterruptedException e) {
            throw new UnableToClaimTokenException(e.getMessage());
        } catch (ExecutionException e) {
            throw new UnableToClaimTokenException(e.getCause().getMessage());
        }
    }

    @Override
    public TrackingToken fetchToken(String processorName, int segment) throws UnableToClaimTokenException {
        return null;
    }

    @Override
    public void releaseClaim(String processorName, int segment) {

    }

    @Override
    public void extendClaim(String processorName, int segment) throws UnableToClaimTokenException {

    }
}
