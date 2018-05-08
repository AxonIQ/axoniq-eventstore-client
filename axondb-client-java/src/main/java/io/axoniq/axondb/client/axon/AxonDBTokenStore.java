package io.axoniq.axondb.client.axon;

import io.axoniq.axondb.client.AxonDBClient;
import io.axoniq.axondb.client.AxonDBConfiguration;
import io.axoniq.platform.SerializedObject;
import org.axonframework.eventhandling.tokenstore.TokenStore;
import org.axonframework.eventhandling.tokenstore.UnableToClaimTokenException;
import org.axonframework.eventsourcing.eventstore.TrackingToken;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.SimpleSerializedObject;

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
    public void initializeTokenSegments(String processorName, int segmentCount) throws UnableToClaimTokenException {
        try {
            axonDBClient.initializeTokenSegments(processorName, segmentCount).get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e.getMessage());
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause().getMessage());
        }

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
            try {
                SerializedObject s = axonDBClient.fetchToken(processorName, segment).get();
                if( s == null) return null;
                SimpleSerializedObject<TrackingToken> serializedObject = new SimpleSerializedObject(s.getData().toByteArray(), byte[].class, s.getType(),
                                                                                     "".equals(s.getRevision()) ? null : s.getRevision());

                return serializer.deserialize(serializedObject);
            } catch (InterruptedException e) {
                throw new UnableToClaimTokenException(e.getMessage());
            } catch (ExecutionException e) {
                throw new UnableToClaimTokenException(e.getCause().getMessage());
            }
    }

    @Override
    public void releaseClaim(String processorName, int segment) {
        try {
            axonDBClient.releaseClaim(processorName, segment).get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e.getMessage());
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause().getMessage());
        }

    }

    @Override
    public int[] fetchSegments(String processorName) {
        try {
            return axonDBClient.fetchSegments(processorName).get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e.getMessage());
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause().getMessage());
        }
    }

    @Override
    public void extendClaim(String processorName, int segment) throws UnableToClaimTokenException {
        try {
            axonDBClient.extendClaim(processorName, segment).get();
        } catch (InterruptedException e) {
            throw new UnableToClaimTokenException(e.getMessage());
        } catch (ExecutionException e) {
            throw new UnableToClaimTokenException(e.getCause().getMessage());
        }

    }
}
