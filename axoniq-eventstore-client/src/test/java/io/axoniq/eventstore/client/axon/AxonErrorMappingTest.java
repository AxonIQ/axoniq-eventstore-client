package io.axoniq.eventstore.client.axon;

import io.axoniq.eventstore.client.util.EventStoreClientException;
import org.axonframework.commandhandling.model.ConcurrencyException;
import org.axonframework.eventsourcing.eventstore.EventStoreException;
import org.junit.Test;


/**
 * Author: marc
 */
public class AxonErrorMappingTest {
    @Test(expected = ConcurrencyException.class)
    public void convert2000() throws Exception {
        throw AxonErrorMapping.convert(new EventStoreClientException("AXONIQ-2000", "Concurrent modification of same aggregate"));
    }

    @Test(expected = EventStoreException.class)
    public void convertUnknown() throws Exception {
        throw AxonErrorMapping.convert(new EventStoreClientException("AXONIQ-10000", "Concurrent modification of same aggregate"));
    }

}