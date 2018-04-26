package io.axoniq.axondb.client.axon;

import io.axoniq.axondb.client.AxonDBConfiguration;
import io.axoniq.axondb.client.StubServer;
import org.axonframework.eventsourcing.eventstore.GlobalSequenceTrackingToken;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.junit.*;

import static org.junit.Assert.*;

/**
 * Author: marc
 */
public class AxonDBTokenStoreTest {
    private StubServer server;
    private AxonDBTokenStore testSubject;

    @Before
    public void setUp() throws Exception {
        server = new StubServer(6123);
        server.start();
        AxonDBConfiguration config = AxonDBConfiguration.newBuilder("localhost:6123")
                                                        .flowControl(200, 100, 100)
                                                        .build();
        testSubject = new AxonDBTokenStore(config, new XStreamSerializer());
    }

    @After
    public void tearDown() throws Exception {
        server.shutdown();
    }


    @Test
    public void storeToken() {
        testSubject.storeToken(new GlobalSequenceTrackingToken(1000), "processor", 0);
    }

    @Test
    public void fetchToken() {
    }

    @Test
    public void releaseClaim() {
    }

    @Test
    public void extendClaim() {
    }
}