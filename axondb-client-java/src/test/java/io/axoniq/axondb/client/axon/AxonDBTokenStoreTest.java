package io.axoniq.axondb.client.axon;

import io.axoniq.axondb.client.AxonDBConfiguration;
import io.axoniq.axondb.client.StubServer;
import io.axoniq.axondb.client.TokenStoreImpl;
import org.axonframework.eventhandling.tokenstore.UnableToClaimTokenException;
import org.axonframework.eventsourcing.eventstore.GlobalSequenceTrackingToken;
import org.axonframework.eventsourcing.eventstore.TrackingToken;
import org.axonframework.serialization.SerializedObject;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.junit.*;

import static org.junit.Assert.*;

/**
 * Author: marc
 */
public class AxonDBTokenStoreTest {
    private StubServer server;
    private AxonDBTokenStore testSubject;
    private SerializedObject<byte[]> sampleSerializedToken;

    @Before
    public void setUp() throws Exception {
        server = new StubServer(6123);
        server.start();
        Serializer serializer = new XStreamSerializer();
        sampleSerializedToken = serializer.serialize(new GlobalSequenceTrackingToken(50), byte[].class);
        server.storeToken(sampleSerializedToken, "TestProcessor", 0);
        AxonDBConfiguration config = AxonDBConfiguration.newBuilder("localhost:6123")
                                                        .flowControl(200, 100, 100)
                                                        .build();
        testSubject = new AxonDBTokenStore(config, serializer);
    }

    @After
    public void tearDown() throws Exception {
        server.shutdown();
    }


    @Test
    public void storeToken() {
        testSubject.storeToken(new GlobalSequenceTrackingToken(1000), "processor", 0);
    }

    @Test(expected = UnableToClaimTokenException.class)
    public void fetchTokenClaimedByAnother() {
        server.storeToken(sampleSerializedToken, "TestProcessor", 2);
        server.claim("TestProcessor", 2, "MyTokenOwner");

        testSubject.fetchToken("TestProcessor", 2);
    }

    @Test
    public void fetchTokenNotInTokenStore() {
        TrackingToken token = testSubject.fetchToken("TestProcessor", 1);
        assertNull(token);
    }

    @Test
    public void fetchToken() {
        TrackingToken token = testSubject.fetchToken("TestProcessor", 0);
        assertTrue(token instanceof GlobalSequenceTrackingToken);
        assertEquals(50, ((GlobalSequenceTrackingToken)token).getGlobalIndex());
        TokenStoreImpl.TokenClaim claim = server.getToken("TestProcessor", 0);
        assertNotNull(claim.getOwner());
    }

    @Test
    public void releaseClaim() {
        testSubject.fetchToken("TestProcessor", 0);
        TokenStoreImpl.TokenClaim claim = server.getToken("TestProcessor", 0);
        assertNotNull(claim.getOwner());

        testSubject.releaseClaim("TestProcessor", 0);
        claim = server.getToken("TestProcessor", 0);
        assertNull(claim.getOwner());
    }

    @Test(expected = UnableToClaimTokenException.class)
    public void extendClaimNotOwned() throws InterruptedException {
        server.storeToken(sampleSerializedToken, "TestProcessor", 2);
        server.claim("TestProcessor", 2, "MyTokenOwner");

        testSubject.extendClaim("TestProcessor", 2);
        fail("Extending the claim should have failed");
    }

    @Test
    public void extendClaim() throws InterruptedException {
        testSubject.fetchToken("TestProcessor", 0);
        TokenStoreImpl.TokenClaim claim = server.getToken("TestProcessor", 0);
        assertNotNull(claim.getOwner());

        Thread.sleep(100);
        testSubject.extendClaim("TestProcessor", 0);
        TokenStoreImpl.TokenClaim claim2 = server.getToken("TestProcessor", 0);
        assertEquals(claim.getOwner(), claim2.getOwner());
        assertTrue(claim2.getClaimed() > claim.getClaimed());

        Thread.sleep(2000);
        testSubject.extendClaim("TestProcessor", 0);
        TokenStoreImpl.TokenClaim claim3 = server.getToken("TestProcessor", 0);
        assertEquals(claim.getOwner(), claim3.getOwner());
    }
}