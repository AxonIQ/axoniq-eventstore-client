package io.axoniq.axondb.client.axon;

import io.axoniq.axondb.client.AxonDBConfiguration;
import io.axoniq.axondb.client.StubServer;
import org.axonframework.eventsourcing.eventstore.EventStoreException;
import org.axonframework.serialization.json.JacksonSerializer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Author: marc
 */
public class GrpcQueryTest {
    private AxonDBEventStore axonDBEventStore;
    private static StubServer stubServer;

    @BeforeClass
    public static void setupServer() throws Exception {
        stubServer = new StubServer(6123);
        stubServer.start();
    }
    @Before
    public void setup() {

        AxonDBConfiguration axonDb = AxonDBConfiguration.newBuilder("localhost:6123")
                .flowControl(10000, 9000, 1000)
                .build();
        axonDBEventStore = new AxonDBEventStore(axonDb, new JacksonSerializer());

    }

    @AfterClass
    public static void tearDown() throws Exception {
        stubServer.shutdown();
    }

    @Test
    public void performQuery() throws Exception {
        QueryResultStream r = axonDBEventStore.query("", true);
        while(r.hasNext(1, TimeUnit.SECONDS)) {
            System.out.println(r.next());
        }
    }

    @Test(expected = EventStoreException.class)
    public void performInvalidQuery()  {
        QueryResultStream r = axonDBEventStore.query("invalidQuery", true);
        r.hasNext(1, TimeUnit.SECONDS);
    }
}
