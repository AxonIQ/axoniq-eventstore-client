/*
 * Copyright (c) 2017. AxonIQ
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
 * @author Marc Gathier
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
        axonDBEventStore = new AxonDBEventStore(axonDb, JacksonSerializer.builder().build());

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
