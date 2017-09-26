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
