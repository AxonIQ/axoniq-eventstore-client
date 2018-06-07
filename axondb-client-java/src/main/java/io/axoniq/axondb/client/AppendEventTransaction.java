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

package io.axoniq.axondb.client;

import io.axoniq.axondb.Event;
import io.axoniq.axondb.client.axon.AxonErrorMapping;
import io.axoniq.axondb.client.util.EventCipher;
import io.axoniq.axondb.grpc.Confirmation;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 */
public class AppendEventTransaction {
    private final StreamObserver<Event> eventStreamObserver;
    private final CompletableFuture<Confirmation> observer;
    private final long commitTimeout;
    private final EventCipher eventCipher;

    public AppendEventTransaction(StreamObserver<Event> eventStreamObserver, CompletableFuture<Confirmation> observer, long commitTimeout, EventCipher eventCipher) {
        this.eventStreamObserver = eventStreamObserver;
        this.observer = observer;
        this.commitTimeout = commitTimeout;
        this.eventCipher = eventCipher;
    }

    public void append(Event event) {
        eventStreamObserver.onNext(eventCipher.encrypt(event));
    }

    public void commit()  {
        eventStreamObserver.onCompleted();
        try {
            observer.get(commitTimeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw AxonErrorMapping.convert(e);
        } catch (ExecutionException e) {
            throw AxonErrorMapping.convert(e.getCause());
        } catch (TimeoutException e) {
            throw AxonErrorMapping.convert(e);
        }
    }

    public void rollback(Throwable reason) {
        eventStreamObserver.onError(reason);
    }

}
