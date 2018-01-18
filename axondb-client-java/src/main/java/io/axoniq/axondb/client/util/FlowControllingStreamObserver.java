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

package io.axoniq.axondb.client.util;

import io.axoniq.axondb.client.AxonDBConfiguration;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 */
public class FlowControllingStreamObserver<T> implements StreamObserver<T> {
    private final StreamObserver<T> wrappedStreamObserver;

    private final static Logger logger = LoggerFactory.getLogger(FlowControllingStreamObserver.class);
    private final AtomicLong remainingPermits;
    private final int newPermits;
    private final T newPermitsRequest;
    private final Predicate<T> isConfirmationMessage;

    public FlowControllingStreamObserver(StreamObserver<T> wrappedStreamObserver, AxonDBConfiguration configuration,
                                         Function<Integer, T> requestWrapper, Predicate<T> isConfirmationMessage) {
        this.wrappedStreamObserver = wrappedStreamObserver;
        this.remainingPermits = new AtomicLong(configuration.getInitialNrOfPermits()-configuration.getNewPermitsThreshold());
        this.newPermits = configuration.getNrOfNewPermits();
        this.newPermitsRequest = requestWrapper.apply(newPermits);
        this.isConfirmationMessage = isConfirmationMessage;
        wrappedStreamObserver.onNext(requestWrapper.apply(configuration.getInitialNrOfPermits()));
    }

    @Override
    public void onNext(T t) {
        synchronized (wrappedStreamObserver) {
            wrappedStreamObserver.onNext(t);
        }
        logger.debug("Sending response to messaging platform, remaining permits: {}", remainingPermits.get());

        if( isConfirmationMessage.test(t) ) {
            markConsumed(1);
        }
    }

    @Override
    public void onError(Throwable throwable) {
        wrappedStreamObserver.onError(throwable);
    }

    @Override
    public void onCompleted() {
        logger.info("Observer stopped");
        try {
            wrappedStreamObserver.onCompleted();
        } catch(Exception ignore) {

        }
    }

    public void markConsumed(Integer consumed) {
        if( remainingPermits.updateAndGet(old -> old - consumed) == 0) {
            remainingPermits.addAndGet(newPermits);
            synchronized (wrappedStreamObserver) {
                wrappedStreamObserver.onNext(newPermitsRequest);
            }
            logger.debug("Requesting new permits: {}", newPermitsRequest);
        }

    }

}
