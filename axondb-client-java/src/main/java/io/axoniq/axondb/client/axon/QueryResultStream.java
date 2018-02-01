package io.axoniq.axondb.client.axon;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * Author: marc
 */
public interface QueryResultStream extends Iterator<QueryResult>, AutoCloseable {

    default boolean hasNext() {
        return this.hasNext(1, TimeUnit.SECONDS);
    }

    boolean hasNext(int timeout, TimeUnit unit) ;

    QueryResult next();

}
