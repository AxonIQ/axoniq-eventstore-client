package io.axoniq.eventstore.client.util;

import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;

/**
 * Author: marc
 * Converts GRPC Exceptions to EventStoreClientException
 */
public class GrpcExceptionParser {
    private static Metadata.Key<String> ERROR_CODE_KEY = Metadata.Key.of("ErrorCode", Metadata.ASCII_STRING_MARSHALLER);

    public static EventStoreClientException parse(Throwable ex) {
        String code = "AXONIQ-0001";
        if( ex instanceof StatusRuntimeException) {
            if(ex.getCause() instanceof EventStoreClientException) {
                throw (EventStoreClientException)ex.getCause();
            }
            StatusRuntimeException statusRuntimeException = (StatusRuntimeException)ex;
            Metadata trailer = statusRuntimeException.getTrailers();
            String errorCode = trailer.get(ERROR_CODE_KEY);
            if (errorCode != null) {
                code = errorCode;
            }
        }
        return new EventStoreClientException(code, ex.getMessage(), ex);
    }


}
