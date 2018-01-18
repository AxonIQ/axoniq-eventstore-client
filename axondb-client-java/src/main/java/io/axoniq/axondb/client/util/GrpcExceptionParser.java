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

import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;

/**
 * Converts GRPC Exceptions to EventStoreClientException
 */
public class GrpcExceptionParser {
    private static Metadata.Key<String> ERROR_CODE_KEY = Metadata.Key.of("ErrorCode", Metadata.ASCII_STRING_MARSHALLER);

    public static EventStoreClientException parse(Throwable ex) {
        String code = "AXONIQ-0001";
        if( ex instanceof StatusRuntimeException) {
            if(ex.getCause() instanceof EventStoreClientException) {
                return (EventStoreClientException)ex.getCause();
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
