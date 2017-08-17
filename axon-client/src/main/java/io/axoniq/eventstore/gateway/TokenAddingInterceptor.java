package io.axoniq.eventstore.gateway;

import io.grpc.*;

/**
 * Created by marc on 7/17/2017.
 */
public class TokenAddingInterceptor implements ClientInterceptor {
    static final Metadata.Key<String> ACCESS_TOKEN_KEY =
            Metadata.Key.of("Access-Token", Metadata.ASCII_STRING_MARSHALLER);

    private final String token;

    public TokenAddingInterceptor(String token) {
        this.token = token;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions, Channel channel) {

        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(channel.newCall(methodDescriptor, callOptions)) {
            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                if( token != null) headers.put(ACCESS_TOKEN_KEY, token);
                super.start(responseListener, headers);
            }
        };
    }
}
