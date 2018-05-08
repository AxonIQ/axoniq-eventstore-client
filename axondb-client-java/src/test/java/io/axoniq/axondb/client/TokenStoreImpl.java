package io.axoniq.axondb.client;

import com.google.protobuf.ByteString;
import io.axoniq.axondb.grpc.Confirmation;
import io.axoniq.axondb.grpc.ProcessorSegment;
import io.axoniq.axondb.grpc.Token;
import io.axoniq.axondb.grpc.TokenStoreGrpc;
import io.axoniq.axondb.grpc.TokenWithProcessorSegment;
import io.grpc.stub.StreamObserver;
import org.axonframework.eventsourcing.eventstore.GlobalSequenceTrackingToken;
import org.axonframework.serialization.SerializedObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.String.format;

/**
 * Author: marc
 */
public class TokenStoreImpl extends TokenStoreGrpc.TokenStoreImplBase {
    private final static Logger logger = LoggerFactory.getLogger(TokenStoreImpl.class);
    private static final GlobalSequenceTrackingToken NULL_TOKEN = new GlobalSequenceTrackingToken(-1);

    private final static Map<SimpleProcessorSegment, TokenClaim> tokens = new ConcurrentHashMap<>();
    @Override
    public void storeToken(TokenWithProcessorSegment request, StreamObserver<Confirmation> responseObserver) {
        logger.info("Received request: {}", request);
        tokens.put(new SimpleProcessorSegment(request.getSegment()), new TokenClaim(request.getToken(), null, request.getSegment().getOwner()));
        responseObserver.onNext(Confirmation.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void fetchToken(ProcessorSegment request, StreamObserver<Token> responseObserver) {
        TokenClaim claim = tokens.get(new SimpleProcessorSegment(request));
        if( claim == null ) {
            claim = new TokenClaim(null, System.currentTimeMillis(), request.getOwner());
            tokens.put(new SimpleProcessorSegment(request), claim);
            responseObserver.onNext(Token.newBuilder().build());
            responseObserver.onCompleted();
            return;
        }

        if( ! claim(claim, request.getOwner())) {
            responseObserver.onError(new RuntimeException(format("Unable to claim token '%s[%s]'. It is owned by '%s'", request.getProcessor(),
                                                                 request.getSegment(), claim.owner)));
        } else {
            tokens.put(new SimpleProcessorSegment(request), claim.extend(request.getOwner()));
            responseObserver.onNext(claim.token);
            responseObserver.onCompleted();
        }
    }

    private boolean claim(TokenClaim claim, String newOwner) {
        return claim.owner == null || claim.owner.equals(newOwner) || claim.expired();
    }


    @Override
    public void extendClaim(ProcessorSegment request, StreamObserver<Confirmation> responseObserver) {
        TokenClaim claim = tokens.get(new SimpleProcessorSegment(request));
        if( claim == null || ! claim(claim, request.getOwner())) {
            responseObserver.onError(new RuntimeException("Unable to extend the claim on token for processor '" +
                                             request.getProcessor() + "[" + request.getProcessor() + "]'. It is either claimed " +
                                             "by another process, or there is no such token."));
            return;
        }

        tokens.put(new SimpleProcessorSegment(request), claim.extend(request.getOwner()));
        responseObserver.onNext(Confirmation.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void releaseClaim(ProcessorSegment request, StreamObserver<Confirmation> responseObserver) {
        TokenClaim claim = tokens.get(new SimpleProcessorSegment(request));
        if( claim != null) {
            tokens.put(new SimpleProcessorSegment(request), claim.release(request.getOwner()));
        }

        responseObserver.onNext(Confirmation.newBuilder().build());
        responseObserver.onCompleted();
    }

    void putToken(SerializedObject<byte[]> axonSerializedToken, String processorName, int segment) {
        Token token = Token.newBuilder().setToken(io.axoniq.platform.SerializedObject.newBuilder()
                                                                                     .setData(ByteString.copyFrom(axonSerializedToken.getData()))
                                                                                     .setType(axonSerializedToken.getContentType().getName())
                                                                                     .build()).build();
        tokens.put(new SimpleProcessorSegment(processorName, segment),
                   new TokenClaim(token, null, null));
    }

    TokenClaim getToken(String processorName, int segment) {
        return tokens.get(new SimpleProcessorSegment(processorName, segment));
    }

    public void claim(String processorName, int segment, String owner) {
        SimpleProcessorSegment key = new SimpleProcessorSegment(processorName, segment);
        tokens.put(key, tokens.get(key).extend(owner));
    }

    public static class TokenClaim {
        private final Token token;
        private final Long claimed;
        private final String owner;

        public TokenClaim(Token token, Long claimed, String owner) {
            this.token = token;
            this.claimed = claimed;
            this.owner = owner;
        }

        public TokenClaim extend(String owner) {
            return new TokenClaim(token, System.currentTimeMillis(), owner);
        }

        public boolean expired() {
            return claimed == null || claimed + 1000 < System.currentTimeMillis();
        }

        public Token getToken() {
            return token;
        }

        public Long getClaimed() {
            return claimed;
        }

        public String getOwner() {
            return owner;
        }

        public TokenClaim release(String owner) {
            if(! owner.equals(this.owner) ) {
                return this;
            }
            return new TokenClaim(token, null, null);
        }
    }

    private static class SimpleProcessorSegment {

        private final String processorName;
        private final int segment;

        public SimpleProcessorSegment(String processorName, int segment) {

            this.processorName = processorName;
            this.segment = segment;
        }

        public SimpleProcessorSegment(ProcessorSegment segment) {
            this.processorName = segment.getProcessor();
            this.segment = segment.getSegment();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SimpleProcessorSegment that = (SimpleProcessorSegment) o;
            return segment == that.segment &&
                    Objects.equals(processorName, that.processorName);
        }

        @Override
        public int hashCode() {

            return Objects.hash(processorName, segment);
        }
    }
}
