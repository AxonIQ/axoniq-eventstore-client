package io.axoniq.axondb.client;

import io.axoniq.axondb.grpc.Confirmation;
import io.axoniq.axondb.grpc.ProcessorSegment;
import io.axoniq.axondb.grpc.Token;
import io.axoniq.axondb.grpc.TokenStoreGrpc;
import io.axoniq.axondb.grpc.TokenWithProcessorSegment;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Author: marc
 */
public class TokenStoreImpl extends TokenStoreGrpc.TokenStoreImplBase {
    private final static Logger logger = LoggerFactory.getLogger(TokenStoreImpl.class);

    private final static Map<ProcessorSegment, TokenClaim> tokens = new ConcurrentHashMap<>();
    @Override
    public void storeToken(TokenWithProcessorSegment request, StreamObserver<Confirmation> responseObserver) {
        logger.info("Received request: {}", request);
        tokens.put(request.getSegment(), new TokenClaim(request.getToken(), null));
        responseObserver.onNext(Confirmation.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void fetchToken(ProcessorSegment request, StreamObserver<Token> responseObserver) {

    }

    private Long newClaim() {
        return System.currentTimeMillis() + 1000;
    }

    @Override
    public void extendClaim(ProcessorSegment request, StreamObserver<Token> responseObserver) {
        TokenClaim tokenClaim = tokens.computeIfPresent(request, (r,claim) -> new TokenClaim(claim.token, newClaim()));
        super.extendClaim(request, responseObserver);
    }

    @Override
    public void releaseClaim(ProcessorSegment request, StreamObserver<Token> responseObserver) {
        super.releaseClaim(request, responseObserver);
    }

    private static class TokenClaim {
        private final Token token;
        private final Long claimedUntil;

        public TokenClaim(Token token, Long claimedUntil) {
            this.token = token;
            this.claimedUntil = claimedUntil;
        }
    }
}
