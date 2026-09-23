package dev.openfeature.sdk.interleaving;

import dev.openfeature.sdk.ErrorCode;
import dev.openfeature.sdk.EvaluationContext;
import dev.openfeature.sdk.EventProvider;
import dev.openfeature.sdk.Metadata;
import dev.openfeature.sdk.ProviderEvaluation;
import dev.openfeature.sdk.TrackingEventDetails;
import dev.openfeature.sdk.exceptions.GeneralError;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Controlled fake provider for lifecycle and interleaving tests. Never talks to a remote
 * flag service; all behavior is configured through {@link Outcome} and an optional gate.
 *
 * <p>When gated, the boolean evaluation blocks on a latch after recording the call, giving
 * tests a deterministic rendezvous point ("the evaluation has fetched this provider and is
 * now inside the provider call") without any sleeps.
 */
final class GatedProvider extends EventProvider {

    enum Outcome {
        SUCCESS,
        ERROR_DETAILS,
        THROW
    }

    /** A recorded {@code track} call. The context is stored by reference on purpose: it lets
     * tests prove that the SDK copied a mutable context at the call boundary. */
    static final class TrackCall {
        final String eventName;
        final EvaluationContext context;
        final TrackingEventDetails details;

        TrackCall(String eventName, EvaluationContext context, TrackingEventDetails details) {
            this.eventName = eventName;
            this.context = context;
            this.details = details;
        }
    }

    private final String name;
    private final Outcome outcome;
    private final boolean gated;
    private final Trace trace;
    private final CountDownLatch entered = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);
    private final AtomicInteger booleanCalls = new AtomicInteger();
    private final List<TrackCall> trackCalls = new CopyOnWriteArrayList<>();

    GatedProvider(String name, Outcome outcome, boolean gated, Trace trace) {
        this.name = name;
        this.outcome = outcome;
        this.gated = gated;
        this.trace = trace;
    }

    @Override
    public Metadata getMetadata() {
        return () -> name;
    }

    @Override
    public ProviderEvaluation<Boolean> getBooleanEvaluation(String key, Boolean defaultValue, EvaluationContext ctx) {
        booleanCalls.incrementAndGet();
        trace.record("provider", "call", name, ctx, "key=" + key);
        if (gated) {
            entered.countDown();
            awaitRelease();
        }
        switch (outcome) {
            case SUCCESS:
                return ProviderEvaluation.<Boolean>builder()
                        .value(!defaultValue)
                        .variant("variant-" + name)
                        .reason("STATIC")
                        .build();
            case ERROR_DETAILS:
                return ProviderEvaluation.<Boolean>builder()
                        .errorCode(ErrorCode.TYPE_MISMATCH)
                        .errorMessage("simulated type mismatch")
                        .build();
            case THROW:
            default:
                throw new IllegalStateException("simulated provider failure");
        }
    }

    @Override
    public ProviderEvaluation<String> getStringEvaluation(String key, String defaultValue, EvaluationContext ctx) {
        throw new UnsupportedOperationException("only boolean evaluation is supported by this fake");
    }

    @Override
    public ProviderEvaluation<Integer> getIntegerEvaluation(String key, Integer defaultValue, EvaluationContext ctx) {
        throw new UnsupportedOperationException("only boolean evaluation is supported by this fake");
    }

    @Override
    public ProviderEvaluation<Double> getDoubleEvaluation(String key, Double defaultValue, EvaluationContext ctx) {
        throw new UnsupportedOperationException("only boolean evaluation is supported by this fake");
    }

    @Override
    public ProviderEvaluation<dev.openfeature.sdk.Value> getObjectEvaluation(
            String key, dev.openfeature.sdk.Value defaultValue, EvaluationContext ctx) {
        throw new UnsupportedOperationException("only boolean evaluation is supported by this fake");
    }

    @Override
    public void track(String eventName, EvaluationContext context, TrackingEventDetails details) {
        trackCalls.add(new TrackCall(eventName, context, details));
        trace.record("provider", "track", name, context, eventName);
    }

    boolean awaitEntered(long timeout, TimeUnit unit) throws InterruptedException {
        return entered.await(timeout, unit);
    }

    void release() {
        release.countDown();
    }

    private void awaitRelease() {
        try {
            release.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new GeneralError("interrupted while gated");
        }
    }

    int booleanCalls() {
        return booleanCalls.get();
    }

    List<TrackCall> trackCalls() {
        return trackCalls;
    }
}
