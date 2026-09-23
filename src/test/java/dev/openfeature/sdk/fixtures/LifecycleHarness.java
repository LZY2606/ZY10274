package dev.openfeature.sdk.fixtures;

import dev.openfeature.sdk.BooleanHook;
import dev.openfeature.sdk.ErrorCode;
import dev.openfeature.sdk.EvaluationContext;
import dev.openfeature.sdk.EventProvider;
import dev.openfeature.sdk.FlagEvaluationDetails;
import dev.openfeature.sdk.Hook;
import dev.openfeature.sdk.HookContext;
import dev.openfeature.sdk.ImmutableMetadata;
import dev.openfeature.sdk.Metadata;
import dev.openfeature.sdk.ProviderEvaluation;
import dev.openfeature.sdk.Reason;
import dev.openfeature.sdk.TrackingEventDetails;
import dev.openfeature.sdk.Value;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Deterministic harness for asserting the flag-evaluation lifecycle: every step (hook stage, provider
 * call, tracking call, provider switch, event) is recorded with the thread it ran on, the provider
 * generation it observed and a detail string. Interleavings are controlled with latches, never sleeps.
 */
public final class LifecycleHarness {

    private LifecycleHarness() {}

    /** Lifecycle stages that can be recorded. */
    public enum Stage {
        BEFORE,
        PROVIDER,
        AFTER,
        ERROR,
        FINALLY,
        TRACK,
        EVENT,
        SWITCH
    }

    /** One recorded lifecycle step. */
    public static final class Step {
        private final long seq;
        private final String thread;
        private final int generation;
        private final String source;
        private final Stage stage;
        private final String detail;

        Step(long seq, String thread, int generation, String source, Stage stage, String detail) {
            this.seq = seq;
            this.thread = thread;
            this.generation = generation;
            this.source = source;
            this.stage = stage;
            this.detail = detail;
        }

        public long seq() {
            return seq;
        }

        public String thread() {
            return thread;
        }

        public int generation() {
            return generation;
        }

        public String source() {
            return source;
        }

        public Stage stage() {
            return stage;
        }

        public String detail() {
            return detail;
        }

        public String stageAndSource() {
            return stage + ":" + source;
        }
    }

    /** Thread-safe, append-only step recorder. The sequence number is a logical clock for recorded steps. */
    public static final class Recorder {
        private final AtomicLong sequence = new AtomicLong();
        private final ConcurrentLinkedQueue<Step> steps = new ConcurrentLinkedQueue<>();

        public Step record(int generation, String source, Stage stage, String detail) {
            Step step = new Step(
                    sequence.getAndIncrement(), Thread.currentThread().getName(), generation, source, stage, detail);
            steps.add(step);
            return step;
        }

        public List<Step> steps() {
            return new ArrayList<>(steps);
        }

        public List<Step> steps(Stage stage) {
            return steps().stream().filter(step -> step.stage() == stage).collect(Collectors.toList());
        }

        public long count(Stage stage, String source) {
            return steps().stream()
                    .filter(step -> step.stage() == stage && step.source().equals(source))
                    .count();
        }
    }

    /** A boolean hook that records every stage invocation, tagged with its hook source layer. */
    public static final class RecordingHook implements BooleanHook {
        private final Recorder recorder;
        private final String source;
        private final AtomicReference<Exception> errorSeen = new AtomicReference<>();
        private volatile boolean throwInFinally;

        public RecordingHook(Recorder recorder, String source) {
            this.recorder = recorder;
            this.source = source;
        }

        public void setThrowInFinally(boolean throwInFinally) {
            this.throwInFinally = throwInFinally;
        }

        public Exception errorSeen() {
            return errorSeen.get();
        }

        @Override
        public Optional<EvaluationContext> before(HookContext<Boolean> ctx, Map<String, Object> hints) {
            recorder.record(generationOf(ctx), source, Stage.BEFORE, describe(ctx));
            return Optional.empty();
        }

        @Override
        public void after(HookContext<Boolean> ctx, FlagEvaluationDetails<Boolean> details, Map<String, Object> hints) {
            recorder.record(generationOf(ctx), source, Stage.AFTER, describe(ctx));
        }

        @Override
        public void error(HookContext<Boolean> ctx, Exception error, Map<String, Object> hints) {
            errorSeen.set(error);
            recorder.record(
                    generationOf(ctx),
                    source,
                    Stage.ERROR,
                    describe(ctx) + ",error=" + error.getClass().getSimpleName());
        }

        @Override
        public void finallyAfter(
                HookContext<Boolean> ctx, FlagEvaluationDetails<Boolean> details, Map<String, Object> hints) {
            recorder.record(generationOf(ctx), source, Stage.FINALLY, describe(ctx));
            if (throwInFinally) {
                throw new IllegalStateException("finally-hook-failure-" + source);
            }
        }

        private static int generationOf(HookContext<?> ctx) {
            return ControlledProvider.generationOf(ctx.getProviderMetadata().getName());
        }

        private static String describe(HookContext<?> ctx) {
            EvaluationContext merged = ctx.getCtx();
            return "providerMeta=" + ctx.getProviderMetadata().getName()
                    + ",clientMeta=" + ctx.getClientMetadata().getName()
                    + ",precedence=" + probe(merged, "precedence")
                    + ",apiKey=" + probe(merged, "api-key")
                    + ",clientKey=" + probe(merged, "client-key")
                    + ",invocationKey=" + probe(merged, "invocation-key");
        }

        private static String probe(EvaluationContext ctx, String key) {
            Value value = ctx.getValue(key);
            return value == null ? "absent" : value.asString();
        }
    }

    /**
     * A controlled provider with a generation-encoded name. Evaluations and initialization can be
     * paused behind latches so that tests can deterministically interleave a provider switch or a
     * context mutation while an evaluation is in flight.
     */
    public static final class ControlledProvider extends EventProvider {
        public static final String NAME_PREFIX = "controlled-provider-G";

        /** Evaluation behavior of the provider. */
        public enum Mode {
            SUCCESS,
            TYPE_MISMATCH,
            THROW
        }

        private final int generation;
        private final Recorder recorder;
        private final List<Hook> providerHooks;
        private final AtomicInteger evaluationCalls = new AtomicInteger();
        private final AtomicInteger trackCalls = new AtomicInteger();
        private volatile Mode mode = Mode.SUCCESS;
        private volatile RuntimeException toThrow = new RuntimeException("primary");
        private volatile CountDownLatch evaluationEnteredLatch;
        private volatile CountDownLatch resumeEvaluationLatch;
        private volatile CountDownLatch initializeEnteredLatch;
        private volatile CountDownLatch resumeInitializeLatch;
        private volatile CountDownLatch snapshotTakenLatch;
        private volatile CountDownLatch resumeSnapshotLatch;
        private volatile EvaluationContext lastEvaluationContext;
        private volatile EvaluationContext lastTrackingContext;
        private volatile String lastTrackingEventName;

        public ControlledProvider(Recorder recorder, int generation, List<Hook> providerHooks) {
            this.recorder = recorder;
            this.generation = generation;
            this.providerHooks = providerHooks;
        }

        public static int generationOf(String providerName) {
            return Integer.parseInt(providerName.substring(NAME_PREFIX.length()));
        }

        public String providerName() {
            return NAME_PREFIX + generation;
        }

        public int generation() {
            return generation;
        }

        public int evaluationCalls() {
            return evaluationCalls.get();
        }

        public int trackCalls() {
            return trackCalls.get();
        }

        public EvaluationContext lastEvaluationContext() {
            return lastEvaluationContext;
        }

        public EvaluationContext lastTrackingContext() {
            return lastTrackingContext;
        }

        public String lastTrackingEventName() {
            return lastTrackingEventName;
        }

        public void setMode(Mode mode) {
            this.mode = mode;
        }

        public void setToThrow(RuntimeException toThrow) {
            this.toThrow = toThrow;
        }

        /** Pause the next evaluation inside the provider until {@link #resumeEvaluation()} is called. */
        public void armEvaluationGate() {
            evaluationEnteredLatch = new CountDownLatch(1);
            resumeEvaluationLatch = new CountDownLatch(1);
        }

        public boolean awaitEvaluationEntered(long timeoutSeconds) throws InterruptedException {
            return evaluationEnteredLatch.await(timeoutSeconds, TimeUnit.SECONDS);
        }

        public void resumeEvaluation() {
            resumeEvaluationLatch.countDown();
        }

        /**
         * Pause the next evaluation right after the SDK took its provider snapshot (on the
         * {@code getProviderHooks} call) until {@link #resumeSnapshot()} is called.
         */
        public void armSnapshotGate() {
            snapshotTakenLatch = new CountDownLatch(1);
            resumeSnapshotLatch = new CountDownLatch(1);
        }

        public boolean awaitSnapshotTaken(long timeoutSeconds) throws InterruptedException {
            return snapshotTakenLatch.await(timeoutSeconds, TimeUnit.SECONDS);
        }

        public void resumeSnapshot() {
            resumeSnapshotLatch.countDown();
        }

        /** Pause initialization until {@link #resumeInitialize()} is called. */
        public void armInitializeGate() {
            initializeEnteredLatch = new CountDownLatch(1);
            resumeInitializeLatch = new CountDownLatch(1);
        }

        public boolean awaitInitializeEntered(long timeoutSeconds) throws InterruptedException {
            return initializeEnteredLatch.await(timeoutSeconds, TimeUnit.SECONDS);
        }

        public void resumeInitialize() {
            resumeInitializeLatch.countDown();
        }

        @Override
        public Metadata getMetadata() {
            return this::providerName;
        }

        @Override
        public List<Hook> getProviderHooks() {
            CountDownLatch taken = snapshotTakenLatch;
            if (taken != null) {
                recorder.record(generation, "provider", Stage.PROVIDER, "hooks-snapshot");
                taken.countDown();
                awaitQuietly(resumeSnapshotLatch);
            }
            return providerHooks;
        }

        @Override
        public void initialize(EvaluationContext evaluationContext) {
            CountDownLatch entered = initializeEnteredLatch;
            if (entered != null) {
                entered.countDown();
                awaitQuietly(resumeInitializeLatch);
            }
        }

        @Override
        public ProviderEvaluation<Boolean> getBooleanEvaluation(
                String key, Boolean defaultValue, EvaluationContext evaluationContext) {
            evaluationCalls.incrementAndGet();
            CountDownLatch entered = evaluationEnteredLatch;
            if (entered != null) {
                recorder.record(generation, "provider", Stage.PROVIDER, "entered");
                entered.countDown();
                awaitQuietly(resumeEvaluationLatch);
            }
            // read the context only after the gate so a missing call-boundary copy is observable
            lastEvaluationContext = evaluationContext;
            recorder.record(generation, "provider", Stage.PROVIDER, "evaluated probe=" + probe(evaluationContext));
            switch (mode) {
                case TYPE_MISMATCH:
                    return ProviderEvaluation.<Boolean>builder()
                            .errorCode(ErrorCode.TYPE_MISMATCH)
                            .errorMessage("type-mismatch-G" + generation)
                            .build();
                case THROW:
                    throw toThrow;
                default:
                    return ProviderEvaluation.<Boolean>builder()
                            .value(true)
                            .variant("G" + generation)
                            .reason(Reason.STATIC.toString())
                            .flagMetadata(ImmutableMetadata.builder()
                                    .addString("generation", "G" + generation)
                                    .build())
                            .build();
            }
        }

        @Override
        public ProviderEvaluation<String> getStringEvaluation(
                String key, String defaultValue, EvaluationContext evaluationContext) {
            throw new UnsupportedOperationException("only boolean evaluation is controlled");
        }

        @Override
        public ProviderEvaluation<Integer> getIntegerEvaluation(
                String key, Integer defaultValue, EvaluationContext evaluationContext) {
            throw new UnsupportedOperationException("only boolean evaluation is controlled");
        }

        @Override
        public ProviderEvaluation<Double> getDoubleEvaluation(
                String key, Double defaultValue, EvaluationContext evaluationContext) {
            throw new UnsupportedOperationException("only boolean evaluation is controlled");
        }

        @Override
        public ProviderEvaluation<Value> getObjectEvaluation(
                String key, Value defaultValue, EvaluationContext evaluationContext) {
            throw new UnsupportedOperationException("only boolean evaluation is controlled");
        }

        @Override
        public void track(String trackingEventName, EvaluationContext context, TrackingEventDetails details) {
            trackCalls.incrementAndGet();
            lastTrackingEventName = trackingEventName;
            lastTrackingContext = context;
            recorder.record(generation, "provider", Stage.TRACK, "event=" + trackingEventName);
        }

        private static String probe(EvaluationContext ctx) {
            Value value = ctx.getValue("probe");
            return value == null ? "absent" : value.asString();
        }

        private static void awaitQuietly(CountDownLatch latch) {
            try {
                latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while gated", e);
            }
        }
    }
}
