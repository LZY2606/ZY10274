package dev.openfeature.sdk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.openfeature.sdk.fixtures.LifecycleHarness.ControlledProvider;
import dev.openfeature.sdk.fixtures.LifecycleHarness.Recorder;
import dev.openfeature.sdk.fixtures.LifecycleHarness.RecordingHook;
import dev.openfeature.sdk.fixtures.LifecycleHarness.Stage;
import dev.openfeature.sdk.fixtures.LifecycleHarness.Step;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Deterministic interleaving tests: an evaluation is paused inside the provider (after the SDK took
 * its provider snapshot), then the provider is switched or a mutable context is mutated. Assertions
 * only use happens-before relations established by latches, never sleeps or wall-clock ordering.
 */
class ProviderSwitchInterleavingTest {

    private static final long GUARD_TIMEOUT_SECONDS = 10;
    private static final List<String> ALL_SOURCES =
            Collections.unmodifiableList(Arrays.asList("api", "client", "invocation", "provider"));

    private OpenFeatureAPI api;
    private Recorder recorder;
    private ExecutorService evaluationExecutor;

    @BeforeEach
    void setUp() {
        api = new OpenFeatureAPI();
        recorder = new Recorder();
        evaluationExecutor = Executors.newSingleThreadExecutor(runnable -> new Thread(runnable, "evaluation-thread"));
    }

    @AfterEach
    void tearDown() {
        evaluationExecutor.shutdownNow();
        api.shutdown();
    }

    @DisplayName("switching the default provider mid-evaluation keeps the agreed snapshot")
    @Test
    void defaultProviderSwitchDuringInflightEvaluationKeepsSnapshot() throws Exception {
        RecordingHook providerHook = new RecordingHook(recorder, "provider");
        ControlledProvider providerOne =
                new ControlledProvider(recorder, 1, Collections.singletonList(providerHook));
        ControlledProvider providerTwo = new ControlledProvider(recorder, 2, Collections.emptyList());
        RecordingHook apiHook = new RecordingHook(recorder, "api");
        RecordingHook clientHook = new RecordingHook(recorder, "client");
        RecordingHook invocationHook = new RecordingHook(recorder, "invocation");
        api.addHooks(apiHook);
        Client client = api.getClient("switch-client");
        client.addHooks(clientHook);

        CountDownLatch readyEvents = new CountDownLatch(2);
        client.onProviderReady(eventDetails -> {
            recorder.record(
                    ControlledProvider.generationOf(eventDetails.getProviderName()),
                    "event-bus",
                    Stage.EVENT,
                    "ready:" + eventDetails.getProviderName());
            readyEvents.countDown();
        });

        api.setProviderAndWait(providerOne);
        // pause the evaluation right after the SDK took its provider snapshot
        providerOne.armSnapshotGate();

        Future<FlagEvaluationDetails<Boolean>> inFlight = evaluationExecutor.submit(() -> client.getBooleanDetails(
                "flag",
                false,
                new ImmutableContext(),
                FlagEvaluationOptions.builder().hook(invocationHook).build()));

        // happens-before: the evaluation grabbed provider one and is paused before any hook runs
        assertTrue(providerOne.awaitSnapshotTaken(GUARD_TIMEOUT_SECONDS), "evaluation took provider one snapshot");
        api.setProviderAndWait(providerTwo);
        Step switchStep = recorder.record(2, "test", Stage.SWITCH, "default provider switched to G2");
        providerOne.resumeSnapshot();
        FlagEvaluationDetails<Boolean> details = inFlight.get(GUARD_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        // the in-flight evaluation resolved entirely against the provider one snapshot
        assertThat(details.getValue()).isTrue();
        assertThat(details.getVariant()).isEqualTo("G1");
        assertThat(details.getErrorCode()).isNull();
        assertThat(details.getFlagMetadata().getString("generation")).isEqualTo("G1");
        assertThat(providerOne.evaluationCalls()).isEqualTo(1);
        assertThat(providerTwo.evaluationCalls()).isZero();

        // hooks and provider metadata all come from the same snapshot (generation 1)
        List<Step> hookSteps = hookSteps();
        assertThat(hookSteps).isNotEmpty();
        assertThat(hookSteps)
                .allMatch(step -> step.generation() == 1)
                .allMatch(step -> step.detail().contains("providerMeta=controlled-provider-G1"));

        // happens-before: the snapshot was taken before the switch, the switch before the provider ran
        Step snapshot = providerStep(providerOne, "hooks-snapshot");
        Step evaluated = providerStep(providerOne, "evaluated");
        assertThat(snapshot.seq()).isLessThan(switchStep.seq());
        assertThat(switchStep.seq()).isLessThan(evaluated.seq());

        // finally ran exactly once per hook source, after ran exactly once, error never
        for (String source : ALL_SOURCES) {
            assertThat(recorder.count(Stage.FINALLY, source)).as("finally count for %s", source).isEqualTo(1);
            assertThat(recorder.count(Stage.AFTER, source)).as("after count for %s", source).isEqualTo(1);
            assertThat(recorder.count(Stage.ERROR, source)).as("error count for %s", source).isZero();
        }

        // both provider generations emitted their ready event (content assertion, no global order)
        assertTrue(readyEvents.await(GUARD_TIMEOUT_SECONDS, TimeUnit.SECONDS), "both ready events observed");
        assertThat(recorder.steps(Stage.EVENT))
                .anyMatch(step -> step.detail().equals("ready:controlled-provider-G1"))
                .anyMatch(step -> step.detail().equals("ready:controlled-provider-G2"));

        // the next evaluation uses the new provider
        assertThat(client.getBooleanValue("flag", false)).isTrue();
        assertThat(providerTwo.evaluationCalls()).isEqualTo(1);
    }

    @DisplayName("switching a domain-bound provider mid-evaluation keeps the agreed snapshot")
    @Test
    void domainProviderSwitchDuringInflightEvaluationKeepsSnapshot() throws Exception {
        String domain = "interleaving-domain";
        RecordingHook providerHook = new RecordingHook(recorder, "provider");
        ControlledProvider providerOne =
                new ControlledProvider(recorder, 1, Collections.singletonList(providerHook));
        ControlledProvider providerTwo = new ControlledProvider(recorder, 2, Collections.emptyList());
        RecordingHook clientHook = new RecordingHook(recorder, "client");
        api.setProviderAndWait(domain, providerOne);
        Client client = api.getClient(domain);
        client.addHooks(clientHook);

        // pause the evaluation right after the SDK took its provider snapshot
        providerOne.armSnapshotGate();
        Future<FlagEvaluationDetails<Boolean>> inFlight = evaluationExecutor.submit(
                () -> client.getBooleanDetails("flag", false, new ImmutableContext()));

        assertTrue(providerOne.awaitSnapshotTaken(GUARD_TIMEOUT_SECONDS), "evaluation took provider one snapshot");
        api.setProviderAndWait(domain, providerTwo);
        Step switchStep = recorder.record(2, "test", Stage.SWITCH, "domain provider switched to G2");
        providerOne.resumeSnapshot();
        FlagEvaluationDetails<Boolean> details = inFlight.get(GUARD_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(details.getVariant()).isEqualTo("G1");
        assertThat(details.getFlagMetadata().getString("generation")).isEqualTo("G1");
        assertThat(providerOne.evaluationCalls()).isEqualTo(1);
        assertThat(providerTwo.evaluationCalls()).isZero();

        List<Step> hookSteps = hookSteps();
        assertThat(hookSteps).isNotEmpty();
        assertThat(hookSteps)
                .allMatch(step -> step.generation() == 1)
                .allMatch(step -> step.detail().contains("providerMeta=controlled-provider-G1"));

        Step snapshot = providerStep(providerOne, "hooks-snapshot");
        Step evaluated = providerStep(providerOne, "evaluated");
        assertThat(snapshot.seq()).isLessThan(switchStep.seq());
        assertThat(switchStep.seq()).isLessThan(evaluated.seq());

        for (String source : Arrays.asList("client", "provider")) {
            assertThat(recorder.count(Stage.FINALLY, source)).as("finally count for %s", source).isEqualTo(1);
        }

        assertThat(client.getBooleanValue("flag", false)).isTrue();
        assertThat(providerTwo.evaluationCalls()).isEqualTo(1);
    }

    @DisplayName("mutable contexts are copied at the call boundary of an evaluation")
    @Test
    void mutableContextsAreCopiedAtEvaluationCallBoundary() throws Exception {
        ControlledProvider provider = new ControlledProvider(recorder, 1, Collections.emptyList());
        api.setProviderAndWait(provider);
        Client client = api.getClient("mutation-client");

        MutableContext apiContext = new MutableContext(singletonValueMap("api-probe", "api-original"));
        api.setEvaluationContext(apiContext);
        MutableContext clientContext = new MutableContext(singletonValueMap("client-probe", "client-original"));
        client.setEvaluationContext(clientContext);
        MutableContext invocationContext = new MutableContext(singletonValueMap("probe", "invocation-original"));

        provider.armEvaluationGate();
        Future<FlagEvaluationDetails<Boolean>> inFlight = evaluationExecutor.submit(
                () -> client.getBooleanDetails("flag", false, invocationContext));

        assertTrue(provider.awaitEvaluationEntered(GUARD_TIMEOUT_SECONDS), "evaluation reached provider");
        // mutate every mutable layer while the evaluation is in flight
        apiContext.add("api-probe", "api-mutated");
        clientContext.add("client-probe", "client-mutated");
        invocationContext.add("probe", "invocation-mutated");
        provider.resumeEvaluation();
        inFlight.get(GUARD_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        EvaluationContext seenByProvider = provider.lastEvaluationContext();
        assertThat(seenByProvider.getValue("probe").asString()).isEqualTo("invocation-original");
        assertThat(seenByProvider.getValue("api-probe").asString()).isEqualTo("api-original");
        assertThat(seenByProvider.getValue("client-probe").asString()).isEqualTo("client-original");
    }

    @DisplayName("finally runs exactly once and the primary exception is preserved when a finally hook throws")
    @Test
    void finallyRunsExactlyOnceAndPreservesPrimaryException() {
        RecordingHook providerHook = new RecordingHook(recorder, "provider");
        ControlledProvider provider =
                new ControlledProvider(recorder, 1, Collections.singletonList(providerHook));
        RuntimeException primary = new RuntimeException("primary-G1");
        provider.setToThrow(primary);
        provider.setMode(ControlledProvider.Mode.THROW);
        api.setProviderAndWait(provider);

        RecordingHook apiHook = new RecordingHook(recorder, "api");
        apiHook.setThrowInFinally(true);
        RecordingHook clientHook = new RecordingHook(recorder, "client");
        RecordingHook invocationHook = new RecordingHook(recorder, "invocation");
        api.addHooks(apiHook);
        Client client = api.getClient("primary-client");
        client.addHooks(clientHook);

        FlagEvaluationDetails<Boolean> details = client.getBooleanDetails(
                "flag",
                false,
                new ImmutableContext(),
                FlagEvaluationOptions.builder().hook(invocationHook).build());

        // the primary exception wins; the finally-hook failure is suppressed (swallowed by the SDK)
        assertThat(details.getErrorCode()).isEqualTo(ErrorCode.GENERAL);
        assertThat(details.getErrorMessage()).isEqualTo("primary-G1");
        assertThat(details.getValue()).isFalse();

        // every error hook observed the primary exception itself
        assertThat(apiHook.errorSeen()).isSameAs(primary);
        assertThat(clientHook.errorSeen()).isSameAs(primary);
        assertThat(invocationHook.errorSeen()).isSameAs(primary);
        assertThat(providerHook.errorSeen()).isSameAs(primary);

        // finally ran exactly once per hook, even for the throwing one; after never ran
        for (String source : ALL_SOURCES) {
            assertThat(recorder.count(Stage.FINALLY, source)).as("finally count for %s", source).isEqualTo(1);
            assertThat(recorder.count(Stage.ERROR, source)).as("error count for %s", source).isEqualTo(1);
            assertThat(recorder.count(Stage.AFTER, source)).as("after count for %s", source).isZero();
        }
    }

    @DisplayName("track uses the current provider and a merged context snapshot")
    @Test
    void trackUsesMergedContextSnapshot() {
        ControlledProvider provider = new ControlledProvider(recorder, 1, Collections.emptyList());
        api.setProviderAndWait(provider);
        api.setEvaluationContext(new ImmutableContext(singletonValueMap("api-key", "api")));
        Client client = api.getClient("tracking-client");
        client.setEvaluationContext(new ImmutableContext(singletonValueMap("client-key", "client")));

        Map<String, Value> invocationAttributes = new HashMap<>();
        invocationAttributes.put("invocation-key", new Value("invocation"));
        invocationAttributes.put("probe", new Value("original"));
        MutableContext invocationContext = new MutableContext(invocationAttributes);

        client.track("checkout", invocationContext, new MutableTrackingEventDetails(42));
        // mutating the invocation context after the call must not leak into the recorded context
        invocationContext.add("probe", "mutated");

        assertThat(provider.trackCalls()).isEqualTo(1);
        assertThat(provider.lastTrackingEventName()).isEqualTo("checkout");
        EvaluationContext seenByProvider = provider.lastTrackingContext();
        assertThat(seenByProvider.getValue("probe").asString()).isEqualTo("original");
        assertThat(seenByProvider.getValue("api-key").asString()).isEqualTo("api");
        assertThat(seenByProvider.getValue("client-key").asString()).isEqualTo("client");
        assertThat(seenByProvider.getValue("invocation-key").asString()).isEqualTo("invocation");
        assertThat(recorder.count(Stage.TRACK, "provider")).isEqualTo(1);
        assertThat(recorder.steps(Stage.TRACK))
                .allMatch(step -> step.generation() == 1)
                .allMatch(step -> step.thread().equals(Thread.currentThread().getName()));
    }

    private List<Step> hookSteps() {
        return recorder.steps().stream()
                .filter(step -> step.stage() == Stage.BEFORE
                        || step.stage() == Stage.AFTER
                        || step.stage() == Stage.ERROR
                        || step.stage() == Stage.FINALLY)
                .collect(Collectors.toList());
    }

    private Step providerStep(ControlledProvider provider, String detailPrefix) {
        return recorder.steps(Stage.PROVIDER).stream()
                .filter(step -> step.generation() == provider.generation()
                        && step.detail().startsWith(detailPrefix))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no provider step " + detailPrefix + " for " + provider.providerName()));
    }

    private static Map<String, Value> singletonValueMap(String key, String value) {
        Map<String, Value> map = new HashMap<>();
        map.put(key, new Value(value));
        return map;
    }
}
