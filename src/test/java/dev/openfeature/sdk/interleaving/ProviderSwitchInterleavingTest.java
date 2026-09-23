package dev.openfeature.sdk.interleaving;

import static org.assertj.core.api.Assertions.assertThat;

import dev.openfeature.sdk.Client;
import dev.openfeature.sdk.EvaluationContext;
import dev.openfeature.sdk.FeatureProvider;
import dev.openfeature.sdk.FlagEvaluationDetails;
import dev.openfeature.sdk.FlagEvaluationOptions;
import dev.openfeature.sdk.ImmutableContext;
import dev.openfeature.sdk.MutableContext;
import dev.openfeature.sdk.OpenFeatureAPI;
import dev.openfeature.sdk.Value;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Deterministic interleaving tests for provider switches racing an in-flight evaluation.
 *
 * <p>A gated provider pauses the evaluation after the provider reference has been fetched
 * (a controlled rendezvous, no sleeps); the provider is then switched, and the evaluation
 * is released. The single evaluation must complete entirely against the provider snapshot
 * it started with: hooks and provider metadata all come from the original provider, and
 * the new provider must not observe any call belonging to that evaluation.
 *
 * <p>Ordering is only asserted within each thread's program order (happens-before via
 * latches); no cross-thread global ordering is assumed.
 */
class ProviderSwitchInterleavingTest {

    private static final String DOMAIN = "switch-domain";
    private static final String EVAL_THREAD = "eval-thread";

    /** The two provider switch kinds: rebinding the global default and rebinding a domain. */
    enum SwitchKind {
        GLOBAL,
        DOMAIN
    }

    private OpenFeatureAPI api;
    private Trace trace;
    private String mainThread;

    @BeforeEach
    void setUp() {
        api = OpenFeatureAPI.createIsolated();
        trace = new Trace();
        mainThread = Thread.currentThread().getName();
    }

    @AfterEach
    void tearDown() {
        api.shutdown();
    }

    @ParameterizedTest(name = "in-flight evaluation keeps provider snapshot across {0} switch")
    @EnumSource(SwitchKind.class)
    void inFlightEvaluationKeepsProviderSnapshot(SwitchKind kind) throws Exception {
        GatedProvider providerA = new GatedProvider("provider-A", GatedProvider.Outcome.SUCCESS, true, trace);
        GatedProvider providerB = new GatedProvider("provider-B", GatedProvider.Outcome.SUCCESS, false, trace);
        Client client = kind == SwitchKind.DOMAIN ? api.getClient(DOMAIN) : api.getClient();

        List<String> readyEvents = new CopyOnWriteArrayList<>();
        CountDownLatch readyLatch = new CountDownLatch(2);
        client.onProviderReady(details -> {
            readyEvents.add(Thread.currentThread().getName() + " PROVIDER_READY");
            readyLatch.countDown();
        });

        bind(kind, providerA);

        api.addHooks(new TracingHook("api", trace));
        client.addHooks(new TracingHook("client", trace));
        FlagEvaluationOptions options = FlagEvaluationOptions.builder()
                .hook(new TracingHook("invocation", trace))
                .build();

        AtomicReference<FlagEvaluationDetails<Boolean>> result = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread evalThread = new Thread(
                () -> {
                    try {
                        result.set(client.getBooleanDetails("flag", false, null, options));
                    } catch (Throwable t) {
                        failure.set(t);
                    }
                },
                EVAL_THREAD);
        evalThread.start();

        // Controlled rendezvous: the evaluation has fetched provider A, run its before
        // hooks, and is now blocked inside provider A's evaluation. This happens-before
        // the switch below via the latch, with no sleeping involved.
        assertThat(providerA.awaitEntered(5, TimeUnit.SECONDS))
                .as("evaluation must reach the gated provider call")
                .isTrue();

        // Switch the provider while the evaluation is in flight.
        bind(kind, providerB);

        providerA.release();
        evalThread.join(TimeUnit.SECONDS.toMillis(5));
        assertThat(evalThread.isAlive()).as("evaluation thread must finish").isFalse();
        assertThat(failure.get()).isNull();

        // The whole evaluation ran against the provider A snapshot: exact stage sequence
        // on the evaluation thread, and every step observed provider A's metadata.
        List<Trace.Event> evalEvents = trace.onThread(EVAL_THREAD);
        assertThat(evalEvents.stream().map(Trace.Event::token).collect(Collectors.toList()))
                .containsExactly(
                        "api.before",
                        "client.before",
                        "invocation.before",
                        "provider.call",
                        "invocation.after",
                        "client.after",
                        "api.after",
                        "invocation.finally",
                        "client.finally",
                        "api.finally");
        assertThat(evalEvents)
                .as("a single evaluation must not mix provider metadata generations")
                .allSatisfy(event -> assertThat(event.providerTag).isEqualTo("provider-A"));

        // The switched-in provider never participated in the in-flight evaluation.
        assertThat(providerA.booleanCalls()).isEqualTo(1);
        assertThat(providerB.booleanCalls()).isZero();
        assertThat(result.get().getValue()).isTrue();
        assertThat(result.get().getVariant()).isEqualTo("variant-provider-A");

        // Both providers announced readiness (recorded; no cross-thread order asserted).
        assertThat(readyLatch.await(5, TimeUnit.SECONDS))
                .as("both providers must emit PROVIDER_READY")
                .isTrue();
        assertThat(readyEvents).hasSize(2);

        // New evaluations use the new provider generation.
        FlagEvaluationDetails<Boolean> afterSwitch = client.getBooleanDetails("flag", false);
        assertThat(afterSwitch.getVariant()).isEqualTo("variant-provider-B");
        assertThat(providerB.booleanCalls()).isEqualTo(1);
        assertThat(trace.onThread(mainThread))
                .allSatisfy(event -> assertThat(event.providerTag).isEqualTo("provider-B"));
    }

    @Test
    void trackCopiesMutableContextAtCallBoundaryAndTargetsBoundProvider() {
        GatedProvider providerA = new GatedProvider("provider-A", GatedProvider.Outcome.SUCCESS, false, trace);
        GatedProvider providerB = new GatedProvider("provider-B", GatedProvider.Outcome.SUCCESS, false, trace);
        api.setProviderAndWait(providerA);
        Client client = api.getClient();
        api.setEvaluationContext(new ImmutableContext(Map.of(
                "shared", new Value("api"),
                "apiKey", new Value("api"))));
        client.setEvaluationContext(new ImmutableContext(Map.of(
                "shared", new Value("client"),
                "clientKey", new Value("client"))));

        MutableContext mutable = new MutableContext();
        mutable.add("shared", "invocation");
        mutable.add("invocationKey", "original");

        client.track("event-1", mutable);

        // Mutate the context after the call boundary; the provider must have received a
        // copy, so the recorded context still shows the original values.
        mutable.add("invocationKey", "mutated");
        mutable.add("shared", "mutated");

        assertThat(providerA.trackCalls()).hasSize(1);
        GatedProvider.TrackCall trackCall = providerA.trackCalls().get(0);
        assertThat(trackCall.eventName).isEqualTo("event-1");
        EvaluationContext received = trackCall.context;
        assertThat(received.getValue("invocationKey").asString()).isEqualTo("original");
        assertThat(received.getValue("shared").asString())
                .as("invocation context wins over client and api context")
                .isEqualTo("invocation");
        assertThat(received.getValue("apiKey").asString()).isEqualTo("api");
        assertThat(received.getValue("clientKey").asString()).isEqualTo("client");

        // Tracking targets the provider bound at call time: after a switch, new track
        // calls go to the new provider and the old one sees nothing more.
        api.setProviderAndWait(providerB);
        client.track("event-2", mutable);

        assertThat(providerA.trackCalls()).hasSize(1);
        assertThat(providerB.trackCalls()).hasSize(1);
        assertThat(providerB.trackCalls().get(0).eventName).isEqualTo("event-2");
        assertThat(providerB
                        .trackCalls()
                        .get(0)
                        .context
                        .getValue("invocationKey")
                        .asString())
                .isEqualTo("mutated");
    }

    private void bind(SwitchKind kind, FeatureProvider provider) {
        if (kind == SwitchKind.DOMAIN) {
            api.setProviderAndWait(DOMAIN, provider);
        } else {
            api.setProviderAndWait(provider);
        }
    }
}
