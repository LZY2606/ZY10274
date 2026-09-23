package dev.openfeature.sdk.interleaving;

import static org.assertj.core.api.Assertions.assertThat;

import dev.openfeature.sdk.Client;
import dev.openfeature.sdk.ErrorCode;
import dev.openfeature.sdk.EvaluationContext;
import dev.openfeature.sdk.FlagEvaluationDetails;
import dev.openfeature.sdk.FlagEvaluationOptions;
import dev.openfeature.sdk.ImmutableContext;
import dev.openfeature.sdk.OpenFeatureAPI;
import dev.openfeature.sdk.Value;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Behavior table for the hook lifecycle of a single flag evaluation.
 *
 * <p>Covers the four success/failure paths (success, provider returns error details -
 * e.g. a type mismatch, provider throws, short-circuit on a not-ready provider) crossed
 * with the three hook layers (API/global, client, invocation). Every step is recorded in
 * a {@link Trace} with its thread, hook source, stage, visible provider metadata and the
 * merged evaluation context.
 *
 * <p>The exact per-thread token sequences asserted here are deliberately strict: a
 * mutation that swaps the error/finally stages, runs finally twice, or drops the provider
 * call from the middle of the sequence must fail these tests.
 */
class EvaluationBehaviorTableTest {

    private static final String PROVIDER_NAME = "provider-A";
    private static final String NO_OP_NAME = "No-op Provider";

    /** The four evaluation paths of the behavior table. */
    enum Outcome {
        SUCCESS,
        ERROR_DETAILS,
        PROVIDER_THROW,
        SHORT_CIRCUIT
    }

    private OpenFeatureAPI api;
    private Trace trace;
    private Client client;
    private String evalThread;

    @BeforeEach
    void setUp() {
        api = OpenFeatureAPI.createIsolated();
        trace = new Trace();
        evalThread = Thread.currentThread().getName();
    }

    @AfterEach
    void tearDown() {
        api.shutdown();
    }

    @ParameterizedTest(name = "outcome {0}: before -> [call] -> after|error -> finally across api/client/invocation")
    @EnumSource(Outcome.class)
    void hookLifecycleBehaviorTable(Outcome outcome) {
        api.setEvaluationContext(context(Map.of(
                "shared", new Value("api"),
                "apiKey", new Value("api"))));
        api.addHooks(new TracingHook("api", trace));
        client = api.getClient();
        client.setEvaluationContext(context(Map.of(
                "shared", new Value("client"),
                "clientKey", new Value("client"))));
        client.addHooks(new TracingHook("client", trace));

        GatedProvider provider = null;
        String expectedTag = NO_OP_NAME;
        if (outcome != Outcome.SHORT_CIRCUIT) {
            provider = new GatedProvider(PROVIDER_NAME, toProviderOutcome(outcome), false, trace);
            api.setProviderAndWait(provider);
            expectedTag = PROVIDER_NAME;
        }

        EvaluationContext invocationContext = context(Map.of(
                "shared", new Value("invocation"),
                "invocationKey", new Value("invocation")));
        FlagEvaluationOptions options = FlagEvaluationOptions.builder()
                .hook(new TracingHook("invocation", trace))
                .build();

        FlagEvaluationDetails<Boolean> details = client.getBooleanDetails("flag", false, invocationContext, options);

        List<Trace.Event> events = trace.onThread(evalThread);
        List<String> tokens = tokens(events);

        // --- exact stage sequence (the behavior table itself) ---
        switch (outcome) {
            case SUCCESS:
                assertThat(tokens)
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
                break;
            case ERROR_DETAILS:
            case PROVIDER_THROW:
                assertThat(tokens)
                        .containsExactly(
                                "api.before",
                                "client.before",
                                "invocation.before",
                                "provider.call",
                                "invocation.error",
                                "client.error",
                                "api.error",
                                "invocation.finally",
                                "client.finally",
                                "api.finally");
                break;
            case SHORT_CIRCUIT:
            default:
                // provider is never called when short-circuiting
                assertThat(tokens)
                        .containsExactly(
                                "api.before",
                                "client.before",
                                "invocation.before",
                                "invocation.error",
                                "client.error",
                                "api.error",
                                "invocation.finally",
                                "client.finally",
                                "api.finally");
                break;
        }

        // --- every step of one evaluation observes the same provider snapshot ---
        final String snapshotTag = expectedTag;
        assertThat(events)
                .as("all hook stages and the provider call must see the same provider metadata")
                .allSatisfy(event -> assertThat(event.providerTag).isEqualTo(snapshotTag));

        // --- finally runs exactly once per hook; after and error are mutually exclusive ---
        assertThat(countStage(events, "finally")).isEqualTo(3);
        if (outcome == Outcome.SUCCESS) {
            assertThat(countStage(events, "after")).isEqualTo(3);
            assertThat(countStage(events, "error")).isZero();
        } else {
            assertThat(countStage(events, "error")).isEqualTo(3);
            assertThat(countStage(events, "after")).isZero();
        }

        // --- error hooks observe the originating exception ---
        if (outcome != Outcome.SUCCESS) {
            String expectedError = expectedErrorType(outcome);
            assertThat(events.stream().filter(e -> e.stage.equals("error")).collect(Collectors.toList()))
                    .allSatisfy(e -> assertThat(e.detail).isEqualTo(expectedError));
        }

        // --- evaluation result ---
        assertDetails(details, outcome);

        // --- merged context (api < client < invocation precedence) reaches the provider ---
        if (provider != null) {
            assertThat(provider.booleanCalls()).isEqualTo(1);
            Trace.Event call = onlyStage(events, "call");
            assertThat(call.context)
                    .containsEntry("shared", "invocation")
                    .containsEntry("apiKey", "api")
                    .containsEntry("clientKey", "client")
                    .containsEntry("invocationKey", "invocation");
        } else {
            assertThat(countStage(events, "call")).isZero();
        }

        // --- hooks see the merged context before any hook-level enrichment ---
        Trace.Event firstBefore = events.get(0);
        assertThat(firstBefore.context)
                .containsEntry("shared", "invocation")
                .containsEntry("apiKey", "api")
                .containsEntry("clientKey", "client")
                .containsEntry("invocationKey", "invocation");
    }

    @Test
    void beforeHookContextIsMergedForProviderCallAndLaterStages() {
        api.setEvaluationContext(context(Map.of(
                "shared", new Value("api"),
                "apiKey", new Value("api"))));
        api.addHooks(new TracingHook("api", trace));
        client = api.getClient();
        client.addHooks(new TracingHook("client", trace));
        GatedProvider provider = new GatedProvider(PROVIDER_NAME, GatedProvider.Outcome.SUCCESS, false, trace);
        api.setProviderAndWait(provider);

        EvaluationContext hookContext = context(Map.of(
                "shared", new Value("hook"),
                "hookKey", new Value("hook")));
        FlagEvaluationOptions options = FlagEvaluationOptions.builder()
                .hook(new TracingHook("invocation", trace, hookContext))
                .build();
        EvaluationContext invocationContext = context(Map.of(
                "shared", new Value("invocation"),
                "invocationKey", new Value("invocation")));

        FlagEvaluationDetails<Boolean> details = client.getBooleanDetails("flag", false, invocationContext, options);

        assertThat(details.getErrorCode()).isNull();
        List<Trace.Event> events = trace.onThread(evalThread);

        // the provider call happens after the before stage, so hook context is merged in
        Trace.Event call = onlyStage(events, "call");
        assertThat(call.context)
                .containsEntry("shared", "hook")
                .containsEntry("hookKey", "hook")
                .containsEntry("apiKey", "api")
                .containsEntry("invocationKey", "invocation");

        // before hooks ran before the merge: no hook keys visible yet
        Trace.Event apiBefore = events.get(0);
        assertThat(apiBefore.token()).isEqualTo("api.before");
        assertThat(apiBefore.context).containsEntry("shared", "invocation").doesNotContainKey("hookKey");

        // after/finally stages observe the hook-enriched context
        Trace.Event apiAfter = events.stream()
                .filter(e -> e.token().equals("api.after"))
                .findFirst()
                .orElseThrow(AssertionError::new);
        assertThat(apiAfter.context).containsEntry("shared", "hook").containsEntry("hookKey", "hook");
    }

    private static GatedProvider.Outcome toProviderOutcome(Outcome outcome) {
        switch (outcome) {
            case SUCCESS:
                return GatedProvider.Outcome.SUCCESS;
            case ERROR_DETAILS:
                return GatedProvider.Outcome.ERROR_DETAILS;
            case PROVIDER_THROW:
            default:
                return GatedProvider.Outcome.THROW;
        }
    }

    private static String expectedErrorType(Outcome outcome) {
        switch (outcome) {
            case ERROR_DETAILS:
                return "TypeMismatchError";
            case PROVIDER_THROW:
                return "IllegalStateException";
            case SHORT_CIRCUIT:
            default:
                return "ProviderNotReadyError";
        }
    }

    private static void assertDetails(FlagEvaluationDetails<Boolean> details, Outcome outcome) {
        switch (outcome) {
            case SUCCESS:
                assertThat(details.getValue()).isTrue();
                assertThat(details.getErrorCode()).isNull();
                assertThat(details.getVariant()).isEqualTo("variant-" + PROVIDER_NAME);
                break;
            case ERROR_DETAILS:
                assertThat(details.getValue()).isFalse();
                assertThat(details.getErrorCode()).isEqualTo(ErrorCode.TYPE_MISMATCH);
                assertThat(details.getErrorMessage()).isEqualTo("simulated type mismatch");
                assertThat(details.getReason()).isEqualTo("ERROR");
                break;
            case PROVIDER_THROW:
                assertThat(details.getValue()).isFalse();
                assertThat(details.getErrorCode()).isEqualTo(ErrorCode.GENERAL);
                assertThat(details.getErrorMessage()).isEqualTo("simulated provider failure");
                assertThat(details.getReason()).isEqualTo("ERROR");
                break;
            case SHORT_CIRCUIT:
            default:
                assertThat(details.getValue()).isFalse();
                assertThat(details.getErrorCode()).isEqualTo(ErrorCode.PROVIDER_NOT_READY);
                assertThat(details.getReason()).isEqualTo("ERROR");
                break;
        }
    }

    private static ImmutableContext context(Map<String, Value> values) {
        return new ImmutableContext(values);
    }

    private static List<String> tokens(List<Trace.Event> events) {
        return events.stream().map(Trace.Event::token).collect(Collectors.toList());
    }

    private static long countStage(List<Trace.Event> events, String stage) {
        return events.stream().filter(e -> e.stage.equals(stage)).count();
    }

    private static Trace.Event onlyStage(List<Trace.Event> events, String stage) {
        List<Trace.Event> matching =
                events.stream().filter(e -> e.stage.equals(stage)).collect(Collectors.toList());
        assertThat(matching).hasSize(1);
        return matching.get(0);
    }
}
