package dev.openfeature.sdk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.openfeature.sdk.fixtures.LifecycleHarness.ControlledProvider;
import dev.openfeature.sdk.fixtures.LifecycleHarness.Recorder;
import dev.openfeature.sdk.fixtures.LifecycleHarness.RecordingHook;
import dev.openfeature.sdk.fixtures.LifecycleHarness.Stage;
import dev.openfeature.sdk.fixtures.LifecycleHarness.Step;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Behavior table for the evaluation lifecycle. For each success/failure path the exact order of
 * before, provider, after/error and finally stages is asserted across the API (global), client,
 * invocation and provider hook layers, together with the merged context and the provider/client
 * metadata each hook observes.
 */
class EvaluationLifecycleBehaviorTest {

    private static final List<String> BEFORE_ORDER =
            Collections.unmodifiableList(Arrays.asList("api", "client", "invocation", "provider"));
    private static final List<String> FORWARD_ORDER =
            Collections.unmodifiableList(Arrays.asList("provider", "invocation", "client", "api"));
    private static final List<String> ALL_SOURCES =
            Collections.unmodifiableList(Arrays.asList("api", "client", "invocation", "provider"));

    /** The four success/failure paths of an evaluation. */
    enum Scenario {
        SUCCESS,
        SHORT_CIRCUIT_NOT_READY,
        TYPE_MISMATCH,
        PROVIDER_THROW
    }

    private OpenFeatureAPI api;

    @AfterEach
    void tearDown() {
        if (api != null) {
            api.shutdown();
        }
    }

    @DisplayName("evaluation lifecycle follows the behavior table for each path")
    @ParameterizedTest(name = "{0}")
    @EnumSource(Scenario.class)
    void evaluationLifecycleFollowsBehaviorTable(Scenario scenario) throws Exception {
        String testThread = Thread.currentThread().getName();
        api = new OpenFeatureAPI();
        Recorder recorder = new Recorder();
        RecordingHook apiHook = new RecordingHook(recorder, "api");
        RecordingHook clientHook = new RecordingHook(recorder, "client");
        RecordingHook invocationHook = new RecordingHook(recorder, "invocation");
        RecordingHook providerHook = new RecordingHook(recorder, "provider");
        ControlledProvider provider =
                new ControlledProvider(recorder, 1, Collections.singletonList(providerHook));

        api.addHooks(apiHook);
        api.setEvaluationContext(new MutableContext("api-tk", mapOf(
                "api-key", new Value("api"),
                "precedence", new Value("api"))));
        Client client = api.getClient("behavior-client");
        client.addHooks(clientHook);
        client.setEvaluationContext(new MutableContext(mapOf(
                "client-key", new Value("client"),
                "precedence", new Value("client"))));

        if (scenario == Scenario.SHORT_CIRCUIT_NOT_READY) {
            // async provider registration with a blocked initialize keeps the state NOT_READY
            provider.armInitializeGate();
            api.setProvider(provider);
            assertTrue(provider.awaitInitializeEntered(5), "provider initialization was entered");
        } else {
            api.setProviderAndWait(provider);
        }
        if (scenario == Scenario.TYPE_MISMATCH) {
            provider.setMode(ControlledProvider.Mode.TYPE_MISMATCH);
        }
        if (scenario == Scenario.PROVIDER_THROW) {
            provider.setToThrow(new RuntimeException("primary-G1"));
            provider.setMode(ControlledProvider.Mode.THROW);
        }

        MutableContext invocationContext = new MutableContext("invocation-tk", mapOf(
                "invocation-key", new Value("invocation"),
                "precedence", new Value("invocation")));
        FlagEvaluationDetails<Boolean> details = client.getBooleanDetails(
                "flag",
                false,
                invocationContext,
                FlagEvaluationOptions.builder().hook(invocationHook).build());

        if (scenario == Scenario.SHORT_CIRCUIT_NOT_READY) {
            provider.resumeInitialize();
        }

        // the behavior table: exact stage/source sequence for the scenario
        List<String> actualSequence = recorder.steps().stream()
                .map(Step::stageAndSource)
                .collect(Collectors.toList());
        assertThat(actualSequence).isEqualTo(expectedTable(scenario));

        // a synchronous evaluation records every step on the calling thread
        assertThat(recorder.steps()).allMatch(step -> step.thread().equals(testThread));

        // every hook observed the metadata of the same provider snapshot and the owning client
        List<Step> hookSteps = recorder.steps().stream()
                .filter(step -> step.stage() != Stage.PROVIDER)
                .collect(Collectors.toList());
        assertThat(hookSteps)
                .allMatch(step -> step.detail().contains("providerMeta=controlled-provider-G1"))
                .allMatch(step -> step.detail().contains("clientMeta=behavior-client"));
        assertThat(recorder.steps()).allMatch(step -> step.generation() == 1);

        // context merge: api, client and invocation layers are visible, invocation wins conflicts
        assertThat(recorder.steps(Stage.BEFORE))
                .allMatch(step -> step.detail().contains("precedence=invocation"))
                .allMatch(step -> step.detail().contains("apiKey=api"))
                .allMatch(step -> step.detail().contains("clientKey=client"))
                .allMatch(step -> step.detail().contains("invocationKey=invocation"));

        // finally runs exactly once per hook; after/error cardinality depends on the path
        for (String source : ALL_SOURCES) {
            assertThat(recorder.count(Stage.FINALLY, source)).as("finally count for %s", source).isEqualTo(1);
            assertThat(recorder.count(Stage.AFTER, source))
                    .as("after count for %s", source)
                    .isEqualTo(scenario == Scenario.SUCCESS ? 1 : 0);
            assertThat(recorder.count(Stage.ERROR, source))
                    .as("error count for %s", source)
                    .isEqualTo(scenario == Scenario.SUCCESS ? 0 : 1);
        }

        assertThat(provider.evaluationCalls())
                .isEqualTo(scenario == Scenario.SHORT_CIRCUIT_NOT_READY ? 0 : 1);

        switch (scenario) {
            case SUCCESS:
                assertThat(details.getValue()).isTrue();
                assertThat(details.getVariant()).isEqualTo("G1");
                assertThat(details.getErrorCode()).isNull();
                assertThat(details.getFlagMetadata().getString("generation")).isEqualTo("G1");
                break;
            case SHORT_CIRCUIT_NOT_READY:
                assertThat(details.getValue()).isFalse();
                assertThat(details.getErrorCode()).isEqualTo(ErrorCode.PROVIDER_NOT_READY);
                assertThat(recorder.steps(Stage.ERROR))
                        .allMatch(step -> step.detail().contains("error=ProviderNotReadyError"));
                break;
            case TYPE_MISMATCH:
                assertThat(details.getValue()).isFalse();
                assertThat(details.getErrorCode()).isEqualTo(ErrorCode.TYPE_MISMATCH);
                assertThat(details.getErrorMessage()).isEqualTo("type-mismatch-G1");
                assertThat(recorder.steps(Stage.ERROR))
                        .allMatch(step -> step.detail().contains("error=TypeMismatchError"));
                break;
            default:
                assertThat(details.getValue()).isFalse();
                assertThat(details.getErrorCode()).isEqualTo(ErrorCode.GENERAL);
                assertThat(details.getErrorMessage()).isEqualTo("primary-G1");
                assertThat(recorder.steps(Stage.ERROR))
                        .allMatch(step -> step.detail().contains("error=RuntimeException"));
        }
    }

    private static List<String> expectedTable(Scenario scenario) {
        List<String> expected = new ArrayList<>();
        for (String source : BEFORE_ORDER) {
            expected.add("BEFORE:" + source);
        }
        switch (scenario) {
            case SUCCESS:
                expected.add("PROVIDER:provider");
                for (String source : FORWARD_ORDER) {
                    expected.add("AFTER:" + source);
                }
                break;
            case SHORT_CIRCUIT_NOT_READY:
                for (String source : FORWARD_ORDER) {
                    expected.add("ERROR:" + source);
                }
                break;
            default:
                expected.add("PROVIDER:provider");
                for (String source : FORWARD_ORDER) {
                    expected.add("ERROR:" + source);
                }
        }
        for (String source : FORWARD_ORDER) {
            expected.add("FINALLY:" + source);
        }
        return expected;
    }

    private static Map<String, Value> mapOf(String key1, Value value1, String key2, Value value2) {
        Map<String, Value> map = new HashMap<>();
        map.put(key1, value1);
        map.put(key2, value2);
        return map;
    }
}
