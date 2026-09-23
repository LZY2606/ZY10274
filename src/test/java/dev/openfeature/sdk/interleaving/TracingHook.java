package dev.openfeature.sdk.interleaving;

import dev.openfeature.sdk.BooleanHook;
import dev.openfeature.sdk.EvaluationContext;
import dev.openfeature.sdk.FlagEvaluationDetails;
import dev.openfeature.sdk.HookContext;
import dev.openfeature.sdk.Metadata;
import java.util.Map;
import java.util.Optional;

/**
 * Hook that records every stage invocation into a {@link Trace}, tagged with its
 * registration source ("api", "client" or "invocation") and the provider metadata name
 * visible through the hook context. Optionally returns a fixed context from
 * {@code before} to exercise hook-level context merging.
 */
final class TracingHook implements BooleanHook {

    private final String source;
    private final Trace trace;
    private final EvaluationContext beforeContext;

    TracingHook(String source, Trace trace) {
        this(source, trace, null);
    }

    TracingHook(String source, Trace trace, EvaluationContext beforeContext) {
        this.source = source;
        this.trace = trace;
        this.beforeContext = beforeContext;
    }

    @Override
    public Optional<EvaluationContext> before(HookContext<Boolean> ctx, Map<String, Object> hints) {
        trace.record(source, "before", providerTag(ctx), ctx.getCtx(), null);
        return Optional.ofNullable(beforeContext);
    }

    @Override
    public void after(HookContext<Boolean> ctx, FlagEvaluationDetails<Boolean> details, Map<String, Object> hints) {
        trace.record(source, "after", providerTag(ctx), ctx.getCtx(), "value=" + details.getValue());
    }

    @Override
    public void error(HookContext<Boolean> ctx, Exception error, Map<String, Object> hints) {
        trace.record(
                source,
                "error",
                providerTag(ctx),
                ctx.getCtx(),
                error.getClass().getSimpleName());
    }

    @Override
    public void finallyAfter(
            HookContext<Boolean> ctx, FlagEvaluationDetails<Boolean> details, Map<String, Object> hints) {
        trace.record(source, "finally", providerTag(ctx), ctx.getCtx(), null);
    }

    private static String providerTag(HookContext<?> ctx) {
        Metadata metadata = ctx.getProviderMetadata();
        return metadata == null ? "<none>" : metadata.getName();
    }
}
