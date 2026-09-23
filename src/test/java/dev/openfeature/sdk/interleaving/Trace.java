package dev.openfeature.sdk.interleaving;

import dev.openfeature.sdk.EvaluationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * Deterministic, thread-safe recorder of every observable step of a flag evaluation:
 * the executing thread, the hook source and stage, the provider generation (metadata name)
 * that was visible at that point, and a snapshot of the merged evaluation context.
 *
 * <p>Events carry a monotonically growing sequence number per recording thread's program
 * order (list insertion order), so tests can assert happens-before relationships within a
 * thread without relying on wall-clock time or cross-thread global ordering.
 */
final class Trace {

    /** A single recorded step of an evaluation. */
    static final class Event {
        final String thread;
        final String source;
        final String stage;
        final String providerTag;
        final Map<String, String> context;
        final String detail;

        Event(
                String thread,
                String source,
                String stage,
                String providerTag,
                Map<String, String> context,
                String detail) {
            this.thread = thread;
            this.source = source;
            this.stage = stage;
            this.providerTag = providerTag;
            this.context = context;
            this.detail = detail;
        }

        String token() {
            return source + "." + stage;
        }

        @Override
        public String toString() {
            return thread + " " + token() + " provider=" + providerTag + " ctx=" + context
                    + (detail == null ? "" : " (" + detail + ")");
        }
    }

    private final List<Event> events = new CopyOnWriteArrayList<>();

    void record(String source, String stage, String providerTag, EvaluationContext ctx, String detail) {
        events.add(new Event(Thread.currentThread().getName(), source, stage, providerTag, snapshot(ctx), detail));
    }

    private static Map<String, String> snapshot(EvaluationContext ctx) {
        Map<String, String> sorted = new TreeMap<>();
        if (ctx != null) {
            ctx.asObjectMap().forEach((key, value) -> sorted.put(key, String.valueOf(value)));
        }
        return sorted;
    }

    List<Event> events() {
        return new ArrayList<>(events);
    }

    /** Events recorded on the given thread, in that thread's program order. */
    List<Event> onThread(String thread) {
        return events.stream().filter(e -> e.thread.equals(thread)).collect(Collectors.toList());
    }

    /** "source.stage" tokens for the given thread, in that thread's program order. */
    List<String> tokensOnThread(String thread) {
        return onThread(thread).stream().map(Event::token).collect(Collectors.toList());
    }
}
