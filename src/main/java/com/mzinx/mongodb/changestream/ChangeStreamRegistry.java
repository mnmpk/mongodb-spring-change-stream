package com.mzinx.mongodb.changestream;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.mzinx.mongodb.changestream.model.ChangeStreamRuntime;

/**
 * Registry of every change stream runtime on this instance, keyed by change
 * stream id. Shared between the {@link ChangeStreamManager} (config-driven
 * streams and the coordination stream) and the
 * {@link com.mzinx.mongodb.changestream.service.ChangeStreamService}
 * (programmatically started streams), so the manager oversees every runtime.
 */
public class ChangeStreamRegistry {

    private final ConcurrentMap<String, ChangeStreamRuntime<?>> runtimes = new ConcurrentHashMap<>();

    public void register(String streamId, ChangeStreamRuntime<?> runtime) {
        this.runtimes.put(streamId, runtime);
    }

    @SuppressWarnings("unchecked")
    public <T> ChangeStreamRuntime<T> get(String streamId) {
        return (ChangeStreamRuntime<T>) this.runtimes.get(streamId);
    }

    /** Removes and returns the runtime, or {@code null} when not registered. */
    @SuppressWarnings("unchecked")
    public <T> ChangeStreamRuntime<T> deregister(String streamId) {
        return (ChangeStreamRuntime<T>) this.runtimes.remove(streamId);
    }

    public boolean contains(String streamId) {
        return this.runtimes.containsKey(streamId);
    }

    /** Snapshot of the registered change stream ids. */
    public Set<String> ids() {
        return Set.copyOf(this.runtimes.keySet());
    }

    /** Snapshot of the registered runtimes. */
    @SuppressWarnings("unchecked")
    public <T> List<ChangeStreamRuntime<T>> all() {
        List<ChangeStreamRuntime<T>> snapshot = new ArrayList<>();
        this.runtimes.values().forEach(runtime -> snapshot.add((ChangeStreamRuntime<T>) runtime));
        return snapshot;
    }
}
