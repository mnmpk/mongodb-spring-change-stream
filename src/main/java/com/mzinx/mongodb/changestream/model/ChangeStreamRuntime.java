package com.mzinx.mongodb.changestream.model;

import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.mzinx.mongodb.changestream.listener.ChangeStreamListener;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Local runtime record of a change stream on this instance. The distributed
 * state fields (leader, instances, term, epoch, lease) are a <b>cache</b> of
 * the coordination document (see {@link ChangeStreamCoordination}); the
 * database document is the single source of truth and the cache is refreshed
 * from it on every reconcile cycle and on every coordination event.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ChangeStreamRuntime<T> {
    private String collectionName;
    private ChangeStream<T> changeStream;
    private ChangeStreamListener<T> listener;

    /**
     * Completion handle of the running watch loop task. {@code null} or done
     * when no watch loop is active on this instance.
     */
    private CompletableFuture<Object> completableFuture;

    /**
     * The persisted config this registry was created from, set once the stream
     * has been started successfully. {@code null} for streams not driven by a
     * config (e.g. the coordination stream or programmatically started ones).
     */
    private ChangeStreamConfig config;

    /** Partition index of this host among the sorted members (AUTO_SCALE). */
    private int partitionIndex;
    /** Number of members sharing the stream (AUTO_SCALE). */
    private int partitionCount;

    /** Cached leader hostname from the coordination document. */
    private String leader;
    /** Cached leader lease expiry (server time) from the coordination document. */
    private Date leaseUntil;
    /** Cached fencing term from the coordination document. */
    private long term;
    /** Cached sorted member hostnames from the coordination document. */
    private List<String> instances;
    /** Cached membership epoch from the coordination document. */
    private long epoch;

    /**
     * Membership epoch the currently running AUTO_SCALE watch loop was
     * partitioned with; when it differs from {@link #epoch} the stream is
     * repartitioned and restarted. {@code -1} when never started.
     */
    @Builder.Default
    private long appliedEpoch = -1;

    /**
     * Whether a watch loop task is active (scheduled or running) on this
     * instance.
     */
    public boolean isActive() {
        return this.completableFuture != null && !this.completableFuture.isDone();
    }

    /**
     * Cooperatively stops the local watch loop and waits for it to terminate.
     * Must never be called from the watch loop thread itself.
     */
    public void stop() {
        this.changeStream.setRunning(false);
        if (this.completableFuture != null)
            this.completableFuture.join();
    }
}
